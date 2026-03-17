from __future__ import annotations

import asyncio
import html
import json
import re
from datetime import datetime
from typing import Any, Callable, ClassVar
from urllib.parse import urljoin

from bs4 import BeautifulSoup, NavigableString, Tag
from curl_cffi import requests as curl_requests

from astrbot.api import logger

from ...config import PluginConfig
from ...data import MediaContent, Platform, SendGroup, TextContent, VideoContent
from ...download import Downloader
from ...exception import ParseException
from ..base import BaseParser, handle

StatItem = tuple[str, str]
VideoEntry = dict[str, str | None]
BodyBlock = dict[str, str]
RequestContext = dict[str, str | int]


class ZhihuParser(BaseParser):
    platform: ClassVar[Platform] = Platform(name="zhihu", display_name="知乎")
    _CARD_SUMMARY_LIMIT: ClassVar[int] = 80
    _CARD_SENTENCE_MARKERS: ClassVar[tuple[str, ...]] = (
        "。",
        "！",
        "？",
        "；",
        "…",
        "!",
        "?",
        ";",
    )
    _MEDIA_ATTRS: ClassVar[tuple[str, ...]] = (
        "src",
        "data-src",
        "data-original",
        "data-actualsrc",
        "data-default-watermark-src",
        "poster",
        "href",
    )
    _VIDEO_URL_KEYS: ClassVar[tuple[str, ...]] = (
        "playUrl",
        "playUrlHd",
        "videoUrl",
        "src",
        "url",
        "originVideoUrl",
        "videoPlayUrl",
        "playlist",
    )
    _VIDEO_COVER_KEYS: ClassVar[tuple[str, ...]] = (
        "cover",
        "coverUrl",
        "thumbnail",
        "thumbnailUrl",
        "imageUrl",
        "poster",
    )
    _VIDEO_TITLE_KEYS: ClassVar[tuple[str, ...]] = (
        "title",
        "name",
        "description",
    )

    def __init__(self, config: PluginConfig, downloader: Downloader):
        super().__init__(config, downloader)
        self.mycfg = config.parser.zhihu
        self.headers.update(
            {
                "accept": (
                    "text/html,application/xhtml+xml,application/xml;q=0.9,"
                    "image/avif,image/webp,image/apng,*/*;q=0.8"
                ),
                "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
                "referer": "https://www.zhihu.com/",
                "origin": "https://www.zhihu.com",
                "cache-control": "no-cache",
                "pragma": "no-cache",
            }
        )

    @handle(
        "zhuanlan.zhihu.com/p/",
        r"zhuanlan\.zhihu\.com/p/(?P<article_id>\d+)(?:[/?#][^\s]*)?",
    )
    async def _parse_article(self, searched: re.Match[str]):
        return await self.parse_article(searched.group("article_id"))

    @handle(
        "/answer/",
        r"www\.zhihu\.com/question/(?P<question_id>\d+)/answer/(?P<answer_id>\d+)(?:[/?#][^\s]*)?",
    )
    async def _parse_answer(self, searched: re.Match[str]):
        return await self.parse_answer(
            searched.group("question_id"),
            searched.group("answer_id"),
        )

    @handle(
        "www.zhihu.com/question/",
        r"www\.zhihu\.com/question/(?P<question_id>\d+)(?!\d)(?!/answer)(?:[/?#][^\s]*)?",
    )
    async def _parse_question(self, searched: re.Match[str]):
        return await self.parse_question(searched.group("question_id"))

    async def parse_article(self, article_id: str):
        url = self._article_url(article_id)
        initial_data, request_headers = await self._fetch_initial_data(
            url,
            validator=lambda payload: self._has_article_entity(payload, article_id),
        )
        article = (self._entities(initial_data).get("articles") or {}).get(article_id)
        if not isinstance(article, dict):
            raise ParseException("知乎文章数据不存在")

        author = self._build_author(article.get("author"), headers=request_headers)
        body_text, body_blocks, video_entries = await self._extract_content(
            str(article.get("content") or ""),
            initial_data,
            page_url=url,
        )
        article_excerpt = str(article.get("excerpt") or "")
        card_text = self._build_card_summary(
            article_excerpt,
            self._first_text_block(body_blocks),
            body_text,
        )
        ordered_blocks = self._build_section_blocks(None, body_blocks, body_text)
        stats = self._build_content_stats(
            article.get("voteupCount"),
            article.get("commentCount"),
            article.get("favlistsCount") or article.get("favoriteCount"),
            article.get("likedCount"),
            labels=("赞同", "评论", "收藏", "喜欢"),
        )
        header_text = self._compose_article_send_header(article, author)
        contents, send_groups = self._build_contents_and_groups(
            header_text,
            ordered_blocks,
            video_entries,
            request_headers=request_headers,
        )

        return self.result(
            title=str(article.get("title") or "知乎文章"),
            text=card_text,
            author=author,
            timestamp=self._safe_int(article.get("created")),
            url=url,
            contents=contents,
            send_groups=send_groups,
            extra={"info": self._build_article_card_meta(article, stats)},
        )

    async def parse_answer(self, question_id: str, answer_id: str):
        url = self._answer_url(question_id, answer_id)
        initial_data, request_headers = await self._fetch_initial_data(
            url,
            validator=lambda payload: self._has_answer_entities(
                payload,
                question_id,
                answer_id,
            ),
        )
        entities = self._entities(initial_data)
        answer = (entities.get("answers") or {}).get(answer_id)
        question = (entities.get("questions") or {}).get(question_id)
        if not isinstance(answer, dict):
            raise ParseException("知乎回答数据不存在")
        if not isinstance(question, dict):
            raise ParseException("知乎问题数据不存在")

        author = self._build_author(answer.get("author"), headers=request_headers)
        body_text, body_blocks, answer_videos = await self._extract_content(
            str(answer.get("content") or ""),
            initial_data,
            page_url=url,
        )
        answer_excerpt = str(answer.get("excerpt") or "")
        card_text = self._build_card_summary(
            answer_excerpt,
            self._first_text_block(body_blocks),
            body_text,
        )
        answer_stats = self._build_content_stats(
            answer.get("voteupCount"),
            answer.get("commentCount"),
            answer.get("favlistsCount") or answer.get("favoriteCount"),
            answer.get("thanksCount") or answer.get("likedCount"),
            labels=("赞同", "评论", "收藏", "喜欢"),
        )
        header_text = self._compose_answer_send_header(
            question=question,
            author=author,
            answer=answer,
        )
        ordered_blocks = self._build_section_blocks("回答正文:", body_blocks, body_text)
        contents, send_groups = self._build_contents_and_groups(
            header_text,
            ordered_blocks,
            answer_videos,
            request_headers=request_headers,
        )

        return self.result(
            title=str(question.get("title") or "知乎回答"),
            text=card_text,
            author=author,
            timestamp=self._safe_int(answer.get("createdTime")),
            url=url,
            contents=contents,
            send_groups=send_groups,
            extra={"info": self._build_answer_card_meta(answer_stats)},
        )

    async def parse_question(self, question_id: str):
        url = self._question_url(question_id)
        initial_data, request_headers = await self._fetch_initial_data(
            url,
            validator=lambda payload: self._has_question_entity(payload, question_id),
        )
        question = (self._entities(initial_data).get("questions") or {}).get(
            question_id
        )
        if not isinstance(question, dict):
            raise ParseException("知乎问题数据不存在")

        answer_id = self._pick_first_answer_id(initial_data, question_id)
        if not answer_id:
            raise ParseException("知乎问题页未找到默认排序首条回答")

        answer, answer_headers, answer_data = await self._load_answer_for_question(
            question_id=question_id,
            answer_id=answer_id,
            question_data=initial_data,
            question_headers=request_headers,
        )
        author = self._build_author(answer.get("author"), headers=answer_headers)

        question_detail_html = str(question.get("detail") or "").strip()
        detail_text = ""
        if question_detail_html:
            detail_text, _, _ = await self._extract_content(
                question_detail_html,
                initial_data,
                page_url=url,
                include_state_videos=False,
            )

        answer_excerpt = str(answer.get("excerpt") or "")
        answer_text, answer_blocks, answer_videos = await self._extract_content(
            str(answer.get("content") or ""),
            answer_data,
            page_url=self._answer_url(question_id, answer_id),
        )
        answer_text = answer_text or self._normalize_text(answer_excerpt)

        question_stats = self._build_question_stats(question)
        card_text = self._build_card_summary(
            question_detail_html,
            answer_excerpt,
            self._first_text_block(answer_blocks),
            answer_text,
        )
        header_text = self._compose_question_send_header(
            question=question,
            author=author,
            answer=answer,
        )
        ordered_blocks = self._build_section_blocks(
            "默认排序首条回答:",
            answer_blocks,
            answer_text,
        )
        contents, send_groups = self._build_contents_and_groups(
            header_text,
            ordered_blocks,
            answer_videos,
            request_headers=answer_headers,
        )

        return self.result(
            title=str(question.get("title") or "知乎问题"),
            text=card_text,
            author=author,
            timestamp=self._safe_int(answer.get("createdTime")),
            url=url,
            contents=contents,
            send_groups=send_groups,
            extra={"info": self._build_question_card_meta(question_stats)},
        )

    async def _fetch_initial_data(
        self,
        url: str,
        *,
        validator: Callable[[dict[str, Any]], bool],
    ) -> tuple[dict[str, Any], dict[str, str]]:
        last_error: Exception | None = None
        saw_challenge = False
        saw_login = False
        saw_invalid_target = False
        saw_initial_data = False

        for profile_name, profile_url, headers, impersonate in self._request_profiles(
            url
        ):
            try:
                response_ctx = await self._request_text(
                    profile_url,
                    headers=headers,
                    impersonate=impersonate,
                )

                html_text = str(response_ctx["text"])
                final_url = str(response_ctx["final_url"])
                status_code = int(response_ctx["status_code"])

                if self._is_challenge_page(html_text, status_code=status_code):
                    saw_challenge = True
                    logger.debug(
                        f"[知乎] {profile_name} 命中反爬挑战页: {profile_url} -> {final_url}"
                    )
                    continue

                if self._is_login_page(final_url, html_text):
                    saw_login = True
                    logger.debug(
                        f"[知乎] {profile_name} 命中登录页: {profile_url} -> {final_url}"
                    )
                    continue

                initial_data = self._extract_initial_data(html_text)
                if not initial_data:
                    logger.debug(
                        f"[知乎] {profile_name} 未找到可解析 initialData: "
                        f"{profile_url} -> {final_url}, status={status_code}"
                    )
                    continue

                saw_initial_data = True
                if validator(initial_data):
                    logger.debug(
                        f"[知乎] 使用 {profile_name} 请求成功: {profile_url} -> {final_url}"
                    )
                    return initial_data, headers

                saw_invalid_target = True
                logger.debug(
                    f"[知乎] {profile_name} 拿到的页面不是目标页: "
                    f"{profile_url} -> {final_url}, status={status_code}"
                )
            except Exception as exc:
                last_error = exc
                logger.debug(
                    f"[知乎] {profile_name} 请求失败: {profile_url}, error={exc}"
                )

        if saw_challenge:
            if self.mycfg.cookies:
                raise ParseException(
                    "知乎抓取失败：当前 cookies 可能失效，或请求仍被风控拦截"
                )
            raise ParseException("知乎抓取失败：站点返回反爬挑战页，请配置有效 cookies")

        if saw_login:
            if self.mycfg.cookies:
                raise ParseException("知乎抓取失败：当前 cookies 可能失效，或权限不足")
            raise ParseException(
                "知乎抓取失败：当前请求被引导到登录页，请配置有效 cookies"
            )

        if saw_invalid_target or saw_initial_data:
            raise ParseException("知乎抓取失败：未拿到目标知乎页面")

        raise ParseException("知乎页面抓取失败") from last_error

    def _request_profiles(self, url: str) -> list[tuple[str, str, dict[str, str], str]]:
        desktop_headers = self._build_request_headers(self.headers)
        ios_headers = self._build_request_headers(self.ios_headers)
        mobile_headers = self._build_request_headers(self.android_headers)
        return [
            ("desktop", url, desktop_headers, "chrome"),
            ("ios", url, ios_headers, "safari_ios"),
            ("mobile", url, mobile_headers, "chrome_android"),
        ]

    def _build_request_headers(self, base_headers: dict[str, str]) -> dict[str, str]:
        headers = dict(base_headers)
        headers.update(
            {
                "accept": self.headers["accept"],
                "accept-language": self.headers["accept-language"],
                "referer": self.headers["referer"],
                "origin": self.headers["origin"],
                "cache-control": self.headers["cache-control"],
                "pragma": self.headers["pragma"],
            }
        )
        if self.mycfg.cookies:
            headers["cookie"] = str(self.mycfg.cookies).strip()
        return headers

    async def _request_text(
        self,
        url: str,
        *,
        headers: dict[str, str],
        impersonate: str,
    ) -> RequestContext:
        def do_request():
            return curl_requests.get(
                url,
                headers=headers,
                impersonate=impersonate,
                proxies={"https": self.proxy, "http": self.proxy}
                if self.proxy
                else None,
                timeout=self.cfg.common_timeout,
                allow_redirects=True,
            )

        response = await asyncio.to_thread(do_request)
        return {
            "status_code": int(response.status_code),
            "final_url": str(response.url),
            "text": str(response.text),
        }

    def _extract_initial_data(self, html_text: str) -> dict[str, Any] | None:
        soup = BeautifulSoup(html_text, "html.parser")
        node = soup.select_one('script#js-initialData[type="text/json"]')
        if node is None:
            return None
        raw = node.get_text(strip=True)
        if not raw:
            return None
        try:
            payload = json.loads(raw)
        except Exception:
            return None
        initial_state = payload.get("initialState")
        return payload if isinstance(initial_state, dict) else None

    @staticmethod
    def _entities(initial_data: dict[str, Any]) -> dict[str, Any]:
        initial_state = initial_data.get("initialState") or {}
        entities = initial_state.get("entities") or {}
        return entities if isinstance(entities, dict) else {}

    def _has_article_entity(
        self, initial_data: dict[str, Any], article_id: str
    ) -> bool:
        article = (self._entities(initial_data).get("articles") or {}).get(article_id)
        return isinstance(article, dict)

    def _has_answer_entities(
        self,
        initial_data: dict[str, Any],
        question_id: str,
        answer_id: str,
    ) -> bool:
        entities = self._entities(initial_data)
        question = (entities.get("questions") or {}).get(question_id)
        answer = (entities.get("answers") or {}).get(answer_id)
        return isinstance(question, dict) and isinstance(answer, dict)

    def _has_question_entity(
        self, initial_data: dict[str, Any], question_id: str
    ) -> bool:
        question = (self._entities(initial_data).get("questions") or {}).get(
            question_id
        )
        return isinstance(question, dict) and bool(
            self._pick_first_answer_id(initial_data, question_id)
        )

    @staticmethod
    def _is_challenge_page(html_text: str, *, status_code: int) -> bool:
        lowered = html_text.lower()
        return (
            'id="zh-zse-ck"' in lowered
            or "static.zhihu.com/zse-ck/" in lowered
            or 'appname":"zse_ck"' in lowered
            or (status_code == 403 and "zse-ck" in lowered)
        )

    @staticmethod
    def _is_login_page(final_url: str, html_text: str) -> bool:
        lowered_url = final_url.lower()
        lowered_html = html_text.lower()
        return (
            "/signin" in lowered_url
            or "/signup" in lowered_url
            or "<title>知乎 - 有问题，就会有答案</title>" in lowered_html
        )

    def _pick_first_answer_id(
        self, initial_data: dict[str, Any], question_id: str
    ) -> str | None:
        initial_state = initial_data.get("initialState") or {}
        answers = ((initial_state.get("question") or {}).get("answers") or {}).get(
            question_id
        ) or {}
        ids = answers.get("ids") or []
        if not ids or not isinstance(ids[0], dict):
            return None
        target = ids[0].get("target")
        return str(target) if target else None

    async def _load_answer_for_question(
        self,
        *,
        question_id: str,
        answer_id: str,
        question_data: dict[str, Any],
        question_headers: dict[str, str],
    ) -> tuple[dict[str, Any], dict[str, str], dict[str, Any]]:
        answer = (self._entities(question_data).get("answers") or {}).get(
            answer_id
        ) or {}
        if (
            isinstance(answer, dict)
            and answer.get("content")
            and not answer.get("contentNeedTruncated")
        ):
            return answer, question_headers, question_data

        answer_data, answer_headers = await self._fetch_initial_data(
            self._answer_url(question_id, answer_id),
            validator=lambda payload: self._has_answer_entities(
                payload,
                question_id,
                answer_id,
            ),
        )
        answer = (self._entities(answer_data).get("answers") or {}).get(answer_id) or {}
        if not isinstance(answer, dict):
            raise ParseException("知乎首条回答数据不存在")
        return answer, answer_headers, answer_data

    def _build_author(self, author_data: Any, *, headers: dict[str, str]):
        if not isinstance(author_data, dict):
            return None
        name = str(author_data.get("name") or "").strip()
        if not name:
            return None
        avatar_url = str(author_data.get("avatarUrl") or "").strip() or None
        description = (
            self._normalize_text(
                str(author_data.get("headline") or author_data.get("description") or "")
            )
            or None
        )
        return self.create_author(
            name=name,
            avatar_url=avatar_url,
            description=description,
            headers=headers,
        )

    def _build_question_stats(self, question: dict[str, Any]) -> list[StatItem]:
        stats: list[StatItem] = []
        for label, value in (
            ("回答", question.get("answerCount")),
            ("关注", question.get("followerCount")),
            ("浏览", question.get("visitCount")),
        ):
            if value is not None:
                stats.append((label, self._format_count(value)))
        return stats

    def _build_content_stats(
        self,
        voteup: Any,
        comment: Any,
        favorite: Any,
        liked: Any,
        *,
        labels: tuple[str, str, str, str],
    ) -> list[StatItem]:
        stats: list[StatItem] = []
        for label, value in zip(
            labels, (voteup, comment, favorite, liked), strict=True
        ):
            if value is not None:
                stats.append((label, self._format_count(value)))
        return stats

    def _build_article_card_meta(
        self,
        article: dict[str, Any],
        stats: list[StatItem],
    ) -> str | None:
        tokens: list[str] = []
        if column_title := self._truncate_card_token(
            self._article_column_title(article),
            limit=12,
        ):
            tokens.append(column_title)
        for label in ("赞同", "评论", "收藏"):
            if token := self._stat_token(stats, label):
                tokens.append(token)
        return self._build_card_meta("文章", *tokens, max_tokens=5)

    def _build_answer_card_meta(self, stats: list[StatItem]) -> str | None:
        tokens = [
            token
            for label in ("赞同", "评论", "收藏")
            if (token := self._stat_token(stats, label))
        ]
        return self._build_card_meta("回答", *tokens, max_tokens=4)

    def _build_question_card_meta(self, stats: list[StatItem]) -> str | None:
        tokens = [
            token
            for label in ("回答", "关注", "浏览")
            if (token := self._stat_token(stats, label))
        ]
        return self._build_card_meta("问题", *tokens, max_tokens=4)

    def _build_card_summary(self, *sources: Any) -> str | None:
        for source in sources:
            summary = self._clean_card_summary_source(source)
            if summary:
                return self._truncate_card_summary(summary)
        return None

    def _clean_card_summary_source(self, source: Any) -> str:
        if source is None:
            return ""
        value = str(source).strip()
        if not value:
            return ""
        text = (
            self._html_to_text(value)
            if self._looks_like_html(value)
            else self._normalize_text(value)
        )
        return self._normalize_text(
            self._strip_card_prefix(text),
            keep_newlines=False,
        )

    def _truncate_card_summary(self, text: str) -> str:
        value = self._normalize_text(text, keep_newlines=False)
        if len(value) <= self._CARD_SUMMARY_LIMIT:
            return value

        window = value[: self._CARD_SUMMARY_LIMIT + 1]
        last_break = max(
            (window.rfind(marker) for marker in self._CARD_SENTENCE_MARKERS),
            default=-1,
        )
        if last_break >= max(18, self._CARD_SUMMARY_LIMIT // 2):
            return window[: last_break + 1].strip()
        return value[: self._CARD_SUMMARY_LIMIT].rstrip(" ，,；;。！？!?、") + "…"

    def _build_card_meta(
        self,
        kind: str,
        *tokens: str,
        max_tokens: int,
    ) -> str | None:
        items = [kind, *(token for token in tokens if token)]
        items = items[:max_tokens]
        return " · ".join(items) if items else None

    @staticmethod
    def _looks_like_html(text: str) -> bool:
        return bool(re.search(r"<[A-Za-z!/][^>]*>", text))

    @staticmethod
    def _strip_card_prefix(text: str) -> str:
        value = text.strip()
        for prefix in (
            "问题描述:",
            "回答正文:",
            "默认排序首条回答:",
            "专栏:",
            "标题:",
            "问题:",
        ):
            if value.startswith(prefix):
                return value[len(prefix) :].strip()
        return value

    def _first_text_block(self, body_blocks: list[BodyBlock]) -> str | None:
        for block in body_blocks:
            if block.get("kind") != "text":
                continue
            value = self._clean_card_summary_source(block.get("value"))
            if value:
                return value
        return None

    @staticmethod
    def _truncate_card_token(value: str | None, *, limit: int) -> str | None:
        if not value:
            return None
        text = value.strip()
        if not text:
            return None
        if len(text) <= limit:
            return text
        return text[:limit].rstrip() + "…"

    @staticmethod
    def _stat_token(stats: list[StatItem], label: str) -> str | None:
        for current_label, value in stats:
            if current_label == label and value:
                return f"{label} {value}"
        return None

    def _build_contents_and_groups(
        self,
        header_text: str,
        body_blocks: list[BodyBlock],
        video_entries: list[VideoEntry],
        *,
        request_headers: dict[str, str],
    ) -> tuple[list[MediaContent], list[SendGroup]]:
        video_contents = self._build_video_contents(
            video_entries,
            request_headers=request_headers,
        )

        contents: list[MediaContent] = []
        primary_contents: list[MediaContent] = []
        image_urls = [
            block["value"]
            for block in body_blocks
            if block.get("kind") == "image" and block.get("value")
        ]
        image_iter = iter(
            self.create_image_contents(image_urls, headers=request_headers)
        )
        pending_text_parts: list[str] = []

        def append_text_part(text: str) -> None:
            value = text.strip()
            if value:
                pending_text_parts.append(value)

        def flush_text_parts() -> None:
            if not pending_text_parts:
                return
            text_content = TextContent(self._join_sections(pending_text_parts))
            contents.append(text_content)
            primary_contents.append(text_content)
            pending_text_parts.clear()

        append_text_part(header_text)
        for block in body_blocks:
            kind = block.get("kind")
            value = str(block.get("value") or "").strip()
            if not value:
                continue
            if kind == "text":
                append_text_part(value)
                continue
            if kind != "image":
                continue
            flush_text_parts()
            image_content = next(image_iter, None)
            if image_content is None:
                continue
            contents.append(image_content)
            primary_contents.append(image_content)

        flush_text_parts()
        contents.extend(video_contents)

        send_groups: list[SendGroup] = []
        if primary_contents:
            send_groups.append(
                SendGroup(
                    contents=primary_contents,
                    force_merge=False,
                    render_card=True,
                )
            )
        for video in video_contents:
            send_groups.append(SendGroup(contents=[video], force_merge=False))
        return contents, send_groups

    def _compose_article_send_header(
        self,
        article: dict[str, Any],
        author: Any,
    ) -> str:
        sections: list[str] = []
        if title := self._normalize_text(str(article.get("title") or "")):
            sections.append(f"标题: {title}")
        sections.extend(self._author_sections(author, label="作者"))
        if column_title := self._article_column_title(article):
            sections.append(f"专栏: {column_title}")
        if created_text := self._format_timestamp(article.get("created")):
            sections.append(f"发布时间: {created_text}")
        return self._join_sections(sections)

    def _compose_answer_send_header(
        self,
        *,
        question: dict[str, Any],
        author: Any,
        answer: dict[str, Any],
    ) -> str:
        sections: list[str] = []
        if title := self._normalize_text(str(question.get("title") or "")):
            sections.append(f"问题: {title}")
        sections.extend(self._author_sections(author, label="答主"))
        if created_text := self._format_timestamp(answer.get("createdTime")):
            sections.append(f"回答时间: {created_text}")
        return self._join_sections(sections)

    def _compose_question_send_header(
        self,
        *,
        question: dict[str, Any],
        author: Any,
        answer: dict[str, Any],
    ) -> str:
        sections: list[str] = []
        if title := self._normalize_text(str(question.get("title") or "")):
            sections.append(f"问题: {title}")
        sections.extend(self._author_sections(author, label="首条回答作者"))
        if created_text := self._format_timestamp(answer.get("createdTime")):
            sections.append(f"首条回答时间: {created_text}")
        return self._join_sections(sections)

    def _author_sections(self, author: Any, *, label: str) -> list[str]:
        sections: list[str] = []
        if author is None:
            return sections
        sections.append(f"{label}: {author.name}")
        return sections

    @staticmethod
    def _join_sections(sections: list[str]) -> str:
        return "\n\n".join(section for section in sections if section).strip()

    @staticmethod
    def _article_column_title(article: dict[str, Any]) -> str | None:
        column = article.get("column") or {}
        if not isinstance(column, dict):
            return None
        title = str(column.get("title") or "").strip()
        return title or None

    async def _extract_content(
        self,
        html_text: str,
        initial_data: dict[str, Any],
        *,
        page_url: str,
        include_state_videos: bool = True,
    ) -> tuple[str, list[BodyBlock], list[VideoEntry]]:
        body_text = self._html_to_text(html_text, keep_newlines=True)
        body_blocks: list[BodyBlock] = []
        video_entries: list[VideoEntry] = []

        if html_text.strip():
            media_soup = BeautifulSoup(html_text, "html.parser")
            body_blocks = self._extract_ordered_body_blocks(
                media_soup, page_url=page_url
            )
            for node in media_soup.find_all(["video", "source", "iframe"]):
                self._append_video_entry(
                    video_entries,
                    self._extract_video_entry_from_tag(node, page_url),
                )

        if include_state_videos:
            video_entries = self._merge_unique_video_entries(
                video_entries,
                self._extract_video_entries_from_state(initial_data, page_url),
            )

        return body_text, body_blocks, video_entries

    def _build_section_blocks(
        self,
        title: str | None,
        body_blocks: list[BodyBlock],
        fallback_text: str,
    ) -> list[BodyBlock]:
        normalized_fallback = self._normalize_text(
            fallback_text,
            keep_newlines=True,
        )
        if not body_blocks and not normalized_fallback:
            return []

        section_blocks: list[BodyBlock] = []
        if title:
            section_blocks.append(self._make_text_block(title))
        if body_blocks:
            section_blocks.extend(body_blocks)
            return section_blocks

        section_blocks.append(self._make_text_block(normalized_fallback))
        return section_blocks

    def _extract_ordered_body_blocks(
        self,
        root: Tag | BeautifulSoup,
        *,
        page_url: str,
    ) -> list[BodyBlock]:
        body_blocks: list[BodyBlock] = []
        text_blocks: list[str] = []
        seen_images: set[str] = set()
        self._append_ordered_body_container(
            root,
            body_blocks,
            text_blocks,
            seen_images,
            page_url,
        )
        self._flush_body_text_blocks(body_blocks, text_blocks)
        return self._merge_adjacent_body_text_blocks(body_blocks)

    def _append_ordered_body_container(
        self,
        node: Tag | BeautifulSoup,
        body_blocks: list[BodyBlock],
        text_blocks: list[str],
        seen_images: set[str],
        page_url: str,
    ) -> None:
        for child in node.children:
            if isinstance(child, (Tag, NavigableString)):
                self._append_ordered_body_node(
                    child,
                    body_blocks,
                    text_blocks,
                    seen_images,
                    page_url,
                )

    def _append_ordered_body_node(
        self,
        node: Tag | NavigableString,
        body_blocks: list[BodyBlock],
        text_blocks: list[str],
        seen_images: set[str],
        page_url: str,
    ) -> None:
        if isinstance(node, NavigableString):
            text = self._normalize_text(str(node))
            if text:
                self._append_text_block(text_blocks, text)
            return

        if not isinstance(node, Tag):
            return

        name = (node.name or "").lower()
        if not name or name in {
            "script",
            "style",
            "noscript",
            "video",
            "source",
            "iframe",
            "audio",
            "svg",
        }:
            return

        if name == "img":
            self._flush_body_text_blocks(body_blocks, text_blocks)
            self._append_body_image_block(
                body_blocks,
                seen_images,
                self._extract_image_url(node, page_url),
            )
            return

        if name == "br":
            if text_blocks:
                text_blocks[-1] = text_blocks[-1].rstrip() + "\n"
            return

        if name in {"h1", "h2", "h3", "h4", "h5", "h6"}:
            self._append_text_block(text_blocks, self._node_text(node))
            return

        if name == "blockquote":
            self._append_text_block(
                text_blocks,
                self._format_blockquote_text(self._node_text(node, keep_newlines=True)),
            )
            return

        if name in {"ul", "ol"}:
            self._append_text_block(
                text_blocks,
                self._format_list_text(
                    self._collect_list_items(node),
                    ordered=name == "ol",
                ),
            )
            return

        if name == "li":
            item_text = self._node_text(node, keep_newlines=True)
            if item_text:
                self._append_text_block(
                    text_blocks,
                    self._format_list_text([item_text], ordered=False),
                )
            return

        if name == "pre":
            self._append_text_block(
                text_blocks,
                self._format_code_block(
                    self._extract_code_block(node),
                    self._extract_code_language(node),
                ),
            )
            return

        if name == "code":
            parent = node.parent
            if isinstance(parent, Tag) and (parent.name or "").lower() == "pre":
                return
            self._append_text_block(text_blocks, self._node_text(node))
            return

        if name == "hr":
            self._append_text_block(text_blocks, "---")
            return

        block_tags = {
            "p",
            "div",
            "section",
            "article",
            "main",
            "header",
            "footer",
            "aside",
            "figure",
            "figcaption",
            "table",
            "thead",
            "tbody",
            "tfoot",
            "tr",
            "td",
            "th",
        }
        has_block_child = any(
            isinstance(child, Tag)
            and (child.name or "").lower()
            in {
                "p",
                "div",
                "section",
                "article",
                "blockquote",
                "ul",
                "ol",
                "pre",
                "figure",
                "table",
                "hr",
                "h1",
                "h2",
                "h3",
                "h4",
                "h5",
                "h6",
            }
            for child in node.children
        )
        if name in {"figure", "figcaption"} or (
            name in block_tags and self._has_media_child(node)
        ):
            self._append_ordered_body_container(
                node,
                body_blocks,
                text_blocks,
                seen_images,
                page_url,
            )
            return

        if name in block_tags and has_block_child:
            self._append_ordered_body_container(
                node,
                body_blocks,
                text_blocks,
                seen_images,
                page_url,
            )
            return

        if name in block_tags:
            text = self._node_text(node, keep_newlines=True)
            if text:
                self._append_text_block(text_blocks, text)
            return

        text = self._node_text(node, keep_newlines=True)
        if text:
            self._append_text_block(text_blocks, text)
            return

        self._append_ordered_body_container(
            node,
            body_blocks,
            text_blocks,
            seen_images,
            page_url,
        )

    def _flush_body_text_blocks(
        self,
        body_blocks: list[BodyBlock],
        text_blocks: list[str],
    ) -> None:
        if not text_blocks:
            return
        text = self._compact_text_blocks(text_blocks)
        text_blocks.clear()
        if text:
            body_blocks.append(self._make_text_block(text))

    def _append_body_image_block(
        self,
        body_blocks: list[BodyBlock],
        seen_images: set[str],
        candidate: str | None,
    ) -> None:
        normalized = self._normalize_media_url(candidate)
        if not normalized or not self._looks_like_image_url(normalized):
            return
        key = self._media_key(normalized)
        if not key or key in seen_images:
            return
        seen_images.add(key)
        body_blocks.append(self._make_image_block(normalized))

    def _merge_adjacent_body_text_blocks(
        self,
        body_blocks: list[BodyBlock],
    ) -> list[BodyBlock]:
        merged: list[BodyBlock] = []
        pending_texts: list[str] = []

        def flush_texts() -> None:
            if not pending_texts:
                return
            merged.append(self._make_text_block(self._join_sections(pending_texts)))
            pending_texts.clear()

        for block in body_blocks:
            kind = block.get("kind")
            value = str(block.get("value") or "").strip()
            if not value:
                continue
            if kind == "text":
                pending_texts.append(value)
                continue
            flush_texts()
            if kind == "image":
                merged.append(block)

        flush_texts()
        return merged

    @staticmethod
    def _make_text_block(text: str) -> BodyBlock:
        return {"kind": "text", "value": text}

    @staticmethod
    def _make_image_block(url: str) -> BodyBlock:
        return {"kind": "image", "value": url}

    def _append_node_content(
        self,
        node: Tag | NavigableString,
        text_blocks: list[str],
    ) -> None:
        if isinstance(node, NavigableString):
            text = self._normalize_text(str(node))
            if text:
                self._append_text_block(text_blocks, text)
            return

        if not isinstance(node, Tag):
            return

        name = (node.name or "").lower()
        if not name or name in {
            "script",
            "style",
            "noscript",
            "img",
            "video",
            "source",
            "iframe",
            "audio",
            "svg",
        }:
            return

        if name == "br":
            if text_blocks:
                text_blocks[-1] = text_blocks[-1].rstrip() + "\n"
            return

        if name in {"h1", "h2", "h3", "h4", "h5", "h6"}:
            self._append_text_block(text_blocks, self._node_text(node))
            return

        if name == "blockquote":
            self._append_text_block(
                text_blocks,
                self._format_blockquote_text(self._node_text(node, keep_newlines=True)),
            )
            return

        if name in {"ul", "ol"}:
            self._append_text_block(
                text_blocks,
                self._format_list_text(
                    self._collect_list_items(node),
                    ordered=name == "ol",
                ),
            )
            return

        if name == "li":
            item_text = self._node_text(node, keep_newlines=True)
            if item_text:
                self._append_text_block(
                    text_blocks,
                    self._format_list_text([item_text], ordered=False),
                )
            return

        if name == "pre":
            self._append_text_block(
                text_blocks,
                self._format_code_block(
                    self._extract_code_block(node),
                    self._extract_code_language(node),
                ),
            )
            return

        if name == "code":
            parent = node.parent
            if isinstance(parent, Tag) and (parent.name or "").lower() == "pre":
                return
            self._append_text_block(text_blocks, self._node_text(node))
            return

        if name == "hr":
            self._append_text_block(text_blocks, "---")
            return

        block_tags = {
            "p",
            "div",
            "section",
            "article",
            "main",
            "header",
            "footer",
            "aside",
            "figure",
            "figcaption",
            "table",
            "thead",
            "tbody",
            "tfoot",
            "tr",
            "td",
            "th",
        }
        has_block_child = any(
            isinstance(child, Tag)
            and (child.name or "").lower()
            in {
                "p",
                "div",
                "section",
                "article",
                "blockquote",
                "ul",
                "ol",
                "pre",
                "figure",
                "table",
                "hr",
                "h1",
                "h2",
                "h3",
                "h4",
                "h5",
                "h6",
            }
            for child in node.children
        )
        if name in block_tags and has_block_child:
            self._append_container_content(node, text_blocks)
            return

        if name in block_tags:
            if self._has_media_child(node) and not self._node_text(
                node, keep_newlines=True
            ):
                return
            self._append_text_block(
                text_blocks,
                self._node_text(node, keep_newlines=True),
            )
            return

        text = self._node_text(node, keep_newlines=True)
        if text:
            self._append_text_block(text_blocks, text)
            return

        self._append_container_content(node, text_blocks)

    def _append_container_content(
        self,
        node: Tag | BeautifulSoup,
        text_blocks: list[str],
    ) -> None:
        for child in node.children:
            if isinstance(child, (Tag, NavigableString)):
                self._append_node_content(child, text_blocks)

    def _append_text_block(self, text_blocks: list[str], text: str) -> None:
        value = text.replace("\r\n", "\n").replace("\r", "\n").strip()
        if not value:
            return
        if value.startswith("```"):
            text_blocks.append(value)
            return
        text_blocks.append(self._normalize_text(value, keep_newlines=True))

    def _compact_text_blocks(self, text_blocks: list[str]) -> str:
        compacted: list[str] = []

        def is_special(block: str) -> bool:
            stripped = block.lstrip()
            return (
                not stripped
                or stripped.startswith("```")
                or stripped.startswith(">")
                or stripped.startswith("- ")
                or stripped == "---"
                or bool(re.match(r"^\d+\.\s", stripped))
                or "\n" in stripped
            )

        for block in text_blocks:
            value = block.strip()
            if not value:
                continue
            if compacted and not is_special(compacted[-1]) and not is_special(value):
                compacted[-1] = self._normalize_text(f"{compacted[-1]} {value}")
                continue
            compacted.append(value)
        return "\n\n".join(compacted).strip()

    def _format_blockquote_text(self, text: str) -> str:
        value = self._normalize_text(text, keep_newlines=True)
        if not value:
            return ""
        return "\n".join(
            f"> {line}" if line else ">" for line in value.splitlines()
        ).strip()

    def _format_list_text(self, items: list[str], *, ordered: bool) -> str:
        lines: list[str] = []
        for index, item in enumerate(items, start=1):
            value = self._normalize_text(item, keep_newlines=True)
            if not value:
                continue
            prefix = f"{index}. " if ordered else "- "
            parts = value.splitlines() or [value]
            lines.append(prefix + parts[0])
            indent = " " * len(prefix)
            for line in parts[1:]:
                lines.append(indent + line)
        return "\n".join(lines).strip()

    def _format_code_block(self, code: str, language: str | None = None) -> str:
        value = code.replace("\r\n", "\n").replace("\r", "\n").strip("\n")
        if not value.strip():
            return ""
        lang = re.sub(r"[^A-Za-z0-9_+#.-]+", "", language or "")
        if lang:
            return f"```{lang}\n{value}\n```"
        return f"```\n{value}\n```"

    def _has_media_child(self, node: Tag) -> bool:
        return (
            node.find(["img", "video", "source", "iframe"], recursive=True) is not None
        )

    def _collect_list_items(self, list_node: Tag) -> list[str]:
        items: list[str] = []
        for li in list_node.find_all("li", recursive=False):
            li_copy_soup = BeautifulSoup(str(li), "html.parser")
            li_copy = li_copy_soup.find("li")
            if li_copy is None:
                continue

            nested_blocks: list[str] = []
            for nested in li.find_all(["ul", "ol"], recursive=False):
                nested_text = self._format_list_text(
                    self._collect_list_items(nested),
                    ordered=(nested.name or "").lower() == "ol",
                )
                if nested_text:
                    nested_blocks.append(
                        "\n".join(f"  {line}" for line in nested_text.splitlines())
                    )

            for nested in li_copy.find_all(["ul", "ol"]):
                nested.decompose()

            base_text = self._node_text(li_copy, keep_newlines=True)
            combined = "\n".join(
                part for part in [base_text, *nested_blocks] if part
            ).strip()
            if combined:
                items.append(combined)
        return items

    def _extract_code_block(self, pre_tag: Tag) -> str:
        code_tag = pre_tag.find("code")
        source = code_tag if isinstance(code_tag, Tag) else pre_tag
        return html.unescape(source.get_text("", strip=False))

    def _extract_code_language(self, pre_tag: Tag) -> str | None:
        for tag in (pre_tag, pre_tag.find("code")):
            if not isinstance(tag, Tag):
                continue
            for key in ("data-language", "data-lang", "lang"):
                value = tag.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
            for item in self._iter_attr_strings(tag.get("class")):
                matched = re.search(
                    r"(?:lang|language)[-_]?([A-Za-z0-9_+#.-]+)",
                    item,
                    re.I,
                )
                if matched:
                    return matched.group(1)
        return None

    def _append_image_url(self, image_urls: list[str], candidate: str | None) -> None:
        normalized = self._normalize_media_url(candidate)
        if not normalized or not self._looks_like_image_url(normalized):
            return
        key = self._media_key(normalized)
        if not key:
            return
        if any(self._media_key(item) == key for item in image_urls):
            return
        image_urls.append(normalized)

    def _append_video_entry(
        self,
        video_entries: list[VideoEntry],
        candidate: VideoEntry | None,
    ) -> None:
        if not candidate:
            return

        url = self._normalize_media_url(candidate.get("url"))
        if not url or not self._looks_like_video_url(url):
            return

        entry: VideoEntry = {
            "url": url,
            "cover_url": self._normalize_media_url(candidate.get("cover_url")) or None,
            "title": self._normalize_text(str(candidate.get("title") or "")) or None,
        }
        key = self._media_key(url)
        if not key:
            return

        for existing in video_entries:
            if self._media_key(existing.get("url")) != key:
                continue
            if not existing.get("cover_url") and entry.get("cover_url"):
                existing["cover_url"] = entry["cover_url"]
            if not existing.get("title") and entry.get("title"):
                existing["title"] = entry["title"]
            return

        video_entries.append(entry)

    def _extract_image_url(self, tag: Tag, page_url: str) -> str | None:
        for key in (*self._MEDIA_ATTRS, "srcset"):
            if key not in tag.attrs:
                continue
            for raw in self._iter_attr_strings(tag.attrs.get(key)):
                candidates = re.findall(r"https?://[^\s,'\"<>]+|//[^\s,'\"<>]+", raw)
                if not candidates and raw:
                    candidates = [raw.split()[0]]
                for candidate in candidates:
                    normalized = self._normalize_media_url(candidate, page_url)
                    if normalized and self._looks_like_image_url(normalized):
                        return normalized

        for raw in self._iter_attr_strings(tag.attrs):
            for candidate in re.findall(r"https?://[^\s,'\"<>]+|//[^\s,'\"<>]+", raw):
                normalized = self._normalize_media_url(candidate, page_url)
                if normalized and self._looks_like_image_url(normalized):
                    return normalized
        return None

    def _extract_video_entry_from_tag(
        self, tag: Tag, page_url: str
    ) -> VideoEntry | None:
        url: str | None = None

        for key in (*self._VIDEO_URL_KEYS, *self._MEDIA_ATTRS):
            if key not in tag.attrs:
                continue
            for raw in self._iter_attr_strings(tag.attrs.get(key)):
                candidates = re.findall(r"https?://[^\s,'\"<>]+|//[^\s,'\"<>]+", raw)
                if not candidates and raw:
                    candidates = [raw]
                for candidate in candidates:
                    normalized = self._normalize_media_url(candidate, page_url)
                    if normalized and self._looks_like_video_url(normalized):
                        url = normalized
                        break
                if url:
                    break
            if url:
                break

        if not url:
            for raw in self._iter_attr_strings(tag.attrs):
                for candidate in re.findall(
                    r"https?://[^\s,'\"<>]+|//[^\s,'\"<>]+",
                    raw,
                ):
                    normalized = self._normalize_media_url(candidate, page_url)
                    if normalized and self._looks_like_video_url(normalized):
                        url = normalized
                        break
                if url:
                    break

        if not url:
            return None

        title = (
            self._normalize_text(
                str(tag.get("title") or tag.get("aria-label") or tag.get("alt") or "")
            )
            or None
        )
        return {
            "url": url,
            "cover_url": self._extract_cover_url(tag, page_url),
            "title": title,
        }

    def _extract_cover_url(self, tag: Tag, page_url: str) -> str | None:
        for node in (tag, tag.parent if isinstance(tag.parent, Tag) else None):
            if not isinstance(node, Tag):
                continue
            for key in (*self._VIDEO_COVER_KEYS, *self._MEDIA_ATTRS):
                if key not in node.attrs:
                    continue
                for raw in self._iter_attr_strings(node.attrs.get(key)):
                    normalized = self._normalize_media_url(raw, page_url)
                    if normalized and self._looks_like_image_url(normalized):
                        return normalized
        return None

    def _build_video_contents(
        self,
        video_entries: list[VideoEntry],
        *,
        request_headers: dict[str, str],
    ) -> list[VideoContent]:
        contents: list[VideoContent] = []
        for entry in video_entries:
            video_url = str(entry.get("url") or "").strip()
            if not video_url:
                continue
            contents.append(
                self._build_video_content_from_url(
                    video_url,
                    cover_url=entry.get("cover_url"),
                    request_headers=request_headers,
                )
            )
        return contents

    def _build_video_content_from_url(
        self,
        video_url: str,
        *,
        cover_url: str | None = None,
        request_headers: dict[str, str],
    ) -> VideoContent:
        if ".m3u8" in video_url.lower():
            task = self.downloader.ytdlp_download_video_relaxed(
                video_url,
                headers=request_headers,
                proxy=self.proxy,
            )
            return self.create_video_content_by_task(
                task,
                cover_url,
                headers=request_headers,
            )
        return self.create_video_content(
            video_url,
            cover_url,
            headers=request_headers,
        )

    def _extract_video_entries_from_state(
        self,
        initial_data: dict[str, Any],
        page_url: str,
    ) -> list[VideoEntry]:
        entries: list[VideoEntry] = []
        queue: list[Any] = [initial_data.get("initialState") or initial_data]
        seen: set[int] = set()

        while queue:
            value = queue.pop()
            if isinstance(value, dict):
                marker = id(value)
                if marker in seen:
                    continue
                seen.add(marker)
                self._append_video_entry(
                    entries,
                    self._extract_video_entry_from_mapping(value, page_url),
                )
                queue.extend(value.values())
                continue

            if isinstance(value, list):
                marker = id(value)
                if marker in seen:
                    continue
                seen.add(marker)
                queue.extend(value)

        return entries

    def _extract_video_entry_from_mapping(
        self,
        mapping: dict[str, Any],
        page_url: str,
    ) -> VideoEntry | None:
        lowered_keys = {str(key).lower() for key in mapping.keys()}
        if not lowered_keys:
            return None

        has_video_signal = any(
            token in key
            for key in lowered_keys
            for token in ("video", "play", "stream", "playlist", "cover", "poster")
        )
        if not has_video_signal:
            return None

        video_url = self._find_media_value(
            mapping,
            self._looks_like_video_url,
            self._VIDEO_URL_KEYS,
        )
        if not video_url:
            return None

        return {
            "url": self._normalize_media_url(video_url, page_url),
            "cover_url": self._normalize_media_url(
                self._find_media_value(
                    mapping,
                    self._looks_like_image_url,
                    self._VIDEO_COVER_KEYS,
                ),
                page_url,
            )
            or None,
            "title": self._find_text_value(mapping, self._VIDEO_TITLE_KEYS),
        }

    def _find_media_value(
        self,
        value: Any,
        predicate: Any,
        preferred_keys: tuple[str, ...] = (),
    ) -> str | None:
        seen: set[int] = set()
        preferred = {key.lower() for key in preferred_keys}

        def visit(current: Any) -> str | None:
            if isinstance(current, str):
                normalized = self._normalize_state_media_url(current)
                if normalized and predicate(normalized):
                    return normalized
                return None

            if isinstance(current, dict):
                marker = id(current)
                if marker in seen:
                    return None
                seen.add(marker)

                for key in preferred_keys:
                    for current_key, nested in current.items():
                        if str(current_key).lower() == key.lower():
                            found = visit(nested)
                            if found:
                                return found

                for current_key, nested in current.items():
                    if str(current_key).lower() in preferred:
                        continue
                    found = visit(nested)
                    if found:
                        return found
                return None

            if isinstance(current, list):
                marker = id(current)
                if marker in seen:
                    return None
                seen.add(marker)
                for nested in current:
                    found = visit(nested)
                    if found:
                        return found
            return None

        return visit(value)

    def _find_text_value(
        self,
        value: Any,
        preferred_keys: tuple[str, ...] = (),
    ) -> str | None:
        seen: set[int] = set()
        preferred = {key.lower() for key in preferred_keys}

        def pick_text(raw: str) -> str | None:
            text = self._normalize_text(raw)
            if not text:
                return None
            lowered = text.lower()
            if lowered.startswith("http://") or lowered.startswith("https://"):
                return None
            return text

        def visit(current: Any) -> str | None:
            if isinstance(current, str):
                return pick_text(current)

            if isinstance(current, dict):
                marker = id(current)
                if marker in seen:
                    return None
                seen.add(marker)

                for key in preferred_keys:
                    for current_key, nested in current.items():
                        if str(current_key).lower() == key.lower():
                            found = visit(nested)
                            if found:
                                return found

                for current_key, nested in current.items():
                    if str(current_key).lower() in preferred:
                        continue
                    found = visit(nested)
                    if found:
                        return found
                return None

            if isinstance(current, list):
                marker = id(current)
                if marker in seen:
                    return None
                seen.add(marker)
                for nested in current:
                    found = visit(nested)
                    if found:
                        return found
            return None

        return visit(value)

    def _iter_attr_strings(self, value: Any):
        if isinstance(value, str):
            yield value
            return

        if isinstance(value, dict):
            for item in value.values():
                yield from self._iter_attr_strings(item)
            return

        if isinstance(value, (list, tuple, set)):
            for item in value:
                yield from self._iter_attr_strings(item)

    def _node_text(
        self,
        node: Tag | NavigableString,
        *,
        keep_newlines: bool = False,
    ) -> str:
        if isinstance(node, NavigableString):
            return self._normalize_text(str(node), keep_newlines=keep_newlines)
        if not isinstance(node, Tag):
            return ""
        separator = "\n" if keep_newlines else " "
        return self._normalize_text(
            node.get_text(separator=separator, strip=False),
            keep_newlines=keep_newlines,
        )

    def _html_to_text(self, html_text: str, *, keep_newlines: bool = False) -> str:
        if not html_text.strip():
            return ""

        soup = BeautifulSoup(html_text, "html.parser")
        for node in soup.find_all(
            [
                "script",
                "style",
                "noscript",
                "img",
                "video",
                "source",
                "iframe",
                "audio",
                "svg",
            ]
        ):
            node.decompose()

        text_blocks: list[str] = []
        self._append_container_content(soup, text_blocks)
        text = self._compact_text_blocks(text_blocks)
        if keep_newlines:
            return text
        return self._normalize_text(text)

    def _normalize_text(self, text: str, *, keep_newlines: bool = False) -> str:
        if not text:
            return ""
        value = html.unescape(text)
        value = value.replace("\xa0", " ").replace("\u3000", " ")
        value = value.replace("\r\n", "\n").replace("\r", "\n")
        value = re.sub(r"[ \t\f\v]+", " ", value)
        if keep_newlines:
            value = re.sub(r"[ \t]*\n[ \t]*", "\n", value)
            value = re.sub(r"\n{3,}", "\n\n", value)
        else:
            value = re.sub(r"\s+", " ", value)
        return value.strip()

    def _normalize_media_url(
        self,
        url: str | None,
        page_url: str | None = None,
    ) -> str:
        if not url:
            return ""
        value = html.unescape(str(url)).strip().strip("\"'")
        value = value.replace("\\u002F", "/").replace("\\/", "/")
        value = value.replace("&amp;", "&")
        value = value.rstrip(".,);")
        if not value or value.startswith(("data:", "blob:")):
            return ""
        if value.startswith("//"):
            value = "https:" + value
        elif page_url and not re.match(r"^[A-Za-z][A-Za-z0-9+.-]*://", value):
            value = urljoin(page_url, value)
        if not value.startswith(("http://", "https://")):
            return ""
        return value

    def _normalize_state_media_url(self, url: str | None) -> str:
        if not url:
            return ""
        value = html.unescape(str(url)).strip().strip("\"'")
        value = value.replace("\\u002F", "/").replace("\\/", "/")
        matched = re.search(r"https?://[^\s'\"<>]+|//[^\s'\"<>]+", value)
        if matched:
            value = matched.group(0)
        return self._normalize_media_url(value)

    def _looks_like_video_url(self, url: str | None) -> bool:
        if not url:
            return False
        value = url.lower()
        if not value.startswith(("http://", "https://")):
            return False
        if self._looks_like_image_url(value):
            return False
        if re.search(r"\.(mp4|m4v|mov|webm|m3u8)(?:$|[?#])", value):
            return True
        return any(
            marker in value
            for marker in (
                "video.zhihu.com",
                "/playlist.m3u8",
                "/playback/",
                "/stream/",
                ".vod.",
            )
        )

    def _looks_like_image_url(self, url: str | None) -> bool:
        if not url:
            return False
        value = url.lower()
        if not value.startswith(("http://", "https://")):
            return False
        if re.search(r"\.(mp4|m4v|mov|webm|m3u8)(?:$|[?#])", value):
            return False
        if re.search(r"\.(jpg|jpeg|png|webp|gif|bmp|avif)(?:$|[?#])", value):
            return True
        return any(
            host in value
            for host in (
                "picx.zhimg.com",
                "pic1.zhimg.com",
                "pic2.zhimg.com",
                "zhimg.com",
            )
        )

    def _media_key(self, url: str | None) -> str:
        normalized = self._normalize_media_url(url)
        if not normalized:
            return ""
        value = normalized.split("#", 1)[0]
        if self._looks_like_image_url(value) or self._looks_like_video_url(value):
            value = value.split("?", 1)[0]
        return value.replace("http://", "https://")

    def _merge_unique_urls(self, *groups: list[str]) -> list[str]:
        merged: list[str] = []
        seen: set[str] = set()
        for group in groups:
            for url in group:
                key = self._media_key(url)
                if not key or key in seen:
                    continue
                seen.add(key)
                merged.append(url)
        return merged

    def _merge_unique_video_entries(
        self,
        *groups: list[VideoEntry],
    ) -> list[VideoEntry]:
        merged: dict[str, VideoEntry] = {}
        order: list[str] = []

        for group in groups:
            for entry in group:
                url = self._normalize_media_url(entry.get("url"))
                key = self._media_key(url)
                if not key:
                    continue
                normalized_entry: VideoEntry = {
                    "url": url,
                    "cover_url": self._normalize_media_url(entry.get("cover_url"))
                    or None,
                    "title": self._normalize_text(str(entry.get("title") or ""))
                    or None,
                }
                if key not in merged:
                    merged[key] = normalized_entry
                    order.append(key)
                    continue
                if not merged[key].get("cover_url") and normalized_entry.get(
                    "cover_url"
                ):
                    merged[key]["cover_url"] = normalized_entry["cover_url"]
                if not merged[key].get("title") and normalized_entry.get("title"):
                    merged[key]["title"] = normalized_entry["title"]

        return [merged[key] for key in order]

    @staticmethod
    def _format_stats_line(stats: list[StatItem]) -> str:
        return " | ".join(f"{label} {value}" for label, value in stats if value)

    def _format_timestamp(self, value: Any) -> str | None:
        timestamp = self._safe_int(value)
        if timestamp is None or timestamp <= 0:
            return None
        if timestamp >= 10**12:
            timestamp //= 1000
        try:
            dt = datetime.fromtimestamp(timestamp, tz=self.cfg.timezone)
        except Exception:
            return None
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    def _format_count(self, value: Any) -> str:
        number = self._safe_int(value)
        if number is None:
            return self._normalize_text(str(value or ""))
        if abs(number) >= 100000000:
            text = f"{number / 100000000:.1f}".rstrip("0").rstrip(".")
            return f"{text}亿"
        if abs(number) >= 10000:
            text = f"{number / 10000:.1f}".rstrip("0").rstrip(".")
            return f"{text}万"
        return str(number)

    @staticmethod
    def _safe_int(value: Any) -> int | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return None
            try:
                return int(stripped)
            except ValueError:
                try:
                    return int(float(stripped))
                except ValueError:
                    return None
        try:
            return int(value)
        except Exception:
            return None

    @staticmethod
    def _article_url(article_id: str) -> str:
        return f"https://zhuanlan.zhihu.com/p/{article_id}"

    @staticmethod
    def _answer_url(question_id: str, answer_id: str) -> str:
        return f"https://www.zhihu.com/question/{question_id}/answer/{answer_id}"

    @staticmethod
    def _question_url(question_id: str) -> str:
        return f"https://www.zhihu.com/question/{question_id}"
