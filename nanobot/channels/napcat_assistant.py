"""NapCat assistant channel adapter for OneBot-style QQ messaging."""

from __future__ import annotations

import asyncio
import contextlib
import os
import re
import time
from pathlib import Path
from typing import Any

from loguru import logger
from pydantic import Field

from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel
from nanobot.channels.napcat import (
    _ACTION_GET_LOGIN_INFO,
    _ACTION_SEND_GROUP_MSG,
    _ACTION_SEND_PRIVATE_MSG,
    _IDENTITY_INIT_TIMEOUT_SECONDS,
    _coerce_optional_str,
    _coerce_str,
    _guess_media_cq_type,
    _normalize_ws_url,
    _parse_cq_params,
    _parse_internal_chat_id,
    MediaDownloader,
    NapCatMessageParser,
    NapCatNormalizer,
    NapCatTransport,
)
from nanobot.config.paths import get_media_dir
from nanobot.config.schema import Base

_SUPPORTED_COMMANDS = frozenset({"/new", "/stop", "/restart", "/status", "/help"})
_CQ_OUTBOUND_RE = re.compile(r"\[CQ:([^,\]]+)(?:,([^\]]+))?\]")
_ACTION_SEND_FORWARD_MSG = "send_forward_msg"
_DEFAULT_FORWARD_MAX_CHARS = 600
_FORWARD_CHUNK_SIZE = 600


class NapCatAssistantConfig(Base):
    """NapCat Assistant Channel 配置。"""

    enabled: bool = False
    url: str = ""
    token: str = ""
    allow_from: list[str] = Field(default_factory=lambda: ["*"])
    ignore_self_messages: bool = True
    assistant_command: str = "/nanobot"
    long_reply_forward_threshold: int = Field(default=_DEFAULT_FORWARD_MAX_CHARS, ge=0)
    media_dir: str = ""
    media_download_timeout: int = Field(default=30, ge=1)
    media_max_size_mb: int = Field(default=50, ge=1)


class NapCatAssistantOutboundSender:
    """NapCat assistant 出站发送器。"""

    def __init__(
        self,
        transport: NapCatTransport,
        media_root: Path,
        self_id_getter,
        self_name_getter,
        config: NapCatAssistantConfig,
    ):
        self._transport = transport
        self._media_root = media_root
        self._self_id_getter = self_id_getter
        self._self_name_getter = self_name_getter
        self._config = config

    async def send(self, msg: OutboundMessage) -> None:
        try:
            route, target_id = _parse_internal_chat_id(msg.chat_id)
        except ValueError:
            logger.error("NapCat Assistant [SEND] invalid chat_id={}", msg.chat_id)
            return

        content = self._compose_outbound_content(msg.content, msg.media)
        if self._should_send_forward(msg, content):
            response = await self._send_forward(route=route, target_id=target_id, content=content)
        else:
            response = await self._send_standard(route=route, target_id=target_id, content=content)

        message_id = self._extract_message_id(response)
        logger.info(
            "NapCat Assistant [SEND] ok chat_id={} out_msg_id={} content_len={}",
            msg.chat_id,
            message_id or "?",
            len(_coerce_str(msg.content)),
        )

    async def _send_standard(self, route: str, target_id: str, content: str) -> dict[str, Any]:
        if route == "group":
            action = _ACTION_SEND_GROUP_MSG
            params = {"group_id": target_id, "message": content}
        else:
            action = _ACTION_SEND_PRIVATE_MSG
            params = {"user_id": target_id, "message": content}
        return await self._transport.call_action(action, params)

    async def _send_forward(self, route: str, target_id: str, content: str) -> dict[str, Any]:
        params: dict[str, Any] = {
            "message_type": route,
            "message": self._build_forward_nodes(content),
        }
        if route == "group":
            params["group_id"] = target_id
        else:
            params["user_id"] = target_id
        return await self._transport.call_action(_ACTION_SEND_FORWARD_MSG, params)

    def _should_send_forward(self, msg: OutboundMessage, content: str) -> bool:
        if msg.media:
            return False
        threshold = self._config.long_reply_forward_threshold
        if threshold <= 0:
            return False
        return len(content) > threshold

    def _build_forward_nodes(self, content: str) -> list[dict[str, Any]]:
        nickname = self._self_name_getter() or "nanobot"
        uin = self._self_id_getter() or "0"
        chunks = self._split_text_for_forward(content)
        return [
            {
                "type": "node",
                "data": {
                    "nickname": nickname,
                    "uin": uin,
                    "content": [{"type": "text", "data": {"text": chunk}}],
                },
            }
            for chunk in chunks
        ]

    @staticmethod
    def _split_text_for_forward(content: str) -> list[str]:
        text = content.strip()
        if not text:
            return ["[empty message]"]
        return [text[i:i + _FORWARD_CHUNK_SIZE] for i in range(0, len(text), _FORWARD_CHUNK_SIZE)]

    def _compose_outbound_content(self, content: str, media: list[str]) -> str:
        pieces: list[str] = []
        if content:
            pieces.append(content)
        for media_path in media:
            pieces.append(self._build_media_cq_code(media_path))
        merged = "".join(pieces).strip()
        if not merged:
            return "[empty message]"
        return self._normalize_outbound_cq_images(merged)

    def _build_media_cq_code(self, media_path: str) -> str:
        path = Path(media_path).expanduser().resolve()
        cq_type = _guess_media_cq_type(str(path))
        return f"[CQ:{cq_type},file={path.as_uri()},name={path.name}]"

    def _normalize_outbound_cq_images(self, text: str) -> str:
        source = _coerce_str(text)
        if "[CQ:" not in source:
            return source

        out: list[str] = []
        pos = 0
        for match in _CQ_OUTBOUND_RE.finditer(source):
            out.append(source[pos:match.start()])
            cq_text = match.group(0)
            cq_type = _coerce_str(match.group(1)).strip().lower()
            params_raw = _coerce_str(match.group(2))
            if cq_type != "image":
                out.append(cq_text)
                pos = match.end()
                continue

            params = _parse_cq_params(params_raw)
            file_value = _coerce_str(params.get("file")).strip()
            if not file_value or self._is_cq_file_uri(file_value):
                out.append(cq_text)
                pos = match.end()
                continue

            resolved = self._resolve_outbound_image_file(file_value)
            if resolved is None:
                out.append(cq_text)
                pos = match.end()
                continue

            params["file"] = resolved.as_uri()
            params_str = ",".join(f"{key}={value}" for key, value in params.items() if key)
            out.append(f"[CQ:image,{params_str}]")
            pos = match.end()

        out.append(source[pos:])
        return "".join(out)

    @staticmethod
    def _is_cq_file_uri(file_value: str) -> bool:
        lower = _coerce_str(file_value).strip().lower()
        return lower.startswith(("file://", "http://", "https://", "base64://"))

    def _resolve_outbound_image_file(self, file_value: str) -> Path | None:
        path = Path(os.path.expanduser(_coerce_str(file_value).strip()))
        if not path.is_absolute():
            path = self._media_root / _coerce_str(file_value).strip()
        try:
            return path.resolve() if path.exists() and path.is_file() else None
        except Exception:
            return None

    @staticmethod
    def _extract_message_id(response: dict[str, Any]) -> str | None:
        data = response.get("data")
        if isinstance(data, dict):
            return _coerce_optional_str(data.get("message_id"))
        return None


class NapCatAssistantChannel(BaseChannel):
    """NapCat assistant channel using a telegram.py-style direct inbound flow."""

    name = "napcat_assistant"
    display_name = "NapCat Assistant"

    @classmethod
    def default_config(cls) -> dict[str, Any]:
        return NapCatAssistantConfig().model_dump(by_alias=True)

    def __init__(self, config: Any, bus: MessageBus):
        if isinstance(config, dict):
            config = NapCatAssistantConfig.model_validate(config)
        super().__init__(config, bus)
        self.config: NapCatAssistantConfig = config

        self._self_id = ""
        self._self_name = ""
        self._identity_task: asyncio.Task[None] | None = None
        self._transport_task: asyncio.Task[None] | None = None
        self._media_root = (
            Path(self.config.media_dir).expanduser()
            if self.config.media_dir
            else get_media_dir("napcat_assistant")
        )

        self._parser = NapCatMessageParser(self._get_self_id)
        self._normalizer = NapCatNormalizer(self.name, self._parser)
        self._transport = NapCatTransport(
            config=self.config,
            on_event=self._on_transport_event,
            on_connected=self._on_transport_connected,
        )
        self._downloader = MediaDownloader(self.config)
        self._outbound = NapCatAssistantOutboundSender(
            transport=self._transport,
            media_root=self._media_root,
            self_id_getter=self._get_self_id,
            self_name_getter=self._get_self_name,
            config=self.config,
        )

    async def start(self) -> None:
        if not self.config.url.strip():
            logger.error("NapCat Assistant [CONN] missing websocket url")
            return

        self.config.url = _normalize_ws_url(self.config.url)
        if not self.config.url.startswith(("ws://", "wss://")):
            logger.error("NapCat Assistant [CONN] invalid websocket url url={}", self.config.url)
            return

        if self._transport_task is not None and not self._transport_task.done():
            logger.debug("NapCat Assistant [CONN] start ignored because transport task is already running")
            return

        self._running = True
        self._transport_task = asyncio.create_task(self._transport.start())

    async def stop(self) -> None:
        self._running = False
        if self._identity_task is not None:
            self._identity_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._identity_task
            self._identity_task = None
        await self._transport.stop()
        if self._transport_task is not None:
            with contextlib.suppress(asyncio.CancelledError):
                await self._transport_task
            self._transport_task = None

    async def send(self, msg: OutboundMessage) -> None:
        await self._outbound.send(msg)

    def is_allowed(self, sender_id: str) -> bool:
        allow_list = getattr(self.config, "allow_from", [])
        if not allow_list:
            logger.warning("{}: allow_from is empty — all access denied", self.name)
            return False
        if "*" in allow_list:
            return True

        sender_str = str(sender_id)
        if sender_str in allow_list:
            return True
        if sender_str.count("|") == 1:
            user_id, _sep, group_id = sender_str.partition("|")
            return user_id in allow_list or group_id in allow_list
        return False

    async def _on_transport_connected(self) -> None:
        if self._identity_task is not None and not self._identity_task.done():
            self._identity_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._identity_task
        self._identity_task = asyncio.create_task(self._refresh_login_identity())

    async def _refresh_login_identity(self) -> None:
        try:
            timeout_at = time.monotonic() + _IDENTITY_INIT_TIMEOUT_SECONDS
            while not self._self_id and time.monotonic() < timeout_at:
                await asyncio.sleep(0.05)
            if not self._self_id or not self._self_name:
                response = await self._transport.call_action(_ACTION_GET_LOGIN_INFO, {})
                self._apply_login_info(response)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("NapCat Assistant [CONN] refresh login identity failed")

    async def _on_transport_event(self, payload: dict[str, Any]) -> None:
        self._apply_identity_from_event(payload)
        if _coerce_str(payload.get("post_type")).strip().lower() == "meta_event":
            return
        await self._handle_transport_event(payload)

    async def _handle_transport_event(self, payload: dict[str, Any]) -> None:
        normalized = self._normalizer.normalize_event(payload)
        if normalized is None or normalized.is_notice:
            return
        if self.config.ignore_self_messages and self._self_id and normalized.sender_id == self._self_id:
            return

        trigger_reason, forwarded_content = self._decide_trigger(normalized)
        if trigger_reason is None:
            return

        media = await self._downloader.download_many(normalized.media_refs)
        if not forwarded_content and not media:
            forwarded_content = "[empty message]"

        metadata = {
            "message_id": normalized.message_id,
            "napcat_route": normalized.chat.route,
            "napcat_user_id": normalized.chat.user_id,
            "napcat_group_id": normalized.chat.group_id,
            "napcat_sub_type": normalized.chat.sub_type,
            "napcat_trigger_reason": trigger_reason,
            "sender_name": normalized.sender_name,
        }

        sender_id = normalized.sender_id
        if normalized.chat.group_id:
            sender_id = f"{normalized.sender_id}|{normalized.chat.group_id}"

        await self._handle_message(
            sender_id=sender_id,
            chat_id=normalized.chat.chat_id,
            content=forwarded_content,
            media=media,
            metadata=metadata,
        )

    def _decide_trigger(self, normalized: Any) -> tuple[str | None, str]:
        text = (normalized.text or "").strip()
        lowered = text.lower()
        if lowered in _SUPPORTED_COMMANDS:
            return "slash_command", text

        assistant_command = self._assistant_command_prefix()
        lowered_command = assistant_command.lower()
        if lowered == lowered_command or lowered.startswith(f"{lowered_command} "):
            body = text[len(assistant_command):].strip()
            return "assistant_command", body

        if normalized.chat.route == "private":
            return "private", text
        if normalized.at_self:
            return "at_self", text
        return None, text

    def _assistant_command_prefix(self) -> str:
        command = self.config.assistant_command.strip()
        return command or "/nanobot"

    def _apply_identity_from_event(self, payload: dict[str, Any]) -> None:
        event_self_id = _coerce_optional_str(payload.get("self_id"))
        if event_self_id and event_self_id != self._self_id:
            self._self_id = event_self_id
            logger.info("NapCat Assistant [CONN] self_id updated from event self_id={}", self._self_id)

    def _apply_login_info(self, response: dict[str, Any]) -> None:
        data = response.get("data")
        if not isinstance(data, dict):
            logger.warning(
                "NapCat Assistant [CONN] get_login_info missing data preview={!r}",
                repr(response)[:160],
            )
            return
        login_self_id = _coerce_optional_str(data.get("user_id"))
        login_self_name = _coerce_optional_str(data.get("nickname"))
        if login_self_id:
            self._self_id = login_self_id
        if login_self_name:
            self._self_name = login_self_name
        logger.info(
            "NapCat Assistant [CONN] login identity refreshed self_id={} nickname={}",
            self._self_id or "unknown",
            self._self_name or "unknown",
        )

    def _get_self_id(self) -> str:
        return self._self_id

    def _get_self_name(self) -> str:
        return self._self_name
