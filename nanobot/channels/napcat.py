from __future__ import annotations

import asyncio
import contextlib
import json
import os
import hashlib
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Literal
from urllib import request as urllib_request

import websockets
from loguru import logger
from pydantic import Field

from nanobot.bus.events import InboundMessage, OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel
from nanobot.config.paths import get_media_dir
from nanobot.config.schema import Base
from nanobot.security.network import validate_resolved_url, validate_url_target


# ==============================
# 0) Pure helpers
# ==============================

_CHAT_ID_GROUP_PREFIX = "g:"
_CHAT_ID_PRIVATE_PREFIX = "p:"
_CQMSG_VERSION = "CQMSG/1"
_SUPPORTED_COMMANDS = frozenset({"/new", "/stop", "/restart", "/status", "/help"})
_IMAGE_EXTENSIONS = frozenset(
    {".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp", ".heic", ".heif"}
)
_AUDIO_EXTENSIONS = frozenset(
    {".mp3", ".wav", ".ogg", ".opus", ".m4a", ".aac", ".flac", ".amr", ".silk"}
)
_VIDEO_EXTENSIONS = frozenset(
    {".mp4", ".mov", ".avi", ".mkv", ".webm", ".flv", ".wmv"}
)
_IDENTITY_INIT_TIMEOUT_SECONDS = 3.0
_REQUEST_TIMEOUT_SECONDS = 20.0
_RECONNECT_BASE_SECONDS = 1.0
_RECONNECT_MAX_SECONDS = 30.0
_KEEPALIVE_PING_SECONDS = 20
_AGENT_EMPTY_RESPONSE_FALLBACK = "I've completed processing but have no response to give."


def _json_dumps_compact(data: Any) -> str:
    """输出紧凑 JSON 字符串。"""
    return json.dumps(data, ensure_ascii=False, separators=(",", ":"))



def _coerce_str(value: Any) -> str:
    """将任意值稳定转换为字符串。"""
    if value is None:
        return ""
    return str(value)



def _coerce_optional_str(value: Any) -> str | None:
    """将任意值转换为可选字符串。"""
    text = _coerce_str(value).strip()
    return text or None



def _coerce_int(value: Any) -> int | None:
    """将任意值尽量转换为整数。"""
    try:
        return int(value)
    except (TypeError, ValueError):
        return None



def _ensure_dict(value: Any) -> dict[str, Any]:
    """确保输入值为字典。"""
    if isinstance(value, dict):
        return value
    return {}



def _ensure_list(value: Any) -> list[Any]:
    """确保输入值为列表。"""
    if isinstance(value, list):
        return value
    return []


def _normalize_id_list(values: Any) -> frozenset[str]:
    """将单值或列表形式的 ID 配置归一化为去重集合。"""
    if values is None:
        return frozenset()

    if isinstance(values, str):
        items: tuple[Any, ...] | list[Any] | set[Any] | frozenset[Any] = (values,)
    elif isinstance(values, (list, tuple, set, frozenset)):
        items = values
    else:
        items = (values,)

    return frozenset(
        item for item in (_coerce_str(value).strip() for value in items) if item
    )


def _truncate_text(text: str, max_chars: int) -> str:
    """按配置截断文本。"""
    if max_chars <= 0 or len(text) <= max_chars:
        return text
    return text[:max_chars] + "…"



def _basename_only(value: str, fallback: str) -> str:
    """仅保留文件名部分。"""
    name = os.path.basename(value or "").strip()
    return name or fallback



def _cqctx_escape(text: str) -> str:
    """转义 CQMSG/1 中的控制字符。"""
    return (
        text.replace("\\", "\\\\")
        .replace("\r", "\\r")
        .replace("\n", "\\n")
        .replace("|", "\\|")
    )



def _body_join(chunks: list[str]) -> str:
    """将 Body 片段拼接为单行文本。"""
    parts = [chunk.strip() for chunk in chunks if chunk and chunk.strip()]
    return " ".join(parts).strip()



def _normalize_ws_url(url: str) -> str:
    """规范化 WebSocket 地址。"""
    text = url.strip()
    if not text:
        return ""
    return text



def _is_https_url(url: str) -> bool:
    """判断是否为受支持的 HTTPS 资源地址。"""
    return url.startswith("https://")



def _is_http_url(url: str) -> bool:
    """判断是否为 HTTP 或 HTTPS 地址。"""
    return url.startswith("http://") or url.startswith("https://")



def _guess_media_cq_type(path: str) -> Literal["image", "record", "video", "file"]:
    """根据扩展名猜测 CQ 码媒体类型。"""
    suffix = Path(path).suffix.lower()
    if suffix in _IMAGE_EXTENSIONS:
        return "image"
    if suffix in _AUDIO_EXTENSIONS:
        return "record"
    if suffix in _VIDEO_EXTENSIONS:
        return "video"
    return "file"



def _dedupe_keep_order(values: list[str]) -> list[str]:
    """按原顺序去重字符串列表。"""
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            result.append(value)
    return result



def _is_exact_supported_command(text: str) -> bool:
    """判断文本是否为 nanobot 已支持的原生命令。"""
    return text.strip().lower() in _SUPPORTED_COMMANDS


def _to_datetime(timestamp: Any) -> datetime:
    """将 OneBot 时间戳转换为 `datetime`。"""
    ts = _coerce_int(timestamp)
    if ts is None:
        return datetime.now(timezone.utc)
    return datetime.fromtimestamp(ts, tz=timezone.utc)



def _reply_notice_message(sender_name: str) -> str:
    """生成戳一戳等通知的标准文本。"""
    if sender_name:
        return f"[notice:poke] {sender_name} 戳了你"
    return "[notice:poke] 有人戳了你"


def _stable_probability_sample(
    *,
    session_key: str,
    message_id: str,
    sender_id: str,
    message_time: datetime,
) -> float:
    """基于消息身份生成稳定的 [0, 1) 采样值。"""
    seed = (
        f"{session_key}|{message_id}|{sender_id}|{message_time.timestamp():.6f}"
    )
    digest = hashlib.sha256(seed.encode("utf-8")).digest()
    number = int.from_bytes(digest[:8], "big", signed=False)
    return number / (2**64)


def _summarize_event(payload: dict[str, Any]) -> str:
    """生成简短事件摘要，便于日志输出。"""
    post_type = _coerce_str(payload.get("post_type")) or "unknown"
    message_type = _coerce_str(payload.get("message_type"))
    notice_type = _coerce_str(payload.get("notice_type"))
    sub_type = _coerce_str(payload.get("sub_type"))
    user_id = _coerce_str(payload.get("user_id"))
    group_id = _coerce_str(payload.get("group_id"))

    parts = [f"post_type={post_type}"]
    if message_type:
        parts.append(f"message_type={message_type}")
    if notice_type:
        parts.append(f"notice_type={notice_type}")
    if sub_type:
        parts.append(f"sub_type={sub_type}")
    if user_id:
        parts.append(f"user_id={user_id}")
    if group_id:
        parts.append(f"group_id={group_id}")
    return ", ".join(parts)



def _parse_internal_chat_id(chat_id: str) -> tuple[Literal["group", "private"], str]:
    """解析内部 chat_id。"""
    if chat_id.startswith(_CHAT_ID_GROUP_PREFIX):
        return "group", chat_id[len(_CHAT_ID_GROUP_PREFIX):]
    if chat_id.startswith(_CHAT_ID_PRIVATE_PREFIX):
        return "private", chat_id[len(_CHAT_ID_PRIVATE_PREFIX):]
    raise ValueError(f"不支持的 chat_id 格式: {chat_id}")



def _pick_segment_filename(data: dict[str, Any], fallback: str) -> str:
    """从消息段数据中提取尽量稳定的文件名。"""
    for key in ("file", "name", "path", "url"):
        value = _coerce_optional_str(data.get(key))
        if value:
            return _basename_only(value, fallback)
    return fallback


# ==============================
# 1) Config
# ==============================


class NapCatConfig(Base):
    """NapCat Channel 配置。"""

    enabled: bool = False
    url: str = ""
    token: str = ""

    # 说明：
    # nanobot 的 ChannelManager 会在 allow_from == [] 时直接退出进程，
    # 因此这里默认使用 ["*"]，以保持新 channel 安装后的“可启动”行为。
    allow_from: list[str] = Field(default_factory=lambda: ["*"])
    super_admins: list[str] = Field(default_factory=list)
    blacklist_private_ids: list[str] = Field(default_factory=list)
    blacklist_group_ids: list[str] = Field(default_factory=list)

    private_trigger_prob: float = Field(default=0.05, ge=0.0, le=1.0)
    group_trigger_prob: float = Field(default=0.05, ge=0.0, le=1.0)
    nickname_triggers: list[str] = Field(default_factory=list)
    trigger_on_at: bool = True
    trigger_on_reply_to_bot: bool = True
    trigger_on_poke: bool = False
    poke_cooldown_seconds: int = Field(default=60, ge=0)

    context_max_messages: int = Field(default=25, ge=1)
    context_msg_max_chars: int = Field(default=200, ge=0)
    session_buffer_size: int = Field(default=50, ge=1)
    session_idle_ttl_s: int = Field(default=3600, ge=60)
    ignore_self_messages: bool = True

    media_dir: str = ""
    media_download_timeout: int = Field(default=30, ge=1)
    media_max_size_mb: int = Field(default=50, ge=1)

    friend_cache_ttl_s: int = Field(default=600, ge=60)
    group_member_cache_ttl_s: int = Field(default=600, ge=60)


# ==============================
# 2) Types & constants
# ==============================


class NapCatError(RuntimeError):
    """NapCat 适配器基础异常。"""


class NapCatTransportError(NapCatError):
    """NapCat 传输层异常。"""


class NapCatActionError(NapCatError):
    """NapCat 动作调用异常。"""

    def __init__(self, action: str, response: dict[str, Any]):
        """初始化动作调用异常。"""
        self.action = action
        self.response = response
        message = (
            f"NapCat action 调用失败: action={action}, "
            f"status={response.get('status')}, retcode={response.get('retcode')}, "
            f"message={response.get('message') or response.get('wording')}"
        )
        super().__init__(message)


@dataclass(slots=True, frozen=True)
class ChatRef:
    """标准化后的聊天引用。"""

    route: Literal["private", "group"]
    chat_id: str
    session_key: str
    user_id: str | None = None
    group_id: str | None = None
    sub_type: str = ""
    target_id: str | None = None
    temp_source: int | None = None


@dataclass(slots=True)
class MediaReference:
    """入站消息中的媒体引用。"""

    kind: Literal["image", "record", "video", "file"]
    url: str
    filename: str


@dataclass(slots=True)
class BodyToken:
    """消息体中的原子片段。"""

    kind: Literal[
        "text",
        "at",
        "reply",
        "image",
        "record",
        "video",
        "file",
        "face",
        "json",
        "xml",
        "forward",
        "unknown",
    ]
    text: str = ""
    target_id: str | None = None
    message_id: str | None = None
    filename: str | None = None


@dataclass(slots=True)
class ForwardRef:
    """一条消息中的 forward 引用。"""

    forward_id: str | None = None
    embedded_nodes: list[dict[str, Any]] = field(default_factory=list)
    summary: str | None = None


@dataclass(slots=True)
class ParsedMessage:
    """解析后的消息结果。"""

    text: str
    body_tokens: list[BodyToken] = field(default_factory=list)
    at_self: bool = False
    reply_id: str | None = None
    media_refs: list[MediaReference] = field(default_factory=list)
    forward_refs: list[ForwardRef] = field(default_factory=list)


@dataclass(slots=True)
class ForwardNode:
    """转发容器中的单个节点。"""

    message_id: str
    sender_id: str
    sender_name: str
    body_tokens: list[BodyToken] = field(default_factory=list)
    source_chat: str | None = None
    route: Literal["private", "group"] | None = None
    group_id: str | None = None
    sender_card: str | None = None
    sender_nickname: str | None = None


@dataclass(slots=True)
class ForwardBundle:
    """展开后的转发容器。"""

    summary: str | None = None
    nodes: list[ForwardNode] = field(default_factory=list)


@dataclass(slots=True)
class NormalizedInbound:
    """平台无关的标准化入站消息。"""

    chat: ChatRef
    sender_id: str
    sender_name: str
    message_id: str
    time: datetime
    text: str
    raw_message: str
    body_tokens: list[BodyToken] = field(default_factory=list)
    media_refs: list[MediaReference] = field(default_factory=list)
    at_self: bool = False
    reply_id: str | None = None
    forward_refs: list[ForwardRef] = field(default_factory=list)
    target_id: str | None = None
    is_notice: bool = False
    notice_name: str | None = None
    sender_card: str | None = None
    sender_nickname: str | None = None
    raw_event: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class EnrichedInbound:
    """补全引用、转发与媒体后的入站消息。"""

    inbound: NormalizedInbound
    media_paths: list[str] = field(default_factory=list)
    reply_media_paths: list[str] = field(default_factory=list)
    forward_bundles: list[ForwardBundle] = field(default_factory=list)
    forward_media_paths: list[str] = field(default_factory=list)

    @property
    def all_media_paths(self) -> list[str]:
        """返回聚合后的全部媒体路径。"""
        return _dedupe_keep_order(
            self.media_paths + self.reply_media_paths + self.forward_media_paths
        )


@dataclass(slots=True)
class ContextMessageRecord:
    """会话缓冲中的上下文记录。"""

    message_id: str
    role: Literal["user", "assistant"]
    sender_id: str
    sender_name: str
    text: str
    time: datetime
    body_tokens: list[BodyToken] = field(default_factory=list)
    raw_message: str = ""
    at_self: bool = False
    reply_id: str | None = None
    forward_bundles: list[ForwardBundle] = field(default_factory=list)
    media_paths: list[str] = field(default_factory=list)
    is_notice: bool = False
    notice_name: str | None = None


@dataclass(slots=True)
class TriggerDecision:
    """触发判断结果。"""

    should_reply: bool
    reason: str
    detail: str | None = None


def _format_trigger_detail(decision: TriggerDecision) -> str:
    """格式化触发细节，保持日志字段结构稳定。"""
    return decision.detail or "-"


def _find_nickname_trigger(text: str, triggers: list[str]) -> str | None:
    """返回首个命中的昵称触发词，便于日志排查。"""
    lowered_text = text.lower()
    for trigger in triggers:
        normalized = trigger.strip()
        if normalized and normalized.lower() in lowered_text:
            return normalized
    return None


# ==============================
# 3) Transport
# ==============================


class NapCatTransport:
    """NapCat WebSocket 传输层。

    负责：
    1. 建立与 NapCat 的 WebSocket 连接。
    2. 维护 action/echo 响应映射。
    3. 顺序接收并分发 OneBot 事件。
    4. 在断线时进行指数退避重连。

    """

    def __init__(
        self,
        config: NapCatConfig,
        on_event: Callable[[dict[str, Any]], Awaitable[None]],
        on_connected: Callable[[], Awaitable[None]] | None = None,
    ):
        """初始化传输层。"""
        self._config = config
        self._on_event = on_event
        self._on_connected = on_connected

        self._ws: Any | None = None
        self._running = False
        self._send_lock = asyncio.Lock()
        self._connected = asyncio.Event()
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._recv_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """启动 WebSocket 长连接与自动重连循环。"""
        self._running = True
        delay = _RECONNECT_BASE_SECONDS
        url = _normalize_ws_url(self._config.url)
        headers: dict[str, str] = {}
        if self._config.token:
            headers["Authorization"] = f"Bearer {self._config.token}"

        while self._running:
            try:
                logger.info("NapCat [CONN] connecting url={}", url)
                async with websockets.connect(
                    url,
                    additional_headers=headers or None,
                    ping_interval=_KEEPALIVE_PING_SECONDS,
                    open_timeout=_REQUEST_TIMEOUT_SECONDS,
                    close_timeout=5,
                    max_size=None,
                ) as websocket:
                    self._ws = websocket
                    self._connected.set()
                    delay = _RECONNECT_BASE_SECONDS
                    logger.info("NapCat [CONN] connected")

                    self._recv_task = asyncio.create_task(self._recv_loop())
                    if self._on_connected is not None:
                        try:
                            await self._on_connected()
                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            logger.exception("NapCat [CONN] on_connected callback failed")

                    await self._recv_task
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if not self._running:
                    break
                logger.warning(
                    "NapCat [CONN] connect failed retry_in_s={:.1f} error={}",
                    delay,
                    exc,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, _RECONNECT_MAX_SECONDS)
            finally:
                self._connected.clear()
                if self._recv_task is not None:
                    if not self._recv_task.done():
                        self._recv_task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await self._recv_task
                    self._recv_task = None
                self._ws = None
                self._fail_pending(
                    NapCatTransportError("NapCat 连接已断开，未完成动作已取消")
                )

    async def stop(self) -> None:
        """停止传输层并关闭连接。"""
        self._running = False
        self._connected.clear()
        self._fail_pending(NapCatTransportError("NapCat 传输层已停止"))
        if self._recv_task is not None:
            self._recv_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._recv_task
            self._recv_task = None
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                logger.debug("NapCat [CONN] websocket close raised ignored exception", exc_info=True)
            finally:
                self._ws = None

    async def call_action(
        self,
        action: str,
        params: dict[str, Any] | None = None,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """调用 NapCat action。"""
        if not self._running:
            raise NapCatTransportError("NapCat 传输层尚未启动")

        effective_timeout = timeout or _REQUEST_TIMEOUT_SECONDS
        await asyncio.wait_for(self._connected.wait(), timeout=effective_timeout)

        if self._ws is None:
            raise NapCatTransportError("NapCat WebSocket 尚未就绪")

        echo = uuid.uuid4().hex
        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._pending[echo] = future
        payload = {
            "action": action,
            "params": params or {},
            "echo": echo,
        }

        try:
            async with self._send_lock:
                await self._ws.send(_json_dumps_compact(payload))
            response = await asyncio.wait_for(future, timeout=effective_timeout)
        except asyncio.TimeoutError as exc:
            raise NapCatTransportError(f"NapCat action 超时: {action}") from exc
        finally:
            self._pending.pop(echo, None)

        status = _coerce_str(response.get("status")).lower()
        retcode = response.get("retcode")
        if status not in {"ok", ""} or (retcode not in (0, None)):
            raise NapCatActionError(action, response)

        return response

    async def _recv_loop(self) -> None:
        """循环接收 WebSocket 帧。"""
        assert self._ws is not None
        try:
            while self._running and self._ws is not None:
                frame = await self._ws.recv()
                payload = self._decode_frame(frame)
                if payload is None:
                    continue
                if self._is_action_response(payload):
                    self._resolve_pending(payload)
                    continue
                if "post_type" in payload:
                    await self._on_event(payload)
        finally:
            self._connected.clear()
            if self._running:
                logger.warning("NapCat [CONN] recv loop ended, connection dropped")
            self._fail_pending(NapCatTransportError("NapCat 接收循环已结束"))

    def _decode_frame(self, frame: Any) -> dict[str, Any] | None:
        """解码 WebSocket 帧。"""
        if isinstance(frame, bytes):
            try:
                frame = frame.decode("utf-8")
            except UnicodeDecodeError:
                logger.warning("NapCat [CONN] undecodable binary frame")
                return None

        if not isinstance(frame, str):
            logger.warning(
                "NapCat [CONN] unsupported frame_type={}",
                type(frame).__name__,
            )
            return None

        try:
            payload = json.loads(frame)
        except json.JSONDecodeError:
            logger.warning("NapCat [CONN] invalid JSON frame preview={!r}", frame[:120])
            return None

        if not isinstance(payload, dict):
            logger.warning(
                "NapCat [CONN] non-object JSON payload preview={!r}",
                repr(payload)[:120],
            )
            return None
        return payload

    @staticmethod
    def _is_action_response(payload: dict[str, Any]) -> bool:
        """判断当前载荷是否为 action 响应。"""
        return (
            "status" in payload
            and "retcode" in payload
            and ("data" in payload or "message" in payload or "wording" in payload)
        )

    def _resolve_pending(self, payload: dict[str, Any]) -> None:
        """解析 action 响应并唤醒等待方。"""
        echo = _coerce_optional_str(payload.get("echo"))
        if echo:
            future = self._pending.get(echo)
            if future is None or future.done():
                logger.warning(
                    "NapCat [CONN] unexpected echo={} pending={}",
                    echo,
                    len(self._pending),
                )
                return
            future.set_result(payload)
            return

        if len(self._pending) == 1:
            future = next(iter(self._pending.values()))
            if not future.done():
                logger.debug("NapCat [CONN] missing echo matched single pending request")
                future.set_result(payload)
            return

        logger.debug(
            "NapCat [CONN] missing echo ignored pending={} preview={!r}",
            len(self._pending),
            repr(payload)[:120],
        )

    def _fail_pending(self, exc: Exception) -> None:
        """批量终止挂起中的 action。"""
        for future in list(self._pending.values()):
            if not future.done():
                future.set_exception(exc)
        self._pending.clear()


# ==============================
# 4) Router & SessionWorker
# ==============================


class SessionWorker:
    """单 session 串行消费器。"""

    def __init__(
        self,
        session_key: str,
        ttl_seconds: int,
        handler: Callable[[dict[str, Any]], Awaitable[None]],
        on_closed: Callable[[str], None],
    ):
        """初始化 SessionWorker。"""
        self._session_key = session_key
        self._ttl_seconds = ttl_seconds
        self._handler = handler
        self._on_closed = on_closed
        self._queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._task = asyncio.create_task(self._run())

    def enqueue(self, payload: dict[str, Any]) -> None:
        """入队一个事件。"""
        self._queue.put_nowait(payload)

    async def stop(self) -> None:
        """停止 Worker。"""
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task

    async def _run(self) -> None:
        """循环串行处理同一 session 的事件。"""
        try:
            while True:
                try:
                    payload = await asyncio.wait_for(
                        self._queue.get(),
                        timeout=float(self._ttl_seconds),
                    )
                except asyncio.TimeoutError:
                    if self._queue.empty():
                        return
                    continue

                try:
                    await self._handler(payload)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception(
                        "NapCat [ROUTE] session worker failed session={}",
                        self._session_key,
                    )
                finally:
                    self._queue.task_done()
        finally:
            self._on_closed(self._session_key)


class EventRouter:
    """事件路由器。

    负责在最前面执行：
    1. 轻量 session_key 提取。
    2. 来源准入判断。
    3. Worker 创建与串行分发。

    """

    def __init__(
        self,
        config: NapCatConfig,
        self_id_getter: Callable[[], str],
        pipeline: "EventPipeline",
    ):
        """初始化事件路由器。"""
        self._config = config
        self._self_id_getter = self_id_getter
        self._pipeline = pipeline
        self._workers: dict[str, SessionWorker] = {}

    def _remove_worker(self, session_key: str) -> None:
        """移除一个已结束的 session worker。"""
        self._workers.pop(session_key, None)

    async def route(self, payload: dict[str, Any]) -> None:
        """路由事件到对应 session 的串行 Worker。"""
        session_key = extract_session_key(payload, self._self_id_getter())
        if not session_key:
            logger.debug(
                "NapCat [ROUTE] event skipped unroutable summary={}",
                _summarize_event(payload),
            )
            return

        if not check_source_gate(payload, self._config, self._self_id_getter()):
            ctx = _build_event_ctx(payload)
            logger.info(
                "NapCat [ROUTE] source rejected user_id={} group_id={} post_type={}",
                ctx.user_id or "?",
                ctx.source_group_id or "?",
                ctx.post_type or "?",
            )
            return

        worker = self._workers.get(session_key)
        if worker is None:
            worker = SessionWorker(
                session_key=session_key,
                ttl_seconds=self._config.session_idle_ttl_s,
                handler=self._pipeline.handle,
                on_closed=self._remove_worker,
            )
            self._workers[session_key] = worker

        worker.enqueue(payload)

    async def stop(self) -> None:
        """停止全部 Worker。"""
        workers = list(self._workers.values())
        self._workers.clear()
        for worker in workers:
            await worker.stop()


# ==============================
# 5) Normalizer
# ==============================


class NapCatMessageParser:
    """NapCat 消息段解析器。"""

    def __init__(self, self_id_getter: Callable[[], str]):
        """初始化消息段解析器。"""
        self._self_id_getter = self_id_getter

    def parse(self, segments: Any) -> ParsedMessage:
        """解析消息段数组。"""
        if isinstance(segments, str):
            text = segments.strip()
            body_tokens = [BodyToken(kind="text", text=segments)] if segments else []
            return ParsedMessage(text=text or segments, body_tokens=body_tokens)

        parts: list[str] = []
        body_tokens: list[BodyToken] = []
        media_refs: list[MediaReference] = []
        forward_refs: list[ForwardRef] = []
        at_self = False
        reply_id: str | None = None
        self_id = self._self_id_getter()

        for segment in _ensure_list(segments):
            if not isinstance(segment, dict):
                continue

            seg_type = _coerce_str(segment.get("type")).strip().lower()
            data = _ensure_dict(segment.get("data"))

            if seg_type == "text":
                text = _coerce_str(data.get("text"))
                if text:
                    body_tokens.append(BodyToken(kind="text", text=text))
                    parts.append(text)
                continue

            if seg_type == "at":
                qq = _coerce_str(data.get("qq"))
                if qq:
                    body_tokens.append(BodyToken(kind="at", target_id=qq))
                    parts.append("@全体成员" if qq == "all" else f"@{qq}")
                    if self_id and qq == self_id:
                        at_self = True
                continue

            if seg_type == "reply":
                current_reply_id = _coerce_optional_str(data.get("id"))
                if current_reply_id:
                    body_tokens.append(BodyToken(kind="reply", message_id=current_reply_id))
                    reply_id = reply_id or current_reply_id
                    parts.append(f">m:{current_reply_id}")
                continue

            if seg_type == "forward":
                content = [
                    item for item in _ensure_list(data.get("content")) if isinstance(item, dict)
                ]
                forward_refs.append(
                    ForwardRef(
                        forward_id=_coerce_optional_str(data.get("id")),
                        embedded_nodes=content,
                        summary=(
                            _coerce_optional_str(data.get("summary"))
                            or _coerce_optional_str(data.get("title"))
                        ),
                    )
                )
                body_tokens.append(BodyToken(kind="forward"))
                parts.append("[forward]")
                continue

            if seg_type == "image":
                name = _pick_segment_filename(data, "image")
                url = _coerce_str(data.get("url"))
                if _is_http_url(url):
                    media_refs.append(
                        MediaReference(kind="image", url=url, filename=name)
                    )
                body_tokens.append(BodyToken(kind="image", filename=name))
                parts.append(f"[image:{name}]")
                continue

            if seg_type == "record":
                name = _pick_segment_filename(data, "record")
                url = _coerce_str(data.get("url"))
                if _is_http_url(url):
                    media_refs.append(
                        MediaReference(kind="record", url=url, filename=name)
                    )
                body_tokens.append(BodyToken(kind="record", filename=name))
                parts.append("[record]")
                continue

            if seg_type == "video":
                name = _pick_segment_filename(data, "video")
                body_tokens.append(BodyToken(kind="video", filename=name))
                parts.append("[video]")
                continue

            if seg_type == "file":
                name = _pick_segment_filename(data, "file")
                body_tokens.append(BodyToken(kind="file", filename=name))
                parts.append(f"[file:{name}]")
                continue

            if seg_type == "face":
                face_id = _coerce_str(data.get("id")) or "unknown"
                body_tokens.append(BodyToken(kind="face", text=face_id))
                parts.append(f"[face:{face_id}]")
                continue

            if seg_type == "json":
                body_tokens.append(BodyToken(kind="json"))
                parts.append("[json]")
                continue

            if seg_type == "xml":
                body_tokens.append(BodyToken(kind="xml"))
                parts.append("[xml]")
                continue

            body_tokens.append(BodyToken(kind="unknown", text=seg_type or "unknown"))
            parts.append(f"[seg:{seg_type or 'unknown'}]")

        text = _body_join(parts)
        return ParsedMessage(
            text=text,
            body_tokens=body_tokens,
            at_self=at_self,
            reply_id=reply_id,
            media_refs=media_refs,
            forward_refs=forward_refs,
        )


class NapCatNormalizer:
    """NapCat 事件标准化器。"""

    def __init__(self, channel_name: str, parser: NapCatMessageParser):
        """初始化标准化器。"""
        self._channel_name = channel_name
        self._parser = parser

    def normalize_event(self, payload: dict[str, Any]) -> NormalizedInbound | None:
        """标准化实时事件。"""
        post_type = _coerce_str(payload.get("post_type")).strip().lower()
        if post_type in {"message", "message_sent"}:
            return self._normalize_message_payload(payload)
        if post_type == "notice":
            return self._normalize_notice_payload(payload)
        return None

    def normalize_message_object(self, payload: dict[str, Any]) -> NormalizedInbound | None:
        """标准化历史消息或 `get_msg` 返回对象。"""
        cloned = dict(payload)
        cloned["post_type"] = "message"
        return self._normalize_message_payload(cloned)

    def _normalize_message_payload(
        self,
        payload: dict[str, Any],
    ) -> NormalizedInbound | None:
        """标准化消息事件。"""
        message_type = _coerce_str(payload.get("message_type")).strip().lower()
        if message_type not in {"private", "group"}:
            return None

        chat = self._build_chat_ref(payload)
        if chat is None:
            return None

        sender = _ensure_dict(payload.get("sender"))
        sender_id = _coerce_str(payload.get("user_id") or sender.get("user_id"))
        if not sender_id:
            return None

        sender_card = _coerce_optional_str(sender.get("card"))
        sender_nickname = _coerce_optional_str(sender.get("nickname"))
        sender_name = sender_card or sender_nickname or sender_id

        parsed = self._parser.parse(payload.get("message"))
        raw_message = _coerce_str(payload.get("raw_message")).strip()
        text = parsed.text
        if raw_message and not text:
            text = raw_message
        if not text and not parsed.body_tokens and not parsed.media_refs:
            return None

        message_id = _coerce_optional_str(payload.get("message_id"))
        if message_id is None:
            return None

        return NormalizedInbound(
            chat=chat,
            sender_id=sender_id,
            sender_name=sender_name,
            message_id=message_id,
            time=_to_datetime(payload.get("time")),
            text=text or "[empty message]",
            raw_message=raw_message or text or "",
            body_tokens=parsed.body_tokens,
            media_refs=parsed.media_refs,
            at_self=parsed.at_self,
            reply_id=parsed.reply_id,
            forward_refs=parsed.forward_refs,
            target_id=_coerce_optional_str(payload.get("target_id")),
            sender_card=sender_card,
            sender_nickname=sender_nickname,
            raw_event=dict(payload),
        )

    def _normalize_notice_payload(
        self,
        payload: dict[str, Any],
    ) -> NormalizedInbound | None:
        """标准化通知事件。"""
        notice_type = _coerce_str(payload.get("notice_type")).strip().lower()
        sub_type = _coerce_str(payload.get("sub_type")).strip().lower()

        is_poke = (
            (notice_type == "notify" and sub_type == "poke")
            or notice_type == "poke"
        )
        if not is_poke:
            return None

        chat = self._build_notice_chat_ref(payload)
        if chat is None:
            return None

        sender_id = (
            _coerce_str(payload.get("user_id"))
            or _coerce_str(payload.get("sender_id"))
            or "unknown"
        )
        sender_name = sender_id
        text = _reply_notice_message(sender_name)
        message_id = (
            f"notice:poke:{_coerce_str(payload.get('time'))}:"
            f"{sender_id}:{_coerce_str(payload.get('target_id'))}"
        )
        target_id = _coerce_optional_str(payload.get("target_id"))

        return NormalizedInbound(
            chat=chat,
            sender_id=sender_id,
            sender_name=sender_name,
            message_id=message_id,
            time=_to_datetime(payload.get("time")),
            text=text,
            raw_message=text,
            body_tokens=[BodyToken(kind="text", text=text)],
            target_id=target_id,
            is_notice=True,
            notice_name="poke",
            raw_event=dict(payload),
        )

    def _build_chat_ref(self, payload: dict[str, Any]) -> ChatRef | None:
        """构造消息事件的 ChatRef。"""
        message_type = _coerce_str(payload.get("message_type")).strip().lower()
        sub_type = _coerce_str(payload.get("sub_type")).strip().lower()
        user_id = _coerce_optional_str(payload.get("user_id"))

        if message_type == "group":
            group_id = _coerce_optional_str(payload.get("group_id"))
            if not group_id or not user_id:
                return None
            chat_id = f"{_CHAT_ID_GROUP_PREFIX}{group_id}"
            return ChatRef(
                route="group",
                chat_id=chat_id,
                session_key=f"{self._channel_name}:{chat_id}",
                user_id=user_id,
                group_id=group_id,
                sub_type=sub_type,
            )

        if not user_id:
            return None

        chat_id = f"{_CHAT_ID_PRIVATE_PREFIX}{user_id}"
        target_id = _coerce_optional_str(payload.get("target_id"))
        temp_source = _coerce_int(payload.get("temp_source"))

        if sub_type == "group" and target_id:
            session_key = f"{self._channel_name}:pt:{target_id}:{user_id}"
        else:
            session_key = f"{self._channel_name}:{chat_id}"

        return ChatRef(
            route="private",
            chat_id=chat_id,
            session_key=session_key,
            user_id=user_id,
            group_id=None,
            sub_type=sub_type,
            target_id=target_id,
            temp_source=temp_source,
        )

    def _build_notice_chat_ref(self, payload: dict[str, Any]) -> ChatRef | None:
        """构造通知事件的 ChatRef。"""
        group_id = _coerce_optional_str(payload.get("group_id"))
        user_id = (
            _coerce_optional_str(payload.get("user_id"))
            or _coerce_optional_str(payload.get("sender_id"))
        )
        if group_id and user_id:
            chat_id = f"{_CHAT_ID_GROUP_PREFIX}{group_id}"
            return ChatRef(
                route="group",
                chat_id=chat_id,
                session_key=f"{self._channel_name}:{chat_id}",
                user_id=user_id,
                group_id=group_id,
                sub_type="notice",
            )

        if user_id:
            chat_id = f"{_CHAT_ID_PRIVATE_PREFIX}{user_id}"
            return ChatRef(
                route="private",
                chat_id=chat_id,
                session_key=f"{self._channel_name}:{chat_id}",
                user_id=user_id,
                sub_type="notice",
            )
        return None


# ==============================
# 6) Enricher
# ==============================


class MediaDownloader:
    """媒体下载器。"""

    def __init__(self, config: NapCatConfig):
        """初始化媒体下载器。"""
        base_dir = Path(config.media_dir).expanduser() if config.media_dir else get_media_dir("napcat")
        self._root = base_dir
        self._timeout = config.media_download_timeout
        self._max_size_bytes = config.media_max_size_mb * 1024 * 1024

    async def download_many(self, refs: list[MediaReference]) -> list[str]:
        """下载多个媒体引用。"""
        results: list[str] = []
        for ref in refs:
            path = await self.download(ref)
            if path:
                results.append(path)
        return _dedupe_keep_order(results)

    async def download(self, ref: MediaReference) -> str | None:
        """下载单个媒体引用。"""
        if not _is_https_url(ref.url):
            logger.debug("NapCat [MEDIA] skipped non-https media url={}", ref.url)
            return None

        filename = _basename_only(ref.filename, f"{ref.kind}.bin")
        destination = self._root / filename
        if destination.exists():
            return str(destination.resolve())

        ok, error = validate_url_target(ref.url)
        if not ok:
            logger.warning(
                "NapCat [MEDIA] skipped unsafe media url={} err={}",
                ref.url,
                error,
            )
            return None

        self._root.mkdir(parents=True, exist_ok=True)
        tmp_path = destination.with_suffix(destination.suffix + ".part")

        try:
            data = await asyncio.to_thread(self._read_remote_bytes, ref.url)
            await asyncio.to_thread(self._write_atomic, tmp_path, destination, data)
            return str(destination.resolve())
        except Exception:
            with contextlib.suppress(FileNotFoundError):
                tmp_path.unlink()
            logger.exception("NapCat [MEDIA] download failed url={}", ref.url)
            return None

    def _read_remote_bytes(self, url: str) -> bytes:
        """以阻塞方式拉取远端媒体。"""
        request = urllib_request.Request(
            url,
            headers={"User-Agent": "nanobot-napcat/1.0"},
        )
        with urllib_request.urlopen(request, timeout=self._timeout) as response:
            final_url = response.geturl()
            ok, error = validate_resolved_url(final_url)
            if not ok:
                raise NapCatError(f"媒体重定向被拦截: {error}")

            chunks: list[bytes] = []
            total = 0
            while True:
                chunk = response.read(1024 * 128)
                if not chunk:
                    break
                total += len(chunk)
                if total > self._max_size_bytes:
                    raise NapCatError(
                        f"媒体体积超过限制: {total} > {self._max_size_bytes}"
                    )
                chunks.append(chunk)
            return b"".join(chunks)

    @staticmethod
    def _write_atomic(tmp_path: Path, target_path: Path, data: bytes) -> None:
        """使用临时文件原子写入目标路径。"""
        tmp_path.write_bytes(data)
        os.replace(tmp_path, target_path)


class NapCatMessageDetailFetcher:
    """消息细节拉取器。"""

    def __init__(self, transport: NapCatTransport):
        """初始化细节拉取器。"""
        self._transport = transport

    async def get_msg(self, message_id: str) -> dict[str, Any] | None:
        """获取单条消息详情。"""
        response = await self._transport.call_action("get_msg", {"message_id": message_id})
        data = response.get("data")
        if isinstance(data, dict):
            if isinstance(data.get("message"), dict):
                return data["message"]
            return data
        logger.warning("NapCat [MEDIA] get_msg response malformed preview={!r}", repr(response)[:160])
        return None

    async def get_forward_msg(self, forward_id: str) -> list[dict[str, Any]]:
        """获取合并转发详情。"""
        response = await self._transport.call_action("get_forward_msg", {"id": forward_id})
        data = response.get("data")
        if isinstance(data, dict) and isinstance(data.get("messages"), list):
            return [item for item in data["messages"] if isinstance(item, dict)]
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
        logger.warning(
            "NapCat [MEDIA] get_forward_msg response malformed preview={!r}",
            repr(response)[:160],
        )
        return []


class ContactDirectory:
    """联系人与群成员名称解析器。"""

    def __init__(
        self,
        config: NapCatConfig,
        transport: NapCatTransport,
        self_id_getter: Callable[[], str],
        self_name_getter: Callable[[], str],
    ):
        """初始化名称解析器。"""
        self._config = config
        self._transport = transport
        self._self_id_getter = self_id_getter
        self._self_name_getter = self_name_getter

        self._friend_cache: dict[str, str] = {}
        self._friend_cache_expires_at = 0.0
        self._friend_lock = asyncio.Lock()

        self._group_member_cache: dict[tuple[str, str], tuple[str, float]] = {}
        self._group_lock = asyncio.Lock()

    async def resolve_inbound_sender_name(self, inbound: NormalizedInbound) -> str:
        """解析实时/历史消息发送者名称。"""
        if inbound.sender_id == self._self_id_getter() and self._self_name_getter():
            inbound.sender_name = self._self_name_getter()
            return inbound.sender_name

        if inbound.chat.route == "group" and inbound.chat.group_id:
            inbound.sender_name = await self.resolve_group_member_name(
                group_id=inbound.chat.group_id,
                user_id=inbound.sender_id,
                card_hint=inbound.sender_card,
                nickname_hint=inbound.sender_nickname,
            )
            return inbound.sender_name

        inbound.sender_name = await self.resolve_private_name(
            user_id=inbound.sender_id,
            nickname_hint=inbound.sender_nickname,
        )
        return inbound.sender_name

    async def resolve_forward_node_name(self, node: ForwardNode) -> str:
        """解析转发节点中的发送者名称。"""
        if node.sender_id == self._self_id_getter() and self._self_name_getter():
            node.sender_name = self._self_name_getter()
            return node.sender_name

        if node.route == "group" and node.group_id:
            node.sender_name = await self.resolve_group_member_name(
                group_id=node.group_id,
                user_id=node.sender_id,
                card_hint=node.sender_card,
                nickname_hint=node.sender_nickname,
            )
            return node.sender_name

        node.sender_name = await self.resolve_private_name(
            user_id=node.sender_id,
            nickname_hint=node.sender_nickname,
        )
        return node.sender_name

    async def resolve_group_member_name(
        self,
        group_id: str,
        user_id: str,
        card_hint: str | None,
        nickname_hint: str | None,
    ) -> str:
        """解析群成员显示名。

        回退顺序严格为：群昵称 -> QQ 昵称 -> QQ 号码。

        """
        if card_hint:
            return card_hint

        cache_key = (group_id, user_id)
        now = time.monotonic()
        cached = self._group_member_cache.get(cache_key)
        if cached and cached[1] > now:
            return cached[0]

        resolved_name: str | None = None
        async with self._group_lock:
            cached = self._group_member_cache.get(cache_key)
            now = time.monotonic()
            if cached and cached[1] > now:
                return cached[0]

            try:
                response = await self._transport.call_action(
                    "get_group_member_info",
                    {
                        "group_id": group_id,
                        "user_id": user_id,
                    },
                )
                data = response.get("data")
                if isinstance(data, dict):
                    resolved_name = (
                        _coerce_optional_str(data.get("card"))
                        or _coerce_optional_str(data.get("nickname"))
                        or None
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning(
                    "NapCat [MEDIA] get_group_member_info failed group_id={} user_id={}",
                    group_id,
                    user_id,
                )

            final_name = resolved_name or nickname_hint or user_id
            self._group_member_cache[cache_key] = (
                final_name,
                time.monotonic() + float(self._config.group_member_cache_ttl_s),
            )
            return final_name

    async def resolve_private_name(
        self,
        user_id: str,
        nickname_hint: str | None,
    ) -> str:
        """解析私聊对象显示名。

        私聊名称来自 `get_friend_list`，回退顺序为：QQ 昵称 -> QQ 号码。

        """
        self_id = self._self_id_getter()
        if self_id and user_id == self_id:
            return self._self_name_getter() or user_id

        await self._refresh_friend_cache_if_needed()
        return self._friend_cache.get(user_id) or nickname_hint or user_id

    async def _refresh_friend_cache_if_needed(self) -> None:
        """在缓存过期时刷新好友列表。"""
        now = time.monotonic()
        if self._friend_cache and now < self._friend_cache_expires_at:
            return

        async with self._friend_lock:
            now = time.monotonic()
            if self._friend_cache and now < self._friend_cache_expires_at:
                return

            try:
                response = await self._transport.call_action("get_friend_list", {})
                data = response.get("data")
                if not isinstance(data, list):
                    logger.warning(
                        "NapCat [MEDIA] get_friend_list response malformed preview={!r}",
                        repr(response)[:160],
                    )
                    self._friend_cache_expires_at = now + float(self._config.friend_cache_ttl_s)
                    return

                refreshed: dict[str, str] = {}
                for item in data:
                    if not isinstance(item, dict):
                        continue
                    user_id = _coerce_optional_str(item.get("user_id"))
                    nickname = _coerce_optional_str(item.get("nickname"))
                    if user_id and nickname:
                        refreshed[user_id] = nickname
                self._friend_cache = refreshed
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning("NapCat [MEDIA] get_friend_list failed", exc_info=True)
            finally:
                self._friend_cache_expires_at = (
                    time.monotonic() + float(self._config.friend_cache_ttl_s)
                )


class Enricher:
    """入站消息补全器。"""

    def __init__(
        self,
        config: NapCatConfig,
        downloader: MediaDownloader,
        fetcher: NapCatMessageDetailFetcher,
        normalizer: NapCatNormalizer,
        directory: ContactDirectory,
    ):
        """初始化补全器。"""
        self._config = config
        self._downloader = downloader
        self._fetcher = fetcher
        self._normalizer = normalizer
        self._directory = directory

    async def enrich(self, inbound: NormalizedInbound) -> EnrichedInbound:
        """补全主消息、引用消息与转发消息。"""
        await self._directory.resolve_inbound_sender_name(inbound)

        main_task = asyncio.create_task(self._downloader.download_many(inbound.media_refs))
        reply_task = asyncio.create_task(self._expand_reply_media(inbound.reply_id))
        forward_tasks = [
            asyncio.create_task(self._expand_forward(ref, inbound.chat))
            for ref in inbound.forward_refs
        ]

        media_paths: list[str] = []
        reply_media_paths: list[str] = []
        forward_bundles: list[ForwardBundle] = []
        forward_media_paths: list[str] = []

        try:
            media_paths = await main_task
            reply_media_paths = await reply_task
            if forward_tasks:
                forward_results = await asyncio.gather(*forward_tasks)
                for bundle, bundle_media_paths in forward_results:
                    forward_bundles.append(bundle)
                    forward_media_paths.extend(bundle_media_paths)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("NapCat [MEDIA] enrich fanout failed")

        return EnrichedInbound(
            inbound=inbound,
            media_paths=_dedupe_keep_order(media_paths),
            reply_media_paths=_dedupe_keep_order(reply_media_paths),
            forward_bundles=forward_bundles,
            forward_media_paths=_dedupe_keep_order(forward_media_paths),
        )

    async def _expand_reply_media(self, reply_id: str | None) -> list[str]:
        """展开 reply 消息中的媒体。"""
        if not reply_id:
            return []

        try:
            payload = await self._fetcher.get_msg(reply_id)
            if payload is None:
                return []
            normalized = self._normalizer.normalize_message_object(payload)
            if normalized is None:
                return []
            return await self._downloader.download_many(normalized.media_refs)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("NapCat [MEDIA] expand reply failed reply_id={}", reply_id)
            return []

    async def _expand_forward(
        self,
        forward_ref: ForwardRef,
        parent_chat: ChatRef,
    ) -> tuple[ForwardBundle, list[str]]:
        """展开单个 forward 引用。"""
        bundle = ForwardBundle(summary=forward_ref.summary)
        media_paths: list[str] = []

        try:
            if forward_ref.embedded_nodes:
                payloads = [item for item in forward_ref.embedded_nodes if isinstance(item, dict)]
            elif forward_ref.forward_id:
                payloads = await self._fetcher.get_forward_msg(forward_ref.forward_id)
            else:
                payloads = []

            for payload in payloads:
                normalized = self._normalize_forward_node(payload)
                if normalized is None:
                    continue
                await self._directory.resolve_inbound_sender_name(normalized)
                item_media_paths = await self._downloader.download_many(normalized.media_refs)
                media_paths.extend(item_media_paths)
                if normalized.forward_refs:
                    logger.debug(
                        "NapCat [MEDIA] nested forward left as placeholder message_id={}",
                        normalized.message_id,
                    )
                bundle.nodes.append(
                    ForwardNode(
                        message_id=normalized.message_id,
                        sender_id=normalized.sender_id,
                        sender_name=normalized.sender_name,
                        body_tokens=normalized.body_tokens,
                        source_chat=self._resolve_forward_source_chat(normalized, parent_chat),
                        route=normalized.chat.route,
                        group_id=normalized.chat.group_id,
                        sender_card=normalized.sender_card,
                        sender_nickname=normalized.sender_nickname,
                    )
                )
            return bundle, _dedupe_keep_order(media_paths)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "NapCat [MEDIA] expand forward failed forward_id={}",
                forward_ref.forward_id,
            )
            return bundle, []

    def _normalize_forward_node(
        self,
        payload: dict[str, Any],
    ) -> NormalizedInbound | None:
        """标准化转发节点。"""
        if "message_type" in payload:
            return self._normalizer.normalize_message_object(payload)

        node_type = _coerce_str(payload.get("type")).lower()
        if node_type != "node":
            return None

        data = _ensure_dict(payload.get("data"))
        content = data.get("content")
        if not isinstance(content, list):
            return None

        user_id = _coerce_str(data.get("user_id") or "forward")
        nickname = _coerce_optional_str(data.get("nickname")) or "转发节点"
        group_id = _coerce_optional_str(data.get("group_id"))
        message_type: Literal["private", "group"] = "group" if group_id else "private"

        fake_payload: dict[str, Any] = {
            "post_type": "message",
            "message_type": message_type,
            "sub_type": "forward",
            "message_id": _coerce_str(data.get("id") or uuid.uuid4().hex),
            "time": int(time.time()),
            "user_id": user_id,
            "sender": {
                "user_id": user_id,
                "nickname": nickname,
                "card": _coerce_optional_str(data.get("card")),
            },
            "message": content,
            "raw_message": "",
        }
        if group_id:
            fake_payload["group_id"] = group_id
        return self._normalizer.normalize_message_object(fake_payload)

    @staticmethod
    def _resolve_forward_source_chat(
        normalized: NormalizedInbound,
        parent_chat: ChatRef,
    ) -> str | None:
        """解析私聊上下文中 forward 节点的来源标识。"""
        if parent_chat.route != "private":
            return None
        if normalized.chat.route == "group" and normalized.chat.group_id:
            return f"g:{normalized.chat.group_id}"
        return None


# ==============================
# 7) Policy
# ==============================


def extract_session_key(payload: dict[str, Any], self_id: str) -> str | None:
    """从原始载荷提取 session_key。"""
    del self_id
    post_type = _coerce_str(payload.get("post_type")).strip().lower()
    if post_type == "notice":
        group_id = _coerce_optional_str(payload.get("group_id"))
        user_id = (
            _coerce_optional_str(payload.get("user_id"))
            or _coerce_optional_str(payload.get("sender_id"))
        )
        if group_id and user_id:
            return f"napcat:{_CHAT_ID_GROUP_PREFIX}{group_id}"
        if user_id:
            return f"napcat:{_CHAT_ID_PRIVATE_PREFIX}{user_id}"
        return None

    message_type = _coerce_str(payload.get("message_type")).strip().lower()
    user_id = _coerce_optional_str(payload.get("user_id"))
    if message_type == "group":
        group_id = _coerce_optional_str(payload.get("group_id"))
        if group_id and user_id:
            return f"napcat:{_CHAT_ID_GROUP_PREFIX}{group_id}"
        return None

    if message_type == "private" and user_id:
        sub_type = _coerce_str(payload.get("sub_type")).strip().lower()
        target_id = _coerce_optional_str(payload.get("target_id"))
        if sub_type == "group" and target_id:
            return f"napcat:pt:{target_id}:{user_id}"
        return f"napcat:{_CHAT_ID_PRIVATE_PREFIX}{user_id}"

    return None


@dataclass(slots=True, frozen=True)
class _EventCtx:
    """来源判断所需的最小不可变事件上下文。"""

    post_type: str
    message_type: str
    user_id: str
    source_group_id: str | None

    @property
    def has_group(self) -> bool:
        """当前事件是否存在群来源。"""
        return self.source_group_id is not None


def _resolve_source_group_id(
    payload: dict[str, Any],
    post_type: str,
    message_type: str,
) -> str | None:
    """群消息、群通知取 `group_id`；群临时私聊取 `target_id`。"""
    if post_type == "notice" or message_type == "group":
        return _coerce_optional_str(payload.get("group_id"))
    if message_type == "private":
        sub_type = _coerce_str(payload.get("sub_type")).strip().lower()
        if sub_type == "group":
            return _coerce_optional_str(payload.get("target_id"))
    return None


def _build_event_ctx(payload: dict[str, Any]) -> _EventCtx:
    """将原始载荷解析为来源判断所需的上下文。"""
    post_type = _coerce_str(payload.get("post_type")).strip().lower()
    message_type = _coerce_str(payload.get("message_type")).strip().lower()
    user_id = (
        _coerce_optional_str(payload.get("user_id"))
        or _coerce_optional_str(payload.get("sender_id"))
        or ""
    )
    return _EventCtx(
        post_type=post_type,
        message_type=message_type,
        user_id=user_id,
        source_group_id=_resolve_source_group_id(payload, post_type, message_type),
    )


def _is_blacklisted(
    ctx: _EventCtx,
    blacklist_group_ids: frozenset[str],
    blacklist_private_ids: frozenset[str],
) -> bool:
    """群来源查群黑名单，纯私聊查私聊黑名单。"""
    if ctx.has_group:
        return bool(ctx.source_group_id and ctx.source_group_id in blacklist_group_ids)
    return bool(ctx.user_id) and ctx.user_id in blacklist_private_ids


def _is_allowed(
    ctx: _EventCtx,
    allow_ids: frozenset[str],
) -> bool:
    """按来源会话检查 `allow_from`。"""
    if "*" in allow_ids:
        return True
    if ctx.has_group:
        return bool(ctx.source_group_id and ctx.source_group_id in allow_ids)
    return ctx.user_id in allow_ids


def _check_notice(
    payload: dict[str, Any],
    ctx: _EventCtx,
    allow_ids: frozenset[str],
    self_id: str,
) -> bool:
    """通知事件只放行发给自己的 poke，随后再过 allow_from。"""
    notice_type = _coerce_str(payload.get("notice_type")).strip().lower()
    sub_type = _coerce_str(payload.get("sub_type")).strip().lower()
    is_poke = (notice_type == "notify" and sub_type == "poke") or notice_type == "poke"
    if not is_poke:
        return False

    target_id = _coerce_optional_str(payload.get("target_id"))
    if self_id and target_id and target_id != self_id:
        return False

    return _is_allowed(ctx, allow_ids)


def check_source_gate(
    payload: dict[str, Any],
    config: NapCatConfig,
    self_id: str,
) -> bool:
    """检查事件来源是否允许进入后续处理。

    编排顺序：
    1. 先把 payload 解析成不可变上下文。
    2. 黑名单优先，命中即拒。
    3. 再处理 `message_sent` / 自身消息过滤。
    4. notice 只放行发给自己的 poke，并继续走 allow_from。
    5. 普通消息最后按来源会话检查 allow_from。

    授权语义：
    - 群消息、群通知、群临时私聊都按“来源群 ID”授权。
    - 普通私聊按“来源用户 ID”授权。
    - `allow_from` 不能覆盖黑名单。
    """
    ctx = _build_event_ctx(payload)

    allow_ids = _normalize_id_list(config.allow_from)
    blacklist_private_ids = _normalize_id_list(config.blacklist_private_ids)
    blacklist_group_ids = _normalize_id_list(config.blacklist_group_ids)

    if _is_blacklisted(ctx, blacklist_group_ids, blacklist_private_ids):
        return False

    if ctx.post_type == "message_sent":
        return not config.ignore_self_messages

    if config.ignore_self_messages and self_id and ctx.user_id == self_id:
        return False

    if ctx.post_type == "notice":
        return _check_notice(payload, ctx, allow_ids, self_id)

    if ctx.message_type not in {"group", "private"}:
        return False
    if ctx.message_type == "group" and not ctx.has_group:
        return False

    return _is_allowed(ctx, allow_ids)



def decide_notice_trigger(
    enriched: EnrichedInbound,
    config: NapCatConfig,
    last_poke_monotonic: dict[str, float],
    now_monotonic: float,
    self_id: str,
) -> TriggerDecision:
    """判断通知类事件是否触发回复。"""
    inbound = enriched.inbound
    if inbound.notice_name != "poke":
        return TriggerDecision(False, "unsupported_notice")

    if not config.trigger_on_poke:
        return TriggerDecision(False, "poke_disabled")

    if self_id and inbound.target_id and inbound.target_id != self_id:
        return TriggerDecision(False, "poke_not_for_self")

    session_key = inbound.chat.session_key
    previous = last_poke_monotonic.get(session_key, 0.0)
    cooldown = float(config.poke_cooldown_seconds)
    delta = now_monotonic - previous
    if delta < cooldown:
        return TriggerDecision(
            False,
            "poke_cooldown",
            detail=f"delta:{delta:.1f}s<{cooldown:g}s",
        )

    last_poke_monotonic[session_key] = now_monotonic
    detail = f"target:{inbound.target_id}" if inbound.target_id else None
    return TriggerDecision(True, "poke", detail=detail)



def check_trigger(
    enriched: EnrichedInbound,
    state: "SessionBufferState",
    config: NapCatConfig,
    now_monotonic: float,
    last_poke_monotonic: dict[str, float],
    self_id: str,
) -> TriggerDecision:
    """检查当前消息是否应触发上层 Agent。"""
    inbound = enriched.inbound
    text = (inbound.text or "").strip()

    if _is_exact_supported_command(text):
        return TriggerDecision(True, "slash_command")

    if inbound.is_notice:
        return decide_notice_trigger(
            enriched=enriched,
            config=config,
            last_poke_monotonic=last_poke_monotonic,
            now_monotonic=now_monotonic,
            self_id=self_id,
        )

    if config.trigger_on_at and inbound.at_self:
        return TriggerDecision(True, "at_self", detail="mention:bot")

    if (
        config.trigger_on_reply_to_bot
        and inbound.reply_id
        and state.is_bot_message(inbound.reply_id)
    ):
        return TriggerDecision(True, "reply_to_bot", detail=f"reply_id:{inbound.reply_id}")

    nickname_trigger = _find_nickname_trigger(text, config.nickname_triggers)
    if nickname_trigger is not None:
        return TriggerDecision(
            True,
            "nickname_trigger",
            detail=f"keyword:{nickname_trigger}",
        )

    probability = (
        config.private_trigger_prob
        if inbound.chat.route == "private"
        else config.group_trigger_prob
    )
    sample = _stable_probability_sample(
        session_key=state.chat.session_key,
        message_id=inbound.message_id,
        sender_id=inbound.sender_id,
        message_time=inbound.time,
    )
    if sample < probability:
        return TriggerDecision(
            True,
            "probability",
            detail=f"sample:{sample:.3f}<{probability:.3f}",
        )

    return TriggerDecision(
        False,
        "probability",
        detail=f"sample:{sample:.3f}>={probability:.3f}",
    )


# ==============================
# 8) SessionBuffer
# ==============================


@dataclass(slots=True)
class SessionBufferState:
    """单个 session 的缓冲状态。"""

    chat: ChatRef
    buffer_size: int
    records: list[ContextMessageRecord] = field(default_factory=list)
    last_sent_cursor: int = 0
    bot_message_ids: deque[str] = field(default_factory=deque)
    message_ids: deque[str] = field(default_factory=deque)
    _message_id_set: set[str] = field(default_factory=set)
    _bot_message_id_set: set[str] = field(default_factory=set)

    def contains_message_id(self, message_id: str) -> bool:
        """判断消息是否已存在于缓冲。"""
        return message_id in self._message_id_set

    def is_bot_message(self, message_id: str) -> bool:
        """判断某条消息是否为机器人自己发出的消息。"""
        return message_id in self._bot_message_id_set

    def append(self, record: ContextMessageRecord) -> None:
        """追加一条上下文记录。"""
        if self.contains_message_id(record.message_id):
            return

        self.records.append(record)
        self.message_ids.append(record.message_id)
        self._message_id_set.add(record.message_id)
        if record.role == "assistant" and record.message_id:
            self._register_bot_message_id(record.message_id)
        self._trim_if_needed()

    def get_unsent_records(self) -> list[ContextMessageRecord]:
        """返回严格未发送的记录窗口。"""
        if not self.records:
            return []
        cursor = min(max(self.last_sent_cursor, 0), len(self.records))
        return self.records[cursor:]

    def mark_sent_to_end(self) -> None:
        """将游标推进到当前缓冲末尾。"""
        self.last_sent_cursor = len(self.records)

    def record_bot_message(
        self,
        message_id: str,
        text: str,
        sender_id: str,
        sender_name: str,
    ) -> None:
        """记录机器人消息，以支持 reply_to_bot 判断。"""
        if not message_id:
            return

        record = ContextMessageRecord(
            message_id=message_id,
            role="assistant",
            sender_id=sender_id or "bot",
            sender_name=sender_name or sender_id or "bot",
            text=text,
            raw_message=text,
            time=datetime.now(timezone.utc),
            body_tokens=[BodyToken(kind="text", text=text)] if text else [],
        )
        self.append(record)
        # 机器人自己的输出已经被上游 Agent 看到，不应再次作为“未发送窗口”。
        self.mark_sent_to_end()

    def clear(self) -> None:
        """清空当前 session 的全部缓冲状态。"""
        self.records.clear()
        self.last_sent_cursor = 0
        self.bot_message_ids.clear()
        self.message_ids.clear()
        self._message_id_set.clear()
        self._bot_message_id_set.clear()

    def _register_bot_message_id(self, message_id: str) -> None:
        """记录一条机器人消息 ID。"""
        if not message_id or message_id in self._bot_message_id_set:
            return
        self.bot_message_ids.append(message_id)
        self._bot_message_id_set.add(message_id)

    def _trim_if_needed(self) -> None:
        """按 session_buffer_size 修剪缓冲。"""
        while len(self.records) > self.buffer_size:
            removed = self.records.pop(0)
            if self.message_ids:
                removed_id = self.message_ids.popleft()
                self._message_id_set.discard(removed_id)
                if removed_id in self._bot_message_id_set:
                    self._bot_message_id_set.discard(removed_id)
                    with contextlib.suppress(ValueError):
                        self.bot_message_ids.remove(removed_id)
            if self.last_sent_cursor > 0:
                self.last_sent_cursor -= 1
            # logger.info(
            #     "NapCat [SESSION] buffer trimmed session={} removed_id={}",
            #     self.chat.session_key,
            #     removed.message_id,
            # )


class SessionBufferStore:
    """会话缓冲存储。"""

    def __init__(self, config: NapCatConfig):
        """初始化缓冲存储。"""
        self._config = config
        self._states: dict[str, SessionBufferState] = {}

    def get_or_create(self, chat: ChatRef) -> SessionBufferState:
        """获取或创建某个 session 的状态。"""
        state = self._states.get(chat.session_key)
        if state is not None:
            return state

        state = SessionBufferState(
            chat=chat,
            buffer_size=self._config.session_buffer_size,
        )
        self._states[chat.session_key] = state
        return state

    def get(self, session_key: str) -> SessionBufferState | None:
        """按 session_key 获取状态。"""
        return self._states.get(session_key)

    def list_states(self) -> list[SessionBufferState]:
        """列出全部 session 状态。"""
        return list(self._states.values())

    def clear(self, session_key: str) -> None:
        """清空指定 session 的缓冲。"""
        state = self._states.get(session_key)
        if state is not None:
            state.clear()

    def append_record(self, chat: ChatRef, record: ContextMessageRecord) -> SessionBufferState:
        """向某个 session 追加消息记录。"""
        state = self.get_or_create(chat)
        state.append(record)
        return state

    def record_bot_message(
        self,
        chat_id: str,
        message_id: str,
        text: str,
        sender_id: str,
        sender_name: str,
        session_key: str | None = None,
    ) -> None:
        """记录机器人发出的消息。"""
        state = self._states.get(session_key) if session_key else None
        if state is None:
            state = self._find_by_chat_id(chat_id)
        if state is None:
            route, route_id = _parse_internal_chat_id(chat_id)
            if route == "group":
                chat = ChatRef(
                    route="group",
                    chat_id=chat_id,
                    session_key=f"napcat:{chat_id}",
                    group_id=route_id,
                )
            else:
                chat = ChatRef(
                    route="private",
                    chat_id=chat_id,
                    session_key=f"napcat:{chat_id}",
                    user_id=route_id,
                )
            state = self.get_or_create(chat)

        state.record_bot_message(
            message_id=message_id,
            text=text,
            sender_id=sender_id,
            sender_name=sender_name,
        )

    def contains_message_id(self, session_key: str, message_id: str) -> bool:
        """判断消息 ID 是否已存在于指定 session。"""
        state = self._states.get(session_key)
        if state is None:
            return False
        return state.contains_message_id(message_id)

    def _find_by_chat_id(self, chat_id: str) -> SessionBufferState | None:
        """通过 chat_id 查找 session 状态。"""
        for state in self._states.values():
            if state.chat.chat_id == chat_id:
                return state
        return None

    def render_debug_table(self) -> str:
        """渲染当前 session 缓冲的简表，便于运行时排查。"""
        states = sorted(self._states.values(), key=lambda item: item.chat.session_key)
        if not states:
            return "(empty)"

        headers = (
            "session_key",
            "route",
            "records",
            "unsent",
            "cursor",
            "bot_ids",
            "last_message_id",
        )
        rows: list[tuple[str, ...]] = []
        for state in states:
            last_message_id = state.records[-1].message_id if state.records else ""
            rows.append(
                (
                    state.chat.session_key,
                    state.chat.route,
                    str(len(state.records)),
                    str(len(state.get_unsent_records())),
                    str(state.last_sent_cursor),
                    str(len(state.bot_message_ids)),
                    last_message_id,
                )
            )

        widths = [
            max(len(header), *(len(row[index]) for row in rows))
            for index, header in enumerate(headers)
        ]

        def _format_row(values: tuple[str, ...]) -> str:
            return " | ".join(
                value.ljust(widths[index])
                for index, value in enumerate(values)
            )

        divider = "-+-".join("-" * width for width in widths)
        return "\n".join(
            [
                _format_row(headers),
                divider,
                *(_format_row(row) for row in rows),
            ]
        )


# ==============================
# 9) Context — CQMsgSerializer
# ==============================


@dataclass(slots=True)
class _UserRow:
    """CQMSG/1 内部用户表行。"""

    uid: str
    qq: str
    name: str
    is_bot: bool = False


@dataclass(slots=True)
class _ImageRow:
    """CQMSG/1 内部图片表行。"""

    iid: str
    filename: str


class _CQMsgRenderState:
    """CQMSG/1 渲染状态。"""

    def __init__(
        self,
        chat: ChatRef,
        self_id: str,
        self_name: str,
        peer_name: str,
        count: int,
    ):
        """初始化渲染状态。"""
        self._chat = chat
        self._is_group = chat.route == "group"
        self._self_id = self_id or "0"
        self._self_name = self_name or self._self_id or "bot"
        self._peer_id = chat.user_id or ""
        self._peer_name = peer_name or self._peer_id or "peer"
        self._count = count

        self._user_rows: list[_UserRow] = []
        self._uid_by_qq: dict[str, str] = {}
        self._image_rows: list[_ImageRow] = []
        self._iid_by_filename: dict[str, str] = {}
        self._user_seq = 0
        self._forward_seq = 0

        if self._is_group:
            self.bot_uid = self.get_uid(
                qq=self._self_id,
                name=self._self_name,
                is_bot=True,
            )
        else:
            self.bot_uid = "me"
            self._register_reserved_user(
                uid="me",
                qq=self._self_id,
                name=self._self_name,
                is_bot=True,
            )
            self._register_reserved_user(
                uid="peer",
                qq=self._peer_id or "0",
                name=self._peer_name,
                is_bot=False,
            )

    def get_uid(self, qq: str, name: str, is_bot: bool = False) -> str:
        """获取或分配用户 UID。"""
        qq = qq or "0"
        better_name = name or qq

        if not self._is_group:
            if qq == self._self_id:
                self._refresh_user_name("me", better_name, True)
                return "me"
            if self._peer_id and qq == self._peer_id:
                self._refresh_user_name("peer", better_name, False)
                return "peer"

        existing_uid = self._uid_by_qq.get(qq)
        if existing_uid is not None:
            self._refresh_user_name(existing_uid, better_name, is_bot)
            return existing_uid

        uid = f"u{self._user_seq}"
        self._user_seq += 1
        self._uid_by_qq[qq] = uid
        self._user_rows.append(
            _UserRow(
                uid=uid,
                qq=qq,
                name=better_name,
                is_bot=is_bot,
            )
        )
        return uid

    def get_iid(self, filename: str) -> str:
        """获取或分配图片 IID。"""
        safe_filename = _basename_only(filename, "image")
        existing = self._iid_by_filename.get(safe_filename)
        if existing is not None:
            return existing
        iid = f"i{len(self._image_rows)}"
        self._iid_by_filename[safe_filename] = iid
        self._image_rows.append(_ImageRow(iid=iid, filename=safe_filename))
        return iid

    def next_fid(self) -> str:
        """分配新的转发容器 ID。"""
        fid = f"f{self._forward_seq}"
        self._forward_seq += 1
        return fid

    def render(self, records: list[ContextMessageRecord]) -> str:
        """渲染完整 CQMSG/1 文本。"""
        message_lines: list[str] = []
        for record in records:
            message_lines.extend(self._render_record(record))

        header = self._render_header()
        lines: list[str] = [header]
        lines.extend(self._render_user_rows())
        lines.extend(self._render_image_rows())
        lines.extend(message_lines)
        lines.append(f"</{_CQMSG_VERSION}>")
        return "\n".join(lines)

    def _render_header(self) -> str:
        """渲染 Header 行。"""
        parts = [f"<{_CQMSG_VERSION}"]
        if self._is_group and self._chat.group_id:
            parts.append(f"g:{self._chat.group_id}")
        parts.append(f"bot:{self.bot_uid}")
        parts.append(f"n:{self._count}")
        return " ".join(parts) + ">"

    def _render_user_rows(self) -> list[str]:
        """渲染用户表。"""
        lines: list[str] = []
        for row in self._user_rows:
            line = f"U|{row.uid}|{row.qq}|{_cqctx_escape(row.name or row.qq)}"
            if row.is_bot:
                line += "|bot"
            lines.append(line)
        return lines

    def _render_image_rows(self) -> list[str]:
        """渲染图片表。"""
        return [
            f"I|{row.iid}|{_cqctx_escape(row.filename)}"
            for row in self._image_rows
        ]

    def _render_record(self, record: ContextMessageRecord) -> list[str]:
        """渲染单条消息记录。"""
        sender_uid = self.get_uid(
            qq=record.sender_id,
            name=record.sender_name,
            is_bot=(record.role == "assistant" and record.sender_id == self._self_id),
        )
        body, forward_defs = self._render_body_with_forwards(
            body_tokens=record.body_tokens,
            forward_bundles=record.forward_bundles,
        )
        lines = [
            f"M|{_cqctx_escape(record.message_id)}|{sender_uid}|{body or '[empty]'}"
        ]
        for fid, bundle in forward_defs:
            header = f"F|{fid}|{len(bundle.nodes)}"
            if bundle.summary:
                header += f"|{_cqctx_escape(bundle.summary)}"
            lines.append(header)

            node_ref_map = {
                node.message_id: f"{fid}.{idx}"
                for idx, node in enumerate(bundle.nodes)
                if node.message_id
            }
            for idx, node in enumerate(bundle.nodes):
                lines.append(
                    self._render_forward_node(
                        fid=fid,
                        index=idx,
                        node=node,
                        node_ref_map=node_ref_map,
                    )
                )
        return lines

    def _render_forward_node(
        self,
        fid: str,
        index: int,
        node: ForwardNode,
        node_ref_map: dict[str, str],
    ) -> str:
        """渲染单个 forward 节点。"""
        sender_uid = self.get_uid(
            qq=node.sender_id,
            name=node.sender_name,
            is_bot=(node.sender_id == self._self_id),
        )
        body_chunks: list[str] = []
        for token in node.body_tokens:
            chunk = self._render_token(
                token=token,
                inside_forward=True,
                node_ref_map=node_ref_map,
            )
            if chunk:
                body_chunks.append(chunk)
        body = _body_join(body_chunks) or "[empty]"

        if self._is_group:
            return f"N|{fid}.{index}|{sender_uid}|{body}"

        src = _cqctx_escape(node.source_chat or "")
        return f"N|{fid}.{index}|{sender_uid}|{src}|{body}"

    def _render_body_with_forwards(
        self,
        body_tokens: list[BodyToken],
        forward_bundles: list[ForwardBundle],
    ) -> tuple[str, list[tuple[str, ForwardBundle]]]:
        """渲染消息体，并展开 forward 占位。"""
        chunks: list[str] = []
        rendered_forwards: list[tuple[str, ForwardBundle]] = []
        forward_iter = iter(forward_bundles)

        for token in body_tokens:
            if token.kind == "forward":
                fid = self.next_fid()
                bundle = next(forward_iter, ForwardBundle())
                rendered_forwards.append((fid, bundle))
                chunks.append(f"[F:{fid}]")
                continue
            chunk = self._render_token(
                token=token,
                inside_forward=False,
                node_ref_map=None,
            )
            if chunk:
                chunks.append(chunk)

        return _body_join(chunks), rendered_forwards

    def _render_token(
        self,
        token: BodyToken,
        inside_forward: bool,
        node_ref_map: dict[str, str] | None,
    ) -> str | None:
        """渲染单个 BodyToken。"""
        if token.kind == "text":
            if not token.text:
                return None
            return _cqctx_escape(token.text)

        if token.kind == "at":
            target_id = token.target_id or ""
            if not target_id:
                return None
            if target_id == "all":
                return "@all"
            target_uid = self.get_uid(qq=target_id, name=target_id)
            return f"@{target_uid}"

        if token.kind == "reply":
            if not token.message_id:
                return None
            if inside_forward:
                if node_ref_map and token.message_id in node_ref_map:
                    return f">n:{node_ref_map[token.message_id]}"
                return None
            return f">m:{_cqctx_escape(token.message_id)}"

        if token.kind == "image":
            iid = self.get_iid(token.filename or "image")
            return f"[{iid}]"

        if token.kind == "record":
            return "[record]"

        if token.kind == "video":
            return "[video]"

        if token.kind == "file":
            return f"[file:{_cqctx_escape(token.filename or 'file')}]"

        if token.kind == "face":
            return f"[face:{_cqctx_escape(token.text or 'unknown')}]"

        if token.kind == "json":
            return "[json]"

        if token.kind == "xml":
            return "[xml]"

        if token.kind == "forward":
            return "[forward]"

        if token.kind == "unknown":
            return f"[seg:{_cqctx_escape(token.text or 'unknown')}]"

        return None

    def _register_reserved_user(
        self,
        uid: str,
        qq: str,
        name: str,
        is_bot: bool,
    ) -> None:
        """注册私聊模式下的保留用户。"""
        self._uid_by_qq[qq] = uid
        self._user_rows.append(
            _UserRow(
                uid=uid,
                qq=qq,
                name=name or qq,
                is_bot=is_bot,
            )
        )

    def _refresh_user_name(self, uid: str, better_name: str, is_bot: bool) -> None:
        """在拿到更好名称时刷新已有 U 行。"""
        for row in self._user_rows:
            if row.uid != uid:
                continue
            if row.name == row.qq and better_name and better_name != row.qq:
                row.name = better_name
            if is_bot:
                row.is_bot = True
            return


class CQMsgSerializer:
    """将 SessionBuffer 序列化为 CQMSG/1 文本。"""

    def __init__(
        self,
        config: NapCatConfig,
        self_id_getter: Callable[[], str],
        self_name_getter: Callable[[], str],
    ):
        """初始化序列化器。"""
        self._config = config
        self._self_id_getter = self_id_getter
        self._self_name_getter = self_name_getter

    def build(self, state: SessionBufferState) -> str:
        """构建 CQMSG/1 文本。"""
        records = state.get_unsent_records()
        if not records:
            return ""

        if len(records) > self._config.context_max_messages:
            records = records[-self._config.context_max_messages:]

        peer_name = self._guess_peer_name(state)
        renderer = _CQMsgRenderState(
            chat=state.chat,
            self_id=self._self_id_getter(),
            self_name=self._self_name_getter(),
            peer_name=peer_name,
            count=len(records),
        )
        return renderer.render(records)

    @staticmethod
    def _guess_peer_name(state: SessionBufferState) -> str:
        """为私聊 Header 预估 peer 名称。"""
        peer_id = state.chat.user_id or ""
        for record in reversed(state.records):
            if record.sender_id == peer_id and record.sender_name:
                return record.sender_name
        return peer_id


# ==============================
# 10) Outbound
# ==============================


class OutboundSender:
    """NapCat 出站发送器。"""

    def __init__(
        self,
        transport: NapCatTransport,
        session_store: SessionBufferStore,
        self_id_getter: Callable[[], str],
        self_name_getter: Callable[[], str],
    ):
        """初始化出站发送器。"""
        self._transport = transport
        self._session_store = session_store
        self._self_id_getter = self_id_getter
        self._self_name_getter = self_name_getter

    async def send(self, msg: OutboundMessage) -> None:
        """发送一条 nanobot 出站消息。"""
        if self._should_suppress_outbound(msg):
            logger.debug("NapCat [SEND] suppressed empty reply chat_id={}", msg.chat_id)
            return
        if self._should_suppress_llm_connection_error(msg):
            logger.warning(
                "NapCat [SEND] suppressed LLM connection error chat_id={} preview={!r}",
                msg.chat_id,
                _coerce_str(msg.content).strip()[:160],
            )
            return

        try:
            route, target_id = _parse_internal_chat_id(msg.chat_id)
        except ValueError:
            logger.error("NapCat [SEND] invalid chat_id={}", msg.chat_id)
            return

        is_progress = bool(msg.metadata.get("_progress"))
        content = self._compose_outbound_content(
            content=msg.content,
            media=msg.media,
        )

        params: dict[str, Any]
        action: str
        if route == "group":
            action = "send_group_msg"
            params = {
                "group_id": target_id,
                "message": content,
            }
        else:
            action = "send_private_msg"
            params = {
                "user_id": target_id,
                "message": content,
            }

        response = await self._transport.call_action(action, params)
        message_id = self._extract_message_id(response)
        logger.info(
            "NapCat [SEND] ok chat_id={} out_msg_id={} content_len={}",
            msg.chat_id,
            message_id or "?",
            len(_coerce_str(msg.content)),
        )
        if message_id and not is_progress:
            self._session_store.record_bot_message(
                chat_id=msg.chat_id,
                message_id=message_id,
                text=msg.content,
                sender_id=self._self_id_getter() or "bot",
                sender_name=self._self_name_getter() or self._self_id_getter() or "bot",
                session_key=_coerce_optional_str(msg.metadata.get("napcat_session_key")),
            )

    @staticmethod
    def _should_suppress_outbound(msg: OutboundMessage) -> bool:
        """拦截 agent 空回复在 NapCat 侧的兜底发送。"""
        if msg.media:
            return False

        content = _coerce_str(msg.content).strip()
        return content in {"", _AGENT_EMPTY_RESPONSE_FALLBACK}

    @staticmethod
    def _should_suppress_llm_connection_error(msg: OutboundMessage) -> bool:
        """拦截底层 LLM 连接错误，避免将内部异常直接发到聊天侧。"""
        if msg.media:
            return False

        content = _coerce_str(msg.content).strip()
        if not content.startswith("Error calling LLM:"):
            return False

        lowered = content.lower()
        return "connection error" in lowered

    def _compose_outbound_content(
        self,
        content: str,
        media: list[str],
    ) -> str:
        """构造最终发给 NapCat 的消息内容。"""
        pieces: list[str] = []
        if content:
            pieces.append(content)

        for media_path in media:
            pieces.append(self._build_media_cq_code(media_path))

        merged = "".join(pieces).strip()
        return merged or "[empty message]"

    def _build_media_cq_code(self, media_path: str) -> str:
        """将本地媒体路径转换为 CQ 码。"""
        path = Path(media_path).expanduser()
        abs_path = path.resolve()
        cq_type = _guess_media_cq_type(str(abs_path))
        file_uri = abs_path.as_uri()
        filename = abs_path.name
        return f"[CQ:{cq_type},file={file_uri},name={filename}]"

    @staticmethod
    def _extract_message_id(response: dict[str, Any]) -> str | None:
        """从发送响应中提取 message_id。"""
        data = response.get("data")
        if isinstance(data, dict):
            return _coerce_optional_str(data.get("message_id"))
        return None


# ==============================
# 11) Pipeline & Channel
# ==============================


def _record_from_enriched(enriched: EnrichedInbound) -> ContextMessageRecord:
    """由补全后的入站消息构造上下文记录。"""
    inbound = enriched.inbound
    return ContextMessageRecord(
        message_id=inbound.message_id,
        role="user",
        sender_id=inbound.sender_id,
        sender_name=inbound.sender_name,
        text=inbound.text,
        raw_message=inbound.raw_message,
        time=inbound.time,
        body_tokens=inbound.body_tokens,
        at_self=inbound.at_self,
        reply_id=inbound.reply_id,
        forward_bundles=enriched.forward_bundles,
        media_paths=enriched.all_media_paths,
        is_notice=inbound.is_notice,
        notice_name=inbound.notice_name,
    )


class EventPipeline:
    """NapCat 事件处理编排器。"""

    def __init__(
        self,
        channel: "NapCatChannel",
        config: NapCatConfig,
        normalizer: NapCatNormalizer,
        enricher: Enricher,
        session_store: SessionBufferStore,
        serializer: CQMsgSerializer,
        self_id_getter: Callable[[], str],
    ):
        """初始化事件处理流水线。"""
        self._channel = channel
        self._config = config
        self._normalizer = normalizer
        self._enricher = enricher
        self._session_store = session_store
        self._serializer = serializer
        self._self_id_getter = self_id_getter
        self._last_poke_monotonic: dict[str, float] = {}

    async def handle(self, payload: dict[str, Any]) -> None:
        """处理单个 NapCat 原始事件。"""
        normalized = self._normalizer.normalize_event(payload)
        if normalized is None:
            return

        command = normalized.text.strip().lower()
        if _is_exact_supported_command(command):
            if normalized.sender_id not in _normalize_id_list(self._config.super_admins):
                logger.warning(
                    "NapCat [ROUTE] command rejected non-admin sender_id={} cmd={}",
                    normalized.sender_id,
                    command,
                )
                return
            await self._publish_command(normalized)
            if command == "/new":
                logger.info(
                    "NapCat [SESSION] cleared by /new session={}",
                    normalized.chat.session_key,
                )
                self._session_store.clear(normalized.chat.session_key)
            return

        state = self._session_store.get_or_create(normalized.chat)
        enriched = await self._enricher.enrich(normalized)
        decision = check_trigger(
            enriched=enriched,
            state=state,
            config=self._config,
            now_monotonic=time.monotonic(),
            last_poke_monotonic=self._last_poke_monotonic,
            self_id=self._self_id_getter(),
        )

        state = self._session_store.append_record(
            normalized.chat,
            _record_from_enriched(enriched),
        )

        if not decision.should_reply:
            logger.info(
                "NapCat [TRIGGER] passive record reason={} session={} msg_id={} detail={}",
                decision.reason,
                normalized.chat.session_key,
                normalized.message_id,
                _format_trigger_detail(decision),
            )
            return

        content = normalized.text.strip()
        if not _is_exact_supported_command(content):
            content = self._serializer.build(state)

        logger.info(
            "NapCat [TRIGGER] dispatch reason={} session={} msg_id={} detail={}",
            decision.reason,
            normalized.chat.session_key,
            normalized.message_id,
            _format_trigger_detail(decision),
        )

        metadata = {
            "message_id": normalized.message_id,
            "napcat_route": normalized.chat.route,
            "napcat_session_key": normalized.chat.session_key,
            "napcat_user_id": normalized.chat.user_id,
            "napcat_group_id": normalized.chat.group_id,
            "napcat_sub_type": normalized.chat.sub_type,
            "napcat_trigger_reason": decision.reason,
        }

        session_key_override: str | None = None
        default_session_key = f"{self._channel.name}:{normalized.chat.chat_id}"
        if normalized.chat.session_key != default_session_key:
            session_key_override = normalized.chat.session_key

        await self._channel._publish_inbound_message(
            sender_id=normalized.sender_id,
            chat_id=normalized.chat.chat_id,
            content=content,
            media=enriched.all_media_paths,
            metadata=metadata,
            session_key=session_key_override,
        )
        state.mark_sent_to_end()

    async def _publish_command(self, normalized: NormalizedInbound) -> None:
        """直接向上游发布原生命令，不经过 CQMSG 封装。"""
        if normalized.text.strip().lower() == "/status":
            logger.info(
                "NapCat [SESSION] cache table\n{}",
                self._session_store.render_debug_table(),
            )

        metadata = {
            "message_id": normalized.message_id,
            "napcat_route": normalized.chat.route,
            "napcat_session_key": normalized.chat.session_key,
            "napcat_user_id": normalized.chat.user_id,
            "napcat_group_id": normalized.chat.group_id,
            "napcat_sub_type": normalized.chat.sub_type,
            "napcat_trigger_reason": "slash_command",
        }

        session_key_override: str | None = None
        default_session_key = f"{self._channel.name}:{normalized.chat.chat_id}"
        if normalized.chat.session_key != default_session_key:
            session_key_override = normalized.chat.session_key

        await self._channel._publish_inbound_message(
            sender_id=normalized.sender_id,
            chat_id=normalized.chat.chat_id,
            content=normalized.text.strip(),
            media=[],
            metadata=metadata,
            session_key=session_key_override,
        )


class NapCatChannel(BaseChannel):
    """NapCat Channel 适配器。

    该实现遵循 nanobot 现有的 BaseChannel / ChannelManager / MessageBus 约定，
    对上游暴露的仍然是标准 `InboundMessage` / `OutboundMessage`，不会把 OneBot
    平台细节直接泄漏给 Agent 层。

    """

    name = "napcat"
    display_name = "NapCat"

    @classmethod
    def default_config(cls) -> dict[str, Any]:
        """返回默认配置，用于 nanobot onboard 自动补全。"""
        return NapCatConfig().model_dump(by_alias=True)

    def __init__(self, config: Any, bus: MessageBus):
        """初始化 NapCat Channel。"""
        if isinstance(config, dict):
            config = NapCatConfig.model_validate(config)
        super().__init__(config, bus)
        self.config: NapCatConfig = config

        self._self_id = ""
        self._self_name = ""
        self._identity_task: asyncio.Task[None] | None = None

        self._session_store = SessionBufferStore(self.config)
        self._parser = NapCatMessageParser(self._get_self_id)
        self._normalizer = NapCatNormalizer(self.name, self._parser)
        self._transport = NapCatTransport(
            config=self.config,
            on_event=self._on_transport_event,
            on_connected=self._on_transport_connected,
        )
        self._fetcher = NapCatMessageDetailFetcher(self._transport)
        self._downloader = MediaDownloader(self.config)
        self._directory = ContactDirectory(
            config=self.config,
            transport=self._transport,
            self_id_getter=self._get_self_id,
            self_name_getter=self._get_self_name,
        )
        self._enricher = Enricher(
            config=self.config,
            downloader=self._downloader,
            fetcher=self._fetcher,
            normalizer=self._normalizer,
            directory=self._directory,
        )
        self._serializer = CQMsgSerializer(
            self.config,
            self._get_self_id,
            self._get_self_name,
        )
        self._pipeline = EventPipeline(
            channel=self,
            config=self.config,
            normalizer=self._normalizer,
            enricher=self._enricher,
            session_store=self._session_store,
            serializer=self._serializer,
            self_id_getter=self._get_self_id,
        )
        self._router = EventRouter(
            config=self.config,
            self_id_getter=self._get_self_id,
            pipeline=self._pipeline,
        )
        self._outbound = OutboundSender(
            transport=self._transport,
            session_store=self._session_store,
            self_id_getter=self._get_self_id,
            self_name_getter=self._get_self_name,
        )

    async def start(self) -> None:
        """启动 NapCat Channel。"""
        if not self.config.url.strip():
            logger.error("NapCat [CONN] missing websocket url")
            return

        self.config.url = _normalize_ws_url(self.config.url)
        if not self.config.url.startswith(("ws://", "wss://")):
            logger.error("NapCat [CONN] invalid websocket url url={}", self.config.url)
            return

        self._running = True
        await self._transport.start()

    async def stop(self) -> None:
        """停止 NapCat Channel。"""
        self._running = False
        if self._identity_task is not None:
            self._identity_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._identity_task
            self._identity_task = None
        await self._router.stop()
        await self._transport.stop()

    async def send(self, msg: OutboundMessage) -> None:
        """通过 NapCat 发送一条消息。"""
        await self._outbound.send(msg)

    async def _publish_inbound_message(
        self,
        *,
        sender_id: str,
        chat_id: str,
        content: str,
        media: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        session_key: str | None = None,
    ) -> None:
        """直接发布入站消息，避免 BaseChannel 再按 sender_id 重判权限。"""
        meta = metadata or {}
        if self.supports_streaming:
            meta = {**meta, "_wants_stream": True}

        msg = InboundMessage(
            channel=self.name,
            sender_id=str(sender_id),
            chat_id=str(chat_id),
            content=content,
            media=media or [],
            metadata=meta,
            session_key_override=session_key,
        )

        await self.bus.publish_inbound(msg)

    async def _on_transport_connected(self) -> None:
        """处理 NapCat 建连后的初始化逻辑。"""
        if self._identity_task is not None and not self._identity_task.done():
            self._identity_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._identity_task
        self._identity_task = asyncio.create_task(
            self._refresh_login_identity()
        )

    async def _refresh_login_identity(self) -> None:
        """执行建连后的身份初始化。"""
        try:
            timeout_at = time.monotonic() + _IDENTITY_INIT_TIMEOUT_SECONDS
            while not self._self_id and time.monotonic() < timeout_at:
                await asyncio.sleep(0.05)

            if not self._self_id or not self._self_name:
                response = await self._transport.call_action("get_login_info", {})
                self._apply_login_info(response)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("NapCat [CONN] refresh login identity failed")

    async def _on_transport_event(self, payload: dict[str, Any]) -> None:
        """处理 NapCat 上报事件。"""
        self._apply_identity_from_event(payload)
        if _coerce_str(payload.get("post_type")).strip().lower() == "meta_event":
            return
        await self._router.route(payload)

    def _apply_identity_from_event(self, payload: dict[str, Any]) -> None:
        """从事件载荷里提取机器人身份。"""
        event_self_id = _coerce_optional_str(payload.get("self_id"))
        if event_self_id and event_self_id != self._self_id:
            self._self_id = event_self_id
            logger.info("NapCat [CONN] self_id updated from event self_id={}", self._self_id)

    def _apply_login_info(self, response: dict[str, Any]) -> None:
        """应用 `get_login_info` 响应中的身份信息。"""
        data = response.get("data")
        if not isinstance(data, dict):
            logger.warning(
                "NapCat [CONN] get_login_info missing data preview={!r}",
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
            "NapCat [CONN] login identity refreshed self_id={} nickname={}",
            self._self_id or "unknown",
            self._self_name or "unknown",
        )

    def _get_self_id(self) -> str:
        """返回当前登录账号 ID。"""
        return self._self_id

    def _get_self_name(self) -> str:
        """返回当前登录账号昵称。"""
        return self._self_name
