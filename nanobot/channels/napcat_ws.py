"""NapCat Forward WebSocket 渠道（OneBot v11）。

这个文件保持单文件实现，但按 section 分层维护，避免职责混在一起。

当前边界：
- Transport：WebSocket 连接、action 调用、echo/future 关联、任务生命周期。
- Inbound：原始 payload 归一化、消息解析、reply/forward/media 补全。
- Policy：只判断“要不要响应”，不管回复风格。
- Session / Context：维护会话缓存并构建聊天上下文。
- Outbound：负责发送文本/CQ，不负责拼 system prompt 或人格。

当前支持的主要触发：
- 私聊概率触发
- 群聊概率触发
- nickname trigger
- poke trigger
- @bot trigger
- reply-to-bot trigger

当前 Outbound 设计：
- 走 text-first 主路径，不做复杂 segment builder。
- 允许模型直接输出 CQ 码；当前正式校验的 CQ 类型：at、reply、image。
- 发送前只做轻量校验，避免脏 CQ 直接进入主链路。

维护约定：
- 改消息解析，优先看 Inbound。
- 改触发规则，优先看 Policy。
- 改上下文构建，优先看 Session / Context。
- 改发送行为，优先看 Outbound。
- 新逻辑优先接当前主链路，不要把 legacy 实现带回。
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import shutil
import time
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Awaitable, Callable, Deque, Literal, Optional
from urllib.parse import urlparse, urlunparse
from urllib.request import Request, urlopen

import websockets
from loguru import logger
from pydantic import Field
from websockets import ClientConnection

from nanobot.bus.events import InboundMessage, OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel
from nanobot.config.paths import get_media_dir
from nanobot.config.schema import Base

# ==============================
# 0) Pure helpers
# ==============================


def summarize_onebot_event(payload: dict[str, Any]) -> str:
    """生成用于日志的 OneBot 事件摘要。

    要求：输出应尽量稳定、低基数（low-cardinality），并且绝不包含完整消息正文。

    Args:
        payload: OneBot 事件 payload。

    Returns:
        形如 `key=value` 的紧凑摘要字符串；如果 payload 非法或关键信息为空，则返回占位值。
    """

    if not isinstance(payload, dict):
        return "invalid_payload"

    keys = (
        "post_type",
        "message_type",
        "notice_type",
        "sub_type",
        "message_id",
        "user_id",
        "group_id",
        "self_id",
    )
    parts: list[str] = []
    for key in keys:
        value = payload.get(key)
        if value in (None, ""):
            continue
        parts.append(f"{key}={value}")
    return " ".join(parts) if parts else "empty_payload"


# ==============================
# 0.1) Channel config
# ==============================


class NapCatWSConfig(Base):
    """NapCat WS 渠道配置。

    配置路径：`config.channels.napcat_ws`

    说明：
    - 字段定义放在 channel 文件内，便于跟实现一起演进。
    - Python 字段名用 snake_case。
    - 配置文件可同时兼容 snake_case / camelCase，具体取决于 Base 的 alias 规则。
    """

    # --- 连接 ---
    enabled: bool = Field(default=False, description="是否启用 NapCatWS 渠道")
    url: str = Field(default="", description="WebSocket 地址，例如 ws://localhost:3001/")
    token: str = Field(default="", description="NapCat / OneBot token（如果服务端要求）")

    # --- 来源控制 ---
    allow_from: list[str] = Field(
        default_factory=list,
        description=(
            "允许触发的来源列表（字符串）。\n"
            "支持两种值：\n"
            "- user_id：允许指定用户（私聊 / 群内发言者）\n"
            "- group_id：允许指定群\n"
            "为空表示不启用 allow list，仍会受 blacklist 影响。"
        ),
    )
    blacklist_private_ids: list[str] = Field(
        default_factory=list,
        description="私聊 blacklist user_id 列表",
    )
    blacklist_group_ids: list[str] = Field(
        default_factory=list,
        description="群聊 blacklist group_id 列表",
    )

    # --- 概率触发 ---
    private_trigger_probability: float = Field(
        default=0.05,
        description="私聊随机触发概率（0~1）",
    )
    group_trigger_probability: float = Field(
        default=0.05,
        description="群聊随机触发概率（0~1）",
    )

    # --- 直接触发 ---
    nickname_triggers: list[str] = Field(
        default_factory=list,
        description="消息中包含任一 nickname / keyword 即触发",
    )
    trigger_on_at: bool = Field(default=True, description="群聊中 @ 机器人时触发")
    trigger_on_reply_to_bot: bool = Field(default=True, description="引用回复机器人消息时触发")
    trigger_on_poke: bool = Field(default=False, description="收到 poke 通知时触发")
    poke_cooldown_seconds: int = Field(
        default=60,
        description="poke 触发冷却秒数；<=0 表示关闭冷却",
    )

    # --- 上下文 / 会话 ---
    context_max_messages: int = Field(default=25, description="构建上下文时最多读取的消息条数")
    session_buffer_size: int = Field(default=50, description="内存会话 ring buffer 大小")
    context_message_max_chars: int = Field(
        default=200,
        description=(
            "单条上下文消息的最大字符数。超过则截断并追加 <TRUNCATED>；0 或负数表示不截断。"
        ),
    )
    ignore_self_messages: bool = Field(
        default=True,
        description="忽略机器人自身消息，避免自回声 / 死循环",
    )


# NOTE: transport 层也会用到 OneBot action 常量（get_login_info）

# ==============================
# 1) Types & constants
# ==============================

ChatType = Literal["private", "group"]
TriggerType = Literal[
    "private_probability", "group_probability", "nickname", "poke", "at_bot", "reply_to_bot"
]

NoticeKind = Literal["poke", "other_notice"]

# OneBot / NapCat action 名
ACTION_GET_LOGIN_INFO = "get_login_info"
ACTION_GET_MSG = "get_msg"
ACTION_GET_FORWARD_MSG = "get_forward_msg"
ACTION_GET_GROUP_MEMBER_INFO = "get_group_member_info"
ACTION_SEND_POKE = "send_poke"

# Cache TTL（秒）
CACHE_TTL_1D_SECONDS = 60 * 60 * 24
DEFAULT_CACHE_TTL_SECONDS = CACHE_TTL_1D_SECONDS

# message detail cache 上限
REPLY_CACHE_MAXSIZE = 2048
FORWARD_CACHE_MAXSIZE = 1024


TRUNCATION_TAG = "<TRUNCATED>"  # 固定截断标记

@dataclass(slots=True)
class CQCtxReverseMap:
    """短记号到原始平台标识 / 文件名的反向映射。"""

    users: dict[str, dict[str, str]] = field(default_factory=dict)
    images: dict[str, str] = field(default_factory=dict)
    live_replies: dict[str, str] = field(default_factory=dict)
    forward_node_replies: dict[str, str] = field(default_factory=dict)


class InMemoryMessageCache:
    """进程内存消息详情缓存（reply / forward）。"""

    def __init__(self, *, ttl_seconds: int, reply_maxsize: int, forward_maxsize: int) -> None:
        self._ttl = max(0, int(ttl_seconds or 0))
        self._reply_maxsize = max(1, int(reply_maxsize or 1))
        self._forward_maxsize = max(1, int(forward_maxsize or 1))
        self._reply: OrderedDict[str, tuple[float, Any]] = OrderedDict()
        self._forward: OrderedDict[str, tuple[float, Any]] = OrderedDict()

    def _now(self) -> float:
        return time.time()

    def _get(self, store: OrderedDict[str, tuple[float, Any]], key: str) -> Any | None:
        """从缓存中读取一个条目，并按 TTL 规则失效。

        Args:
            store: 具体的缓存存储（reply 或 forward）。
            key: 缓存 key。

        Returns:
            命中且未过期时返回缓存值；否则返回 None。
        """
        k = str(key or "").strip()
        if not k:
            return None
        item = store.get(k)
        if item is None:
            return None
        ts, value = item
        if self._ttl > 0 and (self._now() - ts) > self._ttl:
            store.pop(k, None)
            return None
        store.move_to_end(k)
        return value

    def _set(
        self, store: OrderedDict[str, tuple[float, Any]], key: str, value: Any, maxsize: int
    ) -> None:
        """写入缓存条目，并按 maxsize 做简单 LRU 淘汰。

        Args:
            store: 具体的缓存存储（reply 或 forward）。
            key: 缓存 key。
            value: 缓存值。
            maxsize: 最大容量。
        """
        k = str(key or "").strip()
        if not k:
            return
        store[k] = (self._now(), value)
        store.move_to_end(k)
        while len(store) > maxsize:
            store.popitem(last=False)

    def get_reply(self, message_id: str) -> Any | None:
        return self._get(self._reply, message_id)

    def set_reply(self, message_id: str, value: Any) -> None:
        self._set(self._reply, message_id, value, self._reply_maxsize)

    def get_forward(self, forward_id: str) -> Any | None:
        return self._get(self._forward, forward_id)

    def set_forward(self, forward_id: str, value: Any) -> None:
        self._set(self._forward, forward_id, value, self._forward_maxsize)


@dataclass(slots=True)
class ChatRef:
    """归一化后的聊天定位信息。"""

    chat_type: ChatType
    chat_id: str
    user_id: str = ""
    group_id: str = ""


@dataclass(slots=True)
class QuotedMessage:
    """被引用/回复的消息（quote/reply）。"""

    message_id: str
    sender_id: str = ""
    sender_name: str = ""
    text: str = ""
    media: list[str] = field(default_factory=list)
    media_paths: list[str] = field(default_factory=list)

    # 保留原始 message/raw_message，便于生成精简 CQ（避免把超长 raw_message 原样塞进上下文）
    message: Any = None
    raw_message: Any = None

    # 引用消息自身可能是合并转发（forward），用于在 reply_to 里展开转发内容
    forward_id: str = ""
    forward_items: list[dict[str, Any]] = field(default_factory=list)


@dataclass(slots=True)
class NormalizedInbound:
    """Normalize 层产出的内部消息结构，供 Policy / Context 使用。"""

    chat: ChatRef
    sender_id: str
    sender_name: str
    message_id: str
    timestamp: datetime

    # 纯文本（用于触发规则匹配）：尽量接近“用户打出来的字”，不包含占位符。
    text: str

    # 渲染文本（用于发给大模型）：会把图片/语音/@/文件等转成占位符，便于模型理解上下文。
    rendered_text: str = ""
    rendered_segments: list[str] = field(default_factory=list)

    media: list[str] = field(default_factory=list)
    media_paths: list[str] = field(default_factory=list)

    # 从 segments 提取的信号
    at_self: bool = False
    reply: Optional[QuotedMessage] = None

    # 合并转发（forward）
    forward_id: str = ""
    forward_summary: str = ""
    forward_items: list[dict[str, Any]] = field(default_factory=list)

    raw_event: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class TriggerDecision:
    """Policy 层的触发决策结果。"""

    should_reply: bool
    reason: str
    trigger: TriggerType | None = None
    probability_used: float | None = None


@dataclass(slots=True)
class CQValidationIssue:
    """单个 CQ 码校验问题。"""

    code: str
    message: str
    cq: str = ""


@dataclass(slots=True)
class CQValidationResult:
    """CQ 出站轻校验结果。"""

    valid: bool
    normalized_text: str
    issues: list[CQValidationIssue] = field(default_factory=list)


# =========================================
# 1.1) Inbound context/message types
# =========================================
# 这里只放消息解析 / 展开 / 上下文构建需要的数据结构。


MessageSegmentType = Literal[
    "text",
    "face",
    "image",
    "at",
    "reply",
    "forward",
    "record",
    "video",
    "file",
]


@dataclass(slots=True)
class ParsedSegment:
    """segment 的轻量归一化表示。

    这里只存解析结果，不负责最终渲染。
    后续渲染 / 上下文构建会基于它生成：
    - 上下文里的 `m` 文本
    - 触发匹配用的纯文本
    - reply / forward / at 等信号
    """

    type: MessageSegmentType
    data: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ParsedSegmentResult:
    """单个消息段归一化后的中间结果。"""

    segment: ParsedSegment
    cq_part: str = ""
    plain_part: str = ""
    at_qq: str = ""
    reply_id: str = ""
    forward_id: str = ""
    media_urls: list[str] = field(default_factory=list)
    media_meta: list[dict[str, Any]] = field(default_factory=list)


@dataclass(slots=True)
class ParsedMessage:
    """消息解析后的统一输出。

    这里只存解析结果，供后续 trigger / expand / context 构建复用。
    """

    segments: list[ParsedSegment] = field(default_factory=list)
    plain_text: str = ""
    cq_text: str = ""
    rendered_text: str = ""
    rendered_segments: list[str] = field(default_factory=list)
    media: list[str] = field(default_factory=list)
    media_meta: list[dict[str, Any]] = field(default_factory=list)

    # extracted signals
    at_qq: list[str] = field(default_factory=list)
    reply_id: str = ""
    forward_id: str = ""


@dataclass(slots=True)
class ExpandedForward:
    """合并转发（forward）的展开结果。"""

    id: str
    items: list[ForwardItemRecord] = field(default_factory=list)
    media_meta: list[dict[str, Any]] = field(default_factory=list)
    summary: str = ""


@dataclass(slots=True)
class ExpandedReply:
    """引用消息（reply）的展开结果。

    该结构预期来源于 get_msg 的返回。

    与 ContextMessageRecord 的约定（已定稿）：
      - 当前消息的 text 字段内仍保留 inline 的 `[CQ:reply,id=...]`（以及用户实际追加的文本）
      - 被引用的“原消息”展开内容放到 ContextMessageRecord.reply
      - 如果“原消息”本身包含 forward，可填充到 reply.forward_items（后续可继续展开）

    注意：这里只定义数据结构；拉取/解析/截断规则后续实现。
    """

    id: str
    sender_name: str = ""
    sender_id: str = ""
    text: str = ""
    forward: Optional[ExpandedForward] = None

    # 原始消息体（尽量保留 message segments，便于下载引用消息里的图片）
    message: Any = None
    raw_message: Any = None

    # 结构化媒体元信息（由 parser 提取，便于在 message 为 string 时也能下载图片）
    media_meta: list[dict[str, Any]] = field(default_factory=list)


@dataclass(slots=True)
class ReplyRecord:
    """引用消息的摘要记录（长字段）。

    该结构用于上下文构建阶段的 reply 展开信息。
    """

    sender_name: str
    sender_id: str
    message_id: str
    text: str
    is_bot: bool = False

    # 引用消息本身若包含合并转发，可在这里携带展开结果（可选）。
    forward_items: Optional[list[dict[str, Any]]] = None
    forward_summary: str = ""

    # 引用消息中可见的本地媒体路径（不写入上下文文本）。
    media_paths: list[str] = field(default_factory=list)


@dataclass(slots=True)
class ForwardItemRecord:
    """合并转发的单个节点记录（长字段）。"""

    sender_name: str
    sender_id: str
    message_id: str
    text: str
    source: str = ""
    is_bot: bool = False

    # 节点内可见媒体
    media_paths: list[str] = field(default_factory=list)
    media_meta: list[dict[str, Any]] = field(default_factory=list)


@dataclass(slots=True)
class ContextMessageRecord:
    """规范化后的上下文消息记录（长字段）。

    该结构作为 session buffer 的缓存载体，字段命名更语义化。
    NapCatContextBuilder 会基于这些字段直接序列化为 CQCTX/CQDM，
    确保最终注入给模型的格式不变。
    """

    sender_name: str
    sender_id: str
    message_id: str
    text: str

    # chat scope
    chat_type: ChatType = "group"
    group_id: str = ""
    is_bot: bool = False

    # expansions / structured refs
    forward_items: Optional[list[ForwardItemRecord]] = None
    reply: Optional[ReplyRecord] = None

    # local image paths (not serialized into text context)
    media_paths: list[str] = field(default_factory=list)


# =========================================
# 1.1.1) Inbound implementation（parser/cache/fetcher/context builder）
# =========================================
#
# 说明：
#   - 这部分落地“解析/展开/缓存/上下文构建”的最小闭环，并被 normalize/channel 主链路直接使用。
#   - 仍只支持 9 种 segment 类型：text/face/image/at/reply/forward/record/video/file。
#   - 其它类型输出占位 token（[CQ:type]），后续按需扩展。
#   - Header 不包含 group_name。
#


def _json_dumps_compact(obj: Any) -> str:
    """紧凑 JSON 序列化（省 token）。"""

    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def _cqctx_escape(text: str) -> str:
    """对 CQCtx body 文本做转义。

    该转义用于 CQCtxBodyEncoder 输出，确保以下字符不会破坏内部编码：
    - 反斜杠 `\\`
    - 分隔符 `|`
    - 换行 `\n`

    Args:
        text: 原始文本。

    Returns:
        转义后的文本。
    """
    s = str(text or "")
    s = s.replace("\\", "\\\\")
    s = s.replace("|", "\\|")
    s = s.replace("\n", "\\n")
    return s


def _cqctx_filename_from_path(path: str) -> str:
    """从文件路径提取一个安全的文件名。

    主要用于把本地路径映射为可嵌入 CQCtx 的短文件名，避免把上游的奇怪文件名
    原样带入。

    Args:
        path: 任意形式的路径字符串。

    Returns:
        安全的文件名；提取失败时返回空字符串。
    """
    try:
        return _safe_media_filename(Path(str(path or "")).name)
    except Exception:
        return ""


class _CQCtxSymbolTable:
    """CQCtx 编码过程用到的符号表。

    该符号表负责为用户、图片、转发节点等实体分配短引用（ref），并同步维护
    `CQCtxReverseMap`，用于在需要时从短引用反查回真实平台标识。

    该类只服务于 CQCtx 编码链路，不应当被业务逻辑直接依赖。
    """

    def __init__(self) -> None:
        self.user_by_qq: dict[str, str] = {}
        self.user_rows: list[dict[str, Any]] = []
        self.image_by_name: dict[str, str] = {}
        self.image_rows: list[dict[str, str]] = []
        self.forward_counter = 0
        self.reverse = CQCtxReverseMap()

    def user_ref(self, *, qq: str, name: str, is_bot: bool = False) -> str:
        qq_s = str(qq or "").strip()
        if not qq_s:
            qq_s = "0"
        ref = self.user_by_qq.get(qq_s)
        if ref is not None:
            if is_bot:
                self.reverse.users.setdefault(ref, {}).update(
                    {"qq": qq_s, "name": str(name or qq_s), "bot": "1"}
                )
            return ref
        ref = f"u{len(self.user_rows)}"
        row = {"ref": ref, "qq": qq_s, "name": str(name or qq_s), "bot": bool(is_bot)}
        self.user_by_qq[qq_s] = ref
        self.user_rows.append(row)
        self.reverse.users[ref] = {
            "qq": qq_s,
            "name": str(name or qq_s),
            "bot": "1" if is_bot else "0",
        }
        return ref

    def image_ref(self, filename: str) -> str:
        safe = _safe_media_filename(filename)
        if not safe:
            safe = "unknown"
        ref = self.image_by_name.get(safe)
        if ref is not None:
            return ref
        ref = f"i{len(self.image_rows)}"
        self.image_by_name[safe] = ref
        self.image_rows.append({"ref": ref, "filename": safe})
        self.reverse.images[ref] = safe
        return ref

    def new_forward_ref(self) -> str:
        ref = f"f{self.forward_counter}"
        self.forward_counter += 1
        return ref


class _CQCtxBodyEncoder:
    """把渲染文本中的 CQ token 编码成 CQCtx body。

    输入通常是包含形如 `[CQ:type,key=value]` 的渲染文本。
    编码后会输出一种更紧凑且可逆的内部表示，并通过 `_CQCtxSymbolTable` 把
    用户/图片等实体映射为短引用。

    该编码仅用于上下文注入，不用于出站发送。

    Args:
        symbols: CQCtx 符号表。
        is_group: 是否为群聊（影响 at/reply 等 token 的处理策略）。
    """

    _token_re = re.compile(r"\[CQ:([^,\]]+)(?:,([^\]]+))?\]")

    def __init__(self, *, symbols: _CQCtxSymbolTable, is_group: bool) -> None:
        self.symbols = symbols
        self.is_group = bool(is_group)

    def encode(self, text: str, *, node_ref_map: Optional[dict[str, str]] = None) -> str:
        """把渲染文本编码为 CQCtx body。

        Args:
            text: 输入文本，可能包含 CQ token（例如 `[CQ:at,qq=123]`）。
            node_ref_map: 转发节点 message_id 到 node_ref 的映射；用于把 reply
                映射成对转发节点的引用。

        Returns:
            CQCtx body 字符串。
        """
        src = str(text or "")
        if not src:
            return ""
        out: list[str] = []
        pos = 0
        for m in self._token_re.finditer(src):
            if m.start() > pos:
                out.append(_cqctx_escape(src[pos : m.start()]))
            typ = str(m.group(1) or "").strip().lower()
            params = _parse_cq_params(m.group(2) or "")
            out.append(self._map_token(typ, params, node_ref_map=node_ref_map))
            pos = m.end()
        if pos < len(src):
            out.append(_cqctx_escape(src[pos:]))
        return "".join(out).strip()

    def _map_token(
        self, typ: str, params: dict[str, str], *, node_ref_map: Optional[dict[str, str]]
    ) -> str:
        """把单个 CQ token 映射为 CQCtx 内部表示。

        Args:
            typ: CQ 类型（已经归一化为小写）。
            params: CQ 参数字典。
            node_ref_map: 转发节点映射（见 `encode`）。

        Returns:
            CQCtx 内部表示片段；若该 token 应被忽略则返回空字符串。
        """
        if typ == "at":
            qq = str(params.get("qq") or "").strip()
            if not qq:
                return ""
            ref = self.symbols.user_ref(qq=qq, name=qq)
            return f"@{ref}"
        if typ == "reply":
            rid = str(params.get("id") or params.get("message_id") or "").strip()
            if not rid:
                return ""
            if node_ref_map and rid in node_ref_map:
                self.symbols.reverse.forward_node_replies[node_ref_map[rid]] = rid
                return f">n:{node_ref_map[rid]}"
            self.symbols.reverse.live_replies[rid] = rid
            return f">m:{rid}"
        if typ == "image":
            filename = str(params.get("file") or params.get("name") or "").strip()
            if filename.startswith("file://"):
                filename = _cqctx_filename_from_path(filename)
            else:
                filename = _safe_media_filename(filename)
            ref = self.symbols.image_ref(filename)
            return f"[{ref}]"
        if typ == "forward":
            return ""
        if typ in {"record", "video", "file", "face"}:
            return f"[{typ}]"
        return _cqctx_escape(f"[CQ:{typ}]")


class NapCatContextBuilder:
    """构建 NapCat 上下文注入文本。

    输出格式为紧凑的 CQCTX/3（群聊）或 CQDM/1（私聊）。

    该构建器只负责把已归一化的 `ContextMessageRecord` 序列序列化为文本，并在
    同时生成反向映射（CQCtxReverseMap）供后续媒体回填/引用解析使用。
    """

    def build(
        self,
        *,
        chat: ChatRef,
        bot_name: str,
        bot_id: str,
        time_window_label: str,
        prompts: list[str],
         messages: list[ContextMessageRecord],
    ) -> str:
        """构建用于注入到大模型的上下文文本。

        Args:
            chat: 聊天引用信息（群/私聊）。
            bot_name: 机器人展示名。
            bot_id: 机器人 ID。
            time_window_label: 时间窗口标签（预留字段；当前不参与序列化）。
            prompts: 额外提示词列表（预留字段；当前不参与序列化）。
            messages: 已归一化的上下文记录。

        Returns:
            序列化后的上下文文本。
        """
        if chat.chat_type == "private":
            return self._build_dm(chat=chat, bot_name=bot_name, bot_id=bot_id, messages=messages)
        return self._build_group(chat=chat, bot_name=bot_name, bot_id=bot_id, messages=messages)

    def build_reverse_map(
        self,
        *,
        chat: ChatRef,
        bot_name: str,
        bot_id: str,
         messages: list[ContextMessageRecord],
    ) -> CQCtxReverseMap:
        """构建 CQCtx 反向映射表。

        Args:
            chat: 聊天引用信息。
            bot_name: 机器人展示名。
            bot_id: 机器人 ID。
            messages: 上下文记录。

        Returns:
            反向映射表，用于把 CQCtx 的短引用映射回真实 qq/文件名等。
        """
        symbols = self._collect_symbols(
            chat=chat, bot_name=bot_name, bot_id=bot_id, messages=messages
        )
        return symbols.reverse

    def _build_group(
        self, *, chat: ChatRef, bot_name: str, bot_id: str, messages: list[ContextMessageRecord]
    ) -> str:
        symbols = self._collect_symbols(
            chat=chat, bot_name=bot_name, bot_id=bot_id, messages=messages
        )
        bot_ref = symbols.user_ref(
            qq=str(bot_id or "0"), name=str(bot_name or bot_id or "bot"), is_bot=True
        )
        rows: list[str] = [
            f"<CQCTX/3 g:{_cqctx_escape(chat.chat_id)} bot:{bot_ref} n:{len(messages)}>"
        ]
        for row in symbols.user_rows:
            suffix = "|bot" if row.get("bot") else ""
            rows.append(
                f"U|{row['ref']}|{_cqctx_escape(row['qq'])}|{_cqctx_escape(row['name'])}{suffix}"
            )
        for row in symbols.image_rows:
            rows.append(f"I|{row['ref']}|{_cqctx_escape(row['filename'])}")
        encoder = _CQCtxBodyEncoder(symbols=symbols, is_group=True)
        rows.extend(
            self._serialize_messages(
                messages=messages,
                symbols=symbols,
                encoder=encoder,
                private_mode=False,
                bot_id=str(bot_id or ""),
                bot_name=str(bot_name or bot_id or "bot"),
                chat=chat,
            )
        )
        rows.append("</CQCTX/3>")
        return "\n".join(rows)

    def _build_dm(
        self, *, chat: ChatRef, bot_name: str, bot_id: str, messages: list[ContextMessageRecord]
    ) -> str:
        symbols = self._collect_symbols(
            chat=chat, bot_name=bot_name, bot_id=bot_id, messages=messages
        )
        peer_name = str(chat.user_id or "peer")
        for m in messages:
            if (
                str(m.sender_id or "") == str(chat.user_id or "")
                and str(m.sender_name or "").strip()
            ):
                peer_name = str(m.sender_name)
                break
        rows: list[str] = [f"<CQDM/1 n:{len(messages)}>"]
        rows.append(
            f"P|me|{_cqctx_escape(str(bot_id or ''))}|{_cqctx_escape(str(bot_name or bot_id or 'bot'))}|bot"
        )
        rows.append(
            f"P|peer|{_cqctx_escape(str(chat.user_id or chat.chat_id or ''))}|{_cqctx_escape(peer_name)}"
        )
        for row in symbols.user_rows:
            qq_s = str(row.get("qq") or "")
            if qq_s in {str(bot_id or ""), str(chat.user_id or chat.chat_id or "")}:
                continue
            suffix = "|bot" if row.get("bot") else ""
            rows.append(
                f"U|{row['ref']}|{_cqctx_escape(row['qq'])}|{_cqctx_escape(row['name'])}{suffix}"
            )
        for row in symbols.image_rows:
            rows.append(f"I|{row['ref']}|{_cqctx_escape(row['filename'])}")
        encoder = _CQCtxBodyEncoder(symbols=symbols, is_group=False)
        rows.extend(
            self._serialize_messages(
                messages=messages,
                symbols=symbols,
                encoder=encoder,
                private_mode=True,
                bot_id=str(bot_id or ""),
                bot_name=str(bot_name or bot_id or "bot"),
                chat=chat,
            )
        )
        rows.append("</CQDM/1>")
        return "\n".join(rows)

    def _collect_symbols(
        self, *, chat: ChatRef, bot_name: str, bot_id: str, messages: list[ContextMessageRecord]
    ) -> _CQCtxSymbolTable:
        symbols = _CQCtxSymbolTable()
        if chat.chat_type == "group":
            symbols.user_ref(
                qq=str(bot_id or "0"), name=str(bot_name or bot_id or "bot"), is_bot=True
            )
        else:
            symbols.user_ref(
                qq=str(bot_id or "0"), name=str(bot_name or bot_id or "bot"), is_bot=True
            )
            symbols.user_ref(
                qq=str(chat.user_id or chat.chat_id or "peer"),
                name=str(chat.user_id or chat.chat_id or "peer"),
            )
        for line in messages:
            symbols.user_ref(
                qq=str(line.sender_id or "0"),
                name=str(line.sender_name or line.sender_id or "unknown"),
                is_bot=bool(line.is_bot),
            )
            self._collect_line_assets(line=line, symbols=symbols)
        return symbols

    def _collect_line_assets(self, *, line: ContextMessageRecord, symbols: _CQCtxSymbolTable) -> None:
        """从单条上下文记录收集需要写入符号表的实体。

        目前会收集：
        - 行内图片（CQ image token）
        - reply 引用块中的图片
        - media_paths（本地已下载媒体）
        - forward 节点中的用户与图片

        Args:
            line: 上下文记录。
            symbols: CQCtx 符号表。
        """
        self._collect_images_from_text(line.text, symbols)

        if isinstance(line.reply, ReplyRecord):
            qq = str(line.reply.sender_id or "").strip()
            name = str(line.reply.sender_name or qq or "unknown")
            if qq:
                symbols.user_ref(qq=qq, name=name, is_bot=bool(line.reply.is_bot))
            self._collect_images_from_text(str(line.reply.text or ""), symbols)

        for p in list(getattr(line, "media_paths", None) or []):
            fn = _cqctx_filename_from_path(p)
            if fn:
                symbols.image_ref(fn)

        for node in list(line.forward_items or []):
            if not isinstance(node, ForwardItemRecord):
                continue
            qq = str(node.sender_id or "").strip()
            name = str(node.sender_name or qq or "unknown")
            if qq:
                symbols.user_ref(qq=qq, name=name, is_bot=bool(node.is_bot))
            self._collect_images_from_text(str(node.text or ""), symbols)
            for p in list(node.media_paths or []):
                fn = _cqctx_filename_from_path(p)
                if fn:
                    symbols.image_ref(fn)

    def _collect_images_from_text(self, text: str, symbols: _CQCtxSymbolTable) -> None:
        """扫描文本中的 image token，并把图片文件名记录到符号表。

        Args:
            text: 可能包含 CQ token 的文本。
            symbols: CQCtx 符号表。
        """
        for m in _CQCtxBodyEncoder._token_re.finditer(str(text or "")):
            typ = str(m.group(1) or "").strip().lower()
            if typ != "image":
                continue
            params = _parse_cq_params(m.group(2) or "")
            filename = str(params.get("file") or params.get("name") or "").strip()
            if filename.startswith("file://"):
                filename = _cqctx_filename_from_path(filename)
            else:
                filename = _safe_media_filename(filename)
            if filename:
                symbols.image_ref(filename)

    def _serialize_messages(
        self,
        *,
         messages: list[ContextMessageRecord],
        symbols: _CQCtxSymbolTable,
        encoder: _CQCtxBodyEncoder,
        private_mode: bool,
        bot_id: str,
        bot_name: str,
        chat: ChatRef,
    ) -> list[str]:
        """把上下文记录序列化为 CQCTX/CQDM 的行列表。

        Args:
            messages: 上下文记录。
            symbols: CQCtx 符号表。
            encoder: CQCtx body 编码器。
            private_mode: 是否为私聊模式（决定 sender ref 的规则）。
            bot_id: 机器人 ID。
            bot_name: 机器人展示名（当前仅用于上游构建，序列化阶段不必依赖）。
            chat: 聊天引用信息。

        Returns:
            行列表（不包含外层 `<CQCTX/...>` 包裹）。
        """
        rows: list[str] = []
        peer_id = str(chat.user_id or chat.chat_id or "")
        for line in messages:
            sender_ref = self._sender_ref(
                line=line,
                symbols=symbols,
                private_mode=private_mode,
                bot_id=bot_id,
                peer_id=peer_id,
            )
            body = encoder.encode(line.text)
            body, forward_rows = self._serialize_forward_rows(
                line=line,
                body=body,
                symbols=symbols,
                encoder=encoder,
                private_mode=private_mode,
                bot_id=bot_id,
                peer_id=peer_id,
            )
            rows.append(f"M|{_cqctx_escape(line.message_id)}|{sender_ref}|{body}")
            rows.extend(forward_rows)
        return rows

    def _resolve_forward_summary(self, line: ContextMessageRecord) -> str:
        """提取一条消息对应的 forward summary。"""

        if isinstance(line.reply, ReplyRecord):
            summary = str(line.reply.forward_summary or "").strip()
            if summary:
                return summary
        return str(getattr(line, "forward_summary", "") or "").strip()

    def _build_forward_node_ref_map(
        self, *, fid: str, forward_items: list[ForwardItemRecord]
    ) -> dict[str, str]:
        """构建 forward 节点 message_id 到节点引用的映射。"""

        node_ref_map: dict[str, str] = {}
        for idx, node in enumerate(forward_items):
            if not isinstance(node, ForwardItemRecord):
                continue
            node_msg_id = str(node.message_id or "").strip()
            if node_msg_id:
                node_ref_map[node_msg_id] = f"{fid}.{idx}"
        return node_ref_map

    def _serialize_forward_node(
        self,
        *,
        fid: str,
        idx: int,
        node: ForwardItemRecord,
        node_ref_map: dict[str, str],
        symbols: _CQCtxSymbolTable,
        encoder: _CQCtxBodyEncoder,
        private_mode: bool,
        bot_id: str,
        peer_id: str,
    ) -> str:
        """序列化单个 forward 节点。"""

        node_ref = f"{fid}.{idx}"
        node_sender = self._node_sender_ref(
            node=node,
            symbols=symbols,
            private_mode=private_mode,
            bot_id=bot_id,
            peer_id=peer_id,
        )
        node_body = encoder.encode(str(node.text or ""), node_ref_map=node_ref_map)
        if not private_mode:
            return f"N|{node_ref}|{node_sender}|{node_body}"

        src = str(node.source or "").strip()
        src_part = f"|{_cqctx_escape(src)}" if src else ""
        return f"N|{node_ref}|{node_sender}{src_part}|{node_body}"

    def _serialize_forward_rows(
        self,
        *,
        line: ContextMessageRecord,
        body: str,
        symbols: _CQCtxSymbolTable,
        encoder: _CQCtxBodyEncoder,
        private_mode: bool,
        bot_id: str,
        peer_id: str,
    ) -> tuple[str, list[str]]:
        """序列化一条消息携带的 forward 信息。"""

        if not line.forward_items:
            return body, []

        fid = symbols.new_forward_ref()
        body = f"{body} [F:{fid}]".strip() if body else f"[F:{fid}]"

        summary = self._resolve_forward_summary(line)
        summary_part = f"|{_cqctx_escape(summary)}" if summary else ""
        rows = [f"F|{fid}|{len(line.forward_items)}{summary_part}"]

        node_ref_map = self._build_forward_node_ref_map(
            fid=fid,
            forward_items=list(line.forward_items),
        )
        for idx, node in enumerate(line.forward_items):
            if not isinstance(node, ForwardItemRecord):
                continue
            rows.append(
                self._serialize_forward_node(
                    fid=fid,
                    idx=idx,
                    node=node,
                    node_ref_map=node_ref_map,
                    symbols=symbols,
                    encoder=encoder,
                    private_mode=private_mode,
                    bot_id=bot_id,
                    peer_id=peer_id,
                )
            )
        return body, rows

    def _sender_ref(
        self,
        *,
        line: ContextMessageRecord,
        symbols: _CQCtxSymbolTable,
        private_mode: bool,
        bot_id: str,
        peer_id: str,
    ) -> str:
        qq = str(line.sender_id or "")
        if private_mode:
            if qq == str(bot_id or ""):
                return "me"
            if qq == str(peer_id or ""):
                return "peer"
        return symbols.user_ref(
            qq=qq,
            name=str(line.sender_name or qq or "unknown"),
            is_bot=bool(line.is_bot),
        )

    def _node_sender_ref(
        self,
        *,
        node: ForwardItemRecord,
        symbols: _CQCtxSymbolTable,
        private_mode: bool,
        bot_id: str,
        peer_id: str,
    ) -> str:
        qq = str(node.sender_id or "")
        if private_mode:
            if qq == str(bot_id or ""):
                return "me"
            if qq == str(peer_id or ""):
                return "peer"
        return symbols.user_ref(
            qq=qq,
            name=str(node.sender_name or qq or "unknown"),
            is_bot=bool(node.is_bot),
        )


class NapCatMessageParser:
    """解析 OneBot message 段为简化的 CQ 文本。

    该解析器只处理约定的 9 种 segment 类型，并把它们统一渲染为 CQ 风格的
    文本 `m`（供上下文构建与触发判断使用）。

    未支持的 segment 会被降级为占位文本（例如 `[CQ:foo]`），避免把未知结构
    直接透传到后续链路。
    """

    # 解析 CQ 码的最小正则：匹配形如 [CQ:type,k=v,...]
    # 注意：这里不要写成 \\[CQ:...，否则会变成匹配字面量“\[CQ:”而不是“[CQ:”。
    _cq_re = re.compile(r"\[CQ:([^,\]]+)(?:,([^\]]+))?\]")

    # 配置表：seg_type -> 用于构建精简 CQ 的字段名列表（按优先级取第一个非空）
    # 说明：
    #   - 输出 CQ 参数键使用 field_keys[0]
    #   - image/file 允许用 name 兜底，但仍输出为 file=...
    #   - reply 兼容 message_id
    _CQ_FIELD_MAP: dict[str, list[str]] = {
        "face": ["id"],
        "image": ["file", "name"],
        "at": ["qq"],
        "reply": ["id", "message_id"],
        "forward": ["id"],
        "record": ["file"],
        "video": ["file"],
        "file": ["file", "name"],
        "text": ["text"],
    }

    def parse(self, *, payload: dict[str, Any], self_id: str) -> ParsedMessage:
        """解析 OneBot message payload。

        Args:
            payload: OneBot v11 message 事件 payload。
            self_id: 机器人自身 ID（用于部分字段归一化；当前主要用于保留接口一致性）。

        Returns:
            解析后的 `ParsedMessage`，其中 `m` 为 CQ 风格的文本。
        """
        # 兼容 string/array
        message = payload.get("message")
        raw_message = payload.get("raw_message")
        msg_format = str(payload.get("message_format") or "").strip().lower()

        if isinstance(message, list):
            return self._parse_array(message)

        # message_format 可能未填，但 raw_message/ message 可能是 CQ string
        if msg_format == "string" or isinstance(message, str) or isinstance(raw_message, str):
            cq = ""
            if isinstance(message, str) and message:
                cq = message
            elif isinstance(raw_message, str) and raw_message:
                cq = raw_message
            return self._parse_cq_string(cq)

        return ParsedMessage()

    def parse_unsupported_segment(self, *, seg_type: str, data: dict[str, Any]) -> ParsedSegment:
        """把未支持的 segment 降级为可读占位。

        Args:
            seg_type: segment 类型。
            data: segment data（当前未使用，仅用于保留调试信息入口）。

        Returns:
            一个 text 类型的 `ParsedSegment`，内容为 `[CQ:{seg_type}]`。
        """
        # 占位：输出 [CQ:type]
        return ParsedSegment(type="text", data={"text": f"[CQ:{seg_type}]"})

    @staticmethod
    def _first_non_empty(data: dict[str, Any], keys: list[str]) -> str:
        for k in keys:
            v = data.get(k)
            if v is None:
                continue
            s = str(v).strip()
            if s != "":
                return s
        return ""

    @staticmethod
    def _build_text_segment_result(text: str) -> ParsedSegmentResult:
        """构造纯文本 segment 的统一结果。"""

        value = str(text or "")
        return ParsedSegmentResult(
            segment=ParsedSegment(type="text", data={"text": value}),
            cq_part=value,
            plain_part=value,
        )

    @staticmethod
    def _append_segment_result(out: ParsedMessage, result: ParsedSegmentResult) -> None:
        """把单个 segment 归一化结果合并到 ParsedMessage。"""

        if result.cq_part:
            out.cq_text += result.cq_part
        if result.plain_part:
            out.plain_text += result.plain_part
        if result.at_qq:
            out.at_qq.append(result.at_qq)
        if result.reply_id and not out.reply_id:
            out.reply_id = result.reply_id
        if result.forward_id and not out.forward_id:
            out.forward_id = result.forward_id
        if result.media_urls:
            out.media.extend(result.media_urls)
        if result.media_meta:
            out.media_meta.extend(result.media_meta)
        out.segments.append(result.segment)

    def _parse_segment_result(self, *, seg_type: str, kv: dict[str, Any]) -> ParsedSegmentResult:
        """把单个 segment 归一化为统一中间结果。"""

        field_keys = self._CQ_FIELD_MAP.get(seg_type)
        if field_keys is None:
            unsupported = self.parse_unsupported_segment(seg_type=seg_type, data=kv)
            return ParsedSegmentResult(segment=unsupported, cq_part=f"[CQ:{seg_type}]")

        field_val = self._first_non_empty(kv, field_keys)
        result = ParsedSegmentResult(
            segment=(
                self._build_text_segment_result(field_val).segment
                if seg_type == "text"
                else ParsedSegment(type=seg_type, data=kv)
            )
        )

        if seg_type == "text":
            return self._build_text_segment_result(field_val)

        if seg_type == "at" and field_val and field_val != "all":
            result.at_qq = field_val
        elif seg_type == "reply" and field_val:
            result.reply_id = field_val
        elif seg_type == "forward" and field_val:
            result.forward_id = field_val
        elif seg_type == "image":
            url = str(kv.get("url") or "").strip()
            if url.startswith("https://"):
                result.media_urls.append(url)
                result.media_meta.append(
                    {
                        "type": "image",
                        "url": url,
                        "file": str(kv.get("file") or kv.get("name") or "").strip(),
                    }
                )

        result.cq_part = (
            f"[CQ:{seg_type},{field_keys[0]}={field_val}]" if field_val else f"[CQ:{seg_type}]"
        )
        return result

    def _parse_array(self, message: list[dict[str, Any]]) -> ParsedMessage:
        """解析 OneBot 的 array message。

        Args:
            message: segment 数组，每个元素通常形如 `{type: ..., data: ...}`。

        Returns:
            解析后的消息结构，其中包含：
            - `cq_text`: 精简 CQ 文本
            - `plain_text`: 仅文本部分
            - `segments`: 原始 segment（做过最小清洗）
            - `reply_id/forward_id/at_qq/media`: 解析出来的结构化字段
        """
        out = ParsedMessage()

        for seg in message:
            if not isinstance(seg, dict):
                continue
            seg_type = str(seg.get("type") or "").strip()
            data = seg.get("data")
            kv = dict(data) if isinstance(data, dict) else {}
            self._append_segment_result(
                out,
                self._parse_segment_result(seg_type=seg_type, kv=kv),
            )

        out.cq_text = out.cq_text.strip()
        out.plain_text = out.plain_text.strip()
        return out

    def _parse_cq_string(self, s: str) -> ParsedMessage:
        """解析 CQ 字符串，并重建为“精简 CQ”。

        目标：
          - 避免 raw_message 里携带大量无用字段（url/cache/sub_type/raw 等）污染上下文
          - 仅保留 9 种类型的关键字段（见 _CQ_FIELD_MAP）
          - 保留 CQ 段之间的普通文本
        """

        out = ParsedMessage()
        txt = str(s or "")
        if not txt:
            return out

        pos = 0
        for m in self._cq_re.finditer(txt):
            start, end = m.span()
            if start > pos:
                t = txt[pos:start]
                if t:
                    self._append_segment_result(out, self._build_text_segment_result(t))

            seg_type = str(m.group(1) or "").strip()
            rest = m.group(2) or ""
            kv: dict[str, Any] = {}
            if rest:
                for item in rest.split(","):
                    if "=" in item:
                        k, v = item.split("=", 1)
                        kv[k.strip()] = v.strip()

            self._append_segment_result(
                out,
                self._parse_segment_result(seg_type=seg_type, kv=kv),
            )
            pos = end

        if pos < len(txt):
            t = txt[pos:]
            if t:
                self._append_segment_result(out, self._build_text_segment_result(t))

        out.cq_text = out.cq_text.strip()
        out.plain_text = out.plain_text.strip()
        return out


class NapCatMessageDetailFetcher:
    """获取 reply/forward 的消息详情，并转换为内部可用结构。

    该类封装了对 `NapCatTransport.call_action()` 的调用，并复用
    `NapCatMessageParser` 把返回的 message/raw_message 解析为精简 CQ 文本。

    主要用于：
    - 展开引用消息（reply）
    - 展开合并转发（forward）

    注意：该类只做“抓取与最小解析”，下载媒体与更深层的归一化在其它层完成。
    """

    def __init__(self, *, transport: "NapCatTransport", parser: NapCatMessageParser) -> None:
        """初始化消息详情获取器。

        Args:
            transport: NapCat WS transport（负责 action 调用）。
            parser: 消息解析器（负责把 message/raw_message 解析成精简 CQ）。
        """
        self._transport = transport
        self._parser = parser

    def _parse_detail_message(self, *, message: Any, raw_message: Any) -> ParsedMessage:
        """解析 get_msg / get_forward_msg 返回的消息体。"""

        payload_like = {
            "message": message,
            "raw_message": raw_message,
            "message_format": "array" if isinstance(message, list) else "string",
        }
        return self._parser.parse(payload=payload_like, self_id="")

    def _truncate_detail_text(self, text: str) -> str:
        """按 context_message_max_chars 截断详情文本。"""

        value = str(text or "")
        try:
            limit = int(getattr(self._transport.config, "context_message_max_chars", 0) or 0)
        except Exception:
            limit = 0
        if limit > 0 and len(value) > limit:
            return value[:limit] + TRUNCATION_TAG
        return value

    @staticmethod
    def _build_forward_source(*, chat: ChatRef, group_id: Any) -> str:
        """构造 forward 节点来源标记。"""

        if chat.chat_type != "private":
            return ""
        gid = str(group_id or "").strip()
        return f"g:{gid}" if gid else ""

    async def expand_reply(self, *, message_id: str, chat: ChatRef) -> ExpandedReply:
        """展开引用消息（reply）。

        Args:
            message_id: 被引用的 message_id。
            chat: 当前聊天信息（用于在必要时展开 forward）。

        Returns:
            展开后的引用消息结构。若获取失败，至少会返回包含 `id` 的结构。
        """
        mid = str(message_id or "").strip()
        if not mid:
            return ExpandedReply(id="")

        resp = await self._transport.call_action(ACTION_GET_MSG, {"message_id": mid}, timeout=10.0)
        if not (
            isinstance(resp, dict)
            and resp.get("status") == "ok"
            and int(resp.get("retcode", -1)) == 0
        ):
            return ExpandedReply(id=mid)

        data = resp.get("data")
        if not isinstance(data, dict):
            return ExpandedReply(id=mid)

        sender = data.get("sender")
        if not isinstance(sender, dict):
            sender = {}

        uid = str(sender.get("user_id") or data.get("user_id") or "").strip()
        u = str(sender.get("card") or sender.get("nickname") or uid or "unknown").strip()

        parsed = self._parse_detail_message(
            message=data.get("message"),
            raw_message=data.get("raw_message"),
        )
        text = parsed.cq_text or str(data.get("raw_message") or "").strip()

        # 额外：保留引用消息的原始 segments + 结构化 media_meta（便于下载引用消息图片）
        media_meta = list(getattr(parsed, "media_meta", None) or [])

        # 如果引用原消息里包含 forward，可提前展开引用 forward（后续可复用 forward_cache）
        fw: Optional[ExpandedForward] = None
        if parsed.forward_id:
            fw = await self.expand_forward(forward_id=parsed.forward_id, chat=chat)

        return ExpandedReply(
            id=mid,
            sender_name=u,
            sender_id=uid,
            text=text,
            forward=fw,
            message=data.get("message"),
            raw_message=data.get("raw_message"),
            media_meta=media_meta,
        )

    async def expand_forward(self, *, forward_id: str, chat: ChatRef) -> ExpandedForward:
        """展开合并转发（forward）。

        Args:
            forward_id: forward id。
            chat: 当前聊天信息（用于私聊场景下补充节点来源 src）。

        Returns:
            展开后的转发结构。若获取失败，至少会返回包含 `id` 的结构。
        """
        fid = str(forward_id or "").strip()
        if not fid:
            return ExpandedForward(id="")

        resp = await self._transport.call_action(ACTION_GET_FORWARD_MSG, {"id": fid}, timeout=15.0)
        if not (
            isinstance(resp, dict)
            and resp.get("status") == "ok"
            and int(resp.get("retcode", -1)) == 0
        ):
            return ExpandedForward(id=fid)

        data = resp.get("data")
        if not isinstance(data, dict):
            return ExpandedForward(id=fid)

        nodes = data.get("messages")
        if not isinstance(nodes, list):
            return ExpandedForward(id=fid)

        items: list[ForwardItemRecord] = []
        media_meta: list[dict[str, Any]] = []
        for node in nodes:
            if isinstance(node, str):
                parsed = self._parse_detail_message(message=node, raw_message=node)
                text = self._truncate_detail_text(parsed.cq_text or str(node))
                items.append(
                    ForwardItemRecord(
                        sender_name="unknown",
                        sender_id="",
                        message_id="",
                        text=text,
                    )
                )
                if parsed.media_meta:
                    media_meta.extend(list(parsed.media_meta))
                continue

            if not isinstance(node, dict):
                continue

            sender = node.get("sender")
            if not isinstance(sender, dict):
                sender = {}
            uid = str(node.get("user_id") or sender.get("user_id") or "").strip()
            u = str(sender.get("card") or sender.get("nickname") or uid or "unknown").strip()
            parsed = self._parse_detail_message(
                message=node.get("message"),
                raw_message=node.get("raw_message"),
            )
            text = self._truncate_detail_text(
                parsed.cq_text or str(node.get("raw_message") or "").strip()
            )
            items.append(
                ForwardItemRecord(
                    sender_name=u,
                    sender_id=uid,
                    message_id=str(node.get("message_id") or "").strip(),
                    text=text,
                    source=self._build_forward_source(chat=chat, group_id=node.get("group_id")),
                )
            )
            if parsed.media_meta:
                media_meta.extend(list(parsed.media_meta))

        summary = ""
        try:
            summary = str(data.get("summary") or data.get("prompt") or "").strip()
        except Exception:
            summary = ""
        return ExpandedForward(id=fid, items=items, media_meta=media_meta, summary=summary)


@dataclass(slots=True)
class SessionBufferState:
    """单个 session 的内存状态。

    - messages: 用于构建 <NAPCAT_WS_CONTEXT> 的 JSON-lines 消息窗口。
    - bot_message_ids: bot 已发送的 message_id（用于 reply_to_bot 触发兜底判断）。

    说明：
        - 不做旧兼容：不再维护 ConversationStore / ChatRecord。
        - 只保留最近 N 条（N=buffer_size）。
    """

    messages: Deque[ContextMessageRecord]
    bot_message_ids: Deque[str]


class SessionBufferStore:
    """Session buffer store（进程内存）。

    设计目标：
      - 单一事实来源：上下文与 reply_to_bot 判断都从这里取。
      - 轻量、可控：只存最近 N 条，进程重启即清空。
    """

    def __init__(self, buffer_size: int) -> None:
        self._buffer_size = int(buffer_size or 50)
        self._sessions: dict[str, SessionBufferState] = {}

    def get_or_create(self, session_key: str) -> SessionBufferState:
        """获取或创建 session 状态。

        Args:
            session_key: 会话 key（见 `make_session_key`）。

        Returns:
            对应的 `SessionBufferState`。
        """
        if session_key not in self._sessions:
            self._sessions[session_key] = SessionBufferState(
                messages=deque(maxlen=self._buffer_size),
                bot_message_ids=deque(maxlen=max(self._buffer_size, 50)),
            )
        return self._sessions[session_key]

    def append_message(self, session_key: str, record: ContextMessageRecord) -> None:
        """向指定 session 追加一条上下文消息。

        Args:
            session_key: 会话 key。
            record: 上下文记录。
        """
        self.get_or_create(session_key).messages.append(record)

    def recent_messages(self, session_key: str, limit: int) -> list[ContextMessageRecord]:
        """获取指定 session 的最近 N 条消息。

        Args:
            session_key: 会话 key。
            limit: 返回条数上限；<=0 返回空列表。

        Returns:
            最近的上下文记录列表（按原始顺序）。
        """
        if limit <= 0:
            return []
        state = self.get_or_create(session_key)
        return list(state.messages)[-int(limit) :]

    def remember_bot_message_id(self, session_key: str, message_id: str) -> None:
        """记录机器人发出的 message_id。

        用于后续的 reply_to_bot 触发判断（兜底：引用 bot 的历史 message_id）。

        Args:
            session_key: 会话 key。
            message_id: message_id。
        """
        mid = str(message_id or "").strip()
        if not mid:
            return
        self.get_or_create(session_key).bot_message_ids.append(mid)

    def is_bot_message_id(self, session_key: str, message_id: str) -> bool:
        """判断 message_id 是否属于机器人在该 session 内发出的消息。

        Args:
            session_key: 会话 key。
            message_id: 待判断的 message_id。

        Returns:
            若命中已记录集合则返回 True。
        """
        mid = str(message_id or "").strip()
        if not mid:
            return False
        return mid in self.get_or_create(session_key).bot_message_ids


# ==============================
# 2) Inbound normalize
# ==============================


def make_session_key(chat: ChatRef) -> str:
    """根据 chat 生成稳定的 session key。"""

    return f"napcat:{chat.chat_type}:{chat.chat_id}"


def normalize_inbound(
    payload: dict[str, Any],
    *,
    self_id: str,
    config: NapCatWSConfig,
    message_parser: NapCatMessageParser | None = None,
) -> NormalizedInbound | None:
    """将 OneBot payload 归一化为内部消息结构。

    Inbound 主链路只依赖消息解析器：
        - 仅处理 post_type == "message"
        - 统一在 normalize 阶段完成 plain/rendered/reply/forward/@/media 抽取
        - 不再混用 legacy message parser

    说明：
        - reply/forward 的“展开拉取”（get_msg / get_forward_msg）放在 channel 回调里做
        - system prompt 不在这里注入；这里只产出聊天消息语义
    """

    if not isinstance(payload, dict):
        return None

    # 兼容你贴的 debug 日志格式：{"stringMsg":{...},"arrayMsg":{...}}
    if "post_type" not in payload:
        wrapped = None
        if isinstance(payload.get("arrayMsg"), dict):
            wrapped = payload.get("arrayMsg")
        elif isinstance(payload.get("stringMsg"), dict):
            wrapped = payload.get("stringMsg")
        if isinstance(wrapped, dict):
            payload = wrapped

    if payload.get("post_type") != "message":
        return None

    message_type = payload.get("message_type")
    chat_type: ChatType = "group" if message_type == "group" else "private"

    user_id = str(payload.get("user_id") or "")
    group_id = str(payload.get("group_id") or "")
    chat_id = group_id if chat_type == "group" else user_id
    if not chat_id:
        return None

    sender = payload.get("sender") or {}
    if not isinstance(sender, dict):
        sender = {}

    sender_name = (
        str(sender.get("card") or "").strip()
        or str(sender.get("nickname") or "").strip()
        or str(sender.get("remark") or "").strip()
        or user_id
        or "unknown"
    )

    message_id = str(payload.get("message_id") or "")

    ts = payload.get("time")
    try:
        timestamp = datetime.fromtimestamp(int(ts)) if ts is not None else datetime.now()
    except Exception:
        timestamp = datetime.now()

    message_field = payload.get("message")
    if message_field in (None, ""):
        message_field = payload.get("raw_message")

    # 构造一个最小 payload 供消息解析器使用：
    # - message: 取 message/raw_message 的兜底
    # - raw_message/message_format: 原样透传
    payload_like = {
        "message": message_field,
        "raw_message": payload.get("raw_message"),
        "message_format": payload.get("message_format"),
    }
    parsed_message = (
        message_parser.parse(payload=payload_like, self_id=str(self_id or ""))
        if message_parser
        else ParsedMessage()
    )

    text = str(parsed_message.plain_text or "").strip()
    rendered_text = str(
        parsed_message.rendered_text or parsed_message.cq_text or text or ""
    ).strip()
    rendered_segments = list(parsed_message.rendered_segments or [])

    reply_message_id = str(parsed_message.reply_id or "")
    reply: QuotedMessage | None = None
    if reply_message_id:
        reply = QuotedMessage(message_id=reply_message_id)

    sid = str(self_id or "").strip()
    at_self = bool(sid and sid in [str(x).strip() for x in parsed_message.at_qq])
    forward_id = str(parsed_message.forward_id or "")

    return NormalizedInbound(
        chat=ChatRef(
            chat_type=chat_type,
            chat_id=str(chat_id),
            user_id=user_id,
            group_id=group_id,
        ),
        sender_id=user_id,
        sender_name=sender_name,
        message_id=message_id,
        timestamp=timestamp,
        text=text,
        rendered_text=rendered_text,
        rendered_segments=rendered_segments,
        media=list(parsed_message.media or []),
        at_self=at_self,
        reply=reply,
        forward_id=forward_id,
        raw_event=payload,
    )


def _safe_media_filename(name: str) -> str:
    """把 OneBot/NapCat 的 file/name 变成可展示的安全文件名。"""

    s = str(name or "").strip()
    if not s:
        return ""

    # 防止路径穿越：只取 basename，并兼容 "\\" 分隔符
    s = s.replace("\\", "/")
    s = s.split("/")[-1].strip()

    if not s or s in {".", ".."}:
        return ""

    cleaned: list[str] = []
    for ch in s:
        if ch.isalnum() or ch in {".", "_", "-"}:
            cleaned.append(ch)
        else:
            cleaned.append("_")

    safe = "".join(cleaned).strip("._")
    if not safe:
        return ""

    if len(safe) > 180:
        safe = safe[:180]

    return safe


# ==============================
# 3) Policy
# ==============================


def _normalize_text_for_match(text: str) -> str:
    """用于触发词匹配的标准化：小写 + 压缩空白。"""

    s = (text or "").strip().lower()
    # 不做复杂分词，保持“包含即触发”的直觉规则
    return " ".join(s.split())


def _contains_any(text: str, keywords: list[str]) -> bool:
    """规则：消息任意位置包含即触发（大小写不敏感）。"""

    hay = _normalize_text_for_match(text)
    if not hay:
        return False
    for kw in keywords or []:
        k = _normalize_text_for_match(str(kw))
        if k and k in hay:
            return True
    return False


def _stable_random_0_1(seed: str) -> float:
    """稳定伪随机数（0~1）。

    用途：概率触发不依赖全局 random 状态，便于测试/复现。
    """

    import hashlib

    h = hashlib.sha256(seed.encode("utf-8", "ignore")).digest()
    # 取前 8 字节作为无符号整数
    n = int.from_bytes(h[:8], "big", signed=False)
    return (n % 1_000_000) / 1_000_000.0


def _is_allowed_chat_source(
    *,
    chat_type: ChatType,
    chat_id: str,
    sender_id: str,
    config: NapCatWSConfig,
) -> bool:
    """allow_from 白名单判断的底层实现。

    仅支持三种语义：
      - "*"：全部允许
      - "<群号>"：允许对应群聊
      - "<QQ号>"：允许对应私聊 / 对应发送者
    """

    allow = [str(x).strip() for x in (getattr(config, "allow_from", []) or []) if str(x).strip()]
    if not allow:
        # schema 默认是 ["*"]，这里保持“空=不额外限制”的宽松策略
        return True
    if "*" in allow:
        return True

    if chat_type == "group":
        return bool(chat_id and chat_id in allow)
    return bool((chat_id and chat_id in allow) or (sender_id and sender_id in allow))


def _is_allowed_source(msg: NormalizedInbound, config: NapCatWSConfig) -> bool:
    """allow_from 白名单判断（message 版本）。"""

    sender_id = str(msg.sender_id or "").strip()
    if msg.chat.chat_type == "group":
        chat_id = str(msg.chat.group_id or msg.chat.chat_id or "").strip()
    else:
        chat_id = str(msg.chat.user_id or msg.chat.chat_id or sender_id or "").strip()

    return _is_allowed_chat_source(
        chat_type=msg.chat.chat_type,
        chat_id=chat_id,
        sender_id=sender_id,
        config=config,
    )



def _is_reply_to_bot(
    msg: NormalizedInbound, *, store: SessionBufferStore, session_key: str, self_id: str
) -> bool:
    """判断 msg.reply 是否是在回复 bot。"""

    if not msg.reply or not msg.reply.message_id:
        return False

    # 1) 优先使用 quote expand 得到的 sender_id
    if str(msg.reply.sender_id or "").strip() and str(msg.reply.sender_id).strip() == str(self_id):
        return True

    # 2) 兜底：检查 reply 的 message_id 是否是 bot 近期发出的 message_id
    return store.is_bot_message_id(session_key, msg.reply.message_id)


def _message_has_content(msg: NormalizedInbound) -> bool:
    """判断消息是否包含可响应内容。"""

    return (
        bool(str(msg.text or "").strip())
        or bool(msg.media)
        or bool(msg.forward_items)
        or bool(str(msg.forward_summary or "").strip())
        or bool(str(msg.forward_id or "").strip())
    )


def _probability_for_message(
    msg: NormalizedInbound, config: NapCatWSConfig
) -> tuple[float, TriggerType]:
    """返回当前消息适用的概率及触发类型。"""

    if msg.chat.chat_type == "private":
        return float(
            getattr(config, "private_trigger_probability", 0.0) or 0.0
        ), "private_probability"
    return float(getattr(config, "group_trigger_probability", 0.0) or 0.0), "group_probability"


def _message_probability_seed(msg: NormalizedInbound, *, session_key: str) -> str:
    """构造 message 概率触发使用的稳定 seed。"""

    return f"{session_key}|{msg.message_id}|{msg.sender_id}|{msg.timestamp.timestamp()}"


def _check_message_source_gate(
    msg: NormalizedInbound, *, config: NapCatWSConfig, self_id: str
) -> TriggerDecision | None:
    """执行 message 来源侧门禁；放行时返回 None。"""

    if not _is_allowed_source(msg, config):
        return TriggerDecision(False, "not_allowed")

    if getattr(config, "ignore_self_messages", True) and str(msg.sender_id) == str(self_id):
        return TriggerDecision(False, "ignore_self")

    if msg.chat.chat_type == "private":
        blk = set(str(x) for x in (getattr(config, "blacklist_private_ids", []) or []))
        if str(msg.chat.chat_id) in blk or str(msg.sender_id) in blk:
            return TriggerDecision(False, "blacklist_private")
        return None

    blk = set(str(x) for x in (getattr(config, "blacklist_group_ids", []) or []))
    gid = str(msg.chat.group_id or msg.chat.chat_id)
    if gid in blk:
        return TriggerDecision(False, "blacklist_group")
    return None


def _check_message_direct_trigger(
    msg: NormalizedInbound,
    *,
    config: NapCatWSConfig,
    store: SessionBufferStore,
    session_key: str,
    self_id: str,
) -> TriggerDecision | None:
    """执行 message 直接触发检查；未命中时返回 None。"""

    if (
        msg.chat.chat_type == "group"
        and getattr(config, "trigger_on_at", True)
        and bool(msg.at_self)
    ):
        return TriggerDecision(True, "at_bot", "at_bot")

    if getattr(config, "trigger_on_reply_to_bot", True) and _is_reply_to_bot(
        msg, store=store, session_key=session_key, self_id=self_id
    ):
        return TriggerDecision(True, "reply_to_bot", "reply_to_bot")

    nicknames = list(getattr(config, "nickname_triggers", []) or [])
    if nicknames and _contains_any(msg.text, nicknames):
        return TriggerDecision(True, "nickname", "nickname")

    return None


def _decide_message_probability_trigger(
    msg: NormalizedInbound, *, config: NapCatWSConfig, session_key: str
) -> TriggerDecision:
    """执行 message 概率触发决策。"""

    if not _message_has_content(msg):
        return TriggerDecision(False, "no_content")

    probability, trigger = _probability_for_message(msg, config)
    if probability <= 0.0:
        return TriggerDecision(False, "probability=0", trigger, probability)
    if probability >= 1.0:
        return TriggerDecision(True, "probability=1", trigger, probability)

    sample = _stable_random_0_1(_message_probability_seed(msg, session_key=session_key))
    if sample < probability:
        return TriggerDecision(
            True, f"probability({sample:.3f}<{probability:.3f})", trigger, probability
        )
    return TriggerDecision(
        False, f"probability({sample:.3f}>={probability:.3f})", trigger, probability
    )


def decide_message_trigger(
    msg: NormalizedInbound,
    *,
    config: NapCatWSConfig,
    store: SessionBufferStore,
    session_key: str,
    self_id: str,
) -> TriggerDecision:
    """决定 message 事件是否触发回复。

    Args:
        msg: 入站消息。
        config: 渠道配置。
        store: 会话缓存（用于 reply_to_bot 兜底判断等）。
        session_key: 会话 key。
        self_id: 机器人自身 ID。

    Returns:
        触发决策结果。
    """

    gate = _check_message_source_gate(msg, config=config, self_id=self_id)
    if gate is not None:
        return gate

    direct = _check_message_direct_trigger(
        msg,
        config=config,
        store=store,
        session_key=session_key,
        self_id=self_id,
    )
    if direct is not None:
        return direct

    return _decide_message_probability_trigger(msg, config=config, session_key=session_key)


def _is_allowed_notice_source(
    *, actor_id: str, group_id: str, config: NapCatWSConfig
) -> bool:
    """allow_from 白名单判断（notice 版本）。"""

    chat_type: ChatType = "group" if group_id else "private"
    chat_id = str(group_id or actor_id or "").strip()
    sender_id = str(actor_id or "").strip()
    return _is_allowed_chat_source(
        chat_type=chat_type,
        chat_id=chat_id,
        sender_id=sender_id,
        config=config,
    )



def decide_notice_trigger(
    payload: dict[str, Any],
    *,
    config: NapCatWSConfig,
    self_id: str,
) -> TriggerDecision:
    """决定 notice 事件是否触发回复。当前仅支持 poke。"""

    notice_type = payload.get("notice_type")
    sub_type = payload.get("sub_type") or payload.get("notify_type")
    if not (notice_type == "notify" and sub_type == "poke"):
        return TriggerDecision(False, "unsupported_notice")

    if not getattr(config, "trigger_on_poke", False):
        return TriggerDecision(False, "poke_disabled")

    actor = str(payload.get("sender_id") or payload.get("user_id") or "").strip()
    if not actor:
        return TriggerDecision(False, "poke_missing_actor")
    if actor == str(self_id or ""):
        return TriggerDecision(False, "poke_self")

    group_id = str(payload.get("group_id") or "").strip()
    raw_info = payload.get("raw_info")
    if not group_id and isinstance(raw_info, dict):
        for key in ("group_id", "groupId", "peer_uin", "peerUin", "peer_uid", "peerUid"):
            value = raw_info.get(key)
            if value:
                group_id = str(value).strip()
                if group_id:
                    break

    if group_id:
        blk = set(str(x) for x in (getattr(config, "blacklist_group_ids", []) or []))
        if group_id in blk:
            return TriggerDecision(False, "blacklist_group")
    else:
        blk = set(str(x) for x in (getattr(config, "blacklist_private_ids", []) or []))
        if actor in blk:
            return TriggerDecision(False, "blacklist_private")

    if not _is_allowed_notice_source(actor_id=actor, group_id=group_id, config=config):
        return TriggerDecision(False, "not_allowed")

    return TriggerDecision(True, "poke", "poke")


# =========================================
# 4) Outbound helpers
# =========================================

_CQ_OUTBOUND_RE = re.compile(r"\[CQ:([^,\]]+)(?:,([^\]]+))?\]")


def _parse_cq_params(params_raw: str) -> dict[str, str]:
    """解析 CQ 参数串。

    Args:
        params_raw: CQ token 的参数区（不包含前缀类型），例如：
            `"qq=123,id=456"`。

    Returns:
        解析后的 key/value 字典。无法解析的片段会被忽略。
    """
    params: dict[str, str] = {}
    if not params_raw:
        return params
    for chunk in params_raw.split(","):
        part = str(chunk or "").strip()
        if not part:
            continue
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        key = str(key).strip()
        value = str(value).strip()
        if key:
            params[key] = value
    return params


def _file_uri_to_local_path(file_uri: str) -> str | None:
    """把 `file://` URI 转换为本地文件路径。

    Args:
        file_uri: 形如 `file:///abs/path/to/a.png` 的 URI。

    Returns:
        对应的本地路径字符串；若不是 file URI 或无法解析则返回 None。
    """
    raw = str(file_uri or "").strip()
    if not raw:
        return None
    if not raw.startswith("file://"):
        return None
    parsed = urlparse(raw)
    if parsed.scheme != "file":
        return None
    path = os.path.abspath(os.path.expanduser(parsed.path or ""))
    return path or None


def validate_outbound_cq_text(content: str) -> CQValidationResult:
    """轻校验模型生成的 CQ 出站文本。

    当前仅正式支持：
      - [CQ:at,qq=...]
      - [CQ:reply,id=...]
      - [CQ:image,file=file:///...]

    其它 CQ 先视为未支持，要求模型重写。
    """

    text = str(content or "")
    issues: list[CQValidationIssue] = []

    for match in _CQ_OUTBOUND_RE.finditer(text):
        cq_text = match.group(0)
        cq_type = str(match.group(1) or "").strip().lower()
        params = _parse_cq_params(match.group(2) or "")

        if cq_type == "at":
            qq = str(params.get("qq") or "").strip()
            if not qq:
                issues.append(
                    CQValidationIssue("missing_required_param", "at 缺少 qq 参数", cq_text)
                )
                continue
            if qq != "all" and not qq.isdigit():
                issues.append(
                    CQValidationIssue("invalid_param_value", f"at.qq 非法: {qq}", cq_text)
                )
                continue
            continue

        if cq_type == "reply":
            reply_id = str(params.get("id") or "").strip()
            if not reply_id:
                issues.append(
                    CQValidationIssue("missing_required_param", "reply 缺少 id 参数", cq_text)
                )
                continue
            continue

        if cq_type == "image":
            file_uri = str(params.get("file") or "").strip()
            if not file_uri:
                issues.append(
                    CQValidationIssue("missing_required_param", "image 缺少 file 参数", cq_text)
                )
                continue
            local_path = _file_uri_to_local_path(file_uri)
            if not local_path:
                issues.append(
                    CQValidationIssue(
                        "invalid_param_value", f"image.file 必须是 file:// URI: {file_uri}", cq_text
                    )
                )
                continue
            if not os.path.exists(local_path):
                issues.append(
                    CQValidationIssue(
                        "image_file_not_found", f"image.file 不存在: {local_path}", cq_text
                    )
                )
                continue
            if not os.path.isfile(local_path):
                issues.append(
                    CQValidationIssue(
                        "invalid_param_value", f"image.file 不是普通文件: {local_path}", cq_text
                    )
                )
                continue
            continue

        issues.append(
            CQValidationIssue("unsupported_cq_type", f"暂不支持的 CQ 类型: {cq_type}", cq_text)
        )

    return CQValidationResult(valid=not issues, normalized_text=text, issues=issues)


# ==============================
# 5) Transport
# ==============================


class NapCatTransport:
    """Transport 层：负责 WS 连接与 OneBot actions。

    约束：
        - 本层不包含触发策略/上下文构建等业务逻辑。
        - 只负责：连接、收包、发包、action echo/future 关联与超时。

    重要：
        - 不能在接收循环里 `await on_event(payload)`，否则 on_event 内部若调用 call_action()
          会造成死锁（收包停住 -> action_response 无法被消费 -> call_action 超时）。
        - 因此：事件分发必须以 task 方式执行，让接收循环持续运行。

    额外说明（与旧版对齐）：
        - 连接参数尽量复刻旧 napcat_ws.py.bak（open_timeout/ping/close 以及 headers=... or None）。
        - URL 统一规范化为“末尾带 /”。
    """

    def __init__(
        self,
        config: NapCatWSConfig,
        on_event: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> None:
        self.config = config
        self._on_event = on_event

        self._ws: ClientConnection | None = None
        self._running = False

        # echo 关联：action 请求发出后，用 echo 匹配响应
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._echo_counter = 0
        self._lock = asyncio.Lock()

        # 事件处理 tasks（避免阻塞接收循环）
        self._event_tasks: set[asyncio.Task[None]] = set()
        self._login_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """启动 transport（连接 + 接收循环）。

        说明：
            - 本方法是一个“长循环”：直到 stop() 被调用才返回。
            - 自动重连：断开后按指数退避重连。
            - action_response 会在本层被消费（根据 status/retcode 或 echo 命中 pending）。
        """

        if self._running:
            return
        self._running = True

        backoff_s = 1.0
        while self._running:
            ws: ClientConnection | None = None
            try:
                url, ws = await self._connect_ws()
                self._ws = ws
                logger.info("napcat_ws transport connected: {}", url)

                # 连接建立后主动拉取一次登录信息（必须在接收循环开始后/并行执行，避免死锁）
                if self._login_task is None or self._login_task.done():
                    self._login_task = asyncio.create_task(self._fetch_login_info())

                backoff_s = 1.0

                async for raw in ws:
                    if not self._running:
                        break

                    payload = self._decode_payload(raw)
                    if payload is None:
                        continue

                    if self._resolve_pending_response(payload):
                        continue

                    self._spawn_event_task(payload)

            except Exception as e:
                if not self._running:
                    break
                logger.warning("napcat_ws transport reconnect err={} backoff_s={}", e, backoff_s)
                await asyncio.sleep(backoff_s)
                backoff_s = min(backoff_s * 2.0, 30.0)
            finally:
                await self._cleanup_connection(ws)

        # stop() 退出后：兜底清 pending
        self._fail_pending_futures()

    async def stop(self) -> None:
        """停止 transport 并清理 pending futures。"""

        if not self._running:
            return
        self._running = False

        # 主动关闭 websocket（会让 start() 的 async for 退出）
        await self._close_ws(self._ws)
        self._ws = None

        self._cancel_event_tasks()
        self._cancel_login_task()
        self._fail_pending_futures()

    @staticmethod
    def _build_connect_headers(token: str) -> dict[str, str] | None:
        """构造 websocket 连接头。"""

        token_value = str(token or "").strip()
        if not token_value:
            return None
        return {"Authorization": f"Bearer {token_value}"}

    @staticmethod
    def _build_connect_kwargs(headers: dict[str, str] | None) -> dict[str, Any]:
        """构造 websocket connect 参数。"""

        return {
            "open_timeout": 10,
            "ping_interval": 20,
            "ping_timeout": 20,
            "close_timeout": 10,
            "additional_headers": headers,
        }

    async def _connect_ws(self) -> tuple[str, ClientConnection]:
        """建立 websocket 连接。"""

        url = self._normalize_ws_url(self.config.url)
        connect_kwargs = self._build_connect_kwargs(
            self._build_connect_headers(self.config.token)
        )
        ws = await websockets.connect(url, **connect_kwargs)
        return url, ws

    async def _cleanup_connection(self, ws: ClientConnection | None) -> None:
        """清理单次连接相关状态。"""

        self._cancel_event_tasks()
        self._cancel_login_task()
        self._fail_pending_futures()
        await self._close_ws(ws)
        self._ws = None

    def _cancel_event_tasks(self) -> None:
        """取消并清空事件处理 tasks。"""

        for task in list(self._event_tasks):
            task.cancel()
        self._event_tasks.clear()

    def _cancel_login_task(self) -> None:
        """取消登录信息拉取 task。"""

        if self._login_task is not None and not self._login_task.done():
            self._login_task.cancel()
        self._login_task = None

    def _fail_pending_futures(self) -> None:
        """让所有待返回的 action future 以取消结束。"""

        for fut in list(self._pending.values()):
            if not fut.done():
                fut.set_exception(asyncio.CancelledError())
        self._pending.clear()

    @staticmethod
    async def _close_ws(ws: ClientConnection | None) -> None:
        """关闭 websocket 连接，忽略 close 阶段异常。"""

        try:
            if ws is not None:
                await ws.close()
        except Exception:
            pass

    def _spawn_event_task(self, payload: dict[str, Any]) -> None:
        """以 task 方式执行 on_event，避免阻塞接收循环。"""

        summary = summarize_onebot_event(payload)

        async def _runner() -> None:
            await self._on_event(payload)

        task = asyncio.create_task(_runner())
        self._event_tasks.add(task)

        def _done(t: asyncio.Task[None]) -> None:
            self._event_tasks.discard(t)
            try:
                t.result()
            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception("napcat_ws transport on_event_failed summary={}", summary)

        task.add_done_callback(_done)

    @staticmethod
    def _decode_payload(raw: str | bytes | bytearray) -> dict[str, Any] | None:
        """把 websocket frame 解码成 dict payload。"""

        try:
            if isinstance(raw, (bytes, bytearray)):
                raw = bytes(raw).decode("utf-8", "ignore")
            payload = json.loads(raw)
            return payload if isinstance(payload, dict) else None
        except Exception as e:
            logger.warning("napcat_ws transport invalid_frame err={}", e)
            return None

    def _resolve_pending_response(self, payload: dict[str, Any]) -> bool:
        """处理命中 pending 的 action 响应。"""

        echo = payload.get("echo")
        if echo is None:
            if ("status" in payload) and ("retcode" in payload):
                logger.debug(
                    "napcat_ws transport action_missing_echo keys={}",
                    sorted(payload.keys()),
                )
                return True
            return False

        key = str(echo)
        fut = self._pending.pop(key, None)
        if fut is None or fut.done():
            return ("status" in payload) and ("retcode" in payload)

        if ("status" in payload) and ("retcode" in payload):
            logger.debug(
                "napcat_ws action_recv echo={} status={} retcode={}",
                key,
                payload.get("status"),
                payload.get("retcode"),
            )
        fut.set_result(payload)
        return True

    @staticmethod
    def _normalize_ws_url(url: str) -> str:
        """Normalize WS url for NapCat.

        Keep it conservative:
        - Ensure path is at least "/".
        - Ensure trailing slash exists.
        """

        try:
            u = urlparse(url)
            path = u.path or "/"
            if not path.endswith("/"):
                path = path + "/"
            return urlunparse((u.scheme, u.netloc, path, u.params, u.query, u.fragment))
        except Exception:
            # Fallback: best-effort
            return url if url.endswith("/") else (url + "/")

    async def _fetch_login_info(self) -> None:
        """连接后拉取一次登录信息（可观测性），失败不影响主流程。"""

        try:
            login = await self.call_action(ACTION_GET_LOGIN_INFO, params={}, timeout=10.0)
            if isinstance(login, dict):
                data = login.get("data") if isinstance(login.get("data"), dict) else {}
                if isinstance(data, dict):
                    uid = data.get("user_id")
                    nick = data.get("nickname")
                    if uid is not None:
                        logger.info("napcat_ws login_info user_id={} nickname={}", uid, nick)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.debug("napcat_ws login_info_failed err={}", e)

    async def call_action(
        self, action: str, params: dict[str, Any], timeout: float = 10.0
    ) -> dict[str, Any]:
        """调用 OneBot action，并等待响应。"""

        if not self._ws:
            raise RuntimeError("napcat_ws transport is not connected")

        async with self._lock:
            self._echo_counter += 1
            # Match legacy echo style: prefix + ms timestamp for better uniqueness/compat.
            echo = f"nanobot:{action}:{int(datetime.now().timestamp() * 1000)}:{self._echo_counter}"

        loop = asyncio.get_running_loop()
        fut: asyncio.Future[dict[str, Any]] = loop.create_future()
        self._pending[echo] = fut

        req = {"action": action, "params": params, "echo": echo}
        logger.debug("napcat_ws action_send action={} echo={}", action, echo)
        await self.send_json(req)

        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            self._pending.pop(echo, None)
            raise TimeoutError(f"OneBot action timeout: {action}")

    async def send_json(self, payload: dict[str, Any]) -> None:
        """通过 websocket 发送一条原始 JSON payload。"""

        if not self._ws:
            raise RuntimeError("napcat_ws transport is not connected")

        try:
            await self._ws.send(json.dumps(payload, ensure_ascii=False))
        except Exception as e:
            raise RuntimeError(f"napcat_ws transport send failed: {e}")


# ==============================
# 6) Channel
# ==============================


class NapCatWSChannel(BaseChannel):
    """NapCat WS 渠道。

    该类负责把各层串起来：
        Transport -> Normalize -> Policy -> Context -> BaseChannel._handle_message

    注意：
        - 业务逻辑尽量落在各层函数/类里
        - Channel 本身只做编排与状态持有
    """

    name = "napcat_ws"
    display_name = "NapCat WS"

    @classmethod
    def default_config(cls) -> dict[str, Any]:
        return NapCatWSConfig().model_dump(by_alias=True)

    def __init__(self, config: Any, bus: MessageBus) -> None:
        if isinstance(config, dict):
            config = NapCatWSConfig.model_validate(config)
        super().__init__(config=config, bus=bus)

        self.config: NapCatWSConfig = config
        self.bus: MessageBus = bus

        self._self_id: str = ""
        self._self_account_nickname: str = ""  # get_login_info.nickname

        # bot 在各群的显示名缓存：card -> 账号昵称 -> self_id
        # value: {"name": str, "ts": float}
        self._bot_name_by_group: dict[str, dict[str, Any]] = {}
        self._bot_name_ttl_s: float = 86400.0  # 1d

        self._media_dir = get_media_dir() / "napcat_ws"
        self._media_dir.mkdir(parents=True, exist_ok=True)

        self._store = SessionBufferStore(buffer_size=self.config.session_buffer_size)
        self._transport = NapCatTransport(config=self.config, on_event=self._on_transport_event)

        self._message_parser = NapCatMessageParser()
        self._message_cache = InMemoryMessageCache(
            ttl_seconds=DEFAULT_CACHE_TTL_SECONDS,
            reply_maxsize=REPLY_CACHE_MAXSIZE,
            forward_maxsize=FORWARD_CACHE_MAXSIZE,
        )
        self._detail_fetcher = NapCatMessageDetailFetcher(
            transport=self._transport, parser=self._message_parser
        )
        self._context_builder = NapCatContextBuilder()

        # 戳一戳(poke) 冷却：key 为 "{chat_type}:{chat_id}"，value 为上次触发时间戳(time.time())。
        # 该冷却只用于 notice(poke) 链路，避免被连续戳导致刷屏。
        self._poke_last_ts_by_chat: dict[str, float] = {}

    def _now_ts(self) -> float:
        return datetime.now().timestamp()

    def _truncate_user_text(self, s: str) -> str:
        """Truncate a single user message to keep context readable.

        Rule (per config.context_message_max_chars):
        - limit <= 0: no truncation
        - len(text) > limit: text[:limit] + TRUNCATION_TAG

        NOTE: This is for context/input shaping only (not for outbound bot replies).
        """

        limit = int(getattr(self.config, "context_message_max_chars", 0) or 0)
        text = str(s or "")
        if limit <= 0:
            return text
        if len(text) <= limit:
            return text
        return text[:limit] + TRUNCATION_TAG

    async def _ensure_login_info(self) -> None:
        """按需拉取登录信息，填充 self_id 与账号昵称。"""

        if self._self_id and self._self_account_nickname:
            return

        try:
            resp = await self._transport.call_action(ACTION_GET_LOGIN_INFO, params={}, timeout=10.0)
            if isinstance(resp, dict) and resp.get("status") == "ok":
                data = resp.get("data") if isinstance(resp.get("data"), dict) else {}
                if isinstance(data, dict):
                    uid = data.get("user_id")
                    nick = data.get("nickname")
                    if uid is not None and not self._self_id:
                        self._self_id = str(uid)
                    if nick is not None:
                        self._self_account_nickname = str(nick).strip()
        except Exception:
            return

    async def _get_bot_display_name_for_group(self, group_id: str) -> str:
        """获取 bot 在某群的显示名（缓存 + TTL 刷新）。

        优先级：群名片(card) -> 账号昵称(get_login_info.nickname) -> self_id
        """

        gid = str(group_id or "").strip()
        if not gid:
            return (
                str(getattr(self.config, "self_nickname", "") or "").strip()
                or self._self_account_nickname
                or str(self._self_id or "").strip()
                or "bot"
            )

        await self._ensure_login_info()

        # cache hit
        cached = self._bot_name_by_group.get(gid)
        if isinstance(cached, dict):
            name = str(cached.get("name") or "").strip()
            ts = float(cached.get("ts") or 0.0)
            if name and (self._now_ts() - ts) < self._bot_name_ttl_s:
                return name

        # refresh by get_group_member_info
        try:
            if not self._self_id:
                raise RuntimeError("missing self_id")

            params = {"group_id": gid, "user_id": str(self._self_id), "no_cache": False}
            resp = await self._transport.call_action(
                ACTION_GET_GROUP_MEMBER_INFO, params=params, timeout=10.0
            )
            if isinstance(resp, dict) and resp.get("status") == "ok":
                data = resp.get("data") if isinstance(resp.get("data"), dict) else {}
                if isinstance(data, dict):
                    card = str(data.get("card") or "").strip()
                    nick = str(data.get("nickname") or "").strip()
                    name = card or nick or self._self_account_nickname or str(self._self_id)
                    self._bot_name_by_group[gid] = {"name": name, "ts": self._now_ts()}
                    return name
        except Exception:
            pass

        # fallback
        name = (
            str(getattr(self.config, "self_nickname", "") or "").strip()
            or self._self_account_nickname
            or str(self._self_id or "").strip()
            or "bot"
        )
        self._bot_name_by_group[gid] = {"name": name, "ts": self._now_ts()}
        return name

    async def start(self) -> None:
        """启动渠道。"""

        self._running = True
        logger.info("{} starting", self.name)
        await self._transport.start()

    async def stop(self) -> None:
        """停止渠道。"""

        self._running = False
        logger.info("{} stopping", self.name)

        await self._transport.stop()

    def _normalize_outbound_cq_images(self, text: str) -> str:
        """尽量把模型生成的 CQ:image 参数规范化成 NapCat/OneBot 更可能接受的形式。

        仅处理一种场景：
        - [CQ:image,file=xxxxx.jpg] 这种 *非 URI* 的 file 参数。

        处理策略：
        - 若 file 不是 URI（不以 file:// / http(s):// / base64:// 开头），则视为“本地路径或文件名”
        - 如果是相对路径/文件名，则默认在本 channel 的 media 目录下查找
        - 找到真实文件后改写为 file:// 绝对路径

        注意：这里不做 fallback（例如把 group_id 当 chat_id），也不猜测远端路径。
        """

        s = str(text or "")
        if "[CQ:" not in s:
            return s

        out: list[str] = []
        pos = 0
        for m in _CQ_OUTBOUND_RE.finditer(s):
            out.append(s[pos : m.start()])

            cq_text = m.group(0)
            cq_type = str(m.group(1) or "").strip().lower()
            params_raw = m.group(2) or ""

            if cq_type != "image":
                out.append(cq_text)
                pos = m.end()
                continue

            params = _parse_cq_params(params_raw)
            file_val = str(params.get("file") or "").strip()

            # 已经是 URI（或 base64）则不动
            lower = file_val.lower()
            if (
                not file_val
                or lower.startswith("file://")
                or lower.startswith("http://")
                or lower.startswith("https://")
                or lower.startswith("base64://")
            ):
                out.append(cq_text)
                pos = m.end()
                continue

            p = Path(os.path.expanduser(file_val))
            if not p.is_absolute():
                p = self._media_dir / file_val

            try:
                if p.exists() and p.is_file():
                    params["file"] = f"file://{p.resolve()}"
                    params_str = ",".join(f"{k}={v}" for k, v in params.items() if k)
                    out.append(f"[CQ:image,{params_str}]")
                else:
                    out.append(cq_text)
            except Exception:
                out.append(cq_text)

            pos = m.end()

        out.append(s[pos:])
        return "".join(out)

    async def send(self, msg: OutboundMessage) -> None:
        """从 MessageBus 收到 OutboundMessage 后，发送到平台。

        当前出站层采用 CQ-first 薄架构：
        - 主载体是文本
        - 文本中允许内嵌 CQ 码
        - 发送前只做轻校验，不做复杂 segment builder
        - 当前正式支持的 CQ：at / reply / image
        """

        if not msg or not isinstance(msg, OutboundMessage):
            return

        content = str(msg.content or "")
        content = self._normalize_outbound_cq_images(content)

        if not content.strip() and not msg.media:
            return

        validation = validate_outbound_cq_text(content)
        if not validation.valid:
            # 放宽：CQ 码校验失败也允许出站。
            # 原因：不同 OneBot/NapCat 版本对 CQ 参数要求可能不同（例如 image.file 是否必须是 file:// URI）。
            # 这里仅记录告警，避免因为本地校验过严导致消息被直接丢弃。
            detail = "; ".join(f"{x.code}:{x.message}" for x in validation.issues[:5])
            chat_type = str(msg.metadata.get("chat_type") or "")
            gid = msg.metadata.get("group_id") or msg.chat_id
            uid = msg.metadata.get("user_id") or msg.chat_id
            target = gid if chat_type == "group" else uid
            logger.warning(
                "napcat_ws outbound invalid_cq allow_anyway detail={} chat_type={} target={}",
                detail,
                chat_type,
                target,
            )
        else:
            content = validation.normalized_text

        action = (
            "send_group_msg" if msg.metadata.get("chat_type") == "group" else "send_private_msg"
        )

        params: dict[str, Any] = {"message": content}
        if action == "send_group_msg":
            gid = msg.metadata.get("group_id") or msg.chat_id
            params["group_id"] = int(gid) if str(gid).isdigit() else gid
        else:
            uid = msg.metadata.get("user_id") or msg.chat_id
            params["user_id"] = int(uid) if str(uid).isdigit() else uid

        try:
            resp = await self._transport.call_action(action, params=params, timeout=10.0)
        except Exception as exc:
            logger.debug("napcat_ws send_failed err={}", exc)
            return

        # 记录平台返回的失败，避免“只看到告警但不知道为什么没发出去”。
        if not (isinstance(resp, dict) and resp.get("status") == "ok"):
            logger.warning(
                "napcat_ws send_not_ok action={} target={} resp={}",
                action,
                params.get("group_id") or params.get("user_id") or "",
                resp,
            )
            return

        try:
            if isinstance(resp, dict) and resp.get("status") == "ok":
                data = resp.get("data")
                mid = None
                if isinstance(data, dict):
                    mid = data.get("message_id") or data.get("messageId")
                if mid is not None:
                    session_key = msg.metadata.get("session_key")
                    if session_key:
                        sender_name = None
                        try:
                            if msg.metadata.get("chat_type") == "group":
                                gid = str(msg.metadata.get("group_id") or msg.chat_id or "").strip()
                                sender_name = await self._get_bot_display_name_for_group(gid)
                        except Exception:
                            sender_name = None
                        if not sender_name:
                            sender_name = (
                                str(getattr(self.config, "self_nickname", "") or "").strip()
                                or self._self_account_nickname
                                or str(self._self_id or "").strip()
                                or "bot"
                            )

                        self._store.remember_bot_message_id(session_key, str(mid))
                        self._store.append_message(
                            session_key,
                            ContextMessageRecord(
                                sender_name=str(sender_name),
                                sender_id=str(self._self_id or ""),
                                message_id=str(mid),
                                text=str(content),
                                chat_type=str(msg.metadata.get("chat_type") or "group"),
                                group_id=str(msg.metadata.get("group_id") or ""),
                                is_bot=True,
                            ),
                        )
        except Exception:
            pass

    # ------------------------------
    # Transport callback
    # ------------------------------

    async def _on_transport_event(self, payload: dict[str, Any]) -> None:
        """处理 transport 收到的原始 OneBot payload。"""

        if not isinstance(payload, dict):
            return

        post_type = payload.get("post_type")
        summary = summarize_onebot_event(payload)

        if post_type == "meta_event":
            self._maybe_update_self_id(payload, source="meta_event")
            return

        self._maybe_update_self_id(payload, source="event")

        if post_type == "notice":
            try:
                await self._handle_notice(payload)
            except Exception:
                logger.exception("napcat_ws notice_handling_failed summary={}", summary)
                raise
            return

        if post_type != "message":
            self._log_ignored_event(payload)
            return

        try:
            await self._handle_message(payload)
        except Exception:
            logger.exception("napcat_ws message_handling_failed summary={}", summary)
            raise

    # ------------------------------
    # meta/self id
    # ------------------------------

    def _maybe_update_self_id(self, payload: dict[str, Any], *, source: str) -> None:
        sid = payload.get("self_id")
        if sid is None:
            return

        new_sid = str(sid)
        if not new_sid or new_sid == self._self_id:
            return

        old_sid = self._self_id
        self._self_id = new_sid

        # self_id 通常不变：避免 INFO 刷屏。
        # 首次设置仅用 DEBUG 记录；只有真的发生变化才 WARNING。
        if not old_sid:
            logger.debug("napcat_ws self_id_set source={} self_id={}", source, self._self_id)
        else:
            logger.warning(
                "napcat_ws self_id_changed old={} new={} source={}",
                old_sid,
                self._self_id,
                source,
            )

    # ------------------------------
    # notice
    # ------------------------------

    async def _handle_notice(self, payload: dict[str, Any]) -> None:
        """Handle OneBot notice events.

        当前主链路仅把 poke(notify/poke) 视为可触发事件。

        设计目标：notice 也要进入与 message 同样的主链路：
          Normalize(轻量) -> Policy -> Session/Context(session buffer) -> Agent(bus) -> Outbound(send)

        注意：notice 没有天然的 message_id/message 段，因此这里会生成一个稳定的“事件 id”，并用
        一条 ContextMessageRecord 记录到 session buffer 中。
        """

        decision = decide_notice_trigger(
            payload,
            config=self.config,
            self_id=str(self._self_id or ""),
        )
        if not decision.should_reply:
            if decision.reason != "unsupported_notice":
                self._log_notice_decision(decision)
            else:
                self._log_ignored_event(payload)
            return

        actor = str(payload.get("sender_id") or payload.get("user_id") or "").strip()
        if not actor:
            logger.debug("napcat_ws notice ignored reason=missing_actor")
            return

        sender_name = self._extract_notice_actor_name(payload, fallback=actor)
        group_id = self._extract_group_id(payload)
        chat = self._build_notice_chat(actor_id=actor, group_id=group_id)
        session_key = make_session_key(chat)

        if not self._consume_notice_cooldown(chat=chat):
            return

        event_id, notice_type, sub_type, ts_i = self._build_notice_event_id(
            payload=payload,
            actor_id=actor,
            group_id=group_id,
        )

        self._cache_notice_record(
            session_key=session_key,
            chat=chat,
            actor_id=actor,
            sender_name=sender_name,
            event_id=event_id,
        )

        self._log_notice_decision(decision)
        await self._publish_notice_inbound(
            payload=payload,
            chat=chat,
            session_key=session_key,
            actor=actor,
            sender_name=sender_name,
            event_id=event_id,
            timestamp=datetime.fromtimestamp(ts_i),
            notice_type=notice_type,
            sub_type=sub_type,
            decision=decision,
        )
        return

    def _consume_notice_cooldown(self, *, chat: ChatRef) -> bool:
        """检查并消费 notice 冷却；命中冷却返回 False。"""

        cooldown_s = int(getattr(self.config, "poke_cooldown_seconds", 60) or 0)
        if cooldown_s <= 0:
            return True

        now_ts = time.time()
        cooldown_key = f"{chat.chat_type}:{chat.chat_id}"
        last_ts = float(self._poke_last_ts_by_chat.get(cooldown_key) or 0.0)
        if last_ts and (now_ts - last_ts) < float(cooldown_s):
            logger.debug(
                "napcat_ws poke_cooldown key={} limit_s={}",
                cooldown_key,
                cooldown_s,
            )
            return False

        self._poke_last_ts_by_chat[cooldown_key] = now_ts
        return True

    def _build_notice_chat(self, *, actor_id: str, group_id: str) -> ChatRef:
        """根据 notice 发起者与群号构造 chat 引用。"""

        return ChatRef(
            chat_type="group" if group_id else "private",
            chat_id=str(group_id or actor_id),
            user_id=str(actor_id),
            group_id=str(group_id or ""),
        )

    def _build_notice_event_id(
        self,
        *,
        payload: dict[str, Any],
        actor_id: str,
        group_id: str,
    ) -> tuple[str, str, str, int]:
        """生成 notice 事件 id，并返回 notice 基本元信息。"""

        notice_type = str(payload.get("notice_type") or "notice").strip() or "notice"
        sub_type = str(payload.get("sub_type") or payload.get("notify_type") or "").strip() or ""
        ts_raw = payload.get("time")
        try:
            ts_i = int(ts_raw) if ts_raw is not None else int(time.time())
        except Exception:
            ts_i = int(time.time())
        event_id = (
            f"notice:{notice_type}:{sub_type}:{ts_i}:{actor_id}:{group_id or 'private'}"
        )
        return event_id, notice_type, sub_type, ts_i

    def _cache_notice_record(
        self,
        *,
        session_key: str,
        chat: ChatRef,
        actor_id: str,
        sender_name: str,
        event_id: str,
    ) -> None:
        """把 notice 事件写入上下文缓存。"""

        self._store.append_message(
            session_key,
            ContextMessageRecord(
                sender_name=str(sender_name),
                sender_id=str(actor_id),
                message_id=event_id,
                text="(poke)",
                chat_type=chat.chat_type,
                group_id=str(chat.group_id or ""),
                is_bot=False,
            ),
        )

    async def _publish_notice_inbound(
        self,
        *,
        payload: dict[str, Any],
        chat: ChatRef,
        session_key: str,
        actor: str,
        sender_name: str,
        event_id: str,
        timestamp: datetime,
        notice_type: str,
        sub_type: str,
        decision: TriggerDecision,
    ) -> None:
        """构建并发布 notice 对应的 agent 入站消息。"""

        poke_text = "(poke)"
        bot_name = ""
        try:
            if chat.chat_type == "group" and chat.group_id:
                bot_name = await self._get_bot_display_name_for_group(chat.group_id)
        except Exception:
            bot_name = ""

        msg = NormalizedInbound(
            chat=chat,
            sender_id=str(actor),
            sender_name=str(sender_name),
            message_id=event_id,
            timestamp=timestamp,
            text=poke_text,
            rendered_text=poke_text,
            raw_event=dict(payload),
        )
        content = self._build_chat_context(
            msg=msg,
            session_key=session_key,
            bot_name=bot_name,
        )

        inbound = InboundMessage(
            channel=self.name,
            sender_id=str(actor),
            chat_id=str(chat.chat_id),
            content=content,
            media=[],
            metadata={
                "chat_type": chat.chat_type,
                "user_id": chat.user_id,
                "group_id": chat.group_id,
                "notice_type": notice_type,
                "sub_type": sub_type,
                "session_key": session_key,
                "_napcat_trigger": decision.trigger,
                "_napcat_trigger_reason": decision.reason,
            },
            session_key_override=session_key,
        )
        await self.bus.publish_inbound(inbound)

    def _extract_notice_actor_name(self, payload: dict[str, Any], fallback: str) -> str:
        """Best-effort actor display name for notice events (no network calls)."""

        for key in ("sender_name", "nickname", "card", "remark", "user_name"):
            v = payload.get(key)
            if v:
                s = str(v).strip()
                if s:
                    return s

        raw_info = payload.get("raw_info")
        if isinstance(raw_info, dict):
            for key in (
                "senderNick",
                "sender_nick",
                "senderName",
                "sender_name",
                "nick",
                "nickname",
                "name",
                "display_name",
            ):
                v = raw_info.get(key)
                if v:
                    s = str(v).strip()
                    if s:
                        return s

        return str(fallback or "unknown").strip() or "unknown"

    def _extract_group_id(self, payload: dict[str, Any]) -> str:
        group_id = str(payload.get("group_id") or "").strip()
        if group_id:
            return group_id

        raw_info = payload.get("raw_info")
        if not isinstance(raw_info, dict):
            return ""

        keys = (
            "group_id",
            "groupId",
            "peer_uin",
            "peerUin",
            "peer_uid",
            "peerUid",
        )
        for key in keys:
            value = raw_info.get(key)
            if value:
                return str(value).strip()

        return ""

    # ------------------------------
    # message
    # ------------------------------

    # ------------------------------
    # 上下文记录（立刻启用）
    # ------------------------------

    async def _build_context_record(self, msg: NormalizedInbound) -> ContextMessageRecord:
        """把当前归一化消息投影成上下文记录（长字段）。

        只做 projection/render，不在此处做详情补全（reply/forward 的展开发生在 enrich 阶段）。

        Args:
            msg: 已归一化（必要时已 enrich）的入站消息。

        Returns:
            上下文记录。
        """

        rendered_text = self._render_context_message_text(
            message=msg.raw_event.get("message"),
            raw=msg.raw_event.get("raw_message"),
        )
        if not rendered_text:
            rendered_text = str(
                msg.raw_event.get("raw_message") or msg.rendered_text or msg.text or ""
            ).strip()

        record = ContextMessageRecord(
            sender_name=str(msg.sender_name or msg.sender_id or "unknown"),
            sender_id=str(msg.sender_id or ""),
            message_id=str(msg.message_id or ""),
            text=self._truncate_user_text(rendered_text),
            chat_type=msg.chat.chat_type,
            group_id=str(msg.chat.group_id or ""),
            is_bot=(str(msg.sender_id or "") == str(self._self_id or "")),
        )

        record.reply = self._build_reply_record(msg.reply)
        record.forward_items = self._build_forward_item_records(msg.forward_items)
        return record

    def _build_reply_record(self, reply: QuotedMessage | None) -> ReplyRecord | None:
        """把已 enrich 的 reply 投影成上下文用的 ReplyRecord。"""

        if reply is None or not str(reply.message_id or "").strip():
            return None

        rendered_text = self._render_context_message_text(message=reply.message, raw=reply.raw_message)
        if not rendered_text:
            rendered_text = str(reply.text or "").strip()
        rendered_text = self._truncate_user_text(rendered_text)

        sender_name = str(reply.sender_name or reply.sender_id or "unknown")
        if not sender_name or not rendered_text:
            return None

        rec = ReplyRecord(
            sender_name=sender_name,
            sender_id=str(reply.sender_id or ""),
            message_id=str(reply.message_id or "").strip(),
            text=rendered_text,
            is_bot=(str(reply.sender_id or "") == str(self._self_id or "")),
            forward_summary=str(reply.forward_summary or "").strip(),
            media_paths=list(getattr(reply, "media_paths", None) or []),
        )

        fitems = list(getattr(reply, "forward_items", None) or [])
        rec.forward_items = self._build_forward_item_records(fitems)
        return rec

    def _build_forward_item_records(self, items: list[dict[str, Any]]) -> list[ForwardItemRecord]:
        """把已 enrich 的 forward_items 投影成上下文用的 ForwardItemRecord 列表。"""

        rendered_items: list[ForwardItemRecord] = []
        for item in items or []:
            if not isinstance(item, dict):
                continue

            rendered_text = self._render_context_message_text(
                message=item.get("message"),
                raw=item.get("raw_message"),
            )
            if not rendered_text:
                rendered_text = str(item.get("text") or "").strip()
            rendered_text = self._truncate_user_text(rendered_text)
            if not rendered_text:
                continue

            sender_id = str(item.get("sender_id") or "").strip()
            sender_name = str(item.get("sender_name") or sender_id or "unknown")
            is_bot = bool(sender_id and str(sender_id) == str(self._self_id or ""))

            rendered_items.append(
                ForwardItemRecord(
                    sender_name=sender_name,
                    sender_id=sender_id,
                    message_id=str(item.get("message_id") or "").strip(),
                    text=rendered_text,
                    source=str(item.get("src") or "").strip(),
                    is_bot=is_bot,
                    media_paths=list(item.get("media_paths") or []),
                    media_meta=list(item.get("media_meta") or []),
                )
            )
        return rendered_items

    def _build_chat_context(
        self, *, msg: NormalizedInbound, session_key: str, bot_name: str
    ) -> str:
        """构建本轮发给 agent 的上下文内容。

        Args:
            msg: 已归一化/补全后的入站消息。
            session_key: 会话 key。
            bot_name: 在群内展示的 bot 名字（用于上下文构建）。

        Returns:
            序列化后的上下文文本。
        """
        hist_limit = int(getattr(self.config, "context_max_messages", 20) or 20)
        hist = self._store.recent_messages(session_key, hist_limit)

        return self._context_builder.build(
            chat=msg.chat,
            bot_name=str(bot_name or ""),
            bot_id=str(self._self_id or ""),
            time_window_label=f"latest_{hist_limit}",
            prompts=[],
            messages=hist,
        )

    async def _enrich_message(self, msg: NormalizedInbound) -> None:
        """补全入站消息的引用/转发/媒体。

        该步骤会下载本条消息内的图片、展开引用消息（reply）与合并转发（forward），
        并把转发节点里的媒体路径聚合到 `msg.media_paths`，确保后续构建 agent 输入时
        能把“本轮可见图片”一并传入。

        Args:
            msg: 需要补全的入站消息（原地修改）。
        """
        msg.media_paths = await self._download_images_from_message(msg.raw_event.get("message"))

        await self._expand_quote(msg)
        await self._expand_forward(msg)

        # 关键修复：把合并转发里的图片也喂给大模型。
        # forward_items 的 media_paths 目前是“全集合附在每个 item 上”（不做 per-node 对齐），
        # 这里只需要聚合成一份列表即可。
        try:
            for it in msg.forward_items:
                if not isinstance(it, dict):
                    continue
                for p in it.get("media_paths") or []:
                    if isinstance(p, str) and p and p not in msg.media_paths:
                        msg.media_paths.append(p)
        except Exception:
            pass

    async def _cache_context_record(self, *, msg: NormalizedInbound, session_key: str) -> None:
        """把当前消息写入 session buffer。

        Args:
            msg: 已 enrich 的入站消息。
            session_key: 会话 key。
        """
        try:
            context_record = await self._build_context_record(msg)
            # 把“这条消息可见的图片”也存进上下文记录，但不写入上下文文本。
            # 这样后续触发时可以把历史图片合并进 InboundMessage.media。
            try:
                context_record.media_paths = list(msg.media_paths or [])
            except Exception:
                pass
            self._store.append_message(session_key, context_record)
        except Exception as exc:
            logger.debug("napcat_ws build_context_record_failed err={}", exc)

    def _collect_visible_media(self, *, msg: NormalizedInbound, session_key: str) -> list[str]:
        """汇总本轮对模型“可见”的媒体文件路径。

        会合并：
        - 当前消息的图片（`msg.media_paths`）
        - 引用消息的图片（`msg.reply.media_paths`）
        - 历史窗口中缓存的图片（`ContextMessageRecord.media_paths`）

        Args:
            msg: 已 enrich 的入站消息。
            session_key: 会话 key。

        Returns:
            去重后的本地文件路径列表。
        """
        # 关键修复：历史/引用/合并转发图片的可见性。
        # - 当前消息图片：msg.media_paths
        # - 引用消息图片：msg.reply.media_paths
        # - 历史窗口图片：来自 store 里 ContextMessageRecord.media_paths（不写入上下文文本，仅用于本轮 media 合并）
        merged_media: list[str] = []
        try:
            for p in msg.media_paths or []:
                if isinstance(p, str) and p and p not in merged_media:
                    merged_media.append(p)
        except Exception:
            pass

        try:
            if msg.reply is not None:
                for p in msg.reply.media_paths or []:
                    if isinstance(p, str) and p and p not in merged_media:
                        merged_media.append(p)
        except Exception:
            pass

        try:
            hist_limit = int(getattr(self.config, "context_max_messages", 20) or 20)
            hist = self._store.recent_messages(session_key, hist_limit)
            for record in hist:
                for p in getattr(record, "media_paths", None) or []:
                    if isinstance(p, str) and p and p not in merged_media:
                        merged_media.append(p)
        except Exception:
            pass

        return merged_media

    def _build_agent_inbound(
        self,
        *,
        msg: NormalizedInbound,
        session_key: str,
        content: str,
        merged_media: list[str],
        decision: TriggerDecision,
    ) -> InboundMessage:
        """构造并发布给 agent 的入站消息对象。

        Args:
            msg: 入站消息。
            session_key: 会话 key。
            content: 发给 agent 的文本内容（通常是拼好的上下文）。
            merged_media: 本轮可见的本地媒体路径列表。
            decision: 触发决策信息（用于写入 metadata）。

        Returns:
            `InboundMessage` 实例。
        """
        return InboundMessage(
            channel=self.name,
            sender_id=str(msg.sender_id),
            chat_id=str(msg.chat.chat_id),
            content=content,
            # 这里必须传“本地文件路径列表”，ContextBuilder 才会读文件并转成 data:image/...;base64 给大模型。
            media=merged_media,
            metadata={
                "chat_type": msg.chat.chat_type,
                "user_id": msg.chat.user_id,
                "group_id": msg.chat.group_id,
                "message_id": msg.message_id,
                "session_key": session_key,
                "_napcat_trigger": decision.trigger,
                "_napcat_trigger_reason": decision.reason,
            },
            session_key_override=session_key,
        )

    async def _publish_message_inbound(
        self,
        *,
        msg: NormalizedInbound,
        session_key: str,
        decision: TriggerDecision,
    ) -> None:
        """构建 message 对应的 agent 入站消息并发布。"""

        bot_name = ""
        try:
            if msg.chat.chat_type == "group" and msg.chat.group_id:
                bot_name = await self._get_bot_display_name_for_group(msg.chat.group_id)
        except Exception:
            bot_name = ""

        content = self._build_chat_context(
            msg=msg,
            session_key=session_key,
            bot_name=bot_name,
        )
        merged_media = self._collect_visible_media(msg=msg, session_key=session_key)

        inbound = self._build_agent_inbound(
            msg=msg,
            session_key=session_key,
            content=content,
            merged_media=merged_media,
            decision=decision,
        )
        await self.bus.publish_inbound(inbound)

    async def _handle_message(self, payload: dict[str, Any]) -> None:
        """处理 OneBot message 事件。

        流程：normalize -> enrich -> decide trigger -> cache -> build context -> publish inbound。

        Args:
            payload: OneBot message 事件 payload。
        """
        msg = normalize_inbound(
            payload,
            self_id=str(self._self_id or ""),
            config=self.config,
            message_parser=self._message_parser,
        )
        if msg is None:
            self._log_ignored_event(payload)
            return

        await self._enrich_message(msg)

        session_key = make_session_key(msg.chat)

        # 先决策（避免把当前 msg 写入 store 后又被 history 重复读到）
        decision = decide_message_trigger(
            msg,
            config=self.config,
            store=self._store,
            session_key=session_key,
            self_id=str(self._self_id or ""),
        )

        await self._cache_context_record(msg=msg, session_key=session_key)

        # 缓存 bot 自己的昵称（用于把 @self 渲染成 @昵称；不影响其它逻辑）
        if msg.at_self and msg.chat.chat_type == "group":
            try:
                # 如果当前消息里出现了 @self，而 config 里没填 self_nickname，就用群里看到的 sender_name 兜底不了
                # 这里不做自动学习，避免误学；仅在聊天上下文 render 时使用 config.self_nickname。
                pass
            except Exception:
                pass

        self._log_inbound(msg)
        self._log_message_decision(decision)
        if not decision.should_reply:
            return

        # 交给 agent（MessageBus）生成回复；NapCat 的 send() 会把 OutboundMessage 发回平台
        # 注意：群聊白名单是 group_id 级别的，BaseChannel 的 sender_id allowlist 无法表达。
        # 因此这里不再调用 super()._handle_message()，而是直接 publish inbound。
        await self._publish_message_inbound(
            msg=msg,
            session_key=session_key,
            decision=decision,
        )
        return

    async def _expand_quote(self, msg: NormalizedInbound) -> None:
        """展开引用消息（quote/reply）。

        Args:
            msg: 入站消息（原地修改其 `reply` 字段）。
        """
        if msg.reply is None or not msg.reply.message_id:
            return

        rid = str(msg.reply.message_id).strip()
        if not rid:
            return

        try:
            exp = await self._message_cache.get_or_fetch_reply(
                message_id=rid,
                fetch=lambda: self._detail_fetcher.expand_reply(message_id=rid, chat=msg.chat),
            )
        except Exception as exc:
            logger.debug("napcat_ws quote_expand_failed err={}", exc)
            return

        if exp is None:
            return

        msg.reply.sender_id = str(exp.sender_id or "")
        msg.reply.sender_name = str(exp.sender_name or exp.sender_id or "unknown")
        msg.reply.text = str(exp.text or "")
        msg.reply.media = []

        # 关键修复：引用消息里的图片也要下载到本地并传给大模型。
        # 不做 fallback：优先用 exp.message（segments），如果 message 不是数组，则用 media_meta 构造最小 segments 下载。
        try:
            paths: list[str] = []
            if isinstance(getattr(exp, "message", None), list):
                paths = await self._download_images_from_message(getattr(exp, "message"))
            else:
                meta = list(getattr(exp, "media_meta", None) or [])
                fake_message: list[dict[str, Any]] = []
                for m in meta:
                    if not isinstance(m, dict):
                        continue
                    if str(m.get("type") or "") != "image":
                        continue
                    url = str(m.get("url") or "").strip()
                    name = str(m.get("file") or m.get("safe_name") or "").strip()
                    if not url or not name:
                        continue
                    fake_message.append({"type": "image", "data": {"url": url, "file": name}})
                if fake_message:
                    paths = await self._download_images_from_message(fake_message)
            msg.reply.media_paths = paths
        except Exception:
            msg.reply.media_paths = []

        msg.reply.forward_items = [
            {
                "sender_id": str(item.sender_id or ""),
                "sender_name": str(item.sender_name or item.sender_id or "unknown"),
                "message_id": str(item.message_id or "").strip(),
                "time": None,
                "text": str(item.text or ""),
                "media": [],
                "media_paths": [],
                "media_meta": list(item.media_meta or []),
                "message": None,
                "raw_message": None,
                "src": str(item.source or "").strip(),
            }
            for item in list(getattr(getattr(exp, "forward", None), "items", None) or [])
        ]
        msg.reply.forward_id = str(getattr(exp, "id", "") or rid)

        # 如果引用消息本身是合并转发：也把转发里的图片下载并挂到 reply.media_paths
        if msg.reply.forward_id:
            try:
                fitems = await self._fetch_forward_items(msg.reply.forward_id, msg.chat)
                msg.reply.forward_items = fitems or msg.reply.forward_items
                for it in msg.reply.forward_items or []:
                    if not isinstance(it, dict):
                        continue
                    for p in it.get("media_paths") or []:
                        if isinstance(p, str) and p and p not in msg.reply.media_paths:
                            msg.reply.media_paths.append(p)
            except Exception:
                pass

    async def _fetch_forward_items(self, forward_id: str, chat: ChatRef) -> list[dict[str, Any]]:
        """拉取并展开 forward items。

        Args:
            forward_id: forward id。
            chat: 聊天信息（用于 expand_forward 内部的私聊 src 补充）。

        Returns:
            标准化后的 forward item 字典列表。
        """
        fid = str(forward_id or "").strip()
        if not fid:
            return []

        try:
            fexp = await self._message_cache.get_or_fetch_forward(
                forward_id=fid,
                fetch=lambda: self._detail_fetcher.expand_forward(forward_id=fid, chat=chat),
            )
        except Exception as exc:
            logger.debug("napcat_ws forward_fetch_failed err={}", exc)
            return []

        if fexp is None or not getattr(fexp, "items", None):
            return []

        # 关键修复：合并转发节点里的图片也要下载到本地，让大模型可见。
        # NapCat get_forward_msg 返回的节点消息在 expand_forward 中已被 消息解析器解析，
        # 这里复用其 media_meta（只包含 https 图片 url + file/name）。
        forward_media_meta = list(getattr(fexp, "media_meta", None) or [])
        if forward_media_meta:
            try:
                # 直接走与普通消息同一套下载器：构造最小 segments 供 _extract_https_image_items 识别。
                fake_message: list[dict[str, Any]] = []
                for m in forward_media_meta:
                    if not isinstance(m, dict):
                        continue
                    if str(m.get("type") or "") != "image":
                        continue
                    url = str(m.get("url") or "").strip()
                    name = str(m.get("file") or "").strip()
                    if not url or not name:
                        continue
                    fake_message.append({"type": "image", "data": {"url": url, "file": name}})

                forward_paths = await self._download_images_from_message(fake_message)
            except Exception as exc:
                logger.debug("napcat_ws forward_image_download_failed err={}", exc)
                forward_paths = []
        else:
            forward_paths = []

        items: list[dict[str, Any]] = []
        for item in fexp.items:
            items.append(
                {
                    "sender_id": str(item.sender_id or ""),
                    "sender_name": str(item.sender_name or item.sender_id or "unknown"),
                    "message_id": str(item.message_id or "").strip(),
                    "time": None,
                    "text": str(item.text or ""),
                    "media": [],
                    "media_paths": list(forward_paths),
                    "media_meta": list(item.media_meta or []),
                    "message": None,
                    "raw_message": None,
                    "src": str(item.source or "").strip(),
                }
            )
        return items

    async def _expand_forward(self, msg: NormalizedInbound) -> None:
        """展开当前消息中的合并转发（forward）。

        Args:
            msg: 入站消息（原地写入 `forward_items/forward_summary`）。
        """
        if not msg.forward_id:
            return

        items = await self._fetch_forward_items(msg.forward_id, msg.chat)
        if not items:
            return

        msg.forward_items = items
        msg.forward_summary = self._build_forward_summary(items)

    def _pick_sender_name(self, sender: dict[str, Any], *, fallback: str) -> str:
        card = str(sender.get("card") or "").strip()
        if card:
            return card

        nickname = str(sender.get("nickname") or "").strip()
        if nickname:
            return nickname

        return fallback or "unknown"

    def _parse_payload_message(self, *, message: Any, raw: Any = None) -> ParsedMessage:
        """把 message/raw_message 解析为 `ParsedMessage`。

        Args:
            message: payload 的 message 字段（可能是 list/str/None）。
            raw: payload 的 raw_message 兜底值。

        Returns:
            解析结果。
        """
        payload_msg = message if message not in (None, "") else raw
        message_format = (
            "array"
            if isinstance(payload_msg, list)
            else ("string" if isinstance(payload_msg, str) else None)
        )
        payload_like = {
            "message": payload_msg,
            "raw_message": raw,
            "message_format": message_format,
        }
        return self._message_parser.parse(payload=payload_like, self_id=str(self._self_id or ""))

    def _render_context_message_text(self, *, message: Any, raw: Any = None) -> str:
        """把消息体渲染成上下文用的精简文本。"""

        parsed = self._parse_payload_message(message=message, raw=raw)
        return str(parsed.cq_text or "").strip()

    def _render_text_and_media(
        self, *, message: Any, raw: Any
    ) -> tuple[str, list[str], list[dict[str, Any]]]:
        """渲染消息文本并提取媒体信息。

        Args:
            message: message 字段（segments 或 CQ 字符串）。
            raw: raw_message 兜底。

        Returns:
            三元组 `(effective_text, media, media_meta)`：
            - effective_text: 适合进入上下文的渲染文本
            - media: 媒体 URL 列表（通常为图片）
            - media_meta: 媒体元信息（用于后续下载/回填）
        """
        parsed = self._parse_payload_message(message=message, raw=raw)
        effective_text = str(
            parsed.rendered_text or parsed.cq_text or parsed.plain_text or ""
        ).strip()
        media = list(parsed.media or [])
        media_meta = list(parsed.media_meta or [])
        return effective_text, media, media_meta

    async def _download_images_from_message(self, message: Any) -> list[str]:
        """将 message segments 中的图片下载到本地 media 目录。

        策略：
        - 总是下载（已存在就跳过）
        - 仅支持 https
        - 文件名直接使用消息体里的 file/name（通常本身就是 hash）
        - 保存到 media/napcat_ws/{name}
        - 不做数量/大小限制
        """

        items = self._extract_https_image_items(message)
        if not items:
            return []

        paths: list[str] = []
        for url, name in items:
            dest = self._media_dir / name
            if dest.exists():
                paths.append(str(dest))
                continue

            part = dest.with_name(dest.name + ".part")
            ok = await asyncio.to_thread(self._download_one_http_file, url, part, dest)
            if ok:
                paths.append(str(dest))

        return paths

    def _extract_https_image_items(self, message: Any) -> list[tuple[str, str]]:
        """从 segments 中抽取 (url, safe_filename) 列表。"""

        if not isinstance(message, list):
            return []

        out: list[tuple[str, str]] = []
        for seg in message:
            if not isinstance(seg, dict):
                continue
            if seg.get("type") != "image":
                continue

            data = seg.get("data")
            if not isinstance(data, dict):
                continue

            url = str(data.get("url") or "").strip()
            if not url:
                continue

            parsed = urlparse(url)
            if parsed.scheme.lower() != "https":
                continue

            name = str(data.get("file") or data.get("name") or "").strip()
            safe = self._safe_media_name(name)
            if not safe:
                continue

            out.append((url, safe))

        return out

    def _safe_media_name(self, name: str) -> str:
        """把消息体里的 file/name 变成安全的文件名（不做 hash）。"""

        s = str(name or "").strip()
        if not s:
            return ""

        # 防止路径穿越：只取 basename，并兼容 "\\" 分隔符
        s = s.replace("\\", "/")
        s = s.split("/")[-1].strip()

        if not s or s in {".", ".."}:
            return ""

        # 仅做最小清理：把明显不适合作为文件名的字符替换掉
        cleaned: list[str] = []
        for ch in s:
            if ch.isalnum() or ch in {".", "_", "-"}:
                cleaned.append(ch)
            else:
                cleaned.append("_")

        safe = "".join(cleaned).strip("._")
        if not safe:
            return ""

        # 文件名过长会在某些 FS 上失败：做保守截断（不是下载限制，只是路径安全）
        if len(safe) > 180:
            safe = safe[:180]

        return safe

    def _download_one_http_file(self, url: str, part: Path, dest: Path) -> bool:
        """下载单个 http/https 文件。

        说明：
        - 该函数会在 asyncio.to_thread 中执行（阻塞 IO）。
        - 采用 *.part + atomic replace，避免半截文件。
        """

        part.parent.mkdir(parents=True, exist_ok=True)
        try:
            req = Request(url, headers={"User-Agent": "nanobot/napcat_ws"})
            with urlopen(req, timeout=30) as resp, open(part, "wb") as f:
                shutil.copyfileobj(resp, f)
            os.replace(part, dest)
            return True
        except Exception as exc:
            try:
                if part.exists():
                    part.unlink()
            except Exception:
                pass
            logger.debug("napcat_ws image_download_failed url={} err={}", url, exc)
            return False

    def _build_forward_summary(self, items: list[dict[str, Any]]) -> str:
        """生成合并转发的摘要文本。

        Args:
            items: forward 节点列表（标准化 dict）。

        Returns:
            逐行摘要文本（带保守长度截断）。
        """
        lines: list[str] = []
        for item in items:
            who = str(item.get("sender_name") or item.get("sender_id") or "unknown")
            text = str(item.get("text") or "").strip()

            # 优先使用结构化 media_meta（可区分 sticker/image，并拿到 file 名）
            media_meta = list(item.get("media_meta") or [])
            if media_meta:
                tokens: list[str] = []
                for m in media_meta:
                    if not isinstance(m, dict):
                        continue
                    safe_name = _safe_media_filename(str(m.get("safe_name") or m.get("file") or ""))
                    if safe_name:
                        typ = str(m.get("type") or "image")
                        tokens.append(f"[{typ}:{safe_name}]")
                    else:
                        typ = str(m.get("type") or "image")
                        tokens.append(f"[{typ}]")

                media_token = " ".join(tokens).strip()
            else:
                # fallback：旧逻辑只知道 url 列表
                media = list(item.get("media") or [])
                media_token = f"[image x{len(media)}]" if media else ""

            if text and media_token:
                content = f"{text} {media_token}".strip()
            elif text:
                content = text
            elif media_token:
                content = media_token
            else:
                content = "(empty)"

            lines.append(f"{who}: {content}")

        summary = "\n".join(lines).strip()
        if len(summary) > 1200:
            summary = summary[:1200] + TRUNCATION_TAG
        return summary

    # ------------------------------
    # store & logging
    # ------------------------------

    def _log_inbound(self, msg: NormalizedInbound) -> None:
        """输出入站消息的调试日志。

        Args:
            msg: 入站消息。
        """
        reply_id = msg.reply.message_id if msg.reply else ""
        forward_id = msg.forward_id or ""
        forward_items = len(msg.forward_items or [])

        logger.debug(
            "napcat_ws inbound chat_type={} chat_id={} sender={}({}) text={!r} media={} at_self={} reply_id={} forward_id={} forward_items={}",
            msg.chat.chat_type,
            msg.chat.chat_id,
            msg.sender_name,
            msg.sender_id,
            msg.text,
            len(msg.media),
            msg.at_self,
            reply_id,
            forward_id,
            forward_items,
        )

    def _log_message_decision(self, decision: TriggerDecision) -> None:
        """输出 message 触发决策的调试日志。"""
        logger.debug(
            "napcat_ws message decision should_reply={} reason={} trigger={} probability={}",
            decision.should_reply,
            decision.reason,
            decision.trigger,
            decision.probability_used,
        )

    def _log_notice_decision(self, decision: TriggerDecision) -> None:
        """输出 notice 触发决策的调试日志。"""
        logger.debug(
            "napcat_ws notice decision should_reply={} reason={} trigger={} probability={}",
            decision.should_reply,
            decision.reason,
            decision.trigger,
            decision.probability_used,
        )

    def _log_ignored_event(self, payload: dict[str, Any]) -> None:
        """输出被忽略事件的调试日志。

        Args:
            payload: 原始 OneBot payload。
        """
        post_type = str(payload.get("post_type") or "")
        if not post_type:
            return

        logger.debug(
            "napcat_ws ignored post_type={} notice_type={} sub_type={} message_type={}",
            post_type,
            payload.get("notice_type") or "",
            payload.get("sub_type") or payload.get("notify_type") or "",
            payload.get("message_type") or "",
        )


# 向后兼容导出（如果旧代码用 from ... import NapCatWSChannel）
__all__ = ["NapCatWSChannel"]
