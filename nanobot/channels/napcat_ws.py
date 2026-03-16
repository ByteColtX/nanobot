"""NapCat Forward WebSocket 渠道（OneBot v11）。

本文件采用“单文件分层架构”：NapCat WebSocket 渠道相关的 transport、入站、
会话上下文、出站逻辑都集中在 napcat_ws.py 中维护，但职责边界必须清晰。

当前架构（都在本文件内）：
- Transport 层：WebSocket 连接管理、OneBot action 调用、echo/future 关联、任务生命周期。
- Inbound 层：原始 OneBot/NapCat payload 归一化，V2 parser 负责消息内容语义解析。
- Policy 层：只负责“是否响应”的触发决策，不负责回复风格控制。
- Session / Context 层：基于归一化消息、reply/forward 展开、历史消息构建聊天上下文。
- Outbound 层：以文本为主载体发送消息，允许文本中内嵌 CQ 码，并在发送前做轻量校验。

Policy 当前支持的触发方式：
- 私聊概率触发。
- 群聊概率触发。
- 昵称触发（命中配置列表中的任一昵称即触发）。
- 戳一戳触发。
- @机器人触发。
- 回复机器人触发。

Outbound 当前设计：
- 出站主路径是 text-first，而不是复杂 segment builder。
- 模型可以直接生成 CQ 码出站；当前正式支持校验的 CQ 类型：at、reply、image。
- 发送前会执行轻量校验：
  - at.qq 必须为数字字符串或 all。
  - reply.id 必须非空。
  - image.file 必须是存在的本地 file:// 普通文件路径。
- 暂不支持的 CQ 类型不会静默透传到主链路。
- 未来可扩展的出站类型包括：record、video 等。

边界约束：
- channel 层只提供“聊天上下文”，即 <NAPCAT_WS_CONTEXT> 所需内容。
- system prompt、人格、全局行为规则不在这里拼装；它们由统一 Agent Context 注入。
- Inbound 与 Outbound 是非对称设计：Inbound 负责结构化理解，Outbound 负责 CQ-first 文本发送。
- 因此本文件不应承担完整模型输入/前置 prompt 组装职责。

已移除/不再保留的旧设计：
- legacy parser 主链路。
- smalltalk_*、longform_* 等旧配置与模式切换逻辑。
- 旧 prompt-builder / mode-based reply shaping。

维护约定：
- 修改消息解析时优先看 Inbound。
- 修改触发规则时优先看 Policy。
- 修改聊天上下文时看 Session / Context。
- 修改发送行为时看 Outbound。
- 新功能优先接入当前主链路，不要把旧实现重新带回本文件。
"""

from __future__ import annotations

import asyncio
import json
import random
import re
import os
import shutil
from collections import deque, OrderedDict
from dataclasses import dataclass, field
from datetime import datetime
import time
from pathlib import Path
from typing import Any, Awaitable, Callable, Deque, Dict, Literal, Optional
from urllib.parse import urlparse, urlunparse
from urllib.request import Request, urlopen

from loguru import logger


def summarize_onebot_event(payload: dict[str, Any]) -> str:
    """Build a compact summary string for logs.

    Keep it stable and low-cardinality; never include full message content.
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


from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel
from nanobot.config.paths import get_media_dir
from nanobot.config.schema import Base

from pydantic import Field


class NapCatWSConfig(Base):
    """NapCat WS 渠道配置（按 Telegram 渠道的模式：配置模型放在各自的 channel 文件里）。

    为什么放在这里：
    - 上游的 `nanobot/config/schema.py` 保持干净（channels 配置允许 extra）。
    - 每个 channel 自己维护字段与默认值，方便演进，也方便清理死字段。

    配置路径：
        config.channels.napcatWs

    命名规则：
    - Python 字段使用 snake_case。
    - 配置文件既可以用 snake_case（例如 `poke_cooldown_seconds`），也可以用 camelCase
      （例如 `pokeCooldownSeconds`、`privateTriggerProbability`），具体取决于 Base 的 alias 规则。
    """

    # --- 连接 ---
    enabled: bool = Field(default=False, description="是否启用 NapCatWS 渠道")
    url: str = Field(default="", description="WebSocket 地址，例如 ws://localhost:3001/")
    token: str = Field(default="", description="NapCat/OneBot 的 token（如果服务端要求）")

    # --- 白名单 / 黑名单 ---
    allow_from: list[str] = Field(
        default_factory=list,
        description=(
            "允许触发的来源列表（字符串）。\n"
            "支持两种用法：\n"
            "- 填 user_id：允许指定用户（私聊/群内发言者）\n"
            "- 填 group_id：允许指定群（群内任何人都可触发）\n"
            "为空表示不做白名单限制（全部允许），仍会受黑名单影响。"
        ),
    )
    blacklist_private_ids: list[str] = Field(
        default_factory=list,
        description="私聊黑名单 user_id 列表（字符串）",
    )
    blacklist_group_ids: list[str] = Field(
        default_factory=list,
        description="群聊黑名单 group_id 列表（字符串）",
    )

    # --- 概率触发 ---
    private_trigger_probability: float = Field(
        default=0.05,
        description="私聊随机触发概率（0~1），默认 0.05",
    )
    group_trigger_probability: float = Field(
        default=0.05,
        description="群聊随机触发概率（0~1），默认 0.05",
    )

    # --- 昵称/关键词触发 ---
    nickname_triggers: list[str] = Field(
        default_factory=list,
        description="消息中包含任一昵称/关键词即触发",
    )

    # --- Notice 触发（系统通知类） ---
    trigger_on_poke: bool = Field(default=False, description="收到戳一戳（poke）通知时触发")
    poke_cooldown_seconds: int = Field(
        default=60,
        description="戳一戳触发冷却（秒）。<=0 表示关闭冷却。默认 60 秒。",
    )

    # --- 关系触发（@ / 回复） ---
    trigger_on_at: bool = Field(default=True, description="群聊中 @ 机器人时触发")
    trigger_on_reply_to_bot: bool = Field(default=True, description="回复机器人消息时触发")

    # --- 上下文 / 会话限制 ---
    context_max_messages: int = Field(default=25, description="构建上下文时最多读取的消息条数")
    session_buffer_size: int = Field(default=50, description="内存会话环形缓冲区大小")

    # --- 输入整形 ---
    user_text_truncate_chars: int = Field(
        default=200,
        description="用户文本过长时的截断长度，用于避免上下文变得难读",
    )
    ignore_self_messages: bool = Field(
        default=True,
        description="忽略机器人自身发送的消息，避免自我回声/死循环",
    )

    # --- 多轮回复 ---
    multi_turn_max_replies: int = Field(
        default=3,
        description="一次多轮追问中最多连续回复的次数",
    )

# NOTE: transport层也会用到 OneBot action 常量（get_login_info）

try:
    import websockets
    from websockets import ClientConnection

    WEBSOCKETS_AVAILABLE = True
except ImportError:  # pragma: no cover
    websockets = None
    ClientConnection = Any
    WEBSOCKETS_AVAILABLE = False


# ==============================
# 1) Types & constants（类型与常量）
# ==============================

ChatType = Literal["private", "group"]
TriggerType = Literal["private_probability", "group_probability", "nickname", "poke", "at_bot", "reply_to_bot"]

NoticeKind = Literal["poke", "other_notice"]

# OneBot/NapCat action names（集中管理，避免散落 magic string）
ACTION_GET_LOGIN_INFO = "get_login_info"
ACTION_GET_MSG = "get_msg"
ACTION_GET_FORWARD_MSG = "get_forward_msg"
ACTION_GET_GROUP_MEMBER_INFO = "get_group_member_info"
ACTION_SEND_POKE = "send_poke"

# Cache TTL defaults (seconds). Keep visible.
CACHE_TTL_1D_SECONDS = 60 * 60 * 24
DEFAULT_CACHE_TTL_SECONDS = CACHE_TTL_1D_SECONDS  # default: 1d

# V2 cache max sizes (entries). Keep visible.
V2_REPLY_CACHE_MAXSIZE = 2048
V2_FORWARD_CACHE_MAXSIZE = 1024


TRUNCATION_TAG = "<TRUNCATED>"  # 固定截断标签：在 Prompt/Context 层前置说明其含义
NO_REPLY_TAG = "<NO_REPLY>"  # 模型返回该标签表示不回复（channel 层应直接跳过发送）
NO_FOLLOWUP_TAG = "<NO_FOLLOWUP>"  # 模型返回该标签表示不进行后续多次回复

CONTEXT_BEGIN_TAG = "<NAPCAT_WS_CONTEXT>"  # 聊天上下文块开始标签（避免与 nanobot 其它标签冲突）
CONTEXT_END_TAG = "</NAPCAT_WS_CONTEXT>"  # 聊天上下文块结束标签（避免与 nanobot 其它标签冲突）


@dataclass(slots=True)
class ChatRef:
    """归一化后的聊天定位信息。

    字段说明：
        chat_type: "private" 或 "group"。
        chat_id: 该会话的主键：私聊=对方 user_id；群聊=group_id。
        user_id: 发送者 user_id（如果可得）。
        group_id: 群号（如果可得）。
    """

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

    # V2：保留原始 message/raw_message，便于生成精简 CQ（避免把超长 raw_message 原样塞进上下文）
    message: Any = None
    raw_message: Any = None

    # 引用消息自身可能是合并转发（forward），用于在 reply_to 里展开转发内容
    forward_id: str = ""
    forward_items: list[dict[str, Any]] = field(default_factory=list)


@dataclass(slots=True)
class NormalizedInbound:
    """Normalize 层输出的内部消息结构，供 Policy/Context 使用。"""

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
# 1.1) Inbound V2 types（segments/expansions/context line）
# =========================================
#
# 说明：
#   - Inbound 主链路只使用 V2（下面 1.1.1 的实现区）。
#   - 这里保留 V2 解析/展开/上下文所需的最小数据结构定义。


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
    """消息片段（segment）的轻量归一化表示。

    这不是最终渲染结果。该结构存在的目的，是让后续渲染器/上下文构建器可以稳定地产出：
      - CQ 风格的 `m` 文本（用于 <NAPCAT_WS_CONTEXT> 的 `m` 字段）
      - 触发用的纯文本（通常由 ParsedMessage.plain_text 提供）
      - 各类引用信号：reply_id / forward_id / at_qq 等

    注意：这里只定义数据结构；解析逻辑后续实现。
    """

    type: MessageSegmentType
    data: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ParsedMessage:
    """消息解析后的统一输出。

    字段约定：
      - segments：仅保留当前阶段支持的 segment 类型（见 MessageSegmentType）
      - plain_text：用于触发/策略匹配的纯文本
      - cq_text：用于上下文 `m` 字段的 CQ 风格文本（尽量接近 OneBot/CQ 表达）
      - rendered_text：面向聊天上下文/人类可读的渲染文本（图片/语音等保留占位）
      - rendered_segments：渲染后的片段摘要序列
      - media：从消息中提取出的媒体引用（当前以 URL 为主）
      - media_meta：结构化媒体元数据，供下载/诊断/上下文构建复用
      - at_qq/reply_id/forward_id：用于后续“引用展开/转发展开/@昵称映射”的信号

    注意：这里只定义数据结构；生产级解析逻辑后续实现。
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
    """合并转发（forward）的展开结果。

    字段约定：
      - id：forward id
      - items：转发节点摘要列表（上下文友好）。为了省 token，节点只保留缩写字段：
          {"u": "张三", "m": "好吃"}

    注意：这里只定义数据结构；具体拉取(get_forward_msg)/解析/截断规则后续实现。
    """

    id: str
    items: list[dict[str, str]] = field(default_factory=list)


@dataclass(slots=True)
class ExpandedReply:
    """引用消息（reply）的展开结果。

    该结构预期来源于 get_msg 的返回。

    与 ContextMessageLine 的约定（已定稿）：
      - 当前消息的 `m` 字段内仍保留 inline 的 `[CQ:reply,id=...]`（以及用户实际追加的文本）
      - 被引用的“原消息”展开内容放到 ContextMessageLine.r（方案 A：只保留 {"u","m"}）
      - 如果“原消息”本身包含 forward，可填充到 forward 字段（后续可继续展开）

    注意：这里只定义数据结构；拉取/解析/截断规则后续实现。
    """

    id: str
    u: str = ""
    q: str = ""
    m: str = ""
    forward: Optional[ExpandedForward] = None


@dataclass(slots=True)
class ContextMessageLine:
    """<NAPCAT_WS_CONTEXT> 的单行消息（# Messages）结构。

    为了省 token，字段名做了缩写（已定稿）：
      - u：sender 显示名
      - q：sender qq/user_id
      - id：message_id
      - m：类 CQ 字符串的消息内容（用于直接喂给模型）

    可选展开字段：
      - f：当本消息是 forward 时，附带已展开的转发节点列表（节点只保留 {"u","m"}）
      - r：当本消息包含 reply 时，附带被引用的原消息（方案 A：{"u","m"}）

    例子：
      {"u":"原","q":"424155717","id":"208...","m":"..."}
      {"u":"原","q":"424155717","id":"208...","m":"...","f":[{"u":"张三","m":"好吃"}]}
      {"u":"原","q":"424155717","id":"208...","m":"[CQ:reply,id=...]123","r":{"u":"张三","m":"原消息"}}

    注意：这里只定义数据结构；序列化/截断规则后续实现。
    """

    u: str
    q: str
    id: str
    m: str

    # expansions (optional)
    f: Optional[list[dict[str, str]]] = None
    r: Optional[dict[str, str]] = None


# =========================================
# 1.1.1) Inbound V2 implementation（parser/cache/fetcher/context builder）
# =========================================
#
# 说明：
#   - 这部分落地“解析/展开/缓存/上下文构建”的最小闭环，并被 normalize/channel 主链路直接使用。
#   - 仍只支持 9 种 segment 类型：text/face/image/at/reply/forward/record/video/file。
#   - 其它类型输出占位 token（[CQ:type]），后续按需扩展。
#   - Header 不包含 group_name（按你的要求）。
#


def _json_dumps_compact(obj: Any) -> str:
    """紧凑 JSON 序列化（省 token）。"""

    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


class InMemoryMessageCache:
    """进程内存缓存（最小可用版）。

    目标：
      - TTL 默认 1d
      - 两个 namespace：reply / forward
      - maxsize（条目数）限制：
          reply=2048, forward=1024
      - in-flight 去重：同一 key 并发只会触发一次 action

    说明：
      - 该缓存只用于“详情补全”阶段（reply/forward），
        构建 <NAPCAT_WS_CONTEXT> 时禁止再发 action。
      - 淘汰策略：近似 LRU（基于 OrderedDict）。
    """

    def __init__(
        self,
        *,
        ttl_seconds: int = DEFAULT_CACHE_TTL_SECONDS,
        reply_maxsize: int = V2_REPLY_CACHE_MAXSIZE,
        forward_maxsize: int = V2_FORWARD_CACHE_MAXSIZE,
    ) -> None:
        self._ttl_s = float(ttl_seconds)

        self._reply_max = int(reply_maxsize)
        self._forward_max = int(forward_maxsize)

        # LRU-ish: key -> {ts, v}
        self._reply: "OrderedDict[str, dict[str, Any]]" = OrderedDict()
        self._forward: "OrderedDict[str, dict[str, Any]]" = OrderedDict()

        # in-flight futures
        self._inflight_reply: dict[str, asyncio.Future[ExpandedReply]] = {}
        self._inflight_forward: dict[str, asyncio.Future[ExpandedForward]] = {}

    def _now(self) -> float:
        return time.time()

    def _is_expired(self, ts: float) -> bool:
        return (self._now() - float(ts or 0.0)) > self._ttl_s

    def _lru_get(self, store: "OrderedDict[str, dict[str, Any]]", key: str) -> dict[str, Any] | None:
        rec = store.get(key)
        if not isinstance(rec, dict):
            return None
        ts = float(rec.get("ts") or 0.0)
        if self._is_expired(ts):
            try:
                store.pop(key, None)
            except Exception:
                pass
            return None
        # touch
        try:
            store.move_to_end(key)
        except Exception:
            pass
        return rec

    def _lru_put(self, store: "OrderedDict[str, dict[str, Any]]", key: str, rec: dict[str, Any], maxsize: int) -> None:
        store[key] = rec
        try:
            store.move_to_end(key)
        except Exception:
            pass
        # evict
        if maxsize > 0:
            while len(store) > maxsize:
                try:
                    store.popitem(last=False)
                except Exception:
                    break

    def get_reply(self, message_id: str) -> Optional[ExpandedReply]:
        mid = str(message_id or "").strip()
        if not mid:
            return None
        rec = self._lru_get(self._reply, mid)
        if not rec:
            return None
        v = rec.get("v")
        return v if isinstance(v, ExpandedReply) else None

    def put_reply(self, message_id: str, value: ExpandedReply) -> None:
        mid = str(message_id or "").strip()
        if not mid:
            return
        self._lru_put(self._reply, mid, {"ts": self._now(), "v": value}, self._reply_max)

    def get_forward(self, forward_id: str) -> Optional[ExpandedForward]:
        fid = str(forward_id or "").strip()
        if not fid:
            return None
        rec = self._lru_get(self._forward, fid)
        if not rec:
            return None
        v = rec.get("v")
        return v if isinstance(v, ExpandedForward) else None

    def put_forward(self, forward_id: str, value: ExpandedForward) -> None:
        fid = str(forward_id or "").strip()
        if not fid:
            return
        self._lru_put(self._forward, fid, {"ts": self._now(), "v": value}, self._forward_max)

    async def get_or_fetch_reply(
        self,
        *,
        message_id: str,
        fetch: Callable[[], Awaitable[ExpandedReply]],
    ) -> ExpandedReply:
        """reply：cache miss 时触发一次 fetch，并对并发去重。"""

        mid = str(message_id or "").strip()
        if not mid:
            return ExpandedReply(id="")

        cached = self.get_reply(mid)
        if cached is not None:
            return cached

        fut = self._inflight_reply.get(mid)
        if fut is not None:
            return await fut

        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self._inflight_reply[mid] = fut
        try:
            v = await fetch()
            if isinstance(v, ExpandedReply) and str(v.id or "").strip():
                self.put_reply(mid, v)
            fut.set_result(v)
            return v
        except Exception as exc:
            if not fut.done():
                fut.set_exception(exc)
            raise
        finally:
            self._inflight_reply.pop(mid, None)

    async def get_or_fetch_forward(
        self,
        *,
        forward_id: str,
        fetch: Callable[[], Awaitable[ExpandedForward]],
    ) -> ExpandedForward:
        """forward：cache miss 时触发一次 fetch，并对并发去重。"""

        fid = str(forward_id or "").strip()
        if not fid:
            return ExpandedForward(id="")

        cached = self.get_forward(fid)
        if cached is not None:
            return cached

        fut = self._inflight_forward.get(fid)
        if fut is not None:
            return await fut

        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self._inflight_forward[fid] = fut
        try:
            v = await fetch()
            if isinstance(v, ExpandedForward) and str(v.id or "").strip():
                self.put_forward(fid, v)
            fut.set_result(v)
            return v
        except Exception as exc:
            if not fut.done():
                fut.set_exception(exc)
            raise
        finally:
            self._inflight_forward.pop(fid, None)


class NapCatMessageParserV2:
    """V2 解析器：只解析指定的 9 种 segment 类型，产出 CQ 风格 `m`。"""

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

    def _parse_array(self, message: list[dict[str, Any]]) -> ParsedMessage:
        out = ParsedMessage()
        parts_cq: list[str] = []
        parts_plain: list[str] = []

        for seg in message:
            if not isinstance(seg, dict):
                continue
            seg_type = str(seg.get("type") or "").strip()
            data = seg.get("data")
            if not isinstance(data, dict):
                data = {}
            kv: dict[str, Any] = dict(data)

            field_keys = self._CQ_FIELD_MAP.get(seg_type)
            if field_keys is None:
                # 不支持的类型：占位
                parts_cq.append(f"[CQ:{seg_type}]")
                continue

            field_val = self._first_non_empty(kv, field_keys)

            # 特殊副作用：at/reply/forward 需要写入 out 的字段
            if seg_type == "at" and field_val and field_val != "all":
                out.at_qq.append(field_val)
            elif seg_type == "reply" and field_val and not out.reply_id:
                out.reply_id = field_val
            elif seg_type == "forward" and field_val and not out.forward_id:
                out.forward_id = field_val
            elif seg_type == "image":
                url = str(kv.get("url") or "").strip()
                if url.startswith("https://"):
                    out.media.append(url)
                    out.media_meta.append({
                        "type": "image",
                        "url": url,
                        "file": str(kv.get("file") or kv.get("name") or "").strip(),
                    })

            # 构建精简 CQ
            if seg_type == "text":
                if field_val:
                    parts_cq.append(field_val)
                    parts_plain.append(field_val)
                out.segments.append(ParsedSegment(type="text", data={"text": field_val}))
            else:
                cq_str = (
                    f"[CQ:{seg_type},{field_keys[0]}={field_val}]" if field_val else f"[CQ:{seg_type}]"
                )
                parts_cq.append(cq_str)
                out.segments.append(ParsedSegment(type=seg_type, data=kv))

        out.cq_text = "".join(parts_cq).strip()
        out.plain_text = "".join(parts_plain).strip()
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

        parts_cq: list[str] = []
        parts_plain: list[str] = []

        pos = 0
        for m in self._cq_re.finditer(txt):
            start, end = m.span()
            # leading text
            if start > pos:
                t = txt[pos:start]
                if t:
                    parts_cq.append(t)
                    parts_plain.append(t)
                    out.segments.append(ParsedSegment(type="text", data={"text": t}))

            seg_type = str(m.group(1) or "").strip()
            rest = m.group(2) or ""
            kv: dict[str, Any] = {}
            if rest:
                for item in rest.split(","):
                    if "=" in item:
                        k, v = item.split("=", 1)
                        kv[k.strip()] = v.strip()

            field_keys = self._CQ_FIELD_MAP.get(seg_type)
            if field_keys is None:
                # 暂不支持：占位（不保留参数，避免超长）
                parts_cq.append(f"[CQ:{seg_type}]")
                pos = end
                continue

            field_val = self._first_non_empty(kv, field_keys)

            # 特殊副作用：at/reply/forward 需要写入 out 的字段
            if seg_type == "at" and field_val and field_val != "all":
                out.at_qq.append(field_val)
            elif seg_type == "reply" and field_val and not out.reply_id:
                out.reply_id = field_val
            elif seg_type == "forward" and field_val and not out.forward_id:
                out.forward_id = field_val

            if seg_type == "text":
                # CQ 字符串里几乎不会出现，但保留
                if field_val:
                    parts_cq.append(field_val)
                    parts_plain.append(field_val)
                out.segments.append(ParsedSegment(type="text", data={"text": field_val}))
            else:
                cq_str = (
                    f"[CQ:{seg_type},{field_keys[0]}={field_val}]" if field_val else f"[CQ:{seg_type}]"
                )
                parts_cq.append(cq_str)
                out.segments.append(ParsedSegment(type=seg_type, data=kv))

            pos = end

        # trailing text
        if pos < len(txt):
            t = txt[pos:]
            if t:
                parts_cq.append(t)
                parts_plain.append(t)
                out.segments.append(ParsedSegment(type="text", data={"text": t}))

        out.cq_text = "".join(parts_cq).strip()
        out.plain_text = "".join(parts_plain).strip()
        return out


class NapCatMessageDetailFetcherV2:
    """V2 详情获取器：包装 NapCatTransport.call_action()."""

    def __init__(self, *, transport: "NapCatTransport", parser: NapCatMessageParserV2) -> None:
        self._transport = transport
        self._parser = parser

    async def expand_reply(self, *, message_id: str, chat: ChatRef) -> ExpandedReply:
        mid = str(message_id or "").strip()
        if not mid:
            return ExpandedReply(id="")

        resp = await self._transport.call_action(ACTION_GET_MSG, {"message_id": mid}, timeout=10.0)
        if not (isinstance(resp, dict) and resp.get("status") == "ok" and int(resp.get("retcode", -1)) == 0):
            return ExpandedReply(id=mid)

        data = resp.get("data")
        if not isinstance(data, dict):
            return ExpandedReply(id=mid)

        sender = data.get("sender")
        if not isinstance(sender, dict):
            sender = {}

        uid = str(sender.get("user_id") or data.get("user_id") or "").strip()
        u = str(sender.get("card") or sender.get("nickname") or uid or "unknown").strip()

        payload_like = {
            "message": data.get("message"),
            "raw_message": data.get("raw_message"),
            "message_format": "array" if isinstance(data.get("message"), list) else "string",
        }
        parsed = self._parser.parse(payload=payload_like, self_id="")
        m = parsed.cq_text or str(data.get("raw_message") or "").strip()

        # 如果引用原消息里包含 forward，可提前展开引用 forward（后续可复用 forward_cache）
        fw: Optional[ExpandedForward] = None
        if parsed.forward_id:
            fw = await self.expand_forward(forward_id=parsed.forward_id, chat=chat)

        return ExpandedReply(id=mid, u=u, q=uid, m=m, forward=fw)

    async def expand_forward(self, *, forward_id: str, chat: ChatRef) -> ExpandedForward:
        fid = str(forward_id or "").strip()
        if not fid:
            return ExpandedForward(id="")

        resp = await self._transport.call_action(ACTION_GET_FORWARD_MSG, {"id": fid}, timeout=15.0)
        if not (isinstance(resp, dict) and resp.get("status") == "ok" and int(resp.get("retcode", -1)) == 0):
            return ExpandedForward(id=fid)

        data = resp.get("data")
        if not isinstance(data, dict):
            return ExpandedForward(id=fid)

        nodes = data.get("messages")
        if not isinstance(nodes, list):
            return ExpandedForward(id=fid)

        items: list[dict[str, str]] = []
        for node in nodes:
            if isinstance(node, str):
                payload_like = {"message": node, "raw_message": node, "message_format": "string"}
                parsed = self._parser.parse(payload=payload_like, self_id="")
                m = parsed.cq_text or str(node)
                items.append({"u": "unknown", "m": m})
                continue

            if not isinstance(node, dict):
                continue

            sender = node.get("sender")
            if not isinstance(sender, dict):
                sender = {}
            uid = str(node.get("user_id") or sender.get("user_id") or "").strip()
            u = str(sender.get("card") or sender.get("nickname") or uid or "unknown").strip()

            payload_like = {
                "message": node.get("message"),
                "raw_message": node.get("raw_message"),
                "message_format": "array" if isinstance(node.get("message"), list) else "string",
            }
            parsed = self._parser.parse(payload=payload_like, self_id="")
            m = parsed.cq_text or str(node.get("raw_message") or "").strip()
            items.append({"u": u, "m": m})

        return ExpandedForward(id=fid, items=items)



class NapCatContextBuilderV2Impl:
    """V2 上下文构建器：输出 JSON lines，不包含 group_name。"""

    def build(
        self,
        *,
        chat: ChatRef,
        bot_name: str,
        bot_id: str,
        time_window_label: str,
        prompts: list[str],
        messages: list[ContextMessageLine],
    ) -> str:
        lines: list[str] = []
        lines.append(CONTEXT_BEGIN_TAG)
        lines.append(f"chat_type={chat.chat_type} chat_id={chat.chat_id}")
        lines.append(f"bot={bot_name}(self_id={bot_id})")
        lines.append(f"time_window={time_window_label}")

        if prompts:
            lines.append("\n# Some prompts")
            lines.extend(prompts)

        lines.append("\n# Messages (oldest -> newest)")
        for m in messages:
            obj: dict[str, Any] = {"u": m.u, "q": m.q, "id": m.id, "m": m.m}
            if m.r is not None:
                obj["r"] = m.r
            if m.f is not None:
                obj["f"] = m.f
            lines.append(_json_dumps_compact(obj))

        lines.append(CONTEXT_END_TAG)
        return "\n".join(lines)

@dataclass(slots=True)
class V2SessionState:
    """单个 session 的内存状态（V2）。

    - messages: 用于构建 <NAPCAT_WS_CONTEXT> 的 JSON-lines 消息窗口。
    - bot_message_ids: bot 已发送的 message_id（用于 reply_to_bot 触发兜底判断）。

    说明：
        - 不做旧兼容：不再维护 ConversationStore / ChatRecord。
        - 只保留最近 N 条（N=buffer_size）。
    """

    messages: Deque[ContextMessageLine]
    bot_message_ids: Deque[str]


class V2SessionStore:
    """V2 Session store（进程内存）。

    设计目标：
      - 单一事实来源：上下文与 reply_to_bot 判断都从这里取。
      - 轻量、可控：只存最近 N 条，进程重启即清空。
    """

    def __init__(self, buffer_size: int) -> None:
        self._buffer_size = int(buffer_size or 50)
        self._sessions: dict[str, V2SessionState] = {}

    def get_or_create(self, session_key: str) -> V2SessionState:
        if session_key not in self._sessions:
            self._sessions[session_key] = V2SessionState(
                messages=deque(maxlen=self._buffer_size),
                bot_message_ids=deque(maxlen=max(self._buffer_size, 50)),
            )
        return self._sessions[session_key]

    def append_message(self, session_key: str, line: ContextMessageLine) -> None:
        self.get_or_create(session_key).messages.append(line)

    def recent_messages(self, session_key: str, limit: int) -> list[ContextMessageLine]:
        if limit <= 0:
            return []
        state = self.get_or_create(session_key)
        return list(state.messages)[-int(limit) :]

    def remember_bot_message_id(self, session_key: str, message_id: str) -> None:
        mid = str(message_id or "").strip()
        if not mid:
            return
        self.get_or_create(session_key).bot_message_ids.append(mid)

    def is_bot_message_id(self, session_key: str, message_id: str) -> bool:
        mid = str(message_id or "").strip()
        if not mid:
            return False
        return mid in self.get_or_create(session_key).bot_message_ids


# ==============================
# 2) Transport 层（WS + OneBot actions）
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

        if not WEBSOCKETS_AVAILABLE:
            raise RuntimeError("websockets dependency is not installed")

        if self._running:
            return
        self._running = True

        backoff_s = 1.0
        while self._running:
            ws: ClientConnection | None = None
            try:
                headers: dict[str, str] = {}
                if getattr(self.config, "token", ""):
                    headers["Authorization"] = f"Bearer {self.config.token}"

                # Match legacy transport defaults (from napcat_ws.py.bak)
                connect_kwargs: dict[str, Any] = {
                    "open_timeout": 10,
                    "ping_interval": 20,
                    "ping_timeout": 20,
                    "close_timeout": 10,
                }

                # Normalize url: ensure it ends with "/" for NapCat.
                url = self._normalize_ws_url(self.config.url)

                # websockets 版本差异：v12 使用 additional_headers；旧版使用 extra_headers
                connect_kwargs["additional_headers"] = headers or None

                try:
                    ws = await websockets.connect(url, **connect_kwargs)
                except TypeError:
                    # websockets<12
                    connect_kwargs.pop("additional_headers", None)
                    connect_kwargs["extra_headers"] = headers or None
                    ws = await websockets.connect(url, **connect_kwargs)

                self._ws = ws
                logger.info("napcat_ws transport connected: {}", url)

                # 连接建立后主动拉取一次登录信息（必须在接收循环开始后/并行执行，避免死锁）
                if self._login_task is None or self._login_task.done():
                    self._login_task = asyncio.create_task(self._fetch_login_info())

                backoff_s = 1.0

                async for raw in ws:
                    if not self._running:
                        break

                    try:
                        if isinstance(raw, (bytes, bytearray)):
                            raw = bytes(raw).decode("utf-8", "ignore")
                        payload = json.loads(raw)
                        if not isinstance(payload, dict):
                            continue
                    except Exception as e:
                        logger.warning("napcat_ws transport: invalid frame: {}", e)
                        continue

                    # 1) action_response：优先按旧实现识别（status+retcode），避免被当作普通事件分发。
                    if ("status" in payload) and ("retcode" in payload):
                        echo = payload.get("echo")
                        if echo is not None:
                            key = str(echo)
                            fut = self._pending.pop(key, None)
                            if fut is not None and not fut.done():
                                logger.debug(
                                    "napcat_ws action recv: echo={} status={} retcode={}",
                                    key,
                                    payload.get("status"),
                                    payload.get("retcode"),
                                )
                                fut.set_result(payload)
                        else:
                            logger.debug(
                                "napcat_ws transport: action response missing echo: keys={}",
                                sorted(payload.keys()),
                            )
                        continue

                    # 2) action_response：兼容实现差异——只要 echo 命中 pending 就消化。
                    echo = payload.get("echo")
                    if echo is not None:
                        key = str(echo)
                        fut = self._pending.pop(key, None)
                        if fut is not None and not fut.done():
                            fut.set_result(payload)
                            continue

                    # 3) 普通事件：以 task 方式上抛给 channel（避免阻塞接收循环）
                    self._spawn_event_task(payload)

            except Exception as e:
                if not self._running:
                    break
                logger.warning("napcat_ws transport error: {} (reconnect in {}s)", e, backoff_s)
                await asyncio.sleep(backoff_s)
                backoff_s = min(backoff_s * 2.0, 30.0)
            finally:
                # 取消并清理所有事件 tasks
                for t in list(self._event_tasks):
                    t.cancel()
                self._event_tasks.clear()

                # 取消登录拉取 task（避免跨连接悬挂）
                if self._login_task is not None and not self._login_task.done():
                    self._login_task.cancel()
                self._login_task = None

                # 连接断开：清理 pending futures，避免一直超时
                for fut in list(self._pending.values()):
                    if not fut.done():
                        fut.set_exception(asyncio.CancelledError())
                self._pending.clear()

                try:
                    if ws is not None:
                        await ws.close()
                except Exception:
                    pass
                self._ws = None

        # stop() 退出后：兜底清 pending
        for fut in list(self._pending.values()):
            if not fut.done():
                fut.set_exception(asyncio.CancelledError())
        self._pending.clear()

    async def stop(self) -> None:
        """停止 transport 并清理 pending futures。"""

        if not self._running:
            return
        self._running = False

        # 主动关闭 websocket（会让 start() 的 async for 退出）
        try:
            if self._ws is not None:
                await self._ws.close()
        except Exception:
            pass
        self._ws = None

        # 清理 event tasks
        for t in list(self._event_tasks):
            t.cancel()
        self._event_tasks.clear()

        if self._login_task is not None and not self._login_task.done():
            self._login_task.cancel()
        self._login_task = None

        # 清理 pending
        for fut in list(self._pending.values()):
            if not fut.done():
                fut.set_exception(asyncio.CancelledError())
        self._pending.clear()

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
                logger.exception("napcat_ws transport: on_event task failed: {}", summary)

        task.add_done_callback(_done)

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
                        logger.info("napcat_ws login: user_id={} nickname={}", uid, nick)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.debug("napcat_ws get_login_info failed: {}", e)

    async def call_action(self, action: str, params: dict[str, Any], timeout: float = 10.0) -> dict[str, Any]:
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
        logger.debug("napcat_ws action send: action={} echo={}", action, echo)
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
# 3) Normalize 层（OneBot payload -> 内部结构）
# ==============================


def make_session_key(chat: ChatRef) -> str:
    """根据 chat 生成稳定的 session key。"""

    return f"napcat:{chat.chat_type}:{chat.chat_id}"


def normalize_inbound(
    payload: dict[str, Any],
    *,
    self_id: str,
    config: NapCatWSConfig,
    parser_v2: NapCatMessageParserV2 | None = None,
) -> NormalizedInbound | None:
    """将 OneBot payload 归一化为内部消息结构。

    Inbound 主链路只依赖 V2 parser：
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

    # 构造一个最小 payload 供 V2 parser 使用：
    # - message: 取 message/raw_message 的兜底
    # - raw_message/message_format: 原样透传
    payload_like = {
        "message": message_field,
        "raw_message": payload.get("raw_message"),
        "message_format": payload.get("message_format"),
    }
    parsed_v2 = parser_v2.parse(payload=payload_like, self_id=str(self_id or "")) if parser_v2 else ParsedMessage()

    text = str(parsed_v2.plain_text or "").strip()
    rendered_text = str(parsed_v2.rendered_text or parsed_v2.cq_text or text or "").strip()
    rendered_segments = list(parsed_v2.rendered_segments or [])

    reply_message_id = str(parsed_v2.reply_id or "")
    reply: QuotedMessage | None = None
    if reply_message_id:
        reply = QuotedMessage(message_id=reply_message_id)

    sid = str(self_id or "").strip()
    at_self = bool(sid and sid in [str(x).strip() for x in parsed_v2.at_qq])
    forward_id = str(parsed_v2.forward_id or "")

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
        media=list(parsed_v2.media or []),
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
# 4) Policy 层（触发决策）
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


def _is_allowed_source(msg: NormalizedInbound, config: NapCatWSConfig) -> bool:
    """allow_from 白名单判断。

    兼容几种常见写法：
      - "*"：全部允许
      - "<sender_id>"：按发送者
      - "<chat_id>"：按会话 id（私聊=对方 id；群聊=群号）
      - "group:<group_id>" / "private:<user_id>"：按 chat_type 显式写
    """

    allow = list(getattr(config, "allow_from", []) or [])
    if not allow:
        # schema 默认是 ["*"]，这里保持“空=不额外限制”的宽松策略
        return True

    allow = [str(x).strip() for x in allow if str(x).strip()]
    if "*" in allow:
        return True

    sender_id = str(msg.sender_id or "").strip()
    chat_id = str(msg.chat.chat_id or "").strip()

    if sender_id and sender_id in allow:
        return True
    if chat_id and chat_id in allow:
        return True

    if msg.chat.chat_type == "group":
        gid = str(msg.chat.group_id or msg.chat.chat_id or "").strip()
        if gid and f"group:{gid}" in allow:
            return True
    else:
        uid = str(msg.chat.user_id or msg.chat.chat_id or "").strip()
        if uid and f"private:{uid}" in allow:
            return True

    return False


def _is_reply_to_bot(msg: NormalizedInbound, *, store: V2SessionStore, session_key: str, self_id: str) -> bool:
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


def _probability_for_message(msg: NormalizedInbound, config: NapCatWSConfig) -> tuple[float, TriggerType]:
    """返回当前消息适用的概率及触发类型。"""

    if msg.chat.chat_type == "private":
        return float(getattr(config, "private_trigger_probability", 0.0) or 0.0), "private_probability"
    return float(getattr(config, "group_trigger_probability", 0.0) or 0.0), "group_probability"


def decide_message_trigger(
    msg: NormalizedInbound,
    *,
    config: NapCatWSConfig,
    store: V2SessionStore,
    session_key: str,
    self_id: str,
) -> TriggerDecision:
    """决定 message 事件是否触发回复。"""

    if not _is_allowed_source(msg, config):
        return TriggerDecision(False, "not_allowed")

    if getattr(config, "ignore_self_messages", True) and str(msg.sender_id) == str(self_id):
        return TriggerDecision(False, "ignore_self")

    if msg.chat.chat_type == "private":
        blk = set(str(x) for x in (getattr(config, "blacklist_private_ids", []) or []))
        if str(msg.chat.chat_id) in blk or str(msg.sender_id) in blk:
            return TriggerDecision(False, "blacklist_private")
    else:
        blk = set(str(x) for x in (getattr(config, "blacklist_group_ids", []) or []))
        gid = str(msg.chat.group_id or msg.chat.chat_id)
        if gid in blk:
            return TriggerDecision(False, "blacklist_group")

    if msg.chat.chat_type == "group" and getattr(config, "trigger_on_at", True) and bool(msg.at_self):
        return TriggerDecision(True, "at_bot", "at_bot")

    if getattr(config, "trigger_on_reply_to_bot", True) and _is_reply_to_bot(
        msg, store=store, session_key=session_key, self_id=self_id
    ):
        return TriggerDecision(True, "reply_to_bot", "reply_to_bot")

    nicknames = list(getattr(config, "nickname_triggers", []) or [])
    if nicknames and _contains_any(msg.text, nicknames):
        return TriggerDecision(True, "nickname", "nickname")

    if not _message_has_content(msg):
        return TriggerDecision(False, "no_content")

    probability, trigger = _probability_for_message(msg, config)
    if probability <= 0.0:
        return TriggerDecision(False, "probability=0", trigger, probability)
    if probability >= 1.0:
        return TriggerDecision(True, "probability=1", trigger, probability)

    seed = f"{session_key}|{msg.message_id}|{msg.sender_id}|{msg.timestamp.timestamp()}"
    sample = _stable_random_0_1(seed)
    if sample < probability:
        return TriggerDecision(True, f"probability({sample:.3f}<{probability:.3f})", trigger, probability)
    return TriggerDecision(False, f"probability({sample:.3f}>={probability:.3f})", trigger, probability)


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

    return TriggerDecision(True, "poke", "poke")


_CQ_OUTBOUND_RE = re.compile(r"\[CQ:([^,\]]+)(?:,([^\]]+))?\]")


def _parse_cq_params(params_raw: str) -> dict[str, str]:
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
                issues.append(CQValidationIssue("missing_required_param", "at 缺少 qq 参数", cq_text))
                continue
            if qq != "all" and not qq.isdigit():
                issues.append(CQValidationIssue("invalid_param_value", f"at.qq 非法: {qq}", cq_text))
                continue
            continue

        if cq_type == "reply":
            reply_id = str(params.get("id") or "").strip()
            if not reply_id:
                issues.append(CQValidationIssue("missing_required_param", "reply 缺少 id 参数", cq_text))
                continue
            continue

        if cq_type == "image":
            file_uri = str(params.get("file") or "").strip()
            if not file_uri:
                issues.append(CQValidationIssue("missing_required_param", "image 缺少 file 参数", cq_text))
                continue
            local_path = _file_uri_to_local_path(file_uri)
            if not local_path:
                issues.append(CQValidationIssue("invalid_param_value", f"image.file 必须是 file:// URI: {file_uri}", cq_text))
                continue
            if not os.path.exists(local_path):
                issues.append(CQValidationIssue("image_file_not_found", f"image.file 不存在: {local_path}", cq_text))
                continue
            if not os.path.isfile(local_path):
                issues.append(CQValidationIssue("invalid_param_value", f"image.file 不是普通文件: {local_path}", cq_text))
                continue
            continue

        issues.append(CQValidationIssue("unsupported_cq_type", f"暂不支持的 CQ 类型: {cq_type}", cq_text))

    return CQValidationResult(valid=not issues, normalized_text=text, issues=issues)


# ==============================
# 6) Channel glue（把各层串起来）
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

        self._store = V2SessionStore(buffer_size=self.config.session_buffer_size)
        self._transport = NapCatTransport(config=self.config, on_event=self._on_transport_event)

        self._parser_v2 = NapCatMessageParserV2()
        self._cache_v2 = InMemoryMessageCache(
            ttl_seconds=DEFAULT_CACHE_TTL_SECONDS,
            reply_maxsize=V2_REPLY_CACHE_MAXSIZE,
            forward_maxsize=V2_FORWARD_CACHE_MAXSIZE,
        )
        self._fetcher_v2 = NapCatMessageDetailFetcherV2(transport=self._transport, parser=self._parser_v2)
        self._ctx_builder_v2 = NapCatContextBuilderV2Impl()

        # 多次回复（multi-turn follow-up）任务容器：保留该能力，具体调度逻辑后续实现。
        self._followup_tasks: dict[str, asyncio.Task[None]] = {}

        # 戳一戳(poke) 冷却：key 为 "{chat_type}:{chat_id}"，value 为上次触发时间戳(time.time())。
        # 该冷却只用于 notice(poke) 链路，避免被连续戳导致刷屏。
        self._poke_last_ts_by_chat: dict[str, float] = {}

    def _now_ts(self) -> float:
        return datetime.now().timestamp()

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
            resp = await self._transport.call_action(ACTION_GET_GROUP_MEMBER_INFO, params=params, timeout=10.0)
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

        if not WEBSOCKETS_AVAILABLE:
            raise RuntimeError("websockets dependency is not installed")

        self._running = True
        logger.info("{}: starting", self.name)
        await self._transport.start()

    async def stop(self) -> None:
        """停止渠道。"""

        self._running = False
        logger.info("{}: stopping", self.name)

        for task in list(self._followup_tasks.values()):
            task.cancel()
        self._followup_tasks.clear()

        await self._transport.stop()

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

        # 最后一层保险：绝不把控制标签发到群里/私聊里。
        if NO_REPLY_TAG in content:
            return
        if content:
            content = content.replace(NO_FOLLOWUP_TAG, "")
            content = content.strip()

        if not content.strip() and not msg.media:
            return

        validation = validate_outbound_cq_text(content)
        if not validation.valid:
            detail = "; ".join(f"{x.code}:{x.message}" for x in validation.issues[:5])
            chat_type = str(msg.metadata.get("chat_type") or "")
            gid = msg.metadata.get("group_id") or msg.chat_id
            uid = msg.metadata.get("user_id") or msg.chat_id
            target = gid if chat_type == "group" else uid
            snippet = (content[:200] + "...") if len(content) > 200 else content
            logger.error(
                "napcat_ws drop outbound: invalid cq: {} chat_type={} target={} snippet={}",
                detail,
                chat_type,
                target,
                snippet,
            )
            return

        content = validation.normalized_text

        action = "send_group_msg" if msg.metadata.get("chat_type") == "group" else "send_private_msg"

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
            logger.debug("napcat_ws send failed: {}", exc)
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
                            ContextMessageLine(
                                u=str(sender_name),
                                q=str(self._self_id or ""),
                                id=str(mid),
                                m=str(content),
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
                logger.exception("napcat_ws notice handling failed: {}", summary)
                raise
            return

        if post_type != "message":
            self._log_ignored(payload)
            return

        try:
            await self._handle_message(payload)
        except Exception:
            logger.exception("napcat_ws message handling failed: {}", summary)
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
            logger.debug("napcat_ws self_id set from {}: {}", source, self._self_id)
        else:
            logger.warning(
                "napcat_ws self_id changed from {} to {} (source={})",
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
          Normalize(轻量) -> Policy -> Session/Context(V2 store) -> Agent(bus) -> Outbound(send)

        注意：notice 没有天然的 message_id/message 段，因此这里会生成一个稳定的“事件 id”，并用
        一条 ContextMessageLine 记录到 V2 store 中。
        """

        decision = decide_notice_trigger(
            payload,
            config=self.config,
            self_id=str(self._self_id or ""),
        )
        if not decision.should_reply:
            if decision.reason != "unsupported_notice":
                logger.debug("napcat_ws notice ignored: {}", decision.reason)
            else:
                self._log_ignored(payload)
            return

        actor = str(payload.get("sender_id") or payload.get("user_id") or "").strip()
        if not actor:
            logger.debug("napcat_ws notice ignored: missing_actor")
            return

        sender_name = self._extract_notice_actor_name(payload, fallback=actor)

        group_id = self._extract_group_id(payload)

        chat = ChatRef(
            chat_type="group" if group_id else "private",
            chat_id=str(group_id or actor),
            user_id=str(actor),
            group_id=str(group_id or ""),
        )
        session_key = make_session_key(chat)

        # 戳一戳冷却（可配置）。key 以 chat 为粒度：群=group_id，私聊=user_id。
        cooldown_s = int(getattr(self.config, "poke_cooldown_seconds", 60) or 0)

        if cooldown_s > 0:
            now_ts = time.time()
            cooldown_key = f"{chat.chat_type}:{chat.chat_id}"
            last_ts = float(self._poke_last_ts_by_chat.get(cooldown_key) or 0.0)
            if last_ts and (now_ts - last_ts) < float(cooldown_s):
                logger.debug(
                    "napcat_ws poke cooldown: key={} delta={:.1f}s < {}s",
                    cooldown_key,
                    now_ts - last_ts,
                    cooldown_s,
                )
                return
            self._poke_last_ts_by_chat[cooldown_key] = now_ts

        # 黑名单：与 message 保持一致（notice 不走概率/昵称/@/reply 触发，但仍应尊重黑名单）
        if group_id:
            blk = set(str(x) for x in (getattr(self.config, "blacklist_group_ids", []) or []))
            if str(group_id) in blk:
                logger.debug("napcat_ws notice ignored: blacklist_group")
                return
        else:
            blk = set(str(x) for x in (getattr(self.config, "blacklist_private_ids", []) or []))
            if str(actor) in blk:
                logger.debug("napcat_ws notice ignored: blacklist_private")
                return

        # 生成“事件 id”：notice 没有 message_id，避免使用空字符串造成上下文难以定位
        notice_type = str(payload.get("notice_type") or "notice").strip() or "notice"
        sub_type = str(payload.get("sub_type") or payload.get("notify_type") or "").strip() or ""
        ts_raw = payload.get("time")
        try:
            ts_i = int(ts_raw) if ts_raw is not None else int(time.time())
        except Exception:
            ts_i = int(time.time())
        event_id = f"notice:{notice_type}:{sub_type}:{ts_i}:{actor}:{group_id or 'private'}"

        # 入库一条 V2 context line：让 poke 成为“可见上下文事件”
        poke_text = "(poke)"
        self._store.append_message(
            session_key,
            ContextMessageLine(
                u=str(sender_name),
                q=str(actor),
                id=event_id,
                m=poke_text,
            ),
        )

        logger.debug(
            "napcat_ws notice trigger: trigger={} actor={} group_id={} session_key={}",
            decision.trigger,
            actor,
            group_id,
            session_key,
        )

        # 需要回复：构建 <NAPCAT_WS_CONTEXT> 并交给 agent
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
            timestamp=datetime.fromtimestamp(ts_i),
            text=poke_text,
            rendered_text=poke_text,
            raw_event=dict(payload),
        )
        content = self._build_chat_context_v2(
            msg=msg,
            session_key=session_key,
            bot_name=bot_name,
        )

        await super()._handle_message(
            sender_id=str(actor),
            chat_id=chat.chat_id,
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
            session_key=session_key,
        )

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
    # V2: JSON lines 上下文（立刻启用）
    # ------------------------------


    async def _v2_build_line(self, msg: NormalizedInbound) -> ContextMessageLine:
        """把当前归一化消息投影成 V2 的 ContextMessageLine（含可选 r/f）。

        规则：
          - `m`：严格使用 parser_v2 的 CQ 风格输出（或 raw_message 兜底）
          - `r`：reply 展开（方案 A）：{"u","m"}
          - `f`：forward 展开：[{"u","m"}, ...]
        """

        message_field = msg.raw_event.get("message")
        raw_field = msg.raw_event.get("raw_message")
        if message_field in (None, ""):
            message_field = raw_field

        message_format = msg.raw_event.get("message_format")
        if message_format in (None, ""):
            if isinstance(message_field, list):
                message_format = "array"
            elif isinstance(message_field, str):
                message_format = "string"
            else:
                message_format = None

        payload_like = {
            "message": message_field,
            "raw_message": raw_field,
            "message_format": message_format,
        }
        parsed = self._parser_v2.parse(payload=payload_like, self_id=str(self._self_id or ""))
        m = parsed.cq_text or str(msg.raw_event.get("raw_message") or msg.rendered_text or msg.text or "").strip()

        line = ContextMessageLine(
            u=str(msg.sender_name or msg.sender_id or "unknown"),
            q=str(msg.sender_id or ""),
            id=str(msg.message_id or ""),
            m=m,
        )

        # reply 展开：优先复用现有 msg.reply（已 expand_quote），否则走 cache+fetcher
        rid = str(parsed.reply_id or "").strip()
        if not rid and msg.reply is not None and msg.reply.message_id:
            rid = str(msg.reply.message_id).strip()

        if rid:
            # 优先复用 msg.reply，并统一用 parser_v2 生成精简 CQ
            if msg.reply is not None and str(msg.reply.message_id or "").strip() == rid:
                payload_like_quote = {
                    "message": msg.reply.message,
                    "raw_message": msg.reply.raw_message,
                    "message_format": "array" if isinstance(msg.reply.message, list) else "string",
                }
                pq = self._parser_v2.parse(payload=payload_like_quote, self_id=str(self._self_id or ""))
                qm = str(pq.cq_text or "").strip() or str(msg.reply.text or "").strip()
                qu = str(msg.reply.sender_name or msg.reply.sender_id or "unknown")
                if qu and qm:
                    line.r = {"u": qu, "m": qm}
            else:
                try:
                    exp = await self._cache_v2.get_or_fetch_reply(
                        message_id=rid,
                        fetch=lambda: self._fetcher_v2.expand_reply(message_id=rid, chat=msg.chat),
                    )
                except Exception:
                    exp = None

                if exp is not None and str(exp.u or "").strip() and str(exp.m or "").strip():
                    line.r = {"u": str(exp.u), "m": str(exp.m)}

        # forward 展开：优先复用现有 msg.forward_items（已 expand_forward），否则走 cache+fetcher
        fid = str(parsed.forward_id or "").strip()
        if not fid and msg.forward_id:
            fid = str(msg.forward_id).strip()

        if fid:
            # 如果 normalize/expand 已经拿到 forward_items，我们优先把它们转成 V2 的 {u,m}，避免重复 action
            if msg.forward_items:
                items_v2: list[dict[str, str]] = []
                for it in msg.forward_items:
                    if not isinstance(it, dict):
                        continue
                    u2 = str(it.get("sender_name") or it.get("sender_id") or "unknown")

                    payload_like_node = {
                        "message": it.get("message"),
                        "raw_message": it.get("raw_message"),
                        "message_format": "array" if isinstance(it.get("message"), list) else "string",
                    }
                    parsed_node = self._parser_v2.parse(payload=payload_like_node, self_id=str(self._self_id or ""))
                    t2 = str(parsed_node.cq_text or "").strip()
                    if not t2:
                        # 兜底：退回旧 pipeline 的 rendered_text（仍可能包含占位符）
                        t2 = str(it.get("text") or "").strip()

                    if t2:
                        items_v2.append({"u": u2, "m": t2})

                fexp = ExpandedForward(id=fid, items=items_v2)
                # 写入 cache，避免后续重复
                try:
                    self._cache_v2.put_forward(fid, fexp)
                except Exception:
                    pass
            else:
                try:
                    fexp = await self._cache_v2.get_or_fetch_forward(
                        forward_id=fid,
                        fetch=lambda: self._fetcher_v2.expand_forward(forward_id=fid, chat=msg.chat),
                    )
                except Exception:
                    fexp = None

            if fexp is not None and getattr(fexp, "items", None):
                line.f = list(fexp.items)

        return line

    def _build_chat_context_v2(self, *, msg: NormalizedInbound, session_key: str, bot_name: str) -> str:
        hist_limit = int(getattr(self.config, "context_max_messages", 20) or 20)
        time_window_label = f"latest_{hist_limit}"
        hist = self._store.recent_messages(session_key, hist_limit)

        return self._ctx_builder_v2.build(
            chat=msg.chat,
            bot_name=str(bot_name or ""),
            bot_id=str(self._self_id or ""),
            time_window_label=time_window_label,
            prompts=[],
            messages=hist,
        )

    async def _handle_message(self, payload: dict[str, Any]) -> None:
        msg = normalize_inbound(
            payload,
            self_id=str(self._self_id or ""),
            config=self.config,
            parser_v2=self._parser_v2,
        )
        if msg is None:
            self._log_ignored(payload)
            return

        msg.media_paths = await self._download_images_from_message(msg.raw_event.get("message"))

        await self._expand_quote(msg)
        await self._expand_forward(msg)

        session_key = make_session_key(msg.chat)

        # 先决策（避免把当前 msg 写入 store 后又被 history 重复读到）
        decision = decide_message_trigger(
            msg,
            config=self.config,
            store=self._store,
            session_key=session_key,
            self_id=str(self._self_id or ""),
        )


        # V2: 始终缓存一条 JSON line（用于构建 <NAPCAT_WS_CONTEXT>）
        try:
            v2_line = await self._v2_build_line(msg)
            self._store.append_message(session_key, v2_line)
        except Exception as exc:
            logger.debug("napcat_ws v2 build line failed: {}", exc)

        # 缓存 bot 自己的昵称（用于把 @self 渲染成 @昵称；不影响其它逻辑）
        if msg.at_self and msg.chat.chat_type == "group":
            try:
                # 如果当前消息里出现了 @self，而 config 里没填 self_nickname，就用群里看到的 sender_name 兜底不了
                # 这里不做自动学习，避免误学；仅在聊天上下文 render 时使用 config.self_nickname。
                pass
            except Exception:
                pass

        self._log_inbound(msg)
        logger.debug(
            "napcat_ws trigger decision: should_reply={} reason={} trigger={} probability={}",
            decision.should_reply,
            decision.reason,
            decision.trigger,
            decision.probability_used,
        )

        if not decision.should_reply:
            return

        # allowFrom 语义（NapCatWS 扩展）：
        #
        # 需求：允许“用户白名单”和“群白名单”两种写法，同时“黑名单优先级最高”。
        # 例如：用户A在 allowFrom 中，但用户A在黑名单群里发言 —— 预期仍然不能触发。
        #
        # 说明：BaseChannel 只按 sender_id 做 allowlist 判断；而 napcat_ws 的 allowFrom 既可能填 user_id，
        # 也可能填 group_id（允许白名单群里任何人触发）。因此这里在调用 super()._handle_message() 前先拦一次。

        # 1) 黑名单优先（防御性：即使上游 trigger 决策已做过黑名单判断，这里仍再次确保）
        if msg.chat.chat_type == "group":
            gid = str(msg.chat.group_id or msg.chat.chat_id)
            blk_groups = set(str(x) for x in (getattr(self.config, "blacklist_group_ids", []) or []))
            if gid in blk_groups:
                logger.debug("napcat_ws message ignored: blacklist_group ({})", gid)
                return
        else:
            blk_priv = set(str(x) for x in (getattr(self.config, "blacklist_private_ids", []) or []))
            # 私聊场景：既屏蔽 user_id，也屏蔽 chat_id（通常相同，但保留兼容）
            if str(msg.sender_id) in blk_priv or str(msg.chat.chat_id) in blk_priv:
                logger.debug("napcat_ws message ignored: blacklist_private ({})", msg.sender_id)
                return

        # 2) allowFrom 白名单（为空 → 不限制；包含 * → 全允许）
        allow_list = [str(x) for x in (getattr(self.config, "allow_from", []) or []) if str(x).strip()]
        if allow_list and "*" not in allow_list:
            sender_ok = str(msg.sender_id) in allow_list
            chat_ok = (msg.chat.chat_type == "group" and str(msg.chat.group_id) in allow_list)
            if not (sender_ok or chat_ok):
                logger.warning(
                    "Access denied for sender {} on channel {}. Add them or the group_id({}) to allowFrom list.",
                    msg.sender_id,
                    self.name,
                    msg.chat.chat_id,
                )
                return

        bot_name = ""
        try:
            if msg.chat.chat_type == "group" and msg.chat.group_id:
                bot_name = await self._get_bot_display_name_for_group(msg.chat.group_id)
        except Exception:
            bot_name = ""

        content = self._build_chat_context_v2(
            msg=msg,
            session_key=session_key,
            bot_name=bot_name,
        )

        # 交给 agent（MessageBus）生成回复；NapCat 的 send() 会把 OutboundMessage 发回平台
        await super()._handle_message(
            sender_id=msg.sender_id,
            chat_id=msg.chat.chat_id,
            content=content,
            media=msg.media or [],
            metadata={
                "chat_type": msg.chat.chat_type,
                "user_id": msg.chat.user_id,
                "group_id": msg.chat.group_id,
                "message_id": msg.message_id,
                "session_key": session_key,
                "_napcat_trigger": decision.trigger,
                "_napcat_trigger_reason": decision.reason,
            },
            session_key=session_key,
        )

    async def _expand_quote(self, msg: NormalizedInbound) -> None:
        if msg.reply is None or not msg.reply.message_id:
            return

        rid = str(msg.reply.message_id).strip()
        if not rid:
            return

        try:
            exp = await self._cache_v2.get_or_fetch_reply(
                message_id=rid,
                fetch=lambda: self._fetcher_v2.expand_reply(message_id=rid, chat=msg.chat),
            )
        except Exception as exc:
            logger.debug("napcat_ws quote expand failed: {}", exc)
            return

        if exp is None:
            return

        msg.reply.sender_id = str(exp.q or "")
        msg.reply.sender_name = str(exp.u or exp.q or "unknown")
        msg.reply.text = str(exp.m or "")
        msg.reply.media = []
        msg.reply.media_paths = []
        msg.reply.forward_items = list(getattr(getattr(exp, "forward", None), "items", None) or [])
        msg.reply.forward_id = str(getattr(exp, "id", "") or rid)

    async def _fetch_forward_items(self, forward_id: str, chat: ChatRef) -> list[dict[str, Any]]:
        fid = str(forward_id or "").strip()
        if not fid:
            return []

        try:
            fexp = await self._cache_v2.get_or_fetch_forward(
                forward_id=fid,
                fetch=lambda: self._fetcher_v2.expand_forward(forward_id=fid, chat=chat),
            )
        except Exception as exc:
            logger.debug("napcat_ws forward fetch failed: {}", exc)
            return []

        if fexp is None or not getattr(fexp, "items", None):
            return []

        items: list[dict[str, Any]] = []
        for item in fexp.items:
            if not isinstance(item, dict):
                continue
            items.append(
                {
                    "sender_id": str(item.get("q") or ""),
                    "sender_name": str(item.get("u") or item.get("q") or "unknown"),
                    "time": None,
                    "text": str(item.get("m") or ""),
                    "media": [],
                    "media_paths": [],
                    "media_meta": [],
                    "message": None,
                    "raw_message": None,
                }
            )
        return items

    async def _expand_forward(self, msg: NormalizedInbound) -> None:
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
        payload_msg = message if message not in (None, "") else raw
        message_format = (
            "array" if isinstance(payload_msg, list) else ("string" if isinstance(payload_msg, str) else None)
        )
        payload_like = {
            "message": payload_msg,
            "raw_message": raw,
            "message_format": message_format,
        }
        return self._parser_v2.parse(payload=payload_like, self_id=str(self._self_id or ""))

    def _render_text_and_media(self, *, message: Any, raw: Any) -> tuple[str, list[str], list[dict[str, Any]]]:
        parsed = self._parse_payload_message(message=message, raw=raw)
        effective_text = str(parsed.rendered_text or parsed.cq_text or parsed.plain_text or "").strip()
        media = list(parsed.media or [])
        media_meta = list(parsed.media_meta or [])
        return effective_text, media, media_meta

    async def _download_images_from_message(self, message: Any) -> list[str]:
        """将 message segments 中的图片下载到本地 media 目录。

        策略（按你的要求，方案 A）：
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
            ok = await asyncio.to_thread(self._download_one_https_file, url, part, dest)
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

    def _download_one_https_file(self, url: str, part: Path, dest: Path) -> bool:
        """下载单个 https 文件。

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
            logger.debug("napcat_ws image download failed: url={} err={}", url, exc)
            return False

    def _build_forward_summary(self, items: list[dict[str, Any]]) -> str:
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
        reply_id = msg.reply.message_id if msg.reply else ""
        forward_id = msg.forward_id or ""
        forward_items = len(msg.forward_items or [])

        logger.debug(
            "napcat_ws inbound: chat_type={} chat_id={} sender={}({}) text={!r} "
            "media={} at_self={} reply_id={} forward_id={} forward_items={}",
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

    def _log_trigger_decision(self, session_key: str, msg: NormalizedInbound) -> None:
        try:
            decision = decide_message_trigger(
                msg,
                config=self.config,
                store=self._store,
                session_key=session_key,
                self_id=str(self._self_id or ""),
            )
        except Exception as exc:
            logger.debug("napcat_ws decide_message_trigger failed: {}", exc)
            return

        logger.debug(
            "napcat_ws trigger decision: should_reply={} reason={} trigger={} probability={}",
            decision.should_reply,
            decision.reason,
            decision.trigger,
            decision.probability_used,
        )

    def _log_ignored(self, payload: dict[str, Any]) -> None:
        post_type = payload.get("post_type")
        if post_type is None:
            return

        notice_type = payload.get("notice_type")
        sub_type = payload.get("sub_type") or payload.get("notify_type")

        compact = self._compact_payload_for_log(payload)
        logger.debug(
            "NapCat WS ignored event: post_type={} notice_type={} sub_type={} payload={}",
            post_type,
            notice_type,
            sub_type,
            compact,
        )

    def _compact_payload_for_log(self, payload: dict[str, Any]) -> str:
        """将未知事件 payload 压缩成可读、可控长度的字符串。

        目标：看见关键字段和值，但不刷屏。
        """

        def trunc_str(value: Any, *, limit: int = 16) -> Any:
            if not isinstance(value, str):
                return value
            s = value.replace("\n", " ").replace("\r", " ").strip()
            if len(s) <= limit:
                return s
            return s[:limit] + TRUNCATION_TAG

        def compact(value: Any) -> Any:
            if isinstance(value, dict):
                return {str(k): compact(v) for k, v in list(value.items())[:40]}
            if isinstance(value, list):
                head = [compact(v) for v in value[:10]]
                if len(value) > 10:
                    head.append({"...": f"+{len(value) - 10} more"})
                return head
            if isinstance(value, str):
                return trunc_str(value)
            return value

        filtered: dict[str, Any] = dict(payload)

        # 裁剪常见大字段
        if "raw_message" in filtered:
            filtered["raw_message"] = trunc_str(filtered.get("raw_message"), limit=48)
        if "message" in filtered:
            filtered["message"] = compact(filtered.get("message"))

        data = compact(filtered)
        try:
            out = json.dumps(data, ensure_ascii=False, sort_keys=True)
        except Exception:
            out = str(data)

        # 最终截断，保证一行能看完
        max_len = 220
        if len(out) > max_len:
            out = out[:max_len] + TRUNCATION_TAG
        return out


# 向后兼容导出（如果旧代码用 from ... import NapCatWSChannel）
__all__ = ["NapCatWSChannel"]


