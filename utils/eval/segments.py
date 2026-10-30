"""
utils/eval/segments.py — 轨迹分段判定（纯函数，无 IO）

把一次会话的多个请求按「上下文是否还是增量」切成若干段。核心判据：

    grows(prev, next) == True   → 同一条对话的续写，不切
    grows(prev, next) == False  → 上下文断裂，切一段

为什么不能只看 messages 条数：
    实测 key-Afk3 存在 ``20 → 36``、``24 → 42`` 这种**长度在涨、内容却完全对不上**的
    重开（公共前缀只剩 2 条）。纯长度规则会把它们当成续写而静默丢掉前半段。
    所以这里用「累积前缀指纹」：只有当 next 的前 prev.length 条消息逐条同构、
    且 next 更长时，才算增长。

为什么比较前要剥 reasoning / thinking：
    同一轮请求在多份快照里，推理块可能缺失或被重新生成。身份应只由「可见内容 +
    tool_calls」决定，否则同一段对话会被判成断裂。

行为来源：``temp/reformat_jiuwenswarm_dev.py`` 的 ``_grows`` / ``_fingerprint`` /
``_kind``，此处抽成独立模块以便多个导出模式复用。

注意 ``grows`` 里那条额外的排除规则：若 prev 的**响应**是「有 reasoning 但正文与
tool_calls 都为空」的 assistant，则不算增长。这种响应通常是上游截断，不能当作
下一轮历史的合法前缀。
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

__all__ = [
    "Request",
    "classify_kind",
    "fingerprint",
    "grows",
    "has_substance",
    "match_key",
    "same",
    "signature",
    "text",
]

# ── 压缩标记 ──────────────────────────────────────────────────────────────
# jiuwenswarm 系的「记忆压缩」产物。注意 memory_block 有 round/dialogue/current 三种。
_MEMORY_TAG = r"(?:memory_block_(?:round|dialogue|current)|recovered_context)"
_MEMORY_RE = re.compile(
    rf"<(?P<tag>{_MEMORY_TAG})\b[^>]*>.*?</(?P=tag)>",
    re.I | re.S,
)
_MEMORY_OPEN_RE = re.compile(rf"<{_MEMORY_TAG}\b", re.I)

# 压缩「指令型」请求的指纹：这类请求本身就是在执行压缩，不是对话内容。
_COMPRESSION_AGENT_MARK = "You are an Execution State Compression Assistant."
_COMPRESSION_RULES_MARK = "NON-NEGOTIABLE OUTPUT RULES"
_STATE_SNAPSHOT_MARK = "<state_snapshot>"


def text(value: Any) -> str:
    """把任意 content 结构压成用于分类的纯文本（只用于判定，不用于序列化）。"""
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return "\n".join(text(x) for x in value)
    if isinstance(value, dict):
        for key in ("text", "content", "thinking", "reasoning"):
            if key in value:
                return text(value[key])
        return ""
    return str(value)


def classify_kind(
    req: dict,
    obj: dict,
    history: List[dict],
    source: Any = None,
    *,
    heartbeat_auxiliary: bool = True,
) -> str:
    """判定一条请求的轨迹类型。

    返回 ``compaction`` / ``post_compaction`` / ``auxiliary`` / ``initial_query``。

    heartbeat_auxiliary: 是否把 ``source == "__heartbeat__"`` 判为 auxiliary。
        dev 模式为 True，xiaoyi 模式为 False（心跳请求按普通请求处理）。

    顺序很重要：先用 ``_MEMORY_RE`` **剥掉**记忆块，再找压缩指令标记。
    否则记忆块里若含有压缩指令的字样，会把一次普通请求误判成 compaction。
    """
    if heartbeat_auxiliary and source == "__heartbeat__":
        return "auxiliary"

    actual = _MEMORY_RE.sub("", _actual_instructions(req, history, obj))

    if _COMPRESSION_AGENT_MARK in actual or (
        _COMPRESSION_RULES_MARK in actual and _STATE_SNAPSHOT_MARK in actual
    ):
        return "compaction"

    # 请求里带了记忆块 → 这是压缩**之后**的续写。
    if (
        _MEMORY_OPEN_RE.search(text(req.get("instructions", "")))
        or _MEMORY_OPEN_RE.search(text(obj.get("instructions", "")))
        or any(_MEMORY_OPEN_RE.search(text(m.get("content"))) for m in history)
    ):
        return "post_compaction"

    return "initial_query"


def _actual_instructions(req: dict, history: List[dict], obj: dict) -> str:
    """收集「真正下发给模型」的指令文本：system/instructions + 最后一条 user。"""
    items = []

    for item in (req, obj):
        for key in ("instructions", "system"):
            if key in item:
                items.append(text(item[key]))

    for msg in history:
        if msg["role"] in ("system", "developer"):
            items.append(text(msg.get("content")))

    for msg in reversed(history):
        if msg["role"] == "user":
            items.append(text(msg.get("content")))
            break

    return "\n".join(items)


# ── 指纹 ──────────────────────────────────────────────────────────────────


def signature(msg: dict, *, include_reasoning: bool = False) -> bytes:
    """单条消息的指纹。

    include_reasoning=False（默认）用于**连续性判定**：剥掉 reasoning_content，
    推理有无不影响「这是不是同一条消息」。

    tool_calls 的 arguments 若是 JSON 字符串会先解析再序列化（sort_keys），
    从而消除键序与空白差异造成的假断裂。
    """
    raw = (
        dict(msg)
        if include_reasoning
        else {key: val for key, val in msg.items() if key != "reasoning_content"}
    )

    if isinstance(raw.get("tool_calls"), list):
        calls = []
        for original in raw["tool_calls"]:
            call = dict(original)
            fn = call.get("function")
            if isinstance(fn, dict):
                fn = dict(fn)
                args = fn.get("arguments")
                if isinstance(args, str):
                    try:
                        fn["arguments"] = json.loads(args)
                    except json.JSONDecodeError:
                        pass
                call["function"] = fn
            calls.append(call)
        raw["tool_calls"] = calls

    return hashlib.blake2b(_dumps(raw).encode("utf-8"), digest_size=16).digest()


def _dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def has_text(content: Any) -> bool:
    if isinstance(content, str):
        return bool(content.strip())
    if isinstance(content, list):
        return any(
            isinstance(p, dict)
            and p.get("type") == "text"
            and isinstance(p.get("text"), str)
            and p["text"].strip()
            for p in content
        )
    return False


def has_substance(content: Any) -> bool:
    """有没有「实质内容」：文本，或图片。空的 thinking-only 不算。"""
    return has_text(content) or (
        isinstance(content, list)
        and any(isinstance(p, dict) and p.get("type") == "image_url" for p in content)
    )


def match_key(msg: dict) -> Optional[bytes]:
    """可回填 reasoning 的 assistant 消息才有匹配键；空响应返回 None。"""
    if msg.get("role") != "assistant":
        return None
    if not has_substance(msg.get("content")) and not msg.get("tool_calls"):
        return None
    return signature(msg)


@dataclass(slots=True)
class Request:
    """一条已解析的请求（一次 API 调用）。"""

    timestamp: Optional[float]
    order: int
    filename: str
    user_id: Any
    kind: str
    history: List[dict]
    response: List[dict]
    length: int
    digest: bytes
    full_digest: bytes
    prefixes: List[bytes] = field(default_factory=list)
    reasoned: Dict[int, str] = field(default_factory=dict)
    sequence: int = -1
    tools: List[dict] = field(default_factory=list)


def fingerprint(
    history: List[dict],
    response: List[dict],
) -> Tuple[int, bytes, bytes, List[bytes], Dict[int, str]]:
    """计算累积前缀指纹。

    返回 ``(length, digest, full_digest, prefixes, reasoned)``：
      length      消息总数（history + response）
      digest      剥 reasoning 的累积摘要 —— 连续性判定的依据
      full_digest 含 reasoning 的累积摘要 —— 判「完全一致」用
      prefixes    ``prefixes[i]`` 是前 i+1 条消息的累积摘要
      reasoned    ``{消息位置: reasoning 文本}``

    只维护一个滚动 blake2b，所以对每条请求是 O(n) 且内存只留 n 个 16 字节摘要。
    """
    cumulative = hashlib.blake2b(digest_size=24)
    full = hashlib.blake2b(digest_size=24)
    prefixes: List[bytes] = []
    reasoned: Dict[int, str] = {}

    position = 0
    for msg in history:
        _update(position, msg, cumulative, full, prefixes, reasoned)
        position += 1
    for msg in response:
        _update(position, msg, cumulative, full, prefixes, reasoned)
        position += 1

    return (len(prefixes), cumulative.digest(), full.digest(), prefixes, reasoned)


def _update(position, msg, cumulative, full, prefixes, reasoned) -> None:
    continuity = signature(msg)
    complete = signature(msg, include_reasoning=True) if "reasoning_content" in msg else continuity
    cumulative.update(continuity)
    full.update(complete)
    prefixes.append(cumulative.digest())

    reasoning = msg.get("reasoning_content")
    if isinstance(reasoning, str) and reasoning:
        reasoned[position] = reasoning


def same(a: Request, b: Request) -> bool:
    """两次请求完全相同（含 reasoning）：精确重放，合并进同一段。"""
    return a.length == b.length and a.full_digest == b.full_digest


def grows(
    a: Request,
    b: Request,
    known_reasoning: Optional[Dict[int, str]] = None,
) -> bool:
    """b 是否是 a 的**严格增量**（同一条对话往下走了一步）。

    三个条件同时成立才算：
      1. b 更长；
      2. b 的前 a.length 条消息的累积摘要 == a 的摘要（前缀完全同构）；
      3. a 的响应不是「只有 reasoning、没有正文和 tool_calls」的截断响应。

    另外，若 a 记录的 reasoning 与 b 在同一位置的 reasoning 冲突，也不算增长
    （说明这条消息被重新生成过）。
    """
    if b.length <= a.length:
        return False

    if a.length and b.prefixes[a.length - 1] != a.digest:
        return False

    # 上游截断响应：留着 reasoning 却没有正文/工具调用，不能当下一轮的前缀。
    if any(
        m.get("role") == "assistant"
        and m.get("reasoning_content")
        and not has_substance(m.get("content"))
        and not m.get("tool_calls")
        for m in a.response
    ):
        return False

    for pos, val in (known_reasoning if known_reasoning is not None else a.reasoned).items():
        other = b.reasoned.get(pos)
        if other and val != other:
            return False

    return True
