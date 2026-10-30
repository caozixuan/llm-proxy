#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Jiuwenswarm 分段导出：把已分组的 trace 还原成带分段的 OpenAI 轨迹。

按「上下文是否还是增量」把一次会话切成若干段 trajectory，每段一个 JSON，
段边界原因写进 ``meta_info.unique_info.boundary_reason``。切分判据与指纹算法在
``utils/eval/segments``（纯函数，可被其他模式复用）。

一份实现通过 ``profile`` 区分不同业务的差异，避免复制整个文件：

    dev      清理末尾临时上下文；心跳请求判为 auxiliary
    xiaoyi   心跳请求按普通请求处理

历史上 ``temp/reformat_jiuwenswarm_dev.py`` 与 ``..._xiaoyi.py`` 是两个 1391 行的
近乎重复的文件，diff 29 处里只有 1 处是真实行为差异（就是上面的心跳判定），
其余全是注释措辞。合并到这里后，新增业务只需加一个 profile。

Drop-in interface for reformat_jiuwen.jiuwen_reformat_export.
No regrouping, workspace joining, Task Done inference, database access,
or speculative reasoning.
"""
from __future__ import annotations

import hashlib
import inspect
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from itertools import chain
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from utils.eval.reformat import _load_merged, _resolve_triplet
from utils.eval.segments import (
    Request,
    classify_kind,
    fingerprint,
    grows,
    has_text,
    has_substance,
    match_key,
    same,
    signature,
    text as _text,
)
from utils.session_store import _fmt_ts

# 各业务线的差异参数。新增业务 = 加一个 profile，不要复制文件。
#   heartbeat_auxiliary: source=="__heartbeat__" 的请求是否判为 auxiliary
PROFILES: Dict[str, Dict[str, Any]] = {
    "dev": {"heartbeat_auxiliary": True},
    "xiaoyi": {"heartbeat_auxiliary": False},
}
DEFAULT_PROFILE = "dev"

logger = logging.getLogger(__name__)


def _json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _arguments(value: Any, branch: str = DEFAULT_PROFILE) -> str:
    if isinstance(value, str):
        try:
            json.loads(value)
        except json.JSONDecodeError:
            logger.warning(
                "%s: invalid tool arguments JSON, preserving original string",
                branch,
            )
        return value
    return _json(value)


def _request_tools(req: dict) -> List[dict]:
    """Normalize function definitions to OpenAI Chat Completions tools."""
    raw = req["tools"] if "tools" in req else req.get("functions")
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise ValueError("request tools must be a list")

    result = []
    by_name = {}
    for tool in raw:
        if not isinstance(tool, dict):
            raise ValueError("invalid tool definition")
        if tool.get("type") not in (None, "function"):
            raise ValueError(
                f"unsupported tool definition type: {tool.get('type')!r}"
            )
        fn = tool.get("function", tool)
        if not isinstance(fn, dict):
            raise ValueError("invalid tool function definition")
        name = fn.get("name")
        if not isinstance(name, str) or not name.strip():
            raise ValueError("tool definition missing name")
        parameters = fn.get("parameters")
        if parameters is None:
            parameters = fn.get("input_schema")
        if parameters is None:
            parameters = {"type": "object", "properties": {}}
        if not isinstance(parameters, dict):
            raise ValueError(f"invalid parameters for tool {name!r}")
        function = {"name": name, "parameters": parameters}
        for key in ("description", "strict"):
            if key in fn:
                function[key] = fn[key]
        normalized = {"type": "function", "function": function}
        if name in by_name:
            if by_name[name] != normalized:
                raise ValueError(f"conflicting definitions for tool {name!r}")
            continue
        by_name[name] = normalized
        result.append(normalized)
    return result


def _tools_key(tools: List[dict]) -> str:
    """Compare definitions without depending on tool or object key order."""
    return _json(sorted(tools, key=lambda item: item["function"]["name"]))


def _tool_call(call: dict) -> dict:
    fn = (
        call.get("function")
        if isinstance(call.get("function"), dict)
        else call
    )
    result = {
        "type": "function",
        "function": {
            "name": fn.get("name", ""),
            "arguments": _arguments(
                fn.get("arguments", fn.get("input", {}))
            ),
        },
    }
    if call.get("id") is not None:
        result["id"] = call["id"]
    return result


def _image_block(part: dict) -> dict:
    source = part.get("source")
    url = None
    detail = None

    if isinstance(source, dict):
        if source.get("type") == "base64":
            media = source.get("media_type")
            data = source.get("data")
            if (
                not isinstance(media, str)
                or not media.startswith("image/")
                or not isinstance(data, str)
                or not data
            ):
                raise ValueError("unsupported base64 image source")
            url = f"data:{media};base64,{data}"
        elif source.get("type") == "url":
            url = source.get("url")
        else:
            raise ValueError("unsupported image source type")
    elif "image_url" in part:
        image_url = part["image_url"]
        if isinstance(image_url, dict):
            url = image_url.get("url")
            detail = image_url.get("detail")
        else:
            url = image_url
    elif "url" in part:
        url = part["url"]

    if not isinstance(url, str) or not url:
        raise ValueError(
            "image URL/data missing or unsupported image representation"
        )

    out = {"type": "image_url", "image_url": {"url": url}}
    if detail is not None:
        out["image_url"]["detail"] = detail
    return out


def _content_part(part: Any) -> dict:
    if isinstance(part, str):
        return {"type": "text", "text": part}
    if not isinstance(part, dict):
        raise ValueError("unsupported content part value")

    typ = part.get("type")
    if typ in ("text", "input_text", "output_text", "text_delta"):
        val = part.get("text", "")
        if not isinstance(val, str):
            raise ValueError("non-string text content part")
        return {"type": "text", "text": val}

    if typ in ("image", "image_url", "input_image"):
        return _image_block(part)

    raise ValueError("unsupported content part type")


def _payload(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        if "type" not in value:
            raise ValueError("unsupported content object without type")
        value = [value]
    if not isinstance(value, list):
        raise ValueError("unsupported message content container")

    parts = [_content_part(x) for x in value]
    if all(p["type"] == "text" for p in parts):
        return "\n".join(p["text"] for p in parts)
    return parts


def _normalize(msg: dict) -> List[dict]:
    """Expand tool results in original block order."""
    role = msg.get("role", "assistant")
    raw = msg.get("content", "")

    base_calls = [
        _tool_call(c)
        for c in (msg.get("tool_calls") or [])
        if isinstance(c, dict)
    ]
    base_reasoning = _text(
        msg.get("reasoning_content", msg.get("reasoning", ""))
    )
    extra = {
        key: msg[key]
        for key in ("tool_call_id", "name", "refusal")
        if key in msg and msg[key] is not None
    }

    if not isinstance(raw, list):
        result = {"role": role, "content": _payload(raw), **extra}
        if base_reasoning:
            result["reasoning_content"] = base_reasoning
        if base_calls:
            result["tool_calls"] = base_calls
        return [result]

    out = []
    pending = []
    calls = list(base_calls)
    thoughts = []
    had_result = False
    used_extra = False

    def flush(force: bool = False) -> None:
        nonlocal pending, calls, thoughts, used_extra

        if not (
            pending
            or calls
            or thoughts
            or (force and not out)
        ):
            return

        if all(p["type"] == "text" for p in pending):
            content = "\n".join(p["text"] for p in pending)
        else:
            content = pending

        item = {"role": role, "content": content}

        if not used_extra:
            item.update(extra)
            used_extra = True

        reason = base_reasoning if not out else ""
        if not reason and thoughts:
            reason = "\n".join(t for t in thoughts if t)
        if reason:
            item["reasoning_content"] = reason
        if calls:
            item["tool_calls"] = calls

        out.append(item)
        pending, calls, thoughts = [], [], []

    for part in raw:
        if isinstance(part, dict):
            typ = part.get("type")

            if typ in ("thinking", "reasoning"):
                thought = _text(
                    part.get(
                        "thinking",
                        part.get(
                            "reasoning",
                            part.get("text", part.get("content", "")),
                        ),
                    )
                )
                if thought:
                    thoughts.append(thought)
                continue

            if typ == "redacted_thinking":
                continue

            if typ == "tool_use":
                calls.append(_tool_call(part))
                continue

            if typ == "tool_result":
                flush()
                tool_id = part.get(
                    "tool_use_id",
                    part.get("tool_call_id"),
                )
                if tool_id is None:
                    raise ValueError("tool_result missing tool_use_id")

                out.append({
                    "role": "tool",
                    "tool_call_id": tool_id,
                    "content": _payload(part.get("content", "")),
                })
                had_result = True
                continue

        pending.append(_content_part(part))

    flush(force=not had_result)
    return out


def _messages(raw: list) -> List[dict]:
    result = []
    for msg in raw:
        if not isinstance(msg, dict):
            raise ValueError("invalid message object")
        result.extend(_normalize(msg))
    return result


_DEV_REMINDER_PREFIX = (
    "<system-reminder>\n"
    "The following context is automatically attached for this model call only."
)


def _standalone_text(msg: dict) -> Optional[str]:
    """Only plain text or a single text block; never flatten mixed content."""
    if msg.get("tool_calls"):
        return None
    raw = msg.get("content")
    if isinstance(raw, str):
        return raw
    if (
        isinstance(raw, list)
        and len(raw) == 1
        and isinstance(raw[0], dict)
        and raw[0].get("type") in ("text", "input_text", "output_text")
        and isinstance(raw[0].get("text"), str)
    ):
        return raw[0]["text"]
    return None


def _clean_dev_history(history: List[dict], raw: list) -> List[dict]:
    """Remove only the explicitly marked, standalone trailing call reminder."""
    if not raw or not history or raw[-1].get("role") != "user":
        return history
    value = _standalone_text(raw[-1])
    if value is None:
        return history
    value = value.strip().replace("\r\n", "\n")
    if (
        value.startswith(_DEV_REMINDER_PREFIX)
        and value.endswith("</system-reminder>")
        and value.count("<system-reminder>") == 1
        and value.count("</system-reminder>") == 1
        and history[-1].get("role") == "user"
    ):
        return history[:-1]
    return history


def _wrapped_source(history: List[dict]) -> Any:
    """Read source only from the first user's explicit JSON message envelope."""
    for msg in history:
        if msg.get("role") != "user":
            continue
        value = _standalone_text(msg)
        prefix = "你收到一条消息："
        if value is None or not value.strip().startswith(prefix):
            return None
        try:
            envelope = json.loads(value.strip()[len(prefix):].strip())
        except (ValueError, TypeError):
            return None
        return envelope.get("source") if isinstance(envelope, dict) else None
    return None


def _response(value: Any) -> List[dict]:
    """
    Never fabricate an empty assistant for missing or invalid responses.
    """
    if value is None:
        return []

    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (ValueError, TypeError):
            value = {"role": "assistant", "content": value}

    if isinstance(value, list):
        if not value:
            raise ValueError("empty response")

        if all(
            isinstance(m, dict) and m.get("role")
            for m in value
        ):
            return _messages(value)

        return _normalize({
            "role": "assistant",
            "content": value,
        })

    if not isinstance(value, dict):
        raise ValueError("unsupported response type")

    if not value:
        raise ValueError("empty response")

    if "error" in value and value["error"] is not None:
        raise ValueError("error response")

    choices = value.get("choices")
    if isinstance(choices, list) and choices:
        item = choices[0]
        if isinstance(item, dict):
            msg = item.get("message", item.get("delta"))
            if isinstance(msg, dict):
                return _response(msg)
        raise ValueError("unsupported response structure")

    if isinstance(value.get("messages"), list):
        return _response(value["messages"])

    if isinstance(value.get("output"), list):
        return _response(value["output"])

    for key in ("message", "output_message", "response"):
        if key in value:
            return _response(value[key])

    if any(
        key in value
        for key in (
            "content",
            "reasoning_content",
            "reasoning",
            "tool_calls",
        )
    ):
        return _normalize({"role": "assistant", **value})

    raise ValueError("unsupported response structure")


def _envelope(obj: dict) -> dict:
    for key in ("request", "req", "up_req", "body"):
        val = obj.get(key)
        if isinstance(val, dict) and isinstance(val.get("messages"), list):
            return val

    if isinstance(obj.get("messages"), list):
        return obj

    raise ValueError("request messages missing")


def _response_value(obj: dict) -> Any:
    for key in ("response", "resp", "up_resp", "llmOutput"):
        if key in obj:
            return obj[key]
    raise ValueError("response missing")


def _request_user_id(obj: dict, req: dict) -> Any:
    for item in (req, obj):
        meta = item.get("metadata")
        if isinstance(meta, dict) and meta.get("user_id") is not None:
            return meta["user_id"]
    return None


def _timestamp(
    obj: dict,
    req: dict,
    trace: dict,
) -> Optional[float]:
    for item in (req, obj, trace):
        for key in (
            "request_time",
            "request_timestamp",
            "timestamp",
            "created_at",
            "ts",
            "time",
        ):
            val = item.get(key)
            if isinstance(val, bool):
                continue

            if isinstance(val, (int, float)):
                return float(val) / 1000 if val > 1e11 else float(val)

            if isinstance(val, str) and val:
                try:
                    n = float(val)
                    return n / 1000 if n > 1e11 else n
                except ValueError:
                    try:
                        return datetime.fromisoformat(
                            val.replace("Z", "+00:00")
                        ).timestamp()
                    except ValueError:
                        pass
    return None


def _stem(filename: str) -> str:
    if filename.endswith("-req.json"):
        return filename[:-9]
    if filename.endswith(".json"):
        return filename[:-5]
    return filename


def _filename_time_key(filename: str) -> tuple:
    return tuple(
        (0, int(part)) if part.isdigit() else (1, part.casefold())
        for part in re.split(r"(\d+)", _stem(filename))
    )


def _filenames(session: dict) -> list[tuple[str, dict]]:
    traces = session.get("trace_list") or []

    found = [
        (t["filename"], t)
        for t in traces
        if isinstance(t, dict)
        and isinstance(t.get("filename"), str)
        and t["filename"]
    ]

    if not found and session.get("latest_file"):
        found = [(session["latest_file"], {})]

    seen = set()
    unique = []

    for filename, trace in found:
        if filename not in seen:
            seen.add(filename)
            unique.append((filename, trace))

    return unique


def _accepts_file_dirs(function: Callable) -> bool:
    try:
        return "file_dirs" in inspect.signature(function).parameters
    except (ValueError, TypeError):
        return False


def _direct_merged_json(
    src: Path,
    filename: str,
) -> Optional[dict]:
    """
    Read standalone merged snapshots with or without response.

    Do not bypass the req/res triplet pairing logic for -req.json.
    """
    path = src / filename
    if not path.is_file():
        return None

    with path.open("r", encoding="utf-8-sig") as handle:
        candidate = json.load(handle)

    if (
        not filename.endswith("-req.json")
        and isinstance(candidate, dict)
        and isinstance(candidate.get("messages"), list)
    ):
        return candidate

    return None


def _load_requests(
    src: Path,
    sess: dict,
    file_dirs: Optional[Dict[str, str]],
    branch: str = DEFAULT_PROFILE,
) -> tuple[list[Request], list[str]]:
    dirs = (
        {k: Path(v) for k, v in file_dirs.items() if v}
        if file_dirs
        else None
    )

    tri_supports = bool(dirs) and _accepts_file_dirs(_resolve_triplet)
    load_supports = bool(dirs) and _accepts_file_dirs(_load_merged)

    parsed = []
    errors = []

    for order, (fn, trace) in enumerate(_filenames(sess)):
        try:
            path = Path(fn)

            if path.is_absolute() or ".." in path.parts:
                raise ValueError("invalid relative path")

            obj = _direct_merged_json(src, fn)

            if obj is None:
                stem = _stem(fn)

                tri = (
                    _resolve_triplet(src, stem, file_dirs=dirs)
                    if tri_supports
                    else _resolve_triplet(src, stem)
                )
                obj = (
                    _load_merged(src, stem, fn, tri, file_dirs=dirs)
                    if load_supports
                    else _load_merged(src, stem, fn, tri)
                )

            if not isinstance(obj, dict):
                raise ValueError("missing or damaged file")

            req = _envelope(obj)
            tools = _request_tools(req)
            history = _messages(req["messages"])
            if branch == "dev":
                history = _clean_dev_history(history, req["messages"])

            system = req.get("system", obj.get("system"))

            if system is not None and not any(
                m["role"] == "system" for m in history
            ):
                history.insert(
                    0,
                    {"role": "system", "content": _payload(system)},
                )

            try:
                response_value = _response_value(obj)
            except ValueError as exc:
                if str(exc) != "response missing":
                    raise
                response_value = None

            if response_value is None:
                errors.append(f"{fn}: missing response")
                response = []
            else:
                try:
                    response = _response(response_value)
                except ValueError as exc:
                    if str(exc) not in (
                        "empty response",
                        "error response",
                        "unsupported response structure",
                        "unsupported response type",
                    ):
                        raise

                    errors.append(f"{fn}: {exc}")
                    response = []

            source = obj.get("source", req.get("source", trace.get("source")))
            if source is None:
                source = _wrapped_source(history)
            kind = classify_kind(
                req,
                obj,
                history,
                source,
                heartbeat_auxiliary=PROFILES[branch]["heartbeat_auxiliary"],
            )

            (
                length,
                digest,
                full_digest,
                prefixes,
                reasoned,
            ) = fingerprint(history, response)

            parsed.append(Request(
                _timestamp(obj, req, trace),
                order,
                fn,
                _request_user_id(obj, req),
                kind,
                history,
                response,
                length,
                digest,
                full_digest,
                prefixes,
                reasoned,
                tools=tools,
            ))

        except Exception as exc:
            known = {
                "unsupported content part type",
                "unsupported content part value",
                "unsupported content object without type",
                "unsupported message content container",
                "unsupported image source type",
                "unsupported base64 image source",
                "image URL/data missing or unsupported image representation",
                "non-string text content part",
                "tool_result missing tool_use_id",
                "invalid message object",
                "request messages missing",
                "response missing",
                "missing or damaged file",
                "invalid relative path",
                "unsupported response type",
            }

            reason = (
                str(exc)
                if isinstance(exc, ValueError) and str(exc) in known
                else type(exc).__name__
            )
            logger.warning(
                "%s extract failed: %s (%s)",
                branch, fn, reason,
            )
            errors.append(f"{fn}: {reason}")

    if parsed and all(r.timestamp is not None for r in parsed):
        parsed.sort(
            key=lambda r: (r.timestamp, r.filename, r.order)
        )
    else:
        parsed.sort(
            key=lambda r: (
                _filename_time_key(r.filename),
                r.filename,
                r.order,
            )
        )

    for sequence, req in enumerate(parsed):
        req.sequence = sequence

    return parsed, errors


@dataclass(frozen=True, slots=True)
class ReasonSource:
    sequence: int
    filename: str
    source_length: int
    source_digest: bytes
    reasoning_hash: bytes
    position: int


@dataclass(slots=True)
class ReasoningIndex:
    # Exact: (user, position, message key) -> length -> digest -> hash -> sources.
    # Only identical context + reasoning replays are deduplicated.
    exact: dict
    loose: dict
    texts: dict
    system_whitespace: dict
    requests_by_sequence: dict
    normalized_prefixes: dict
    first_seen: dict
    tool_first_seen: dict
    tool_generations: dict


def _call_ids(msg: dict) -> Optional[tuple]:
    calls = msg.get("tool_calls")
    if not isinstance(calls, list) or not calls:
        return None
    ids = []
    for call in calls:
        if not isinstance(call, dict):
            return None
        call_id = call.get("id")
        if not isinstance(call_id, str) or not call_id:
            return None
        ids.append(call_id)
    return tuple(ids) if len(ids) == len(set(ids)) else None


def _reasoning_prefixes(history: list[dict], response: list[dict]) -> list[bytes]:
    """Backfill-only fingerprints; collapse blank lines only in system text.

    Keep nonblank line contents (including indentation), other message roles,
    original messages and the segmentation fingerprints untouched.
    """
    def normalize(msg: dict) -> dict:
        content = msg.get("content")
        if msg.get("role") == "system" and isinstance(content, str):
            return {
                **msg,
                "content": re.sub(r"(\r?\n)(?:[ \t]*\r?\n)+", r"\1", content),
            }
        return msg

    return fingerprint(
        [normalize(msg) for msg in history],
        [normalize(msg) for msg in response],
    )[3]


def _source_index(requests: list[Request]) -> ReasoningIndex:
    index = ReasoningIndex({}, {}, {}, {}, {}, {}, {}, {}, {})
    seen_replays = set()
    # Preserve all original requests, including reasoning-free histories.
    for req in sorted(requests, key=lambda item: item.sequence):
        index.requests_by_sequence[req.sequence] = req
        uid = _json(req.user_id)
        normalized = _reasoning_prefixes(req.history, req.response)
        index.normalized_prefixes[req.sequence] = normalized
        for pos, msg in enumerate(req.history):
            if msg.get("role") != "assistant":
                continue
            key = match_key(msg)
            if key is None:
                continue
            index.first_seen.setdefault((uid, pos, normalized[pos]), req.sequence)
            if _call_ids(msg):
                index.tool_first_seen.setdefault((uid, key), req.sequence)

        # Full reasoning-sensitive fingerprint prevents merging different branches.
        context = (req.full_digest, _tools_key(req.tools), req.kind)
        for offset, msg in enumerate(req.response):
            if msg.get("role") != "assistant":
                continue
            key = match_key(msg)
            if key is None:
                continue
            pos = len(req.history) + offset
            ids = _call_ids(msg)
            # Track repeated generations even when they have no reasoning or are
            # isolated calls. Future generations are filtered at query time.
            for call_id in ids or ():
                index.tool_generations.setdefault((uid, call_id), {}).setdefault(
                    (context, pos), (req.sequence, key)
                )
            if req.kind in ("compaction", "auxiliary"):
                continue
            reasoning = msg.get("reasoning_content")
            if not isinstance(reasoning, str) or not reasoning.strip():
                continue
            rh = hashlib.sha256(reasoning.encode("utf-8")).digest()
            replay_key = (uid, context, pos, key, rh)
            if replay_key in seen_replays:
                continue
            seen_replays.add(replay_key)
            index.texts.setdefault(rh, reasoning)
            item = ReasonSource(req.sequence, req.filename, req.length, req.digest, rh, pos)
            index.exact.setdefault((uid, pos, key), {}).setdefault(
                req.length, {}
            ).setdefault(req.digest, {}).setdefault(rh, []).append(item)
            index.system_whitespace.setdefault((uid, pos, key), {}).setdefault(
                req.length, {}
            ).setdefault(normalized[-1], {}).setdefault(rh, []).append(item)
            index.loose.setdefault((uid, key), {}).setdefault(rh, []).append(item)
    return index


def _reasoning_compatible(source: Request, target: Request, positions=None) -> bool:
    """Compare immutable request evidence, never the messages being filled."""
    for pos, value in source.reasoned.items():
        target_pos = pos if positions is None else positions.get(pos)
        if target_pos is None:
            continue
        other = target.reasoned.get(target_pos)
        if value and other and value.strip() and other.strip() and value != other:
            return False
    return True


def _is_alignment_reminder(msg: dict) -> bool:
    """Ignore only a standalone user reminder with this confirmed fixed marker.

    This affects fallback comparison only; exported messages are untouched.
    """
    if msg.get("role") != "user":
        return False
    value = _standalone_text(msg)
    if value is None:
        return False
    value = value.strip().replace("\r\n", "\n")
    prefix = (
        "<system-reminder>\n"
        "The following content does not represent the user's intent"
    )
    return (
        value.startswith(prefix)
        and value[len(prefix):len(prefix) + 1] in ("", " ", "\n", ".")
        and value.endswith("</system-reminder>")
        and value.count("<system-reminder>") == 1
        and value.count("</system-reminder>") == 1
    )


def _tool_alignment(source: Request, target: Request) -> Optional[dict]:
    """Align tool-ID fallback evidence, not a strict conversation prefix.

    Preserve user/developer/assistant order and signatures. Ignore only system
    and explicitly marked standalone dynamic reminders. Consecutive tool blocks
    require identical unique call-ID sets; their result bodies may differ.
    Return original positions so reasoning conflicts are checked after alignment.
    """
    def relevant(raw):
        return [(pos, msg) for pos, msg in enumerate(raw)
                if msg.get("role") != "system" and not _is_alignment_reminder(msg)]

    def tool_block(items, start):
        found = {}
        end = start
        while end < len(items) and items[end][1].get("role") == "tool":
            pos, msg = items[end]
            call_id = msg.get("tool_call_id")
            if not isinstance(call_id, str) or not call_id or call_id in found:
                return None, end
            found[call_id] = pos
            end += 1
        return found, end

    left = relevant(source.history + source.response)
    right = relevant(target.history)
    positions = {}
    i = j = 0
    while i < len(left):
        if j >= len(right):
            return None
        source_pos, before = left[i]
        target_pos, after = right[j]
        if before.get("role") == "tool":
            if after.get("role") != "tool":
                return None
            source_block, i = tool_block(left, i)
            target_block, j = tool_block(right, j)
            if (
                source_block is None or target_block is None
                or source_block.keys() != target_block.keys()
            ):
                return None
            positions.update({source_block[key]: target_block[key]
                              for key in source_block})
        else:
            if signature(before) != signature(after):
                return None
            positions[source_pos] = target_pos
            i += 1
            j += 1
    return positions


def _fill_reasoning(
    messages: list[dict],
    index: ReasoningIndex,
    selected: Request,
) -> tuple[list[str], dict]:
    """Session-wide lookup, but uniqueness is judged only after identity, time
    and branch checks. All evidence comes from original requests. Future sources
    cannot veto a historical fill. Text displaced by compression is not inferred.
    """
    audit = {
        "scope": "session",
        "filled_count": 0,
        "unresolved_count": 0,
        "conflict_count": 0,
        "compression_fallback_count": 0,
        "tool_id_fallback": 0,
        "no_source_count": 0,
        # Per message with rejected evidence, even if another source succeeds.
        # Multiple rejection reasons may apply to the same message.
        "rejection_counts": dict.fromkeys((
            "no_source", "source_too_late", "branch_conflict",
            "ambiguous_identity", "reasoning_conflict",
        ), 0),
    }
    utilized = []
    if selected.kind in ("compaction", "auxiliary"):
        return utilized, audit
    uid = _json(selected.user_id)
    normalized = index.normalized_prefixes[selected.sequence]
    counts = {}
    for msg in messages:
        if msg.get("role") == "assistant":
            key = match_key(msg)
            if key is not None:
                counts[key] = counts.get(key, 0) + 1
    # Pair checks are cached within an export rather than rescanning a context
    # for each assistant message. Filled messages never enter these caches.
    compatible = {}
    alignments = {}

    for pos, msg in enumerate(messages[:len(selected.history)]):
        if msg.get("role") != "assistant" or msg.get("reasoning_content"):
            continue
        key = match_key(msg)
        if key is None:
            continue
        reasons = set()
        blocked = set()
        eligible = {}
        saw_source = False

        def accept(item, first_seen, *, aligned=False):
            nonlocal saw_source
            # Ignore future evidence entirely, including in rejection audit.
            if item.sequence >= selected.sequence:
                return
            saw_source = True
            if item.sequence in blocked:
                return
            source = index.requests_by_sequence[item.sequence]
            if source.user_id != selected.user_id:
                return
            if first_seen is None or item.sequence >= first_seen:
                reasons.add("source_too_late")
                return
            cache_key = (item.sequence, aligned)
            if cache_key not in compatible:
                positions = alignments.get(item.sequence) if aligned else None
                compatible[cache_key] = _reasoning_compatible(source, selected, positions)
            if not compatible[cache_key]:
                reasons.add("branch_conflict")
                blocked.add(item.sequence)
                return
            eligible.setdefault(item.reasoning_hash, item)

        first_seen = index.first_seen.get((uid, pos, normalized[pos]))
        # Both positional paths enforce the same original reasoning check.
        for pool, prefixes in (
            (index.exact, selected.prefixes),
            (index.system_whitespace, normalized),
        ):
            for length, by_digest in pool.get((uid, pos, key), {}).items():
                if not length or length > len(selected.history):
                    continue
                for items in by_digest.get(prefixes[length - 1], {}).values():
                    for item in items:
                        accept(item, first_seen)

        fallback = False
        if not eligible:
            candidates = index.loose.get((uid, key), {})
            prior = [item for items in candidates.values() for item in items
                     if item.sequence < selected.sequence]
            saw_source = saw_source or bool(prior)
            ids = _call_ids(msg)
            if prior and (ids is None or counts.get(key) != 1):
                # No text-only position-free inference, including post_compaction.
                reasons.add("ambiguous_identity")
            elif prior:
                reused = any(
                    sum(seq < selected.sequence for seq, _ in
                        index.tool_generations.get((uid, call_id), {}).values()) > 1
                    for call_id in ids
                )
                if reused:
                    reasons.add("ambiguous_identity")
                else:
                    tool_seen = index.tool_first_seen.get((uid, key))
                    for item in prior:
                        if item.sequence in blocked:
                            continue
                        source = index.requests_by_sequence[item.sequence]
                        if item.sequence not in alignments:
                            alignments[item.sequence] = _tool_alignment(source, selected)
                        positions = alignments[item.sequence]
                        if positions is None or positions.get(item.position) != pos:
                            reasons.add("ambiguous_identity")
                            continue
                        accept(item, tool_seen, aligned=True)
                    fallback = bool(eligible)
        if len(eligible) > 1:
            reasons.add("reasoning_conflict")
            eligible.clear()
        if not eligible:
            if not saw_source:
                reasons.add("no_source")
            if not reasons:
                reasons.add("ambiguous_identity")
            audit["unresolved_count"] += 1
            audit["no_source_count"] += int("no_source" in reasons)
            audit["conflict_count"] += int(bool(
                reasons & {"branch_conflict", "reasoning_conflict"}
            ))
            for reason in reasons:
                audit["rejection_counts"][reason] += 1
            continue
        rh, item = next(iter(eligible.items()))
        for reason in reasons:
            audit["rejection_counts"][reason] += 1
        msg["reasoning_content"] = index.texts[rh]
        utilized.append(item.filename)
        audit["filled_count"] += 1
        audit["tool_id_fallback"] += int(fallback)
        audit["compression_fallback_count"] += int(
            fallback and selected.kind == "post_compaction"
        )
    return list(dict.fromkeys(utilized)), audit


def _ending(messages: list[dict]) -> bool:
    if not messages:
        return False

    msg = messages[-1]
    return (
        msg.get("role") == "assistant"
        and has_text(msg.get("content"))
        and not msg.get("tool_calls")
    )


def _safe_path_component(raw: str) -> str:
    stem = re.sub(r"[^\w.-]+", "_", raw).strip("._")[:96]
    return stem or hashlib.sha256(
        raw.encode()
    ).hexdigest()[:16]


def _result_record(
    sess: dict,
    src: Path,
    log_dir: str,
    first: str,
) -> dict:
    models = sess.get("models") or []
    traces = sess.get("trace_list") or []

    return {
        "session": first,
        "start_time": _fmt_ts(
            sess.get("first_ts") or sess.get("_key", "")
        ),
        "end_time": _fmt_ts(sess.get("last_ts", "")),
        "duration_s": None,
        "api_call_count": len(traces) if traces else 1,
        "api_errors": 0,
        "user_turns": 0,
        "total_messages": 0,
        "tool_use_count": 0,
        "tool_result_count": 0,
        "tool_success": 0,
        "tool_fail_flag": 0,
        "tool_fail_keyword": 0,
        "tool_fail_total": 0,
        "tool_success_rate": None,
        "model": models[0] if models else "",
        "q1": "",
        "latest_file": sess.get("latest_file", ""),
        "log_dir": log_dir or src.name,
        "tool_use_detail": {},
        "tool_success_detail": {},
        "tool_fail_detail": {},
        "skills_used": {},
        "api_key": sess.get("api_key", ""),
        "models": models,
        "trace_list": traces,
        "_key": sess.get("first_ts") or sess.get("_key", ""),
        "first_ts": sess.get("first_ts") or sess.get("_key", ""),
        "last_ts": sess.get("last_ts", ""),
        "msg_count": 0,
        "completed": 0,
        "completed_note": "",
        "request_files": [],
        "segment_count": 0,
    }


def _one_session(
    task: tuple,
) -> tuple[Optional[dict], list[str]]:
    src_s, out_s, sess, log_dir, file_dirs, profile = task
    src, out = Path(src_s), Path(out_s)
    branch = profile
    first = str(
        sess.get("first_ts") or sess.get("_key") or "session"
    )

    requests, errors = _load_requests(src, sess, file_dirs, branch)

    if not requests:
        if not errors:
            errors = [f"{first}: no trace files"]
        return None, errors

    reasoning_index = _source_index(requests)
    result = _result_record(sess, src, log_dir, first)

    directory = out / branch / _safe_path_component(first)
    directory.mkdir(parents=True, exist_ok=True)

    selected = None
    sources = []
    original_type = "initial_query"
    known_reasoning = {}

    def emit(reason: str, *, preserve_chain: bool = False) -> None:
        nonlocal selected, sources, original_type, known_reasoning

        if selected is None:
            return

        messages = [dict(m) for m in selected.history]
        messages.extend(dict(m) for m in selected.response)

        reasoning_sources, reasoning_audit = _fill_reasoning(
            messages, reasoning_index, selected,
        )

        index = len(result["request_files"]) + 1

        filenames = list(dict.fromkeys(
            [s.filename for s in sources] + reasoning_sources
        ))

        stable = hashlib.sha256(
            _json([
                branch,
                first,
                index,
                selected.filename,
                selected.full_digest.hex(),
            ]).encode("utf-8")
        ).hexdigest()[:24]

        record_id = f"{branch}_{stable}"

        payload = {
            "id": record_id,
            "messages": messages,
            "tools": selected.tools,
            "meta_info": {
                "unique_info": {
                    "reasoning_backfill": reasoning_audit,
                    "branch": branch,
                    "user_id": selected.user_id,
                    "upstream_session": first,
                    "segment_index": index,
                    "trajectory_type": original_type,
                    "boundary_reason": reason,
                    "source_files": filenames,
                    "reasoning_source_files": reasoning_sources,
                    "complete_text_ending": _ending(messages),
                }
            },
        }

        target = directory / (record_id + ".json")
        target.write_text(
            json.dumps(
                payload,
                ensure_ascii=False,
                separators=(",", ":"),
            ) + "\n",
            encoding="utf-8",
        )

        result["request_files"].append(
            target.relative_to(out).as_posix()
        )
        result["segment_count"] += 1
        result["total_messages"] += len(messages)
        result["user_turns"] += sum(
            m.get("role") == "user" for m in messages
        )
        result["tool_result_count"] += sum(
            m.get("role") == "tool" for m in messages
        )
        result["tool_use_count"] += sum(
            len(m.get("tool_calls") or []) for m in messages
        )

        selected = None
        sources = []
        original_type = "initial_query"
        if not preserve_chain:
            known_reasoning = {}

    for req in requests:
        isolated = req.kind in ("compaction", "auxiliary")
        tools_changed = (
            selected is not None
            and _tools_key(selected.tools) != _tools_key(req.tools)
        )

        if isolated:
            if (
                selected is not None
                and selected.kind == req.kind
                and same(selected, req)
                and selected.user_id == req.user_id
                and not tools_changed
            ):
                sources.append(req)
                continue
            emit(
                "before_compaction"
                if req.kind == "compaction"
                else "before_auxiliary"
            )
        elif selected is not None:
            if selected.kind == "compaction":
                emit("compaction_request")
            elif selected.kind == "auxiliary":
                emit("auxiliary_request")
            elif selected.user_id != req.user_id:
                emit("user_id_changed")
            else:
                identical = same(selected, req)
                growing = not identical and grows(selected, req, known_reasoning)
                if identical or growing:
                    if tools_changed:
                        # Preserve continuity state; the session index is immutable.
                        emit("tools_changed", preserve_chain=True)
                        selected = req
                        sources = [req]
                        original_type = req.kind
                    else:
                        sources.append(req)
                        if growing:
                            selected = req
                    known_reasoning.update(req.reasoned)
                    continue
                emit("history_changed")

        selected = req
        sources = [req]
        original_type = req.kind
        known_reasoning = dict(req.reasoned)

    if selected is not None:
        emit(
            "compaction_request"
            if selected.kind == "compaction"
            else (
                "auxiliary_request"
                if selected.kind == "auxiliary"
                else "group_end"
            )
        )

    result["msg_count"] = result["total_messages"]
    result["api_errors"] = len(errors)

    return result, errors


def jiuwenswarm_segments_export(
    src_dir: str,
    out_dir: str,
    session_entries: List[dict],
    api_key: Optional[str] = None,
    workers: int = 4,
    progress_cb: Optional[Callable[[str], None]] = None,
    log_dir: Optional[str] = None,
    should_cancel: Optional[Callable[[], bool]] = None,
    on_session_done: Optional[Callable[[], None]] = None,
    file_dirs: Optional[Dict[str, str]] = None,
    profile: str = DEFAULT_PROFILE,
) -> dict:
    """Compatible signature and top-level return structure.

    profile: 业务线参数，决定心跳判定及 dev 末尾临时上下文清理。
             非法值直接抛 ValueError —— 配置写错时宁可失败，也不要静默用默认值
             产出一份语义不对的数据。
    """
    if profile not in PROFILES:
        raise ValueError(
            f"unknown profile {profile!r}; expected one of {sorted(PROFILES)}"
        )
    branch = profile
    _log = progress_cb or (lambda message: None)
    _should_cancel = should_cancel or (lambda: False)

    workers = max(1, min(int(workers), 32))
    Path(out_dir).mkdir(parents=True, exist_ok=True)

    entries = (
        session_entries
        if not api_key
        else [
            s for s in session_entries
            if s.get("api_key") == api_key
        ]
    )

    result = {
        "total_sessions": len(entries),
        "total_files": 0,
        "errors": [],
        "results": [],
        "cancelled": False,
    }

    if not entries:
        return result

    if _should_cancel():
        _log(f"检测到取消，跳过本目录 {branch} 重构")
        result["cancelled"] = True
        return result

    tasks = [
        (
            src_dir,
            out_dir,
            sess,
            log_dir if log_dir is not None else Path(src_dir).name,
            file_dirs,
            profile,
        )
        for sess in entries
        if sess.get("first_ts") or sess.get("_key")
    ]

    if not tasks:
        return result

    _log(
        f"进入 {branch} 重构: "
        f"{len(tasks)} sessions, workers={workers}..."
    )

    from utils.eval.runner import run_tasks

    def _collect(worker_result) -> None:
        """把 (record, errors) 并入总结果。"""
        record, errs = worker_result
        result["errors"].extend(errs)
        if record is not None:
            result["results"].append(record)
            result["total_files"] += len(record["request_files"])
        elif not errs:
            result["errors"].append("处理失败")

    def _cancelled() -> bool:
        if _should_cancel():
            _log(f"检测到取消，停止 {branch} 重构剩余 session")
            return True
        return False

    # 并行骨架统一走 runner（有界提交窗口 + 取消 + 进度），并行度仍由调用方决定。
    # runner 只收集 payload，逐条并入需要在其 on_task_done 里做，故这里改用
    # 一次性遍历 payload 的方式：取消语义由 runner 的 should_cancel 保证。
    run = run_tasks(
        tasks,
        _one_session,
        workers=workers,
        label=branch,
        progress_cb=_log,
        should_cancel=_cancelled,
        on_task_done=on_session_done,
    )
    for worker_result in run["payload"]:
        _collect(worker_result)
    if run["cancelled"]:
        result["cancelled"] = True
    result["errors"].extend(e for e in run["errors"] if e.startswith("处理失败"))

    return result


# Backward-compatible public entries.
# 历史调用方（temp/ 下的两个旧脚本、既有 CLI）按这些名字导入，保留以免破坏用法。
dev_reformat_export = jiuwenswarm_segments_export


def xiaoyi_reformat_export(**kwargs) -> dict:
    """xiaoyi 业务：与 dev 唯一的差别是心跳请求不判 auxiliary。"""
    kwargs["profile"] = "xiaoyi"
    return jiuwenswarm_segments_export(**kwargs)


jiuwen_reformat_export = jiuwenswarm_segments_export
