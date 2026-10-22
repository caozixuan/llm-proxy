#!/usr/bin/env python3
"""Aggregate and repair Hermes proxy trajectory snapshots.

Chronologically adjacent proxy records are not necessarily from one linear
conversation. Hermes can run a foreground turn and a background memory/skill
review at the same time, while provider retries can replay an identical
request. This script groups records by their last user-message anchor and
retains every maximal request branch instead of treating every message-count
drop as context compression.

Within each run, exact request replays are deduplicated, every divergent
terminal branch is retained, and foreground/recovery/background runs remain
separate. The chronologically final proxy record is always retained.

For retained records, missing ``reasoning_content`` values in historical
assistant messages are restored from matching earlier streaming responses.
Both ``req`` and ``up_req`` are repaired when they contain a messages array.
An ``_manifest.jsonl`` file explains how records were grouped and selected.

Responses are assembled from OpenAI-compatible SSE chunks or Anthropic SSE
events (``content_block`` / ``thinking_delta`` / ``input_json_delta``), and
request histories may carry either format; the two are normalized to the same
internal shape so grouping, selection and backfill are format-independent.
"""

from __future__ import annotations

import argparse
import copy
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence


JSONDict = dict[str, Any]
Signature = tuple[str, tuple[tuple[str, str, str, str], ...]]


@dataclass
class Record:
    index: int
    path: Path
    outer: JSONDict
    request: JSONDict
    request_messages: list[JSONDict]
    canonical_messages: list[str]
    response_message: JSONDict | None
    response_signature: Signature | None


@dataclass(frozen=True)
class ResponseSource:
    record_index: int
    reasoning_content: str
    thinking_signature: str = ""


@dataclass
class RunGroup:
    key: tuple[str, ...]
    user_message_index: int
    user_content: str
    record_indices: list[int]
    selected_indices: list[int]


def parse_json_value(value: Any) -> Any:
    """Decode a JSON string while accepting an already-decoded value."""
    if isinstance(value, str):
        return json.loads(value)
    return value


def encode_like_original(original: Any, decoded: Any) -> Any:
    """Keep nested JSON fields as strings when that is how they arrived."""
    if isinstance(original, str):
        return json.dumps(decoded, ensure_ascii=False, separators=(",", ":"))
    return decoded


def iter_stream_objects(raw: Any) -> Iterator[JSONDict]:
    """Yield JSON objects from SSE, JSON-lines, or one completion object."""
    if not isinstance(raw, str) or not raw.strip():
        return

    # Proxy exports use one SSE/JSON object per line.  This also handles a
    # single non-streaming JSON object because it occupies one line.
    for raw_line in raw.splitlines():
        line = raw_line.strip()
        if line.startswith("data:"):
            line = line[5:].strip()
        if not line or line == "[DONE]":
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            yield value


def append_text(target: JSONDict, key: str, value: Any) -> None:
    if isinstance(value, str):
        target[key] = target.get(key, "") + value


def _assemble_anthropic_blocks(blocks: Iterable[JSONDict]) -> JSONDict:
    """Normalize Anthropic content blocks to the internal message shape.

    thinking/text/tool_use blocks map onto the same ``reasoning_content`` /
    ``content`` / ``tool_calls`` shape produced by assemble_streamed_message,
    so signature matching, branch selection and reasoning backfill behave
    identically for both proxy formats.
    """
    message: JSONDict = {"role": "assistant", "content": ""}
    text_parts: list[str] = []
    thinking_parts: list[str] = []
    signature = ""
    tool_calls: list[JSONDict] = []
    for block in blocks:
        if not isinstance(block, dict):
            continue
        block_type = block.get("type")
        if block_type == "text":
            if isinstance(block.get("text"), str):
                text_parts.append(block["text"])
        elif block_type == "thinking":
            if isinstance(block.get("thinking"), str):
                thinking_parts.append(block["thinking"])
            if not signature and isinstance(block.get("signature"), str):
                signature = block["signature"]
        elif block_type == "tool_use":
            tool_calls.append(
                {
                    "id": block.get("id", ""),
                    "type": "function",
                    "function": {
                        "name": block.get("name", ""),
                        "arguments": normalize_arguments(block.get("input")),
                    },
                }
            )
    message["content"] = "".join(text_parts)
    if thinking_parts:
        message["reasoning_content"] = "".join(thinking_parts)
    if signature:
        message["thinking_signature"] = signature
    if tool_calls:
        message["tool_calls"] = tool_calls
    return message


def assemble_anthropic_stream(raw: Any) -> JSONDict | None:
    """Reassemble an Anthropic SSE (or single-object) assistant message.

    Handles ``message_start`` / ``content_block_start`` /
    ``content_block_delta`` (``thinking_delta``, ``text_delta``,
    ``signature_delta``, ``input_json_delta``) / ``content_block_stop`` /
    ``message_stop`` and returns None for anything that is not an Anthropic
    response, mirroring assemble_streamed_message's contract.
    """
    if not isinstance(raw, str) or not raw.strip():
        return None

    # A single non-streaming Anthropic response is one message JSON object.
    try:
        single = json.loads(raw)
        if isinstance(single, dict) and isinstance(single.get("content"), list):
            message = _assemble_anthropic_blocks(single["content"])
            if message.get("content") or message.get("tool_calls") or message.get(
                "reasoning_content"
            ):
                return message
    except json.JSONDecodeError:
        pass

    blocks: dict[int, JSONDict] = {}
    block_order: list[int] = []
    saw_message = False

    for raw_line in raw.splitlines():
        line = raw_line.strip()
        if line.startswith("event:"):
            continue
        if not line.startswith("data:"):
            continue
        payload = line[5:].strip()
        if not payload:
            continue
        try:
            event = json.loads(payload)
        except json.JSONDecodeError:
            continue
        if not isinstance(event, dict):
            continue
        event_type = event.get("type")
        if event_type == "message_start":
            saw_message = True
            initial = event.get("message")
            if isinstance(initial, dict) and isinstance(initial.get("content"), list):
                for block in initial["content"]:
                    if isinstance(block, dict) and isinstance(block.get("index"), int):
                        blocks[block["index"]] = block
                        block_order.append(block["index"])
        elif event_type == "content_block_start":
            saw_message = True
            index = event.get("index")
            content_block = event.get("content_block")
            if isinstance(index, int) and isinstance(content_block, dict):
                blocks[index] = dict(content_block)
                if index not in block_order:
                    block_order.append(index)
        elif event_type == "content_block_delta":
            saw_message = True
            index = event.get("index")
            delta = event.get("delta")
            if not isinstance(index, int) or not isinstance(delta, dict):
                continue
            block = blocks.get(index)
            if block is None:
                block = {"type": "unknown"}
                blocks[index] = block
                block_order.append(index)
            delta_type = delta.get("type")
            if delta_type == "text_delta":
                text = delta.get("text", "")
                if isinstance(text, str):
                    block["text"] = block.get("text", "") + text
            elif delta_type == "thinking_delta":
                thinking = delta.get("thinking", "")
                if isinstance(thinking, str):
                    block["thinking"] = block.get("thinking", "") + thinking
            elif delta_type == "signature_delta":
                sig_fragment = delta.get("signature", "")
                if isinstance(sig_fragment, str):
                    block["signature"] = block.get("signature", "") + sig_fragment
            elif delta_type == "input_json_delta":
                partial = delta.get("partial_json", "")
                if isinstance(partial, str):
                    current = block.get("input")
                    if isinstance(current, str):
                        block["input"] = current + partial
                    elif isinstance(current, dict) and not current:
                        block["input"] = partial
                    elif isinstance(current, dict):
                        block["input"] = json.dumps(current) + partial
                    else:
                        block["input"] = partial
        elif event_type in (
            "content_block_stop",
            "message_delta",
            "message_stop",
        ):
            saw_message = True

    if not saw_message or not blocks:
        return None
    message = _assemble_anthropic_blocks(blocks[i] for i in block_order)
    if not (
        message.get("content")
        or message.get("tool_calls")
        or message.get("reasoning_content")
    ):
        return None
    return message


def assemble_streamed_message(raw: Any) -> JSONDict | None:
    """Reassemble an OpenAI-compatible streamed assistant message."""
    message: JSONDict = {"role": "assistant", "content": ""}
    tool_calls: dict[int, JSONDict] = {}
    saw_choice = False

    for event in iter_stream_objects(raw):
        choices = event.get("choices")
        if not isinstance(choices, list):
            continue
        for choice in choices:
            if not isinstance(choice, dict):
                continue
            saw_choice = True

            # Non-streaming completions use `message`; streaming completions
            # use `delta`.  Treat a complete message as a single delta.
            delta = choice.get("delta")
            if not isinstance(delta, dict):
                delta = choice.get("message")
            if not isinstance(delta, dict):
                continue

            append_text(message, "content", delta.get("content"))
            append_text(
                message, "reasoning_content", delta.get("reasoning_content")
            )

            raw_tool_calls = delta.get("tool_calls")
            if not isinstance(raw_tool_calls, list):
                continue
            for fallback_index, fragment in enumerate(raw_tool_calls):
                if not isinstance(fragment, dict):
                    continue
                index = fragment.get("index", fallback_index)
                if not isinstance(index, int):
                    index = fallback_index
                assembled = tool_calls.setdefault(
                    index,
                    {
                        "id": "",
                        "type": "function",
                        "function": {"name": "", "arguments": ""},
                    },
                )
                append_text(assembled, "id", fragment.get("id"))
                if isinstance(fragment.get("type"), str):
                    assembled["type"] = fragment["type"]
                function_fragment = fragment.get("function")
                if isinstance(function_fragment, dict):
                    function = assembled["function"]
                    append_text(function, "name", function_fragment.get("name"))
                    append_text(
                        function, "arguments", function_fragment.get("arguments")
                    )

    if not saw_choice:
        return None
    if tool_calls:
        message["tool_calls"] = [tool_calls[i] for i in sorted(tool_calls)]
    return message


_SNIFF_WINDOW = 4096
_ANTHROPIC_MARKERS = (
    "event: ",
    '"message_start"',
    '"content_block_start"',
    '"content_block_delta"',
    '"content_block_stop"',
    '"message_stop"',
)


def sniff_stream_format(raw: Any) -> str:
    """Cheaply guess which assembler should handle ``raw``.

    Both assemblers parse the whole payload line by line before deciding the
    text is not theirs, so trying both costs a wasted full pass on every
    response.  In real proxy logs ``resp`` is Anthropic and ``up_resp`` is
    OpenAI almost exclusively, so sniffing the head saves ~45% of the
    assembly CPU.  This is only a hint: an ``unknown`` verdict falls back to
    trying both assemblers, which keeps the result identical to before.
    """
    if not isinstance(raw, str) or not raw.strip():
        return "empty"
    head = raw[:_SNIFF_WINDOW]
    if any(marker in head for marker in _ANTHROPIC_MARKERS):
        return "anthropic"
    if '"choices"' in head:
        return "openai"
    return "unknown"


def _assemblers_for(kind: str):
    """Return the assemblers to try, in order, for a sniffed payload kind.

    An ``unknown`` payload keeps the old exhaustive behavior (both
    assemblers, all results collected).  A sniffed payload only needs its own
    assembler: the two formats are mutually exclusive, which the cross-checks
    in test_hermes_anthropic cover, so stopping at the first success gives an
    identical message for a fraction of the work.
    """
    if kind == "anthropic":
        return (assemble_anthropic_stream,)
    if kind == "openai":
        return (assemble_streamed_message,)
    return (assemble_streamed_message, assemble_anthropic_stream)


def extract_response_message(outer: JSONDict) -> JSONDict | None:
    """Prefer the downstream stream, falling back to the upstream stream.

    Both sides are tried with the OpenAI and Anthropic assemblers; the
    format is identified by whichever assembler recognizes the raw text.
    ``sniff_stream_format`` lets the likely assembler run first so the other
    one is usually skipped, but an unrecognized payload still falls through
    to both, so behavior is unchanged.
    """
    candidates: list[JSONDict] = []
    for raw in (outer.get("resp"), outer.get("up_resp")):
        kind = sniff_stream_format(raw)
        if kind == "empty":
            continue
        for assembler in _assemblers_for(kind):
            message = assembler(raw)
            if message is not None:
                candidates.append(message)

    if not candidates:
        return None

    # Usually all forms are identical.  Prefer the form containing the most
    # recoverable reasoning if a proxy layer removed that field on one side.
    return max(candidates, key=lambda m: len(m.get("reasoning_content", "")))


def normalize_arguments(value: Any) -> str:
    if not isinstance(value, str):
        return json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError:
        return value
    return json.dumps(
        decoded, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )


def _message_content_text(message: JSONDict) -> str:
    """Text from OpenAI string content or Anthropic text blocks."""
    content = message.get("content")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "".join(
            block.get("text", "")
            for block in content
            if isinstance(block, dict)
            and block.get("type") == "text"
            and isinstance(block.get("text"), str)
        )
    if content is None:
        return ""
    return json.dumps(content, ensure_ascii=False, sort_keys=True)


def assistant_signature(message: Any) -> Signature | None:
    """Return a reasoning-independent identity for an assistant message.

    Anthropic block-list content is reduced to its text and tool_use blocks,
    so thinking never participates in identity — a message with and without
    recovered reasoning still matches.
    """
    if not isinstance(message, dict) or message.get("role") != "assistant":
        return None

    normalized_calls: list[tuple[str, str, str, str]] = []
    raw_calls = message.get("tool_calls")
    if isinstance(raw_calls, list):
        for call in raw_calls:
            if not isinstance(call, dict):
                continue
            function = call.get("function")
            if not isinstance(function, dict):
                function = {}
            normalized_calls.append(
                (
                    str(call.get("id", "")),
                    str(call.get("type", "function")),
                    str(function.get("name", "")),
                    normalize_arguments(function.get("arguments", "")),
                )
            )

    content = message.get("content")
    if isinstance(content, list):
        for block in content:
            if not isinstance(block, dict) or block.get("type") != "tool_use":
                continue
            normalized_calls.append(
                (
                    str(block.get("id", "")),
                    "function",
                    str(block.get("name", "")),
                    normalize_arguments(block.get("input")),
                )
            )

    return _message_content_text(message), tuple(normalized_calls)


def is_visible_response(message: JSONDict | None) -> bool:
    return bool(message and (message.get("content") or message.get("tool_calls")))


def canonical_message(message: JSONDict) -> str:
    """Canonicalize a message without reasoning for lineage comparisons.

    Both the OpenAI ``reasoning_content`` key and Anthropic ``thinking``
    content blocks are stripped, so request deduplication and run grouping
    are unaffected by whether reasoning survived in a later snapshot.
    """
    canonical = {
        key: value for key, value in message.items() if key != "reasoning_content"
    }
    content = canonical.get("content")
    if isinstance(content, list):
        canonical["content"] = [
            block
            for block in content
            if not (
                isinstance(block, dict) and block.get("type") == "thinking"
            )
        ]
    return json.dumps(
        canonical,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def read_record(path: Path, index: int) -> Record:
    outer = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(outer, dict):
        raise ValueError("top-level JSON value is not an object")
    if "req" not in outer:
        raise ValueError("missing top-level 'req' field")

    request = parse_json_value(outer["req"])
    if not isinstance(request, dict):
        raise ValueError("'req' does not decode to an object")
    messages = request.get("messages")
    if not isinstance(messages, list) or not all(
        isinstance(message, dict) for message in messages
    ):
        raise ValueError("'req.messages' is not an array of objects")

    response = extract_response_message(outer)
    response_signature = (
        assistant_signature(response) if is_visible_response(response) else None
    )
    return Record(
        index=index,
        path=path,
        outer=outer,
        request=request,
        request_messages=messages,
        canonical_messages=[canonical_message(message) for message in messages],
        response_message=response,
        response_signature=response_signature,
    )


def load_records(input_dir: Path) -> list[Record]:
    files = sorted(input_dir.glob("*.json"), key=lambda path: path.name)
    if not files:
        raise ValueError(f"no JSON files found in {input_dir}")

    records: list[Record] = []
    for index, path in enumerate(files):
        try:
            records.append(read_record(path, index))
        except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
            raise ValueError(f"cannot parse {path}: {exc}") from exc
    return records


def response_registry(
    records: Sequence[Record], *, before_index: int | None = None
) -> dict[Signature, list[ResponseSource]]:
    registry: dict[Signature, list[ResponseSource]] = {}
    for record in records:
        if before_index is not None and record.index >= before_index:
            break
        if record.response_signature is None or record.response_message is None:
            continue
        reasoning = record.response_message.get("reasoning_content", "")
        if not isinstance(reasoning, str):
            reasoning = ""
        thinking_signature = record.response_message.get("thinking_signature", "")
        if not isinstance(thinking_signature, str):
            thinking_signature = ""
        registry.setdefault(record.response_signature, []).append(
            ResponseSource(record.index, reasoning, thinking_signature)
        )
    return registry


def last_user_anchor(record: Record) -> tuple[tuple[str, ...], int, str]:
    """Identify one Hermes run by its history through the final user message."""
    user_indices = [
        index
        for index, message in enumerate(record.request_messages)
        if message.get("role") == "user"
    ]
    if not user_indices:
        return tuple(record.canonical_messages), -1, ""

    user_index = user_indices[-1]
    content = record.request_messages[user_index].get("content", "")
    if not isinstance(content, str):
        content = json.dumps(content, ensure_ascii=False, sort_keys=True)
    return tuple(record.canonical_messages[: user_index + 1]), user_index, content


def is_strict_prefix(left: tuple[str, ...], right: tuple[str, ...]) -> bool:
    return len(left) < len(right) and right[: len(left)] == left


def response_quality(record: Record) -> tuple[int, int, int, int, int]:
    """Rank replayed calls without preferring an empty late retry."""
    response = record.response_message or {}
    content = response.get("content", "")
    if not isinstance(content, str):
        content = ""
    reasoning = response.get("reasoning_content", "")
    if not isinstance(reasoning, str):
        reasoning = ""
    has_tools = bool(response.get("tool_calls"))
    is_final_text = bool(content) and not has_tools
    is_visible = bool(content) or has_tools
    token_out = record.outer.get("usage", {}).get("token_out", 0)
    if not isinstance(token_out, int):
        token_out = 0
    return (
        int(is_final_text),
        int(is_visible),
        int(token_out > 0),
        len(reasoning) + len(content),
        record.index,
    )


def classify_run(user_content: str) -> str:
    normalized = " ".join(user_content.lower().split())
    if "review the conversation above" in normalized and (
        "skill library" in normalized or "memory" in normalized
    ):
        return "background_review"
    if normalized.startswith("[continuing toward your standing goal]"):
        return "synthetic_continuation"
    if any(
        phrase in normalized
        for phrase in (
            "system crashed",
            "getting an error",
            "get this back on track",
            "retry",
        )
    ):
        return "recovery"
    return "foreground"


def select_branch_records(
    records: Sequence[Record],
) -> tuple[list[RunGroup], set[int]]:
    """Select all maximal request branches, deduplicating exact replays."""
    grouped: dict[tuple[str, ...], RunGroup] = {}
    for record in records:
        key, user_index, user_content = last_user_anchor(record)
        group = grouped.get(key)
        if group is None:
            group = RunGroup(
                key=key,
                user_message_index=user_index,
                user_content=user_content,
                record_indices=[],
                selected_indices=[],
            )
            grouped[key] = group
        group.record_indices.append(record.index)

    run_groups = sorted(
        grouped.values(), key=lambda group: min(group.record_indices)
    )
    selected: set[int] = set()

    for group in run_groups:
        request_variants: dict[tuple[str, ...], list[Record]] = {}
        for index in group.record_indices:
            record = records[index]
            request_key = tuple(record.canonical_messages)
            request_variants.setdefault(request_key, []).append(record)

        request_keys = list(request_variants)
        terminal_keys = [
            key
            for key in request_keys
            if not any(is_strict_prefix(key, other) for other in request_keys)
        ]

        for request_key in terminal_keys:
            candidates = request_variants[request_key]
            visible_variants: dict[Signature, list[Record]] = {}
            for candidate in candidates:
                if candidate.response_signature is not None:
                    visible_variants.setdefault(
                        candidate.response_signature, []
                    ).append(candidate)

            if visible_variants:
                for variant_records in visible_variants.values():
                    selected.add(max(variant_records, key=response_quality).index)
            else:
                selected.add(max(candidates, key=lambda record: record.index).index)

        group.selected_indices = sorted(
            index for index in selected if index in group.record_indices
        )

    selected.add(records[-1].index)
    for group in run_groups:
        if records[-1].index in group.record_indices:
            group.selected_indices = sorted(
                set(group.selected_indices) | {records[-1].index}
            )
            break

    return run_groups, selected


def backfill_messages(
    messages: Iterable[JSONDict],
    registry: dict[Signature, list[ResponseSource]],
) -> int:
    filled = 0
    for message in messages:
        if message.get("role") != "assistant":
            continue
        content = message.get("content")
        is_anthropic_blocks = isinstance(content, list)

        # Skip if reasoning is already present in the message's own shape.
        if is_anthropic_blocks:
            has_reasoning = any(
                isinstance(block, dict)
                and block.get("type") == "thinking"
                and isinstance(block.get("thinking"), str)
                and block["thinking"]
                for block in content
            )
        else:
            existing = message.get("reasoning_content")
            has_reasoning = isinstance(existing, str) and bool(existing)
        if has_reasoning:
            continue

        signature = assistant_signature(message)
        if signature is None:
            continue
        sources = registry.get(signature, [])
        if not sources:
            continue

        # Use the nearest matching response.  Empty reasoning means the proxy
        # export did not contain recoverable reasoning for that exact call.
        source = sources[-1]
        reasoning = source.reasoning_content
        if not reasoning:
            continue

        if is_anthropic_blocks:
            # Fill an empty thinking stub in place; otherwise insert before
            # the first non-thinking block to keep the block order valid.
            target = next(
                (
                    block
                    for block in content
                    if isinstance(block, dict) and block.get("type") == "thinking"
                ),
                None,
            )
            if target is not None and not (
                isinstance(target.get("thinking"), str) and target["thinking"]
            ):
                target["thinking"] = reasoning
                if source.thinking_signature:
                    target["signature"] = source.thinking_signature
            else:
                thinking_block = {"type": "thinking", "thinking": reasoning}
                if source.thinking_signature:
                    thinking_block["signature"] = source.thinking_signature
                insert_at = len(content)
                for index, block in enumerate(content):
                    if not (
                        isinstance(block, dict) and block.get("type") == "thinking"
                    ):
                        insert_at = index
                        break
                content.insert(insert_at, thinking_block)
        else:
            message["reasoning_content"] = reasoning
        filled += 1
    return filled


def repair_nested_request(
    outer: JSONDict,
    field_name: str,
    registry: dict[Signature, list[ResponseSource]],
) -> int:
    original = outer.get(field_name)
    if original is None:
        return 0
    try:
        decoded = parse_json_value(original)
    except json.JSONDecodeError:
        return 0
    if not isinstance(decoded, dict):
        return 0
    messages = decoded.get("messages")
    if not isinstance(messages, list):
        return 0

    filled = backfill_messages(
        (message for message in messages if isinstance(message, dict)), registry
    )
    outer[field_name] = encode_like_original(original, decoded)
    return filled


def atomic_write_json(path: Path, value: JSONDict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(text, encoding="utf-8")
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def manifest_json_lines(
    input_dir: Path,
    records: Sequence[Record],
    run_groups: Sequence[RunGroup],
    repair_counts: dict[int, tuple[int, int]],
) -> str:
    rows: list[JSONDict] = [
        {
            "type": "summary",
            "schema_version": 2,
            "source_directory": str(input_dir),
            "source_file_count": len(records),
            "run_count": len(run_groups),
            "selected_file_count": sum(
                len(group.selected_indices) for group in run_groups
            ),
            "selection_rule": (
                "group by final user anchor; retain every maximal request "
                "branch; deduplicate exact request/response replays"
            ),
        }
    ]

    for run_number, group in enumerate(run_groups, start=1):
        group_records = [records[index] for index in group.record_indices]
        request_variant_count = len(
            {tuple(record.canonical_messages) for record in group_records}
        )
        rows.append(
            {
                "type": "run",
                "run_id": f"run-{run_number:03d}",
                "run_kind": classify_run(group.user_content),
                "user_message_index": group.user_message_index,
                "user_message_preview": " ".join(group.user_content.split())[:240],
                "first_timestamp": group_records[0].outer.get("ts"),
                "last_timestamp": group_records[-1].outer.get("ts"),
                "proxy_call_count": len(group_records),
                "request_variant_count": request_variant_count,
                "source_files": [record.path.name for record in group_records],
                "selected_files": [
                    records[index].path.name for index in group.selected_indices
                ],
                "selected_message_counts": [
                    len(records[index].request_messages)
                    for index in group.selected_indices
                ],
                "reasoning_backfill": {
                    records[index].path.name: {
                        "req": repair_counts.get(index, (0, 0))[0],
                        "up_req": repair_counts.get(index, (0, 0))[1],
                    }
                    for index in group.selected_indices
                },
            }
        )

    return "".join(
        json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n"
        for row in rows
    )


def process_directory(
    input_dir: Path, output_dir: Path
) -> tuple[int, int, int, int]:
    """Aggregate every terminal run branch and repair selected snapshots."""
    records = load_records(input_dir)
    run_groups, selected = select_branch_records(records)

    req_filled = 0
    up_req_filled = 0
    repair_counts: dict[int, tuple[int, int]] = {}
    for index in sorted(selected):
        record = records[index]
        output = copy.deepcopy(record.outer)
        registry = response_registry(records, before_index=index)
        req_count = repair_nested_request(output, "req", registry)
        up_req_count = repair_nested_request(output, "up_req", registry)
        req_filled += req_count
        up_req_filled += up_req_count
        repair_counts[index] = (req_count, up_req_count)
        atomic_write_json(output_dir / record.path.name, output)

    atomic_write_text(
        output_dir / "_manifest.jsonl",
        manifest_json_lines(input_dir, records, run_groups, repair_counts),
    )
    return len(selected), len(run_groups), req_filled, up_req_filled


def discover_input_dirs(root: Path, output: Path) -> list[Path]:
    output_resolved = output.resolve()
    result = []
    for child in sorted(root.iterdir(), key=lambda path: path.name):
        if not child.is_dir() or child.resolve() == output_resolved:
            continue
        if any(child.glob("*.json")):
            result.append(child)
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "保留 Hermes 上下文压缩前及最终轨迹，并从早期响应回填 reasoning_content。"
        )
    )
    parser.add_argument(
        "input_dirs",
        nargs="*",
        type=Path,
        help="输入目录；省略时自动发现当前目录下含 JSON 的一级子目录",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("processed_trajectories"),
        help="输出根目录（默认：processed_trajectories）",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output: Path = args.output.resolve()
    inputs = [path.resolve() for path in args.input_dirs]
    if not inputs:
        inputs = discover_input_dirs(Path.cwd(), output)
    if not inputs:
        print("错误：没有找到包含 JSON 文件的输入目录。", file=sys.stderr)
        return 2

    failures = 0
    for input_dir in inputs:
        if not input_dir.is_dir():
            print(f"错误：输入目录不存在：{input_dir}", file=sys.stderr)
            failures += 1
            continue
        destination = output / input_dir.name
        try:
            files, runs, req_count, up_req_count = process_directory(
                input_dir, destination
            )
        except (OSError, ValueError) as exc:
            print(f"错误：{exc}", file=sys.stderr)
            failures += 1
            continue
        print(
            f"{input_dir.name}: 保留 {files} 个文件；"
            f"req 回填 {req_count} 条，up_req 回填 {up_req_count} 条；"
            f"输出到 {destination}"
        )

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
