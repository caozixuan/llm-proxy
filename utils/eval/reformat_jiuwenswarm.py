"""utils/eval/reformat_jiuwenswarm.py — Jiuwenswarm 开发导出（快照分支聚合 + 权重 + reasoning 回填）

把已落盘的 trace 文件转成带 assistant 权重的 OpenAI chat 训练记录。默认保留
「最大可证明快照分支」，并从原始 response 回填历史 reasoning（与 reformat_jiuwen
同思路，但聚合逻辑在本文件内实现）。Compaction / 歧义 / 重启分支各自独立保留；
每代生成只训练一次，被携带的副本权重为 0；非 assistant 消息不带权重。
任务归属与 Task Done 过滤仍然推迟（task_outcome=unknown）。

两种用法：

1) 导出模式处理器（网页 /keys/export、离线 CLI tools/offline_reformat_export.py、
   公开入口 /export/submit 共用同一个入口函数）：
       processor: "utils.eval.reformat_jiuwenswarm:jiuwenswarm_export"
   支持 file_dirs 契约 —— 打开 merge_hours 时引擎会把跨整点被切开的同一段对话
   先合并成一条会话再交过来，各 trace 文件分散在多个叶子目录里，靠 file_dirs 定位。

2) 独立离线 CLI（包含 --restore 反解能力）：
       python utils/eval/reformat_jiuwenswarm.py --input-dir HOUR_12 --input-dir HOUR_13 --output-dir OUT
       python utils/eval/reformat_jiuwenswarm.py --input-dir IN --output-dir OUT --response-weight 1 --compression-weight 1
       python utils/eval/reformat_jiuwenswarm.py --restore --input-dir WEIGHTED --output-dir MERGED

产物布局（聚合模式）：<out>/segments/<namespace>/<sha256>.json +
<out>/manifests/<namespace>.jsonl + <out>/manifests/<namespace>.summary.json。
--per-request 退化为逐请求导出（<out>/requests/<namespace>/）。
选中快照与回填来源保留在 meta_info.unique_info 里供审计/反解。
现有输出目录永不被覆盖：manifest 以 "x" 模式创建，命名空间是确定性的 digest。

整点切分只是存储桶，不是会话边界 —— 导出时应当把所有小时放在一起聚合；
被复制的采集会去重，独立重试不会。
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import deepcopy
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sys
import tempfile

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

REASONING_BLOCK_TYPES = {"thinking", "reasoning", "redacted_thinking"}

SCHEMA = "jiuwen.request.weighted.openai.v2"
AGGREGATE_SCHEMA = "jiuwen.segment.weighted.openai.v3"
SUPPORTED_SCHEMAS = {SCHEMA, AGGREGATE_SCHEMA, "jiuwen.request.weighted.v1"}
SECRET_FIELDS = {"api_key", "api-key", "apikey", "authorization", "proxy-authorization",
                 "x-api-key", "access_token", "secret_key", "cookie", "set-cookie"}
SOURCE_HEADERS = {"x-request-id", "request-id", "x-trace-id", "traceparent", "tracestate",
                  "x-session-id", "x-conversation-id", "anthropic-version", "content-type"}


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def validate_weight(value):
    if isinstance(value, bool):
        raise ValueError("Weight must be a finite nonnegative number, not bool")
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise ValueError("Weight must be finite and nonnegative")
    return result


def redact(value):
    """Remove known credential fields; do not alter user content or thinking text."""
    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            name = str(key).lower()
            if name in SECRET_FIELDS:
                continue
            if name in {"header", "headers"} and isinstance(item, dict):
                result[key] = {k: redact(v) for k, v in item.items() if k.lower() in SOURCE_HEADERS}
            else:
                result[key] = redact(item)
        return result
    if isinstance(value, list):
        return [redact(item) for item in value]
    return deepcopy(value)


def text_content(value):
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return "".join(str(block.get("text", block.get("content", "")))
                       for block in value if isinstance(block, dict)
                       and block.get("type") in {"text", "input_text"})
    return ""


def classify(document):
    metadata = document.get("metadata") or {}
    purpose = metadata.get("request_purpose", metadata.get("purpose")) if isinstance(metadata, dict) else None
    users = [text_content(m.get("content", m.get("parts"))) for m in document["messages"] if m.get("role") == "user"]
    if purpose == "compaction":
        return "compaction", "explicit_metadata"
    if users and "NON-NEGOTIABLE OUTPUT RULES" in users[-1] and "<state_snapshot>" in users[-1]:
        return "compaction", "compression_instruction_hint"
    if users and users[0].lstrip().startswith(("<memory_block", "<recovered_context>")):
        return "post_compaction", "summary_prefix_hint"
    first = users[0] if users else ""
    if first.startswith("\u4f60\u6536\u5230\u4e00\u6761\u6d88\u606f\uff1a"):
        try:
            wrapped = json.loads(first[first.index("{"):])
        except (ValueError, TypeError):
            wrapped = {}
        if isinstance(wrapped, dict) and wrapped.get("source") == "__heartbeat__":
            return "auxiliary", "heartbeat_source"
    return ("initial_query", "uncompressed_context_hint") if users else ("unknown", "no_user_message")


def response_message(response):
    if response.get("choices"):
        choices = response["choices"]
        if not isinstance(choices, list) or not isinstance(choices[0], dict):
            raise ValueError("Invalid response choices")
        message = choices[0].get("message", {})
        if not isinstance(message, dict):
            raise ValueError("Invalid response message")
        return {**deepcopy(message), "role": "assistant"}
    message = {"role": "assistant", "content": deepcopy(response.get("content", []))}
    for name in ("reasoning_content", "tool_calls", "refusal"):
        if name in response:
            message[name] = deepcopy(response[name])
    return message


def openai_content(value):
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if not isinstance(value, list):
        return canonical(value)
    blocks = []
    for block in value:
        if isinstance(block, str):
            blocks.append({"type": "text", "text": block})
            continue
        if not isinstance(block, dict):
            raise ValueError("Unsupported content block")
        kind = block.get("type")
        if kind in {"text", "input_text", "output_text"}:
            blocks.append({"type": "text", "text": block.get("text", block.get("content", ""))})
        elif kind == "image":
            source = block.get("source", {})
            url = source.get("url") if source.get("type") == "url" else None
            if source.get("type") == "base64":
                url = "data:" + source.get("media_type", "image/png") + ";base64," + source["data"]
            if not url:
                raise ValueError("Image source must contain a URL or base64 data")
            blocks.append({"type": "image_url", "image_url": {"url": url}})
        elif kind in {"image_url", "input_audio", "file", "audio"}:
            blocks.append({k: deepcopy(v) for k, v in block.items() if k != "cache_control"})
        elif kind == "document":
            source = block.get("source", {})
            if source.get("type") == "text":
                blocks.append({"type": "text", "text": source.get("data", source.get("text", ""))})
            elif source.get("type") == "base64":
                blocks.append({"type": "file", "file": {"filename": block.get("title") or "attachment",
                    "file_data": "data:" + source.get("media_type", "application/pdf") + ";base64," + source["data"]}})
            elif source.get("file_id"):
                blocks.append({"type": "file", "file": {"file_id": source["file_id"]}})
            else:
                raise ValueError("Document requires text, base64 data or file_id for OpenAI conversion")
        else:
            raise ValueError("Unsupported OpenAI content conversion: " + str(kind))
    if all(b["type"] == "text" for b in blocks):
        return "".join(b["text"] for b in blocks)
    return blocks


def openai_tool_call(call):
    function = call.get("function", call)
    arguments = function.get("arguments", function.get("input", {}))
    return {"id": call.get("id", call.get("call_id", "")), "type": "function",
            "function": {"name": function.get("name", ""),
                         "arguments": arguments if isinstance(arguments, str) else canonical(arguments)}}


def openai_messages(original):
    """Expand native tool results without reordering or inventing call IDs."""
    role = original["role"]
    value = original.get("parts", original.get("content", ""))
    parts = value if isinstance(value, list) else []
    if role == "assistant":
        body, thinking = [], []
        calls = [openai_tool_call(c) for c in original.get("tool_calls", []) or []]
        for block in parts:
            kind = block.get("type") if isinstance(block, dict) else None
            if kind in {"tool_use", "tool_call"}:
                calls.append(openai_tool_call(block))
            elif kind in REASONING_BLOCK_TYPES:
                if kind != "redacted_thinking":
                    text = block.get("thinking", block.get("reasoning", block.get("text", block.get("content", ""))))
                    if not isinstance(text, str):
                        raise ValueError("Reasoning text must be a string")
                    thinking.append(text)
            elif kind in {"tool_result", "tool_call_response"}:
                raise ValueError("Assistant response cannot contain a tool result")
            else:
                body.append(block)
        message = {"role": role, "content": openai_content(body if isinstance(value, list) else value)}
        existing = original.get("reasoning_content") or ""
        reason = "".join(thinking)
        reason = existing if existing == reason else existing + reason
        if reason:
            message["reasoning_content"] = reason
        if calls:
            by_id = {}
            for call in calls:
                key = call["id"]
                if key and key in by_id:
                    if call != by_id[key]:
                        raise ValueError("Conflicting tool-call representations for ID " + key)
                    continue
                by_id[key if key else (None, len(by_id))] = call
            message["tool_calls"] = list(by_id.values())
        for name in ("name", "refusal", "audio"):
            if name in original:
                message[name] = deepcopy(original[name])
        if original.get("function_call"):
            raise ValueError("Legacy function_call without tool call ID cannot be guessed")
        return [message]
    if role == "tool" and not any(isinstance(b, dict) and b.get("type") in {"tool_result", "tool_call_response"} for b in parts):
        return [{"role": "tool", "tool_call_id": original.get("tool_call_id", original.get("id", "")),
                 "content": openai_content(value)}]
    result, pending = [], []
    for block in parts:
        kind = block.get("type") if isinstance(block, dict) else None
        if kind in {"tool_result", "tool_call_response"}:
            if pending:
                result.append({"role": role, "content": openai_content(pending)})
                pending = []
            result.append({"role": "tool", "tool_call_id": block.get("tool_use_id", block.get("id", "")),
                           "content": openai_content(block.get("response", block.get("content", "")))})
        else:
            pending.append(block)
    if pending or not result:
        result.append({"role": role, "content": openai_content(pending if isinstance(value, list) else value)})
    if "name" in original:
        for message in result:
            if message["role"] == role:
                message["name"] = original["name"]
    return result


def meaningful(message):
    if message.get("reasoning_content") or message.get("tool_calls") or message.get("refusal"):
        return True
    body = message.get("content")
    if isinstance(body, list):
        return any(isinstance(b, dict) and (
            bool(b.get("text") or b.get("thinking") or b.get("reasoning") or b.get("data"))
            or b.get("type") in {"tool_use", "image", "image_url", "document"}) for b in body)
    return bool(body)


def candidate_weight(document, message, source, response_weight, compression_weight, kind):
    response = document["response"]
    status = response.get("status_code")
    if isinstance(status, bool) or not isinstance(status, int) or not 200 <= status < 300:
        return 0.0, "response_not_successful_or_status_unknown"
    if response.get("error") or source.get("capture_has_error"):
        return 0.0, "response_error"
    choice = response.get("choices", [{}])[0] if response.get("choices") else {}
    stop = response.get("stop_reason", response.get("finish_reason", choice.get("finish_reason")))
    if stop in {"length", "max_tokens", "model_context_window_exceeded"}:
        return 0.0, "response_truncated"
    if source.get("stream_complete") is False:
        return 0.0, "incomplete_stream"
    if not meaningful(message):
        return 0.0, "empty_response"
    value = compression_weight if kind == "compaction" else response_weight
    return value, "configured_compaction_candidate" if kind == "compaction" else "configured_response_candidate"


def convert_document(document: dict, *, source: dict, response_weight=1.0, compression_weight=None,
                     retain_raw_response=True):
    """Emit OpenAI messages while retaining native blocks in provenance.

    Weights are candidate weights, not a task-done or quality verdict. Historical
    assistant copies are zero; non-assistant messages never carry a weight.
    """
    response_weight = validate_weight(response_weight)
    compression_weight = response_weight if compression_weight is None else validate_weight(compression_weight)
    if not isinstance(document, dict) or not isinstance(document.get("messages"), list):
        raise ValueError("Document must contain a messages array")
    if not isinstance(document.get("response"), dict):
        raise ValueError("Document must contain a response object")
    if any(not isinstance(m, dict) or not isinstance(m.get("role"), str) for m in document["messages"]):
        raise ValueError("Every input message must be an object with a role")
    kind, classification = classify(document)
    messages, annotations = [], []
    if document.get("system") is not None:
        messages.extend(openai_messages({"role": "system", "content": document["system"]}))
        annotations.append({"origin": "request_system", "input_index": None})
    for index, original in enumerate(document["messages"]):
        for normalized_index, message in enumerate(openai_messages(original)):
            if message["role"] == "assistant":
                message["weight"] = 0.0
            annotation = {"origin": "request_history", "input_index": index, "normalized_index": normalized_index}
            messages.append(message)
            annotations.append(annotation)
    native_response = response_message(document["response"])
    message = openai_messages(native_response)[0]
    value, reason = candidate_weight(document, native_response, source, response_weight, compression_weight, kind)
    if not meaningful(message):
        value, reason = 0.0, "empty_normalized_response"
    if any(not c["id"] or not c["function"]["name"] for m in [*messages, message] for c in m.get("tool_calls", [])) or any(
            m["role"] == "tool" and not m.get("tool_call_id") for m in messages):
        value, reason = 0.0, "incomplete_tool_identity"
    message["weight"] = value
    messages.append(message)
    annotations.append({"origin": "current_response", "input_index": None, "weight_reason": reason})
    request = {k: v for k, v in document.items() if k not in {"messages", "response"}}
    info = {"schema": SCHEMA, "message_format": "openai", "aggregation": "none_one_api_request_per_record",
            "trajectory_type": kind, "classification_basis": classification,
            "classification_is_task_verdict": False, "task_outcome": "unknown",
            "training_eligibility": "pending_offline_task_association",
            "source": redact({k: v for k, v in source.items() if k != "raw_response"}),
            "request": redact(request), "response": redact(document["response"]),
            "source_messages": deepcopy(document["messages"]),
            "message_annotations": annotations, "current_response_index": len(messages) - 1,
            "response_reasoning_blocks": sum(isinstance(b, dict) and b.get("type") in REASONING_BLOCK_TYPES
                for b in (native_response["content"] if isinstance(native_response.get("content"), list) else [])),
            "weight_policy": {"history_assistant": 0.0, "response": response_weight,
                              "compaction_response": compression_weight, "scope": "candidate_only"}}
    if retain_raw_response and "raw_response" in source:
        info["raw_response"] = redact(source["raw_response"])
    # Export-session keys are provenance only, never execution-session identities.
    record_id = digest([SCHEMA, source.get("log_dir"), source.get("filename"), redact(document)])
    result = {"id": record_id, "messages": messages, "meta_info": {"unique_info": info}}
    validate_record(result)
    return result


def restore_document(record):
    """Recover the unmodified selected request, not a fabricated raw backfill."""
    validate_record(record)
    info = record["meta_info"]["unique_info"]
    document = deepcopy(info["request"])
    if "source_messages" in info:
        document["messages"] = deepcopy(info["source_messages"])
        document["response"] = deepcopy(info["response"])
        return document
    restored = []
    for message, annotation in zip(record["messages"], info["message_annotations"]):
        if annotation["origin"] != "request_history":
            continue
        item = deepcopy(message)
        item.pop("weight", None)
        if "original_weight" in annotation:
            item["weight"] = deepcopy(annotation["original_weight"])
        restored.append((annotation["input_index"], item))
    document["messages"] = [message for _, message in sorted(restored)]
    document["response"] = deepcopy(info["response"])
    return document


def validate_record(record):
    if set(record) != {"id", "messages", "meta_info"}:
        raise ValueError("Invalid top-level export contract")
    info = record["meta_info"]["unique_info"]
    annotations, messages = info["message_annotations"], record["messages"]
    aggregated = info.get("schema") == AGGREGATE_SCHEMA
    if info.get("schema") in {SCHEMA, AGGREGATE_SCHEMA} and (info.get("message_format") != "openai" or not isinstance(info.get("source_messages"), list)):
        raise ValueError("OpenAI export requires source_messages for lossless restoration")
    if len(messages) != len(annotations) or not messages:
        raise ValueError("Message/annotation length mismatch")
    current = info["current_response_index"]
    if current != len(messages) - 1:
        raise ValueError("Current response must be the final message")
    if messages[current].get("role") != "assistant" or annotations[current]["origin"] != "current_response":
        raise ValueError("Final message must be the current assistant response")
    for index, (message, annotation) in enumerate(zip(messages, annotations)):
        if info.get("message_format") == "openai":
            if message["role"] not in {"system", "developer", "user", "assistant", "tool"}:
                raise ValueError("Unsupported OpenAI message role")
            if "parts" in message:
                raise ValueError("Native message parts remain in OpenAI output")
            for call in message.get("tool_calls", []):
                if message["role"] != "assistant" or call.get("type") != "function" or not isinstance(call.get("id"), str) or not isinstance(call.get("function", {}).get("arguments"), str):
                    raise ValueError("Invalid OpenAI tool call")
            if message["role"] == "tool" and not isinstance(message.get("tool_call_id"), str):
                raise ValueError("Tool message requires an original tool_call_id")
            if isinstance(message.get("content"), list) and any(not isinstance(b, dict) or b.get("type") not in {
                    "text", "image_url", "input_audio", "file", "audio"} for b in message["content"]):
                raise ValueError("Native content block remains in OpenAI output")
        if message["role"] == "assistant":
            if "weight" not in message:
                raise ValueError("Assistant weight is missing")
            value = validate_weight(message["weight"])
            if index != current and value != 0 and not aggregated:
                raise ValueError("Historical assistant is trainable")
            if aggregated and value != 0 and not annotation.get("generation_id"):
                raise ValueError("Trainable aggregate assistant lacks a generation source")
            if aggregated and value != 0 and annotation.get("weight_reason") != "first_generation_occurrence":
                raise ValueError("Aggregate weight lacks first-occurrence ownership")
        elif "weight" in message:
            raise ValueError("Non-assistant messages must not carry weight")
        if annotation["origin"] == "current_response" and index != current:
            raise ValueError("Multiple current responses")
    if aggregated:
        sources = info.get("source_requests", [])
        by_id = {r["id"]: r for r in sources}
        if len(by_id) != len(sources) or info.get("selected_request_id") not in by_id:
            raise ValueError("Invalid aggregate source references")
        if messages[current].get("role") != "assistant" or annotations[current].get("generation_id") != info["selected_request_id"]:
            raise ValueError("Aggregate current response source mismatch")
        if not set(info.get("backfill_responses", {})).issubset(by_id):
            raise ValueError("Backfill response lacks an original source")
        positive = []
        for message, annotation in zip(messages, annotations):
            generation = annotation.get("generation_id")
            if generation is not None and generation not in by_id:
                raise ValueError("Aggregate generation source is missing")
            if message.get("weight", 0) > 0:
                if message["weight"] != by_id[generation]["candidate_weight"]:
                    raise ValueError("Aggregate weight differs from original generation")
                positive.append(generation)
        if len(positive) != len(set(positive)) or set(positive) != set(info.get("trainable_generation_ids", [])):
            raise ValueError("Aggregate generation ownership is inconsistent")
    return True


def _without_cache(value):
    if isinstance(value, dict):
        return {k: _without_cache(v) for k, v in value.items() if k != "cache_control"}
    if isinstance(value, list):
        return [_without_cache(v) for v in value]
    return value


def _message_identity(message):
    """Match visible content like reformat_jiuwen; ignore only known metadata."""
    value = {k: deepcopy(v) for k, v in message.items() if k not in {"weight", "reasoning_content"}}
    for call in value.get("tool_calls", []):
        function = call["function"]
        try:
            arguments = json.loads(function["arguments"])
        except (ValueError, TypeError):
            continue
        if call.get("id") and isinstance(arguments, dict):
            # Jiuwen omits this runtime-only field when carrying tool history.
            arguments.pop("call_goal", None)
        function["arguments"] = canonical(arguments)
    return digest(_without_cache(value))


def _transient(message):
    return (message.get("role") == "user" and isinstance(message.get("content"), str)
            and message["content"].lstrip().startswith("<system-reminder>"))


def _record_order(record):
    source = record["meta_info"]["unique_info"]["source"]
    return (source.get("filename", ""), source.get("source_path", ""),
            source.get("log_dir", ""), record["id"])


def _prepare_records(records):
    """Order all hourly buckets together and remove only copied captures.

    The original filename remains part of capture identity: equal prompts or
    equal answers under different filenames may be independent API calls.
    JSON serialization and download paths do not define generation identity.
    """
    unique, aliases = {}, {}
    for record in sorted(records, key=_record_order):
        info = record["meta_info"]["unique_info"]
        source = info["source"]
        capture_key = digest([source.get("filename") or record["id"], info["request"],
            info["source_messages"], info["response"], info.get("raw_response"),
            {k: source.get(k) for k in ("format", "response_sha256", "stream_complete",
                "capture_has_error", "response_status_source")},
            record["messages"][info["current_response_index"]]["weight"]])
        if capture_key not in unique:
            primary = {**record, "meta_info": {**record["meta_info"], "unique_info": {
                **info, "source": deepcopy(source)}}}
            unique[capture_key] = primary
        else:
            primary = unique[capture_key]
            provenance = primary["meta_info"]["unique_info"]["source"]
            path = source.get("source_path")
            if path and path != provenance.get("source_path"):
                copies = provenance.setdefault("export_copies", [])
                if path not in copies:
                    copies.append(path)
            provenance.setdefault("export_copy_sources", []).append({
                "request_record_id": record["id"], "source": deepcopy(source)})
        aliases[record["id"]] = primary["id"]
    return list(unique.values()), aliases


def aggregate_records(records):
    """Keep maximal proven branches; backfill without overwriting existing reasoning.

    Hourly inputs are sorted globally by original filename, not export folder.
    Snapshot continuation requires a full prefix including a prior response,
    never merely the first query or proxy user_id. Restoration uses unique
    visible response content, scoped by identity and execution/query evidence.
    """
    input_count = len(records)
    records, _ = _prepare_records(records)
    nodes, leaves = [], []
    indexed = defaultdict(list)
    stats = Counter(input_requests=input_count, unique_input_requests=len(records),
                    duplicate_export_copies=input_count - len(records))
    for position, record in enumerate(records):
        info = record["meta_info"]["unique_info"]
        request = info["request"]
        metadata = request.get("metadata") or {}
        metadata = metadata if isinstance(metadata, dict) else {}
        identity = metadata.get("user_id")
        execution = metadata.get("harness_session_id", metadata.get("session_id", metadata.get("conversation_id")))
        isolated = None if identity or execution else record["id"]
        identity_scope = digest([identity, execution, isolated])
        scope = digest([identity, execution, isolated, request.get("model"),
                        _without_cache(request.get("system")), _without_cache(request.get("tools"))])
        current = info["current_response_index"]
        core = [(i, _message_identity(m)) for i, m in enumerate(record["messages"]) if not _transient(m)]
        keys = [key for _, key in core]
        output = record["messages"][current]
        first_user = digest(next((m.get("content") for m in record["messages"]
                                  if m["role"] == "user" and not _transient(m)), None))
        anchored = bool(output.get("content") or output.get("tool_calls") or output.get("refusal"))
        node = {"record": record, "position": position, "scope": scope, "identity_scope": identity_scope,
                "first_user": first_user, "execution": execution, "core": core, "keys": keys,
                "ancestors": [], "kind": info["trajectory_type"], "anchored": anchored}
        candidates = [n for n in leaves if n["scope"] == scope and n["kind"] != "compaction"
                      and node["kind"] != "compaction" and n["anchored"]
                      and len(n["keys"]) <= len(keys) - 1 and n["keys"] == keys[:len(n["keys"])]]
        if candidates:
            length = max(len(n["keys"]) for n in candidates)
            candidates = [n for n in candidates if len(n["keys"]) == length]
        if len(candidates) == 1:
            parent = candidates[0]
            node["ancestors"] = [*parent["ancestors"], parent]
            leaves.remove(parent)
            stats["absorbed_snapshots"] += 1
            before = parent["record"]["meta_info"]["unique_info"]["source"]
            after = info["source"]
            before_directory = str(Path(before["source_path"]).parent) if before.get("source_path") else before.get("log_dir")
            after_directory = str(Path(after["source_path"]).parent) if after.get("source_path") else after.get("log_dir")
            if before_directory and after_directory and before_directory != after_directory:
                stats["cross_directory_continuations"] += 1
            hours = [re.match(r"^(\d{4}-\d\d-\d\d[_T]\d\d)", s.get("filename", ""))
                     for s in (before, after)]
            if all(hours) and hours[0][1] != hours[1][1]:
                stats["cross_hour_continuations"] += 1
        elif candidates:
            stats["ambiguous_continuations"] += 1
        leaves.append(node)
        nodes.append(node)
        if anchored:
            indexed[(identity_scope, _message_identity(output))].append(node)

    owned = set()
    results = []
    for leaf in sorted(leaves, key=lambda n: n["position"]):
        result = deepcopy(leaf["record"])
        info = result["meta_info"]["unique_info"]
        original_id = result["id"]
        ancestors = {len(n["keys"]) - 1: n for n in leaf["ancestors"]}
        core_positions = {index: number for number, (index, _) in enumerate(leaf["core"])}
        chain = [*leaf["ancestors"], leaf]
        references = {n["record"]["id"]: n for n in chain}
        reasoning_sources = {}
        used = []
        for index, (message, annotation) in enumerate(zip(result["messages"], info["message_annotations"])):
            if message["role"] != "assistant":
                continue
            current = index == info["current_response_index"]
            source = leaf if current else ancestors.get(core_positions.get(index))
            if source is None:
                options = [n for n in indexed[(leaf["identity_scope"], _message_identity(message))]
                           if n["position"] < leaf["position"] and (message.get("tool_calls")
                                or leaf["execution"] or n["first_user"] == leaf["first_user"])]
                if len(options) == 1:
                    source = options[0]
                elif options:
                    stats["ambiguous_backfill_occurrences"] += 1
            message["weight"] = 0.0
            if source is None:
                annotation["weight_reason"] = "unmatched_carried_context"
                stats["unmatched_history_assistants"] += 1
                continue
            source_record = source["record"]
            source_info = source_record["meta_info"]["unique_info"]
            generated = source_record["messages"][source_info["current_response_index"]]
            if _message_identity(generated) != _message_identity(message):
                raise ValueError("Aggregate response position does not match selected history")
            generation_id = source_record["id"]
            references[generation_id] = source
            annotation["generation_id"] = generation_id
            annotation["source_request_id"] = generation_id
            annotation["match_basis"] = ("current_response" if current else "proven_snapshot_prefix" if source in leaf["ancestors"]
                                         else "unique_tool_identity" if message.get("tool_calls") else "unique_visible_content_with_context")
            expected, existing = generated.get("reasoning_content", ""), message.get("reasoning_content", "")
            native = source_info["response"]
            native_blocks = native.get("content", [])
            if not current and (expected or (isinstance(native_blocks, list) and any(
                    isinstance(b, dict) and b.get("type") in REASONING_BLOCK_TYPES for b in native_blocks))):
                reasoning_sources[generation_id] = deepcopy(native)
            if expected and not existing:
                message["reasoning_content"] = expected
                annotation["reasoning_backfilled"] = True
                stats["reasoning_backfilled_messages"] += 1
                stats["reasoning_backfilled_characters"] += len(expected)
            if expected and existing and expected != existing:
                annotation["weight_reason"] = "reasoning_conflict_preserved"
                stats["reasoning_conflicts"] += 1
            elif generation_id in owned:
                annotation["weight_reason"] = "already_trained_carried_generation"
                stats["repeated_generations_masked"] += 1
            elif generated["weight"] > 0:
                message["weight"] = generated["weight"]
                annotation["weight_reason"] = "first_generation_occurrence"
                owned.add(generation_id)
                used.append(generation_id)
                stats["trainable_generations"] += 1
            else:
                annotation["weight_reason"] = "source_response_not_trainable"
        info["schema"] = AGGREGATE_SCHEMA
        info["aggregation"] = "maximal_proven_snapshot_branches_with_reasoning_backfill"
        info["selected_request_id"] = original_id
        info["absorbed_request_ids"] = [n["record"]["id"] for n in leaf["ancestors"]]
        info["source_requests"] = [{"id": key, "source": deepcopy(n["record"]["meta_info"]["unique_info"]["source"]),
                                    "candidate_weight": n["record"]["messages"][-1]["weight"]}
                                   for key, n in sorted(references.items(), key=lambda item: item[1]["position"])]
        # Backfill sources keep native blocks/signatures, not repeated full inputs.
        info["backfill_responses"] = reasoning_sources
        info["trainable_generation_ids"] = used
        info["weight_policy"]["scope"] = "unique_original_generation_once_across_segments"
        info["weight_policy"]["history_assistant"] = "matched_generation_candidate_else_zero"
        result["id"] = digest([AGGREGATE_SCHEMA, original_id, info["absorbed_request_ids"]])
        validate_record(result)
        results.append(result)
    stats["output_segments"] = len(results)
    return results, dict(stats)


def _aggregate_export(src_dir, out_dir, session_entries, api_key, workers, progress_cb,
                      log_dir, should_cancel, on_session_done, response_weight,
                      compression_weight, retain_raw_response, source_directories=None,
                      file_dirs=None):
    src, out = Path(src_dir).resolve(), Path(out_dir).resolve()
    if not src.is_dir():
        raise ValueError("Source directory does not exist")
    if out == src or out.is_relative_to(src):
        raise ValueError("Export output must not be inside the source directory")
    stop, log = should_cancel or (lambda: False), progress_cb or (lambda message: None)
    entries = [s for s in session_entries if not api_key or s.get("api_key") == api_key]
    result = {"total_sessions": len(entries), "total_files": 0, "errors": [], "results": [], "cancelled": False}
    if stop():
        result["cancelled"] = True
        return result
    namespace = digest([str(src), log_dir or src.name, AGGREGATE_SCHEMA])[:24]
    manifest = out / "manifests" / (namespace + ".jsonl")
    manifest.parent.mkdir(parents=True, exist_ok=True)
    jobs = defaultdict(list)
    directories = {}
    # 源目录解析：优先 file_dirs（跨小时合并后同一会话的 trace 可能分散在多个叶子，
    # 只能按文件名逐个定位）；其次 source_directories（离线 CLI 多 input-dir 时按目录
    # 逐个成组）；最后退回 src_dir。
    resolved_file_dirs = {name: Path(where).resolve()
                          for name, where in (file_dirs or {}).items() if where}
    for index, session in enumerate(entries):
        directories[index] = Path(source_directories[index]).resolve() if source_directories else src
        names = trace_names(session)
        if not names:
            result["errors"].append({"session_index": index, "error": "no_trace_files"})
        for name in names:
            jobs[(resolved_file_dirs.get(name, directories[index]), name)].append(index)
    loaded = {}
    processed = set()

    def load(directory, name):
        document, source = load_source(directory, name)
        source["log_dir"] = str(directory) if source_directories or resolved_file_dirs else log_dir or src.name
        return convert_document(document, source=source, response_weight=response_weight,
                                compression_weight=compression_weight, retain_raw_response=retain_raw_response)

    # Reserve the namespace before work, including partial failures/cancellations.
    with manifest.open("x", encoding="utf-8") as stream:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {}
            for key in sorted(jobs, key=lambda key: (key[1], str(key[0]))):
                if stop():
                    result["cancelled"] = True
                    break
                futures[pool.submit(load, *key)] = key
            for future in as_completed(futures):
                if stop():
                    result["cancelled"] = True
                    for pending in futures:
                        pending.cancel()
                if future.cancelled():
                    continue
                key = futures[future]
                processed.add(key)
                try:
                    loaded[key] = future.result()
                except Exception as exc:
                    result["errors"].append({"filename": key[1], "source_dir": str(key[0]), "error_type": type(exc).__name__, "error": str(exc)})
        prepared, aliases = _prepare_records(list(loaded.values()))
        owners = defaultdict(set)
        for key, record in loaded.items():
            owners[aliases[record["id"]]].update(jobs[key])
        records, stats = aggregate_records(prepared)
        stats["duplicate_export_copies"] = len(loaded) - len(prepared)
        stats["input_requests"] = len(loaded)
        result["aggregation_stats"] = stats
        types = Counter()
        outputs = defaultdict(list)
        for record in records:
            if stop():
                result["cancelled"] = True
                break
            info = record["meta_info"]["unique_info"]
            path = out / "segments" / namespace / (record["id"] + ".json")
            write_json(path, record)
            names = {r["source"]["filename"] for r in info["source_requests"]}
            item = {"id": record["id"], "filename": info["source"]["filename"],
                    "output_file": str(path.relative_to(out)).replace("\\", "/"),
                    "source_files": sorted(names), "trajectory_type": info["trajectory_type"]}
            stream.write(json.dumps(item, ensure_ascii=False) + "\n")
            stream.flush()
            indexes = {index for ref in info["source_requests"] for index in owners[ref["id"]]}
            for index in indexes:
                outputs[index].append(item["output_file"])
            result["total_files"] += 1
            types[info["trajectory_type"]] += 1
        result["trajectory_types"] = dict(types)
    for index, files in sorted(outputs.items()):
        session = entries[index]
        result["results"].append({"session": session.get("first_ts", session.get("_key", "")),
            "first_ts": session.get("first_ts"), "last_ts": session.get("last_ts"),
            "_key": session.get("_key", session.get("first_ts", "")), "latest_file": session.get("latest_file"),
            "log_dir": log_dir or src.name, "api_call_count": len(trace_names(session)),
            "request_files": sorted(set(files)), "models": session.get("models", []),
            "training_eligibility": "pending_offline_task_association"})
    # 兜底：聚合把多条会话并成了一个 segment 时，owners 有可能映射不到任何 session
    # （例如整段内容都来自被去重的复制采集），于是 results 空而 total_files 非空。
    # 导出引擎用 `not all_results` 判「无 session 数据 → 任务失败」，这里补出条目，
    # 避免明明写出了 segment 却被判失败。
    if result["total_files"] and not result["results"]:
        log(f"Jiuwen compact export: {result['total_files']} segments produced but no session "
            f"could be attributed; emitting {len(entries)} placeholder result(s)")
        for index, session in enumerate(entries):
            result["results"].append({"session": session.get("first_ts", session.get("_key", "")),
                "first_ts": session.get("first_ts"), "last_ts": session.get("last_ts"),
                "_key": session.get("_key", session.get("first_ts", "")),
                "latest_file": session.get("latest_file"),
                "log_dir": log_dir or src.name, "api_call_count": len(trace_names(session)),
                "request_files": [], "models": session.get("models", []),
                "training_eligibility": "pending_offline_task_association",
                "attribution": "unresolved"})
    if on_session_done:
        for index, session in enumerate(entries):
            if not result["cancelled"] or all((directories[index], name) in processed for name in trace_names(session)):
                try:
                    on_session_done()
                except Exception:
                    pass
    result["input_requests"] = len(loaded)
    result["manifest"] = str(manifest)
    log(f"Jiuwen compact export: {len(loaded)} requests -> {result['total_files']} segments; {stats.get('reasoning_backfilled_messages', 0)} reasoning backfills")
    write_json(out / "manifests" / (namespace + ".summary.json"), result)
    return result


def load_source(src_dir: Path, filename: str):
    """Reuse the service loader, including native triplets and new-api SSE."""
    if not filename or Path(filename).name != filename or "/" in filename or "\\" in filename or ":" in filename:
        raise ValueError("Trace filename must be a safe leaf filename")
    path = (src_dir / filename).resolve()
    if path.parent != src_dir.resolve() or not path.is_file():
        raise ValueError("Trace file does not exist inside source directory")
    original = json.loads(path.read_text(encoding="utf-8-sig"))
    stem = filename[:-9] if filename.endswith("-req.json") else Path(filename).stem
    if not isinstance(original, dict):
        raise ValueError("Source JSON must be an object")
    if original.get("meta_info", {}).get("unique_info", {}).get("schema") in SUPPORTED_SCHEMAS:
        raise ValueError("Source is already weighted; use restore_document before a later conversion")
    direct = isinstance(original.get("messages"), list) and isinstance(original.get("response"), dict)
    if not direct and not filename.endswith("-req.json") and "req" not in original:
        raise ValueError("Not a merged response, native request triplet or new-api capture")
    metadata = {"filename": filename, "stem": stem, "source_path": str(path),
                "source_sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
    if direct:
        document = original
        metadata["format"] = "merged"
        metadata["response_status_source"] = "merged_status_field"
    else:
        from utils.eval.reformat import _load_merged, _resolve_triplet
        tri = _resolve_triplet(src_dir, stem)
        document = _load_merged(src_dir, stem, filename, tri)
        if document is None:
            raise ValueError("Existing reformat loader could not decode this trace")
        if "req" in tri:
            metadata["format"] = "native_triplet"
            metadata["response_status_source"] = "service_response_parser"
            if "res" in tri:
                capture = json.loads(tri["res"].read_text(encoding="utf-8-sig"))
                metadata["raw_response"] = capture
                metadata["response_sha256"] = hashlib.sha256(tri["res"].read_bytes()).hexdigest()
                chunks = capture.get("chunks", [])
                if capture.get("type") == "anthropic_passthrough_sse_capture":
                    metadata["stream_complete"] = any(c.get("type") == "message_stop" for c in chunks)
                elif capture.get("type") == "openai_passthrough_sse_capture":
                    metadata["stream_complete"] = any(
                        choice.get("finish_reason") is not None for c in chunks for choice in c.get("choices", []))
                metadata["capture_has_error"] = bool(capture.get("error")) or any(c.get("type") == "error" or c.get("error") for c in chunks)
        else:
            metadata["format"] = "newapi"
            metadata["response_status_source"] = "derived_from_newapi_output_usage_not_http"
            metadata["proxy_capture"] = redact({k: v for k, v in original.items() if k not in {"req", "resp", "up_req", "up_resp"}})
            metadata["raw_response"] = original.get("resp", "")
            raw = original.get("resp", "")
            if isinstance(raw, str):
                from utils.newapi_format import split_sse_text
                chunks = split_sse_text(raw)
                if chunks:
                    anthropic = any(c.get("type") == "message_start" for c in chunks)
                    metadata["stream_complete"] = any(c.get("type") == "message_stop" for c in chunks) if anthropic else any(
                        choice.get("finish_reason") is not None for c in chunks for choice in c.get("choices", []))
                    metadata["capture_has_error"] = any(c.get("type") == "error" or c.get("error") for c in chunks)
    return document, metadata


def write_json(path, record):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent,
                                         prefix=".export-", suffix=".tmp", delete=False) as stream:
            temporary = Path(stream.name)
            json.dump(record, stream, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
            stream.write("\n")
        os.replace(temporary, path)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


def trace_names(session):
    values = [t["filename"] for t in session.get("trace_list", []) or []
              if isinstance(t, dict) and t.get("filename")]
    if not values and session.get("latest_file"):
        values = [session["latest_file"]]
    return sorted(set(values))


def _one(src, out, namespace, name, provenance, response_weight, compression_weight, retain_raw_response):
    document, source = load_source(src, name)
    source.update(provenance)
    record = convert_document(document, source=source, response_weight=response_weight,
                              compression_weight=compression_weight, retain_raw_response=retain_raw_response)
    path = out / "requests" / namespace / (record["id"] + ".json")
    write_json(path, record)
    info = record["meta_info"]["unique_info"]
    assistant = record["messages"][info["current_response_index"]]
    return {"id": record["id"], "filename": name, "output_file": str(path.relative_to(out)).replace("\\", "/"),
            "trajectory_type": info["trajectory_type"], "candidate_weight": assistant["weight"],
            "source_sha256": source["source_sha256"]}


def jiuwenswarm_export(src_dir, out_dir, session_entries, api_key=None, workers=4,
                       progress_cb=None, log_dir=None, should_cancel=None,
                       on_session_done=None, file_dirs=None,
                       *, response_weight=None, compression_weight=None,
                       retain_raw_response=True, aggregate=True):
    """导出模式处理器入口：默认产出快照分支聚合记录；任务判定仍然推迟。

    前 9 个形参与 reformat_and_analyze / reconstruct_and_export / jiuwen_reformat_export
    一致（见 test/test_processor_signatures.py 的硬性约束），外加：

    file_dirs: filename -> 叶子目录。跨小时合并（merge_hours）时由引擎注入，用于在
        会话被合并成一条后仍能按文件名找到散落在各叶子的源文件；逐目录导出时为 None。
        声明了这个形参，引擎的签名探测才会允许本模式走跨小时聚合路径。
    response_weight / compression_weight: 候选权重，缺省读
        JIUWEN_EXPORT_ASSISTANT_WEIGHT / JIUWEN_EXPORT_COMPACTION_WEIGHT（默认 1）。
    aggregate=False 退化为「每请求一条记录」的旧契约。

    不做任务级成功推断，也不对相同 prompt 去重。
    """
    response_weight = validate_weight(os.getenv("JIUWEN_EXPORT_ASSISTANT_WEIGHT", "1") if response_weight is None else response_weight)
    configured = os.getenv("JIUWEN_EXPORT_COMPACTION_WEIGHT") if compression_weight is None else compression_weight
    compression_weight = response_weight if configured is None else validate_weight(configured)
    workers = max(1, min(int(workers), 32))
    if aggregate:
        return _aggregate_export(src_dir, out_dir, session_entries, api_key, workers, progress_cb,
                                 log_dir, should_cancel, on_session_done, response_weight,
                                 compression_weight, retain_raw_response, file_dirs=file_dirs)
    src, out = Path(src_dir).resolve(), Path(out_dir).resolve()
    if not src.is_dir():
        raise ValueError("Source directory does not exist")
    if out == src or out.is_relative_to(src):
        raise ValueError("Export output must not be inside the source directory")
    entries = [s for s in session_entries if not api_key or s.get("api_key") == api_key]
    stop, log = should_cancel or (lambda: False), progress_cb or (lambda message: None)
    result = {"total_sessions": len(entries), "total_files": 0, "errors": [], "results": [], "cancelled": False}
    if stop():
        result["cancelled"] = True
        return result
    namespace = digest([str(src), log_dir or src.name])[:24]
    manifest = out / "manifests" / (namespace + ".jsonl")
    manifest.parent.mkdir(parents=True, exist_ok=True)
    # Never replace an earlier export namespace, including a partial failed run.
    stream = manifest.open("x", encoding="utf-8")
    jobs, remaining, outputs = defaultdict(list), {}, defaultdict(list)
    errors, completed = [], set()
    for index, session in enumerate(entries):
        names = trace_names(session)
        remaining[index] = len(names)
        for name in names:
            jobs[name].append(index)

    def notify(index):
        if index in completed:
            return
        completed.add(index)
        if on_session_done:
            try:
                on_session_done()
            except Exception:
                pass

    for index, count in remaining.items():
        if count == 0:
            errors.append({"session_index": index, "error": "no_trace_files"})
            notify(index)
    log(f"Jiuwen per-request weighted export: {len(jobs)} unique requests, {len(entries)} export groups, workers={workers}")
    types = Counter()
    try:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {}
            for name, indexes in sorted(jobs.items()):
                if stop():
                    result["cancelled"] = True
                    break
                groups = []
                for index in indexes:
                    session = entries[index]
                    groups.append({"first_ts": session.get("first_ts", session.get("_key")),
                        "last_ts": session.get("last_ts"), "latest_file": session.get("latest_file"),
                        "trace_entries": [redact(t) for t in session.get("trace_list", []) or []
                                          if isinstance(t, dict) and t.get("filename") == name]})
                provenance = {"log_dir": log_dir or src.name, "export_session_groups": groups}
                futures[pool.submit(_one, src, out, namespace, name, provenance,
                                    response_weight, compression_weight, retain_raw_response)] = name
            for number, future in enumerate(as_completed(futures), 1):
                if stop():
                    result["cancelled"] = True
                    for pending in futures:
                        pending.cancel()
                if future.cancelled():
                    continue
                name = futures[future]
                try:
                    item = future.result()
                    stream.write(json.dumps(item, ensure_ascii=False) + "\n")
                    stream.flush()
                    types[item["trajectory_type"]] += 1
                    result["total_files"] += 1
                    for index in jobs[name]:
                        outputs[index].append(item["output_file"])
                except Exception as exc:
                    errors.append({"filename": name, "error_type": type(exc).__name__, "error": str(exc)})
                for index in jobs[name]:
                    remaining[index] -= 1
                    if remaining[index] == 0:
                        notify(index)
                if number % 100 == 0:
                    log(f"Exported {result['total_files']}/{len(jobs)} requests; errors={len(errors)}")
    finally:
        stream.close()
    for index, files in sorted(outputs.items()):
        session = entries[index]
        result["results"].append({"session": session.get("first_ts", session.get("_key", "")),
            "first_ts": session.get("first_ts"), "last_ts": session.get("last_ts"),
            "_key": session.get("_key", session.get("first_ts", "")), "latest_file": session.get("latest_file"),
            "log_dir": log_dir or src.name, "api_call_count": len(trace_names(session)),
            "request_files": sorted(files), "models": session.get("models", []),
            "training_eligibility": "pending_offline_task_association"})
    result["errors"] = errors
    result["trajectory_types"] = dict(types)
    result["manifest"] = str(manifest)
    write_json(out / "manifests" / (namespace + ".summary.json"), result)
    return result


def restore_tree(input_dir, output_dir):
    source, out = Path(input_dir).resolve(), Path(output_dir).resolve()
    if not source.is_dir():
        raise ValueError("Input directory does not exist")
    if out == source or out.is_relative_to(source):
        raise ValueError("Output must not be inside the input directory")
    out.mkdir(parents=True, exist_ok=False)
    errors, seen, count = [], set(), 0
    with (out / "restore_manifest.jsonl").open("w", encoding="utf-8") as manifest:
        for path in sorted(source.rglob("*.json")):
            try:
                with path.open(encoding="utf-8-sig") as stream:
                    record = json.load(stream)
                info = record.get("meta_info", {}).get("unique_info", {}) if isinstance(record, dict) else {}
                if info.get("schema") not in SUPPORTED_SCHEMAS:
                    continue
                restored = restore_document(record)
                name = info["source"]["filename"]
                request_id = record["id"]
                if not isinstance(request_id, str) or len(request_id) != 64 or any(c not in "0123456789abcdef" for c in request_id):
                    raise ValueError("Invalid request ID")
                if Path(name).name != name or any(c in name for c in ("/", "\\", ":")):
                    raise ValueError("Unsafe source filename")
                if request_id in seen:
                    continue
                target = out / request_id / name
                write_json(target, restored)
                manifest.write(json.dumps({"request_id": request_id, "weighted_file": str(path),
                    "merged_file": str(target), "candidate_weight": record["messages"][info["current_response_index"]]["weight"],
                    "trajectory_type": info["trajectory_type"], "source": info["source"]}, ensure_ascii=False) + "\n")
                seen.add(request_id)
                count += 1
            except (OSError, UnicodeError, ValueError, KeyError, TypeError) as exc:
                errors.append({"path": str(path), "error": str(exc)})
    summary = {"restored_requests": count, "errors": errors, "task_aggregation": "not_performed"}
    if count == 0:
        summary["errors"].append({"error": "No weighted request records were found"})
    write_json(out / "restore_summary.json", summary)
    return summary


def discover(root):
    grouped = {}
    for path in sorted(root.rglob("*.json")):
        if path.name.endswith(("-headers.json", "-res.json")):
            continue
        if path.name.endswith("-req.json"):
            grouped.setdefault(path.parent, []).append(path.name)
            continue
        try:
            with path.open(encoding="utf-8-sig") as stream:
                value = json.load(stream)
        except (OSError, UnicodeError, ValueError):
            # Include damaged files: the exporter must report rather than hide them.
            grouped.setdefault(path.parent, []).append(path.name)
            continue
        if isinstance(value, dict) and ("messages" in value or "req" in value):
            grouped.setdefault(path.parent, []).append(path.name)
    return grouped


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, action="append", required=True,
                        help="Repeat for all hourly exports; aggregate them together in one run")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--response-weight", type=float, default=None)
    parser.add_argument("--compression-weight", type=float, default=None)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--omit-raw-response", action="store_true")
    parser.add_argument("--per-request", action="store_true", help="Disable compact aggregation and historical reasoning backfill")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--restore", action="store_true", help="Restore the original selected raw snapshots; backfilled training messages remain in the export")
    args = parser.parse_args()
    roots = sorted({p.resolve() for p in args.input_dir})
    out = args.output_dir.resolve()
    if not roots or any(not p.is_dir() for p in roots):
        parser.error("All input directories must exist")
    if any(out == root or out.is_relative_to(root) for root in roots):
        parser.error("Output must not be inside an input directory")
    if args.restore:
        if len(roots) != 1:
            parser.error("--restore requires exactly one input directory")
        summary = restore_tree(roots[0], out)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 1 if summary["errors"] else 0
    out.mkdir(parents=True, exist_ok=False)
    groups = {}
    for root in roots:
        for directory, names in discover(root).items():
            groups.setdefault(directory, set()).update(names)
    if not args.per_request:
        entries, directories = [], {}
        for directory, names in sorted(groups.items()):
            directories[len(entries)] = directory
            entries.append({"first_ts": str(directory), "trace_list": [{"filename": name} for name in sorted(names)]})
        response_weight = validate_weight(os.getenv("JIUWEN_EXPORT_ASSISTANT_WEIGHT", "1") if args.response_weight is None else args.response_weight)
        configured = os.getenv("JIUWEN_EXPORT_COMPACTION_WEIGHT") if args.compression_weight is None else args.compression_weight
        compression_weight = response_weight if configured is None else validate_weight(configured)
        result = _aggregate_export(roots[0], out, entries, None, max(1, min(args.workers, 32)),
            (lambda message: print(message, flush=True)) if args.verbose else None,
            "offline:" + digest([str(root) for root in roots]), None, None, response_weight,
            compression_weight, not args.omit_raw_response, source_directories=directories)
        result.update(aggregation="maximal_proven_snapshot_branches_with_reasoning_backfill",
                      task_association="deferred", weight_scope="assistant_only_unique_generation_weights")
        write_json(out / "export_summary.json", result)
        print(json.dumps({k: result[k] for k in ("total_files", "input_requests", "aggregation_stats", "errors")}, ensure_ascii=False, indent=2))
        return 1 if result["errors"] else 0
    results, errors = [], []
    print(f"Selected {sum(len(names) for names in groups.values())} requests across {len(groups)} source directories", flush=True)
    for number, (directory, names) in enumerate(sorted(groups.items()), 1):
        session = {"first_ts": "offline-selection", "trace_list": [{"filename": name} for name in sorted(names)]}
        result = jiuwenswarm_export(str(directory), str(out), [session], workers=args.workers,
            progress_cb=(lambda message: print(message, flush=True)) if args.verbose else None, log_dir=str(directory),
            response_weight=args.response_weight, compression_weight=args.compression_weight,
            retain_raw_response=not args.omit_raw_response, aggregate=not args.per_request)
        results.append({"source_dir": str(directory), "files": result["total_files"],
                        "manifest": result["manifest"], "trajectory_types": result["trajectory_types"]})
        errors.extend(result["errors"])
        if number % 100 == 0 or number == len(groups):
            print(f"Processed {number}/{len(groups)} source directories; errors={len(errors)}", flush=True)
    summary = {"total_files": sum(r["files"] for r in results), "errors": errors,
               "sources": results, "aggregation": "none" if args.per_request else "maximal_proven_snapshot_branches_with_reasoning_backfill", "task_association": "deferred",
               "weight_scope": "assistant_only_candidate_weights"}
    (out / "export_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"total_files": summary["total_files"], "error_count": len(errors),
                      "source_directories": len(results), "summary": str(out / "export_summary.json")}, ensure_ascii=False))
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())