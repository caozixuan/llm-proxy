"""请求 index.jsonl 写入和计数。"""

import asyncio
import json
import os
import threading
import time

from utils.log_paths import build_index_path
from utils.message_common import build_chain_key, compute_q1_hash, get_first_user_text, q1_hash_from_text


# ---------------------------------------------------------------------------
# 多 worker 计数：内存累加只反映本进程处理的请求，多 worker 下会偏小。
# 根 index.jsonl 是所有 worker 共享 append 的，读回文件才是全局真值。
# 用「增量读尾部 + 短 TTL 缓存」实现：记录已读到的字节偏移，每次只解析新追加
# 的行，避免大文件全量重扫。跨进程各自维护偏移，但都读同一份文件 → 结果一致。
# ---------------------------------------------------------------------------

_COUNTS_TTL = 2.0  # 秒；/metrics/index-stats 轮询频率远低于此，足够

# ---------------------------------------------------------------------------
# 写入通知：index.jsonl 追加成功后回调，供 new-api 增量索引（见
# utils/newapi_watcher）在秒级内把新行折叠进 index.db，而不是等小时级回填。
# 回调在 append_index_*_async 的线程池线程里执行，不阻塞事件循环；异常一律吞掉，
# 绝不能让「索引通知失败」影响请求响应或记录落盘。
# ---------------------------------------------------------------------------
_index_hooks = []
_hooks_guard = threading.Lock()


def register_index_hook(fn) -> None:
    with _hooks_guard:
        if fn not in _index_hooks:
            _index_hooks.append(fn)


def _fire_index_hooks(index_file: str, entry: dict) -> None:
    if not _index_hooks:
        return
    with _hooks_guard:
        hooks = list(_index_hooks)
    for fn in hooks:
        try:
            fn(index_file, entry)
        except Exception:  # noqa: BLE001 — 通知失败不影响写入
            import logging
            logging.debug("index hook 失败", exc_info=True)

_agg_lock = threading.Lock()
_agg_path: str = ""
_agg_offset: int = 0
_agg_first: int = 0
_agg_total: int = 0
_agg_valid: int = 0
_agg_ts: float = 0.0


def _aggregate_from_disk(index_path: str) -> tuple:
    """增量读取 index_path 新追加的行，累加到全局计数并返回快照。"""
    global _agg_path, _agg_offset, _agg_first, _agg_total, _agg_valid
    # 文件路径变化（或首次），重置偏移
    if index_path != _agg_path:
        _agg_path = index_path
        _agg_offset = 0
        _agg_first = _agg_total = _agg_valid = 0
    try:
        size = os.path.getsize(index_path)
    except OSError:
        return _agg_first, _agg_total, _agg_valid
    # 文件被截断/轮转（变小）→ 从头重读
    if size < _agg_offset:
        _agg_offset = 0
        _agg_first = _agg_total = _agg_valid = 0
    if size == _agg_offset:
        return _agg_first, _agg_total, _agg_valid
    try:
        with open(index_path, "r", encoding="utf-8") as f:
            f.seek(_agg_offset)
            data = f.read()
            _agg_offset = f.tell()
    except OSError:
        return _agg_first, _agg_total, _agg_valid
    # 只处理以换行结尾的完整行；不完整的尾部留待下次（回退偏移）
    if data and not data.endswith("\n"):
        last_nl = data.rfind("\n")
        if last_nl == -1:
            _agg_offset -= len(data.encode("utf-8"))
            return _agg_first, _agg_total, _agg_valid
        trailing = data[last_nl + 1:]
        _agg_offset -= len(trailing.encode("utf-8"))
        data = data[: last_nl + 1]
    for line in data.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        _agg_first += 1
        _agg_total += entry.get("total_attempts", 1)
        if entry.get("valid") or entry.get("success"):
            _agg_valid += 1
    return _agg_first, _agg_total, _agg_valid


def get_index_counts() -> tuple:
    """全局首次/总体/有效计数。多 worker 下读磁盘聚合，带短 TTL 缓存。

    未初始化根路径（load_index 尚未调用）时退回进程内内存计数。
    """
    global _agg_ts
    if not _agg_path and not _INDEX_ROOT:
        return _first_count, _total_count, _valid_count
    root = _agg_path or _INDEX_ROOT
    now = time.monotonic()
    with _agg_lock:
        if now - _agg_ts >= _COUNTS_TTL:
            _aggregate_from_disk(root)
            _agg_ts = now
        return _agg_first, _agg_total, _agg_valid


_first_count: int = 0
_total_count: int = 0
_valid_count: int = 0
_INDEX_ROOT: str = ""


def index_path_for_req_file(req_file: str, logs_dir: str) -> str:
    req_path = os.path.normpath(req_file)
    main_root = os.path.normpath(logs_dir)
    if req_path == main_root or req_path.startswith(main_root + os.sep):
        return build_index_path(logs_dir)
    return build_index_path(os.path.dirname(req_file) or ".")


def load_index(logs_dir: str):
    global _first_count, _total_count, _valid_count, _INDEX_ROOT
    root_index = build_index_path(logs_dir)
    _INDEX_ROOT = root_index  # 供 get_index_counts 多 worker 磁盘聚合定位根 index
    if not os.path.exists(root_index):
        return
    with open(root_index, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
                _first_count += 1
                _total_count += entry.get("total_attempts", 1)
                if entry.get("valid") or entry.get("success"):
                    _valid_count += 1
            except json.JSONDecodeError:
                pass


def _extract_q1_preview(messages, kind="anthropic"):
    return get_first_user_text(messages or [])[:100]


def _extract_chain_key_responses(input_data) -> str:
    if isinstance(input_data, str):
        return input_data[:500]
    if isinstance(input_data, list):
        for item in input_data:
            if not isinstance(item, dict):
                continue
            if item.get("role") == "user":
                content = item.get("content", "")
                if isinstance(content, str):
                    return content[:500]
                if isinstance(content, list):
                    texts = []
                    for part in content:
                        if isinstance(part, dict) and part.get("type") == "input_text":
                            texts.append(part.get("text", ""))
                    return "|".join(texts)[:500]
    return ""


def _extract_q1_preview_responses(input_data) -> str:
    if isinstance(input_data, str):
        return input_data[:100]
    if isinstance(input_data, list):
        for item in input_data:
            if not isinstance(item, dict):
                continue
            if item.get("role") == "user":
                content = item.get("content", "")
                if isinstance(content, str):
                    return content[:100]
                if isinstance(content, list):
                    for part in content:
                        if isinstance(part, dict) and part.get("type") == "input_text":
                            return part.get("text", "")[:100]
    return ""


def _extract_q1_full_responses(input_data) -> str:
    """responses provider 的完整（未截断）首条 user 文本，供 q1_hash 使用。"""
    if isinstance(input_data, str):
        return input_data
    if isinstance(input_data, list):
        for item in input_data:
            if not isinstance(item, dict):
                continue
            if item.get("role") == "user":
                content = item.get("content", "")
                if isinstance(content, str):
                    return content
                if isinstance(content, list):
                    for part in content:
                        if isinstance(part, dict) and part.get("type") == "input_text":
                            return part.get("text", "")
    return ""


def append_index(ts: str, req_file: str, provider: str, logs_dir: str, model: str = "",
                 tok_in: int = 0, tok_out: int = 0, cache_in: int = 0,
                 success: bool = True,
                 api_key: str = "", chain_key: str = "", q1_preview: str = "",
                 q1_hash: str = "",
                 total_attempts: int = 1, start_turn: int = 0,
                 channel_key: str = "", usage: dict = None,
                 debug_file: str = "",
                 msg_count: int = 0, user_turns: int = 0,
                 timing: dict = None):
    global _first_count, _total_count, _valid_count
    entry = {
        "ts": ts,
        "req_file": req_file,
        "provider": provider,
        "model": model,
        "tok_in": tok_in,
        "tok_out": tok_out,
        "cache_in": cache_in,
        "success": success,
        "api_key": api_key,
        "chain_key": chain_key,
        "q1_preview": q1_preview,
        "q1_hash": q1_hash,
        "total_attempts": total_attempts,
        "retried": total_attempts > 1,
        "start_turn": start_turn,
        "channel_key": channel_key,
        "usage": usage or {},
        "msg_count": msg_count,
        "user_turns": user_turns,
    }
    # 分段耗时（毫秒）：t_connect / t_ttfb / t_upstream / t_total —— 只在有值时写入
    if timing:
        entry["timing"] = {k: v for k, v in timing.items() if v is not None}
    if debug_file:
        entry["debug_file"] = debug_file
    index_file = index_path_for_req_file(req_file, logs_dir)
    os.makedirs(os.path.dirname(index_file) or ".", exist_ok=True)
    with open(index_file, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    _first_count += 1
    _total_count += total_attempts
    if success:
        _valid_count += 1
    _fire_index_hooks(index_file, entry)


def append_index_anthropic(ts, req_path, total_attempts, valid, logs_dir, model="", tok_in=0, tok_out=0, cache_in=0, api_key="", messages=None, channel_key="", usage=None, debug_file="", timing=None):
    msgs = messages or []
    from utils.message_common import count_real_user_turns
    append_index(
        ts, req_path, provider="anthropic", logs_dir=logs_dir, model=model,
        tok_in=tok_in, tok_out=tok_out, cache_in=cache_in, success=valid,
        api_key=api_key,
        chain_key=build_chain_key(msgs),
        q1_preview=_extract_q1_preview(msgs),
        q1_hash=compute_q1_hash(msgs),
        total_attempts=total_attempts,
        start_turn=get_first_user_text(msgs, return_index=True)[1],
        channel_key=channel_key,
        usage=usage,
        debug_file=debug_file,
        msg_count=len(msgs),
        user_turns=count_real_user_turns(msgs),
        timing=timing,
    )


def append_index_openai(ts, req_path, logs_dir, model="", tok_in=0, tok_out=0, success=True, api_key="", messages=None, channel_key="", usage=None, debug_file="", timing=None):
    msgs = messages or []
    from utils.message_common import count_real_user_turns
    append_index(
        ts, req_path, provider="openai", logs_dir=logs_dir, model=model,
        tok_in=tok_in, tok_out=tok_out, success=success,
        api_key=api_key,
        chain_key=build_chain_key(msgs),
        q1_preview=_extract_q1_preview(msgs),
        q1_hash=compute_q1_hash(msgs),
        channel_key=channel_key,
        usage=usage,
        debug_file=debug_file,
        msg_count=len(msgs),
        user_turns=count_real_user_turns(msgs),
        timing=timing,
    )


def append_index_responses(ts, req_path, logs_dir, model="", tok_in=0, tok_out=0, success=True, api_key="", input_data=None, channel_key="", usage=None, debug_file="", timing=None):
    append_index(
        ts, req_path, provider="responses", logs_dir=logs_dir, model=model,
        tok_in=tok_in, tok_out=tok_out, success=success,
        api_key=api_key,
        chain_key=_extract_chain_key_responses(input_data),
        q1_preview=_extract_q1_preview_responses(input_data),
        q1_hash=q1_hash_from_text(_extract_q1_full_responses(input_data)),
        channel_key=channel_key,
        usage=usage,
        debug_file=debug_file,
        timing=timing,
    )


# ---------------------------------------------------------------------------
# 异步包装：index 写入含 chain_key/q1_hash 等 CPU 计算 + 文件 append，
# 在事件循环上执行会阻塞其它并发请求。用 to_thread 丢到线程池。
# 调用点用 `await append_index_*_async(...)`。
# ---------------------------------------------------------------------------

async def append_index_anthropic_async(*args, **kwargs):
    try:
        return await asyncio.to_thread(append_index_anthropic, *args, **kwargs)
    except Exception as ex:  # 记录失败但不影响响应
        import logging
        logging.warning(f"append_index_anthropic_async failed: {ex}")


async def append_index_openai_async(*args, **kwargs):
    try:
        return await asyncio.to_thread(append_index_openai, *args, **kwargs)
    except Exception as ex:
        import logging
        logging.warning(f"append_index_openai_async failed: {ex}")


async def append_index_responses_async(*args, **kwargs):
    try:
        return await asyncio.to_thread(append_index_responses, *args, **kwargs)
    except Exception as ex:
        import logging
        logging.warning(f"append_index_responses_async failed: {ex}")
