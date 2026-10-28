"""
/logs/* 路由：列表、聚合、单文件读取（Anthropic + OpenAI）
优先使用 index.jsonl 的最近窗口；缺失时降级为目录扫描。
"""

import json
import logging
import os
import threading
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse

from utils.log_paths import build_index_path, get_log_dir
from utils.message_common import (
    build_chain_key,
    compute_q1_hash,
    count_real_user_turns,
    extract_res_usage,
    get_first_user_text,
    get_text_from_content,
    load_json_safe,
    parse_openai_streaming_response,
    parse_streaming_response_content,
)
from utils.q1_index import get_effective_q1, should_update_q1, update_q1
import utils.session_store as _ss
import utils.newapi_index_db as nidb
from utils.timing import StageTimer

_CACHE_LOCK = threading.Lock()
_LOG_CACHE: Dict[Tuple[str, str], Dict[str, Any]] = {}


def _resolve_req_path(root: Path, req_file: str) -> Optional[Path]:
    raw = (req_file or "").strip()
    if not raw:
        return None

    rf = Path(raw)
    candidates = []
    if rf.is_absolute():
        candidates.append(rf.resolve())
    else:
        candidates.append((root.parent / rf).resolve())
        candidates.append((root / rf.name).resolve())
        candidates.append(rf.resolve())

    for path in candidates:
        if path.is_file():
            return path
    return None


def _load_json(path: Path) -> Optional[Dict[str, Any]]:
    return load_json_safe(path)


def _format_time(ts: str) -> str:
    return ts.replace("_", " ", 1).replace("_", ".").replace("-", ":") if ts else ""


def _read_new_index_entries(
    index_path: Path, root: Path, byte_offset: int
) -> Tuple[List[Dict[str, Any]], int]:
    """从 byte_offset 处增量读取 index.jsonl 新增行。
    返回 (rows, new_byte_offset)。
    文件被截断时返回 ([], 0) 表示需要全量重建。
    """
    try:
        file_size = index_path.stat().st_size
    except OSError:
        return [], 0

    if byte_offset > file_size:
        return [], 0

    if byte_offset == file_size:
        return [], byte_offset

    rows: List[Dict[str, Any]] = []
    try:
        with index_path.open("rb") as f:
            f.seek(byte_offset)
            raw_data = f.read()
            new_offset = f.tell()
    except OSError:
        return [], byte_offset

    for raw_line in raw_data.split(b"\n"):
        line = raw_line.decode("utf-8", errors="ignore").strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        req_path = _resolve_req_path(root, str(entry.get("req_file", "")))
        if req_path is None:
            continue
        rows.append({"entry": entry, "req_path": req_path})

    return rows, new_offset


def _collect_req_files(root: Path) -> List[Path]:
    return sorted(root.glob("*-req.json"))


def _extract_anthropic_res_content(res_path: Path):
    data = _load_json(res_path)
    if not data:
        return None

    rtype = data.get("type")
    if rtype == "anthropic_passthrough_sse_capture":
        chunks = data.get("chunks", [])
        return parse_streaming_response_content(
            [c for c in chunks if c.get("type") != "anthropic_passthrough_sse_meta"]
        )

    if isinstance(data.get("json"), dict):
        msg = data["json"]
        if msg.get("content") is not None:
            return msg["content"]
    return None


def _extract_openai_res_content(res_path: Path):
    data = _load_json(res_path)
    if not data:
        return None

    if data.get("type") == "openai_passthrough_sse_capture":
        chunks = data.get("chunks", [])
        if not chunks:
            return None
        msg = parse_openai_streaming_response(chunks)
        if msg.get("role") == "assistant":
            return msg
        return None

    msg = data.get("json", {}).get("choices", [{}])[0].get("message")
    if isinstance(msg, dict) and msg.get("role") == "assistant":
        return msg
    return None


def _get_text_from_content(content) -> str:
    return get_text_from_content(content)


def _build_state(root_dir: str) -> Dict[str, Any]:
    return {
        "root_dir": root_dir,
        "index_path": build_index_path(root_dir),
        "initialized": False,
        "line_count": 0,
        "byte_offset": 0,
        "known_keys": set(),
        "_chain_map": {},  # lookup_key -> session_key，启动时从 DB 重建
        "_last_refresh_ts": 0.0,
    }


def _load_state_from_db(state: Dict[str, Any]) -> bool:
    """从 DB 恢复内存 chain_map 和 index 进度。"""
    root_dir = state["root_dir"]
    try:
        byte_offset, line_count = _ss.get_progress(root_dir)
        state["byte_offset"] = byte_offset
        state["line_count"] = line_count

        chain_map = _ss.get_all_chain_index(root_dir)
        state["_chain_map"] = chain_map

        # known_keys 从 sessions 表重建
        sessions = _ss.list_sessions(root_dir)
        state["known_keys"] = {s.get("api_key", "") for s in sessions if s.get("api_key")}

        return bool(byte_offset > 0 or chain_map)
    except Exception:
        return False


def _state_key(kind: str, root_dir: str) -> Tuple[str, str]:
    return kind, os.path.normpath(root_dir)


def _state(kind: str, root_dir: str) -> Dict[str, Any]:
    key = _state_key(kind, root_dir)
    state = _LOG_CACHE.get(key)
    if state is None:
        state = _build_state(root_dir)
        _LOG_CACHE[key] = state
    return state


def _process_req_row(kind: str, state: Dict[str, Any], req_path: Path, index_entry: Optional[Dict[str, Any]] = None) -> bool:
    _noise_prefixes = ("(session bootstrap)",)
    ie = index_entry or {}

    # 快速路径：index_entry 已含 msg_count/user_turns/chain_key，跳过读 req.json
    _idx_msg_count = ie.get("msg_count", 0)
    _idx_user_turns = ie.get("user_turns")
    _idx_chain_key = ie.get("chain_key", "")
    _idx_q1_hash = ie.get("q1_hash", "")
    _needs_req = (
        not _idx_msg_count  # 旧格式 index 没有 msg_count
        or not _idx_chain_key
        or not _idx_q1_hash  # 旧格式 index 没有 q1_hash，需读 req.json 现算
        or (_idx_chain_key and any(_idx_chain_key.startswith(p) for p in _noise_prefixes))
        or (_idx_user_turns is None)
    )

    messages = None
    data = None
    if _needs_req:
        data = _load_json(req_path)
        if not data:
            return False
        messages = data.get("messages")
        if not isinstance(messages, list):
            return False

    root_dir = state["root_dir"]
    filename = req_path.name
    ts = str(ie.get("ts") or filename.replace("-req.json", ""))
    model = str((data.get("model", "") if data else "") or ie.get("model", "") or "")
    api_key = str(ie.get("api_key", "") or "")

    if _idx_chain_key and not any(_idx_chain_key.startswith(p) for p in _noise_prefixes):
        chain_key = _idx_chain_key
    else:
        chain_key = build_chain_key(messages)

    # 聚合键：优先用 index 里的 q1_hash（完整 Q1 的 md5）；旧 index 无此字段时
    # 从 messages 现算，保证不误聚合。messages 在 _needs_req 分支已加载。
    if _idx_q1_hash:
        q1_hash = _idx_q1_hash
    else:
        q1_hash = compute_q1_hash(messages) if messages is not None else ""

    lookup_key = f"{api_key}||{q1_hash}"
    q1_preview = ie.get("q1_preview", "")
    if q1_preview and any(q1_preview.startswith(p) for p in _noise_prefixes):
        q1_preview = get_first_user_text(messages)[:200] if messages is not None else ""

    if _idx_msg_count and not _needs_req:
        message_count = _idx_msg_count
        res_path = req_path.with_name(filename.replace("-req.json", "-res.json"))
        has_res = res_path.is_file()
        full_message_count = message_count + (1 if has_res else 0)
    else:
        message_count = len(messages)
        res_path = req_path.with_name(filename.replace("-req.json", "-res.json"))
        has_res = res_path.is_file()
        full_message_count = message_count + (1 if has_res else 0)

    trace_entry: Dict[str, Any] = {
        "filename": filename,
        "model": model,
        "msg_count": full_message_count,
        "ts": ts,
    }
    if index_entry:
        if not ie.get("success", True):
            trace_entry["success"] = False
            trace_entry["total_attempts"] = ie.get("total_attempts", 1)
        if ie.get("debug_file"):
            trace_entry["debug_file"] = ie["debug_file"]

    if _idx_user_turns is not None and not _needs_req:
        real_user_turns = _idx_user_turns
    else:
        real_user_turns = count_real_user_turns(messages)

    # 用 _CACHE_LOCK 短暂读取内存 chain_map（微秒级）
    with _CACHE_LOCK:
        state["known_keys"].add(api_key)
        session_key = state["_chain_map"].get(lookup_key)

    # DB 读在锁外（不阻塞其他请求）
    session: Optional[Dict[str, Any]] = None
    if session_key:
        session = _ss.get_session(session_key)

    if session is not None and real_user_turns <= 1 and \
       real_user_turns < session.get("_max_real_turns", 1):
        # 新会话检测：需要找一个未被占用的 suffix key
        with _CACHE_LOCK:
            suffix = 1
            new_lookup = f"{lookup_key}##session_{suffix}"
            while new_lookup in state["_chain_map"]:
                suffix += 1
                new_lookup = f"{lookup_key}##session_{suffix}"
        lookup_key = new_lookup
        session = None
        session_key = None

    if session is None:
        session_key = ts
        new_session = {
            "q1": q1_preview or chain_key[:200],
            "models": [model] if model else [],
            "latest_file": filename,
            "msg_count": full_message_count,
            "api_key": api_key,
            "first_ts": ts,
            "last_ts": ts,
            "_best_req_count": message_count,
            "_max_real_turns": real_user_turns,
        }
        # DB 写在锁外
        _ss.create_session(root_dir, session_key, new_session)
        _ss.append_trace(root_dir, session_key, trace_entry)
        _ss.set_chain_index(root_dir, lookup_key, session_key)
        # 内存 chain_map 更新需要锁
        with _CACHE_LOCK:
            state["_chain_map"][lookup_key] = session_key
    else:
        models = session.get("models", [])
        if model and model not in models:
            models = models + [model]

        max_real_turns = max(real_user_turns, session.get("_max_real_turns", 0))
        best_req_count = session.get("_best_req_count", 0)
        latest_file = session.get("latest_file", "")
        msg_count = session.get("msg_count", 0)

        if message_count > best_req_count or \
           (message_count == best_req_count and has_res):
            latest_file = filename
            msg_count = full_message_count
            best_req_count = message_count

        updated = {
            "last_ts": ts,
            "models": models,
            "latest_file": latest_file,
            "msg_count": msg_count,
            "_max_real_turns": max_real_turns,
            "_best_req_count": best_req_count,
        }
        # DB 写在锁外
        _ss.update_session(session_key, updated)
        _ss.append_trace(root_dir, session_key, trace_entry)

    return True

    return True


_REFRESH_TTL = 10  # 秒：已初始化的目录在此时间内跳过重复刷新
_INIT_LOCK: Dict[str, threading.Lock] = {}  # root_dir -> 首次初始化锁
_INIT_LOCK_GUARD = threading.Lock()

_FMT_CACHE: Dict[str, str] = {}  # root_dir -> 'native'/'newapi'（缓存 detect_format）


def _root_format(root_dir: str) -> str:
    key = os.path.normpath(root_dir)
    fmt = _FMT_CACHE.get(key)
    if fmt is None:
        try:
            from utils.log_scan import detect_format
            fmt = detect_format(root_dir)
        except Exception:
            fmt = "native"
        _FMT_CACHE[key] = fmt
    return fmt


def _get_init_lock(root_dir: str) -> threading.Lock:
    with _INIT_LOCK_GUARD:
        if root_dir not in _INIT_LOCK:
            _INIT_LOCK[root_dir] = threading.Lock()
        return _INIT_LOCK[root_dir]


def _refresh_state(kind: str, root_dir: str, force: bool = False) -> None:
    # new-api 叶子：视图只读 index.db。构建/补 meta 一律经「日志管理」批量路径
    # （dispatcher + leader_lock 单写者）；此处不写库、不解析合并文件，保证视图请求恒亚秒。
    # 待构建/待补 meta 的信息由 _attach_backfill_status 以徽标形式带给前端。
    if _root_format(root_dir) == "newapi":
        return

    # 快速路径：TTL 内直接返回，只读内存变量，不需要全局锁
    with _CACHE_LOCK:
        state = _state(kind, root_dir)
        if not force and state["initialized"] and time.time() - state["_last_refresh_ts"] < _REFRESH_TTL:
            return
        already_initialized = state["initialized"]

    root = Path(root_dir)
    index_path = Path(state["index_path"])

    # Phase 1：首次初始化 —— 用 per-root 锁防止并发重复初始化，主锁不参与
    if not already_initialized:
        init_lock = _get_init_lock(root_dir)
        with init_lock:
            # 二次检查，防止排队的第二个请求重复初始化
            with _CACHE_LOCK:
                if state["initialized"]:
                    return
            if not _load_state_from_db(state):
                state["_chain_map"].clear()
                state["known_keys"].clear()
                state["byte_offset"] = 0
                state["line_count"] = 0

    # Phase 2：锁外读 index.jsonl 新增内容（纯文件 I/O，不竞争）
    with _CACHE_LOCK:
        current_offset = state["byte_offset"]

    if index_path.is_file():
        rows, new_offset = _read_new_index_entries(index_path, root, current_offset)

        if new_offset == 0 and current_offset > 0:
            # 文件被截断/轮转 — 全量重建，需要锁保护内存清理
            with _CACHE_LOCK:
                state["_chain_map"].clear()
                state["known_keys"].clear()
                state["byte_offset"] = 0
                state["line_count"] = 0
            rows, new_offset = _read_new_index_entries(index_path, root, 0)

        if rows:
            # 锁外处理每行：读 req.json（文件 I/O）+ 写 DB
            # chain_map 读写在 _process_req_row 内部用 _CACHE_LOCK 短暂保护
            for row in rows:
                _process_req_row(kind, state, row["req_path"], row["entry"])
            with _CACHE_LOCK:
                state["line_count"] += len(rows)
                state["byte_offset"] = new_offset
            _ss.set_progress(root_dir, new_offset, state["line_count"])
        elif new_offset != current_offset:
            with _CACHE_LOCK:
                state["byte_offset"] = new_offset
            _ss.set_progress(root_dir, new_offset, state["line_count"])
    else:
        if not already_initialized:
            for req_path in _collect_req_files(root):
                _process_req_row(kind, state, req_path)

    with _CACHE_LOCK:
        state["initialized"] = True
        state["_last_refresh_ts"] = time.time()


def _attach_backfill_status(payload: Dict[str, Any], root_dir: str) -> None:
    """new-api 叶子给 payload 带上 index.db 构建状态，供前端区分「未构建 / 待补 meta /
    有新增待重建 / 就绪」并给出「去日志管理批量构建」的提示。native 目录不附带此字段。"""
    if _root_format(root_dir) != "newapi":
        return
    try:
        st = nidb.status(root_dir)
        # index.jsonl 是否有超出已摄取偏移的新增行（stale：已构建但落后于最新）
        stale = st.get("built", False) and nidb.needs_build(root_dir)
        payload["backfill"] = {
            "format": "newapi",
            "built": st.get("built", False),
            "ingested": st.get("ingested", 0),
            "pending": st.get("pending", 0),
            "sessions": st.get("sessions", 0),
            "stale": bool(stale),
        }
    except Exception:
        pass


class _NidbBackend:
    """把 newapi 叶子 index.db 适配成 session_store 同名读 API，供 payload 逻辑无差别复用。

    session_store 是「单库按 session_key 全局寻址」，故其 get_traces_batch 只收 session_keys；
    index.db 是「每叶子一库」，故这里把 root_dir 记在实例上，get_traces_batch 内部带上叶子路径。
    """

    def __init__(self, root_dir: str):
        self._root = root_dir

    def list_sessions(self, root_dir, api_key="", model="", min_msg_count=0, offset=0, limit=0):
        return nidb.list_sessions(root_dir, api_key=api_key, model=model,
                                  min_msg_count=min_msg_count, offset=offset, limit=limit)

    def count_sessions(self, root_dir, api_key="", model="", min_msg_count=0):
        return nidb.count_sessions(root_dir, api_key=api_key, model=model, min_msg_count=min_msg_count)

    def get_traces_batch(self, session_keys):
        return nidb.get_traces_batch(self._root, session_keys)

    def get_trace_counts(self, session_keys):
        return nidb.get_trace_counts(self._root, session_keys)

    def get_known_models(self, root_dir):
        # 优先用 token_stats_json 缓存（免全表 json_each），缺失才回退 SQL 扫描。
        cached = nidb.get_known_models_cached(root_dir)
        if cached is not None:
            return cached
        return nidb.get_known_models(root_dir)


def _read_backend(root_dir: str):
    """按格式选读后端：newapi → 叶子 index.db；native → 全局 session_cache.db（_ss）。"""
    if _root_format(root_dir) == "newapi":
        return _NidbBackend(root_dir)
    return _ss


def _payload_known_keys(kind: str, root_dir: str) -> Tuple[List[str], float]:
    """返回 (known_keys, last_refresh_ts)。newapi 从 index.db 取，native 从内存 state 取。"""
    if _root_format(root_dir) == "newapi":
        try:
            keys = nidb.get_known_keys(root_dir)
            ts = float(nidb.status(root_dir).get("updated_at", 0) or 0)
        except Exception:
            keys, ts = [], 0.0
        return keys, ts
    with _CACHE_LOCK:
        current_state = _state(kind, root_dir)
        return sorted(current_state["known_keys"]), current_state.get("_last_refresh_ts", 0)


def _list_payload(kind: str, root_dir: str, min_messages: int, offset: int = 0, limit: int = 50, api_key: str = "", refresh: bool = False, model: str = "") -> Dict[str, Any]:
    """单文件（trace 级）列表：分页在 SQL 里下推。

    旧实现把整叶的 session 与 trace 全部拉回内存再排序切片——请求只要 50 条，
    却要读 22w 条 trace（同叶实测 38s，冷态更高）。现在由后端直接
    JOIN + ORDER BY filename DESC + LIMIT，只取需要的那一页。
    排序与过滤口径与旧实现逐条一致（已用 4 种过滤组合对拍验证）。
    """
    _refresh_state(kind, root_dir, force=refresh)
    backend = _read_backend(root_dir)

    if _root_format(root_dir) == "newapi":
        page = nidb.list_files_page(root_dir, min_msg_count=min_messages, api_key=api_key,
                                    model=model, offset=offset, limit=limit)
        items, total = page["items"], page["total"]
    else:
        page = _ss.list_files_page(root_dir, min_msg_count=min_messages, api_key=api_key,
                                   model=model, offset=offset, limit=limit)
        items, total = page["items"], page["total"]

    known_models = backend.get_known_models(root_dir)
    known_keys, last_ts = _payload_known_keys(kind, root_dir)

    payload = {"items": items, "total": total, "known_keys": known_keys, "known_models": known_models, "last_refresh_ts": last_ts}
    _attach_backfill_status(payload, root_dir)
    return payload


def _content_contains_keyword(content, kw: str) -> bool:
    if isinstance(content, str):
        return kw in content.lower()
    if isinstance(content, list):
        for block in content:
            if isinstance(block, dict):
                text = block.get("text", "")
                if isinstance(text, str) and kw in text.lower():
                    return True
    return False


def _match_messages_content(root_dir: str, filename: str, keyword: str, newapi: bool = False) -> bool:
    req_path = Path(root_dir) / filename
    if not req_path.is_file():
        return False
    kw = keyword.lower()
    # new-api：合并单文件（键 req/resp/up_req/up_resp，无顶层 messages、无 -res.json 兄弟文件）。
    # 直接对整份文件字节做子串扫描——覆盖请求+响应全部内容，免 json 解析与 utf-8 解码（快得多）。
    # 关键词含 ASCII 字母时才做大小写折叠（bytes.lower 只影响 ASCII 字节，多字节 UTF-8 不受影响，
    # 故中文关键词走零拷贝的直接字节查找）。
    if newapi:
        try:
            kwb = keyword.encode("utf-8", "ignore")
            fold = any(("a" <= ch.lower() <= "z") for ch in keyword if ch.isascii())
            with open(req_path, "rb") as f:
                data = f.read()
            return (kwb.lower() in data.lower()) if fold else (kwb in data)
        except Exception:
            return False
    try:
        with open(req_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        messages = data.get("messages")
        if isinstance(messages, list):
            for msg in messages:
                if _content_contains_keyword(msg.get("content", ""), kw):
                    return True
        res_path = req_path.with_name(filename.replace("-req.json", "-res.json"))
        res_content = _extract_anthropic_res_content(res_path)
        if res_content is not None and _content_contains_keyword(res_content, kw):
            return True
        if res_content is None:
            openai_content = _extract_openai_res_content(res_path)
            if isinstance(openai_content, dict):
                if _content_contains_keyword(openai_content.get("content", ""), kw):
                    return True
        return False
    except Exception:
        return False


def _filter_sessions_by_content(root_dir: str, sessions: List[dict], keyword: str, newapi: bool) -> List[dict]:
    """并行对每个 session 的 latest_file 做内容匹配，返回命中的 session。

    单线程逐个 open+json.load 数千个合并文件会长时间卡住（前端一直转圈）；这里用线程池并发，
    I/O 等待可重叠，配合 newapi 的免解析子串扫描，把「深度搜索」从数分钟降到秒级。
    """
    files = [(i, s.get("latest_file", "")) for i, s in enumerate(sessions)]
    workers = min(32, (os.cpu_count() or 4) * 4)

    def _hit(item):
        i, fn = item
        return i if (fn and _match_messages_content(root_dir, fn, keyword, newapi)) else -1

    matched_idx = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for r in ex.map(_hit, files):
            if r >= 0:
                matched_idx.append(r)
    matched_idx.sort()
    return [sessions[i] for i in matched_idx]


def _aggregate_payload(kind: str, root_dir: str, min_messages: int, offset: int = 0, limit: int = 50, api_key: str = "", refresh: bool = False, model: str = "", search: str = "", q1search: str = "") -> Dict[str, Any]:
    _refresh_state(kind, root_dir, force=refresh)
    backend = _read_backend(root_dir)

    search = (search or "").strip()
    q1search = (q1search or "").strip()

    if search:
        # search 需要全量加载再过滤（无法下推 SQL），分页在过滤后处理。
        # 内容扫描并行化 + newapi 免解析，避免前端长时间转圈。
        sessions = backend.list_sessions(root_dir, api_key=api_key, model=model, min_msg_count=min_messages)
        is_newapi = _root_format(root_dir) == "newapi"
        sessions = _filter_sessions_by_content(root_dir, sessions, search, is_newapi)
        total = len(sessions)
        paged = sessions[offset:offset + limit] if limit > 0 else sessions[offset:]
    elif q1search:
        # 首句过滤：只匹配 session 的 q1（内存过滤，不读合并文件），覆盖全部会话后再分页。
        needle = q1search.lower()
        sessions = backend.list_sessions(root_dir, api_key=api_key, model=model, min_msg_count=min_messages)
        sessions = [s for s in sessions if needle in (s.get("q1", "") or "").lower()]
        total = len(sessions)
        paged = sessions[offset:offset + limit] if limit > 0 else sessions[offset:]
    else:
        # 无 search：COUNT + 分页完全下推 SQL
        total = backend.count_sessions(root_dir, api_key=api_key, model=model, min_msg_count=min_messages)
        paged = backend.list_sessions(root_dir, api_key=api_key, model=model, min_msg_count=min_messages, offset=offset, limit=limit if limit > 0 else 0)

    known_models = backend.get_known_models(root_dir)
    # 列表页只用 file_count / has_failure，不需要 trace 明细（明细由前端点开会话时
    # 通过 /logs/session-traces 按需取）。这里复用 _build_agg_items 的 COUNT 路径。
    items = _build_agg_items([(s, root_dir) for s in paged], with_traces=False)

    known_keys, last_refresh_ts = _payload_known_keys(kind, root_dir)

    agg_payload = {"items": items, "total": total, "known_keys": known_keys, "known_models": known_models, "last_refresh_ts": last_refresh_ts}
    _attach_backfill_status(agg_payload, root_dir)
    return agg_payload


def _aggregate_all_payload(kind: str, env_dir: str, min_messages: int, offset: int = 0, limit: int = 50, api_key: str = "", refresh: bool = False, model: str = "", search: str = "", q1search: str = "") -> Dict[str, Any]:
    env_path = Path(env_dir)
    if not env_path.is_dir():
        return {"items": [], "total": 0, "known_keys": [], "known_models": []}

    sub_dirs = sorted(
        [d for d in env_path.iterdir() if d.is_dir()],
        key=lambda d: d.name, reverse=True,
    )

    all_sessions = []
    all_known_keys: set = set()
    all_known_models: set = set()

    for sub in sub_dirs:
        root_dir = str(sub)
        _refresh_state(kind, root_dir, force=refresh)

        sessions = _ss.list_sessions(root_dir, api_key=api_key, model=model, min_msg_count=min_messages)
        for session in sessions:
            for m in session.get("models", []):
                if m:
                    all_known_models.add(m)
            all_known_keys.add(session.get("api_key", ""))
            all_sessions.append((session, root_dir))

    q1search = (q1search or "").strip()
    if q1search:
        needle = q1search.lower()
        all_sessions = [p for p in all_sessions if needle in (p[0].get("q1", "") or "").lower()]

    if search and search.strip():
        search = search.strip()

        def _hit(pair):
            s, rd = pair
            fn = s.get("latest_file", "")
            ok = bool(fn) and _match_messages_content(rd, fn, search, _root_format(rd) == "newapi")
            return pair if ok else None

        workers = min(32, (os.cpu_count() or 4) * 4)
        with ThreadPoolExecutor(max_workers=workers) as ex:
            all_sessions = [p for p in ex.map(_hit, all_sessions) if p is not None]

    all_sessions.sort(key=lambda t: t[0].get("last_ts", ""), reverse=True)
    total = len(all_sessions)
    paged = all_sessions[offset:offset + limit] if limit > 0 else all_sessions[offset:]

    # 按 root_dir 分组批量获取 traces
    from collections import defaultdict
    by_root: Dict[str, List[str]] = defaultdict(list)
    for session, rd in paged:
        by_root[rd].append(session["session_key"])

    traces_map: Dict[str, Dict[str, List]] = {}
    for rd, keys in by_root.items():
        traces_map[rd] = _ss.get_traces_batch(keys)

    items = []
    for session, rd in paged:
        sk = session["session_key"]
        trace_list = traces_map.get(rd, {}).get(sk, [])
        models = session.get("models", [])
        payload: Dict[str, Any] = {
            "first_time": _format_time(session["first_ts"]),
            "last_time": _format_time(session["last_ts"]),
            "file_count": len(trace_list),
            "message_count": session.get("msg_count", 0),
            "models": models,
            "latest_file": session.get("latest_file", ""),
            "api_key": session.get("api_key", ""),
            "q1_preview": session.get("q1", ""),
            "trace_list": trace_list,
        }
        if any(not t.get("success", True) for t in trace_list):
            payload["has_failure"] = True
        items.append(payload)

    return {"items": items, "total": total, "known_keys": sorted(all_known_keys), "known_models": sorted(all_known_models)}


def _export_view_leaves(roots, env_dir: str) -> List[str]:
    """枚举导出浏览要覆盖的叶子目录（跨全部来源，去掉 mtime 层级语义）。

    只从 leaf_status（同步产物，含绝对 leaf_path）读，不再扫盘兜底：每个登记
    根在 sources 表里都有行，leaf_path 与 leaf_status 同源，保证「列表里有」≡
    「DB 有 leaf_path」。root_id 用 sources 里按 root_path 归一化匹配出的
    **实际存储** root_id（而非 get_root_id(root, env_dir)——那会把活跃 env 目录
    强制折叠成 'default'，偏偏本系统里活跃目录的叶子挂在 md5 哈希 root_id 下，
    bulk_get('default') 恒为空，旧实现只得退回 iter_index_dirs 全盘扫）。
    返回去重后的叶子绝对路径 list。
    """
    import utils.logdir_store as _lds
    from utils.logs_config import get_root_id
    leaves: List[str] = []
    seen = set()
    for root in roots or []:
        if not root:
            continue
        try:
            rp = Path(root)
        except (TypeError, ValueError):
            continue
        if not rp.is_dir():
            continue
        rid = None
        try:
            # 按 root_path 归一化匹配 sources 行，取其真实 root_id（不折叠活跃目录）
            norm = os.path.normpath(str(root))
            src = _lds.get_source_by_path(norm)
            if src:
                rid = src.get("root_id")
            if not rid:
                rid = get_root_id(str(root), env_dir)
        except Exception:  # noqa: BLE001
            rid = None
        if not rid:
            continue
        try:
            for row in _lds.bulk_get(rid):
                lp = row.get("leaf_path") or ""
                if not lp:
                    continue
                full = lp if os.path.isabs(lp) else os.path.join(str(root), lp)
                if Path(full).is_dir():
                    key = os.path.normpath(full)
                    if key not in seen:
                        seen.add(key)
                        leaves.append(full)
        except Exception:  # noqa: BLE001
            continue
    return leaves


def _leaf_dir_key(leaf: str) -> str:
    """叶子的时间目录键（YYMMDDHH），用于「新到旧」排序。

    取叶子路径最后一段；少数来源的叶子名不是纯时间目录（如 native 根），此时退化为
    用父目录 + 自身，保证排序稳定且不会把最新叶子排到后面。
    """
    p = os.path.normpath(leaf or "")
    tail = os.path.basename(p)
    if len(tail) == 8 and tail.isdigit():
        return tail
    parent = os.path.basename(os.path.dirname(p))
    return parent + tail


def _export_key_leaves(roots, env_dir: str, api_key: str) -> List[str]:
    """导出浏览按单个 key 取叶子：优先读 leaf_status.stats_json 的现成倒排索引。

    stats_json 是同步阶段按 index.jsonl 算好的「{api_key}|{date} -> 计数」，等价于
    「该 key 落在哪些叶子」。直接建倒排索引约 50ms，而 build_stats_multi 每次要重算
    key→mtime 分布（实测 0.7~1.3s）且受 600s 内存 TTL 控制。索引里没有该 key
    （新 key 首条会话刚写入）或索引为空时，回落到 build_stats_multi + _resolve_mt_for；
    api_key 为空（看全量）时仍走 _export_view_leaves 枚举全部叶子。
    """
    if not api_key:
        return _export_view_leaves(roots, env_dir)

    # 快路径：leaf_status.stats_json 倒排索引
    try:
        import utils.logdir_store as _lds
        if _lds._ready():
            hit = _lds.key_to_leaves().get(api_key)
            if hit:
                existing = {os.path.normpath(r) for r in roots if r}
                leaves = [lp for lp in hit if Path(lp).is_dir()
                          and (not existing or any(
                              os.path.normpath(lp).startswith(e + os.sep) or os.path.normpath(lp) == e
                              for e in existing))]
                if leaves:
                    # 这个倒排索引是 SELECT 出来的，没有 ORDER BY，实际是 leaf_status
                    # 的插入顺序（最旧的目录在前）。流式聚合按叶子顺序逐叶下推，前端
                    # 又只渲染前 50 条，不排序就会让「本小时的数据」排到滚动加载之后，
                    # 首屏永远是最旧的内容。按目录名 YYMMDDHH 倒序，新到旧。
                    return sorted(leaves, key=_leaf_dir_key, reverse=True)
    except Exception:  # noqa: BLE001
        pass

    existing = [r for r in roots if Path(r).is_dir()]
    if not existing:
        return []
    try:
        from utils.stats_index import build_stats_multi
        stats = build_stats_multi(existing, active_env_dir=env_dir)
    except Exception:  # noqa: BLE001
        return _export_view_leaves(roots, env_dir)
    mts = []
    for row in stats.get("rows", []):
        if row.get("api_key") == api_key:
            mts = sorted(row.get("mtime_cells", {}).keys(), reverse=True)
            break
    if not mts:
        # 新 key（第一条会话刚写入）尚未进入 stats_index 的 rows —— 不能返回 []，
        # 否则这个 key 在内存索引 TTL（600s）到期前永远是空白页。
        # 回落到全量叶子枚举：慢，但只对「索引里还没有的 key」发生，且保证正确性。
        logging.getLogger("export-view").info(
            "_export_key_leaves: key 未命中 stats 索引，回落全量枚举（%s）", (api_key or "")[:8])
        return _export_view_leaves(roots, env_dir)
    try:
        from utils.export_routes import _resolve_mt_for
    except Exception:  # noqa: BLE001
        return _export_view_leaves(roots, env_dir)
    leaves: List[str] = []
    seen = set()
    for mt in mts:
        try:
            lp = _resolve_mt_for(env_dir, mt)
        except Exception:  # noqa: BLE001
            lp = None
        if lp:
            key = os.path.normpath(lp)
            if key not in seen:
                seen.add(key)
                leaves.append(lp)
    return leaves or _export_view_leaves(roots, env_dir)


def export_view_aggregate_payload(roots, env_dir: str, min_messages: int = 1, offset: int = 0, limit: int = 50,
                                  api_key: str = "", model: str = "", search: str = "", q1search: str = "",
                                  refresh: bool = False) -> Dict[str, Any]:
    """导出浏览聚合：按 key 汇总会话，分页返回。

    与 _aggregate_all_payload 的区别：_read_backend 按叶子选后端（newapi→index.db，
    native→session_cache.db），api_key 下推过滤。叶子集用 _export_key_leaves ——
    直接取 build_stats_multi **已算好的** key→mtime 分布（rows[].mtime_cells），
    只加载该 key 落到的叶子，而非跨全部登记根逐叶扫（那要连 /mnt 上 1900+ 个叶子，
    分钟级）。首句(q1search)/内容(search)过滤在合并后做。分页前先按 last_ts 全局
    倒序，traces 仅对分页切片按叶子批量取。
    """
    leaves = _export_key_leaves(roots, env_dir, api_key)
    _ptm = StageTimer()
    _ptm.add("叶子解析", 0.0)  # 占位，保持汇总里阶段顺序稳定
    known_keys: set = set()
    known_models: set = set()
    # 搜索（首句 q1search / 内容 search）需要读全部候选文件才能确定命中与否，
    # 无法下推分页，保持「全量收集→过滤→切页」；无搜索时（打开页面的默认情形）
    # 才走分页下推：每叶子只取最新 offset+limit 条，用 COUNT 求和得到精确 total。
    has_search = bool((q1search or "").strip() or (search or "").strip())

    if has_search:
        all_sessions: List[Tuple[dict, str]] = []
        with _ptm.stage("读取会话"):
            for leaf in leaves:
                try:
                    _refresh_state("anthropic", leaf, force=refresh)
                except Exception:  # noqa: BLE001
                    pass
                try:
                    backend = _read_backend(leaf)
                    sels = backend.list_sessions(leaf, api_key=api_key, model=model, min_msg_count=min_messages)
                    for s in sels:
                        all_sessions.append((s, leaf))
                        if s.get("api_key"):
                            known_keys.add(s["api_key"])
                        for m in s.get("models", []):
                            if m:
                                known_models.add(m)
                    try:
                        for m in backend.get_known_models(leaf):
                            if m:
                                known_models.add(m)
                    except Exception:  # noqa: BLE001
                        pass
                except Exception:  # noqa: BLE001
                    continue
    else:
        # 分页下推：全局按 last_ts DESC 取前 offset+limit 条，来自某叶子的部分必然是
        # 该叶子 last_ts 最新的前 offset+limit 条（LEAF_PAGE 下推保证覆盖），
        # 因此每叶子 list_sessions(limit=need) 一次即可拼出全局前 need 条。
        # 逐叶串行时 52 叶要 27s（瓶颈是每叶建连 + 索引寻址，与叶子大小无关），
        # 用线程池并发读到 _AGG_LEAF_WORKERS 路可降到秒级。
        all_sessions = []
        need = max(offset + limit, limit)

        def _read_leaf(leaf: str):
            try:
                _refresh_state("anthropic", leaf, force=refresh)
            except Exception:  # noqa: BLE001
                pass
            try:
                backend = _read_backend(leaf)
                sels = backend.list_sessions(leaf, api_key=api_key, model=model,
                                             min_msg_count=min_messages, offset=0, limit=need)
                models = []
                try:
                    models = backend.get_known_models(leaf)
                except Exception:  # noqa: BLE001
                    pass
                return [(s, leaf) for s in sels], models
            except Exception:  # noqa: BLE001
                return [], []

        with _ptm.stage("读取会话"):
            with ThreadPoolExecutor(max_workers=_AGG_LEAF_WORKERS,
                                    thread_name_prefix="agg-read") as ex:
                for pairs, models in ex.map(_read_leaf, leaves):
                    for s, _leaf in pairs:
                        all_sessions.append((s, _leaf))
                        if s.get("api_key"):
                            known_keys.add(s["api_key"])
                        for m in s.get("models", []):
                            if m:
                                known_models.add(m)
                    for m in models:
                        if m:
                            known_models.add(m)

    q1search = (q1search or "").strip()
    if q1search:
        needle = q1search.lower()
        all_sessions = [p for p in all_sessions if needle in (p[0].get("q1", "") or "").lower()]

    search = (search or "").strip()
    if search:
        def _hit(pair):
            s, ld = pair
            fn = s.get("latest_file", "")
            try:
                ok = bool(fn) and _match_messages_content(ld, fn, search, _root_format(ld) == "newapi")
            except Exception:  # noqa: BLE001
                ok = False
            return pair if ok else None
        workers = min(32, (os.cpu_count() or 4) * 4)
        with _ptm.stage("内容搜索"):
            with ThreadPoolExecutor(max_workers=workers) as ex:
                all_sessions = [p for p in ex.map(_hit, all_sessions) if p is not None]

    all_sessions.sort(key=lambda t: t[0].get("last_ts", "") or t[0].get("first_ts", "") or "", reverse=True)
    if has_search:
        # 搜索路径已全量收集，total 即当前命中数。
        total = len(all_sessions)
    else:
        # 分页下推路径：收集的只是每叶子前 need 条，total 需用 COUNT 跨叶子精确求和。
        # COUNT 和「读取会话」一样逐叶建连寻址，串行实测 13.5s、16 并发 1.65s，故同样并发。
        def _count_leaf(leaf: str) -> int:
            try:
                return _read_backend(leaf).count_sessions(
                    leaf, api_key=api_key, model=model, min_msg_count=min_messages)
            except Exception:  # noqa: BLE001
                return 0

        with _ptm.stage("COUNT 求总数"):
            with ThreadPoolExecutor(max_workers=_AGG_LEAF_WORKERS,
                                    thread_name_prefix="agg-count") as ex:
                total = sum(ex.map(_count_leaf, leaves))

    with _ptm.stage("组装 items"):
        # 合并必须发生在切片之前：一条逻辑对话的各段分散在不同小时叶子里，
        # 先切页会切到「半条对话」，first_time/file_count 都不对。
        seg_limit = offset + limit if limit > 0 else 0
        window = all_sessions if seg_limit <= 0 else all_sessions[:seg_limit + _MERGE_SLACK]
        merged_all = _merge_chain_segments(_build_agg_items(window))
        if seg_limit > 0:
            items = merged_all[offset:offset + limit]
        else:
            items = merged_all[offset:]

    # total 语义随合并改变：以前是「session 段数」，现在是「逻辑对话数」。
    #
    # 分页下推时只收了每叶子前 need 段，没收集到的段里有一部分会并进已收集的对话里，
    # 所以「已合并条数 + 未收集段数」只会偏大，永远收敛不了——前端据此显示的总数
    # 会一直比实际多。这里改成诚实的表达：
    #   全部收集到（无未收集段）→ 精确值
    #   没收集全 → 标 total_approx，前端显示「约 N 条」
    total_approx = False
    if not has_search:
        uncovered = max(0, total - len(all_sessions))
        total = len(merged_all) + uncovered
        total_approx = uncovered > 0

    _export_view_logger.info(
        "export-view aggregate key=%s model=%s 叶子=%d 命中=%d 合并后=%d%s %s",
        api_key[-8:] if api_key else "", model, len(leaves), total, len(merged_all),
        "（约）" if total_approx else "",
        _ptm.summary_line("aggregate 耗时"))

    return {"items": items, "total": total, "total_approx": total_approx,
            "known_keys": sorted(known_keys), "known_models": sorted(known_models)}


def _build_agg_items(preloaded: List[Tuple[dict, str]], with_traces: bool = False) -> List[Dict[str, Any]]:
    """把 [(session, leaf), ...] 组装成聚合页 items。

    with_traces=False（默认，列表/聚合/stream 用）：只取每条会话的 trace 条数与
    是否含失败，不拉明细。列表页只用 file_count 与 has_failure，而拉全量明细单叶
    实测 17.2s、payload 92MB；COUNT 版 0.07s、52MB。前端点击会话时才按需取明细。
    with_traces=True：保留旧的 trace_list 语义（供需要明细的调用方）。

    供 export_view_aggregate_payload（分页切片后）与 export_view_aggregate_stream
    （每叶子增量）共用，保证两种路径产出的 item 结构一致。
    """
    from collections import defaultdict
    by_root: Dict[str, List[str]] = defaultdict(list)
    for session, ld in preloaded:
        by_root[ld].append(session["session_key"])

    traces_map: Dict[str, Dict[str, List]] = {}
    counts_map: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for ld, keys in by_root.items():
        backend = _read_backend(ld)
        try:
            if with_traces:
                traces_map[ld] = backend.get_traces_batch(keys)
            elif _root_format(ld) == "newapi":
                counts_map[ld] = nidb.get_trace_counts(ld, keys)
            else:
                # native 后端（session_cache.db）没有 COUNT 接口，只能取明细再数长度。
                counts_map[ld] = {
                    k: {"count": len(v), "has_failure": any(not t.get("success", True) for t in v)}
                    for k, v in (backend.get_traces_batch(keys) or {}).items()
                }
        except Exception:  # noqa: BLE001
            if with_traces:
                traces_map[ld] = {}
            else:
                counts_map[ld] = {}

    items: List[Dict[str, Any]] = []
    for session, ld in preloaded:
        sk = session["session_key"]
        if with_traces:
            trace_list = traces_map.get(ld, {}).get(sk, [])
            file_count = len(trace_list)
            has_failure = any(not t.get("success", True) for t in trace_list)
        else:
            trace_list = []
            stat = counts_map.get(ld, {}).get(sk) or {}
            file_count = stat.get("count", 0)
            has_failure = bool(stat.get("has_failure"))
        payload: Dict[str, Any] = {
            "first_time": _format_time(session.get("first_ts", "")),
            "last_time": _format_time(session.get("last_ts", "")),
            "file_count": file_count,
            "message_count": session.get("msg_count", 0),
            "models": session.get("models", []),
            "latest_file": session.get("latest_file", ""),
            "api_key": session.get("api_key", ""),
            "q1_preview": session.get("q1", ""),
            # 跨叶合并用的链标识（api_key||q1_hash）。缺失时由 _merge_chain_segments
            # 回退到 (api_key, q1_preview) 分组。
            "chain_lookup": session.get("chain_lookup", "") or "",
        }
        if with_traces:
            payload["trace_list"] = trace_list
        if has_failure:
            payload["has_failure"] = True
        items.append(payload)
    return items


# 跨小时目录切段的合并间隔上限（秒）。newapi 的 index.db 按「日/时」分叶子，每个
# 叶子各自维护 chain_index，同一段对话跨过整点就会在新叶子里另起一条 session
# （新建会话的 session_key 就是 ts）。实测某 key 20 条对话被切成 50 段，20/20 全跨叶。
# 合并时若相邻两段时间差超过此值就当作两段独立对话——压测回放会把同一 prompt 重复
# 跑很多遍，时间上并不连续，不能只按 q1 合并。
_MERGE_MAX_GAP_SEC = 90 * 60

# 分页下推时多读几段再合并：一条逻辑对话可能横跨多个小时叶子，若只取 offset+limit
# 段就切页，页尾那条对话会被截断。多读一些能覆盖绝大多数跨叶情形，代价很小。
_MERGE_SLACK = 200


def _merge_chain_segments(items: List[Dict[str, Any]],
                          max_gap_sec: float = _MERGE_MAX_GAP_SEC) -> List[Dict[str, Any]]:
    """把同一段逻辑对话跨小时叶子的多个 segment 合并成一条（浏览页封装）。

    实际算法在 utils/session_merge.py，导出的 reformat/copy 链路复用同一份实现。

    **只按 chain_lookup（强身份）合并**，没有该字段的记录原样保留、不参与分组。
    早先版本在没有 chain_lookup 时会退回按 (api_key, q1_preview) 文本分组，这是错的：
    实测单叶子内同一段 q1 文本最多对应 5749 条不同 session（压测/agent 的固定系统提示词），
    按文本分组会把这几千条并成一条。宁可少合并，也不能错并。
    """
    from utils.session_merge import merge_segments
    return merge_segments(
        items,
        first_field="first_time",
        last_field="last_time",
        count_field="message_count",
        file_count_field="file_count",
        models_field="models",
        identity_field="chain_lookup",
        latest_file_field="latest_file",
        max_gap_sec=max_gap_sec,
    )


def _stamp_chain_lookup(pairs: List[Tuple[dict, str]]) -> None:
    """给非 newapi 后端的会话补 chain_lookup（就地修改）。

    newapi 的 list_sessions 已经在 SQL 里 JOIN chain_index 带出该字段；native 后端
    （session_cache.db）没有链表，留空后由 _merge_chain_segments 回退按 (api_key, q1)
    分组——native 叶子按小时也分库，同一段对话同样会跨叶重复。
    """
    by_leaf: Dict[str, List[str]] = {}
    for s, leaf in pairs:
        if s.get("chain_lookup"):
            continue
        by_leaf.setdefault(leaf, []).append(s.get("session_key") or "")
    for leaf, keys in by_leaf.items():
        if _root_format(leaf) == "newapi":
            continue
        try:
            m = nidb.get_sessions_chain_map(leaf, keys)
        except Exception:  # noqa: BLE001
            continue
        for s, lf in pairs:
            if lf == leaf and not s.get("chain_lookup"):
                hit = m.get(s.get("session_key") or "")
                if hit:
                    s["chain_lookup"] = hit


def session_traces_payload(roots, env_dir: str, api_key: str, filename: str,
                           chain_lookup: str = "") -> Dict[str, Any]:
    """返回某个会话（由其某个请求文件名定位）的完整 trace 明细列表。

    列表接口只回 file_count；前端点击会话后调这里取「关联列表」，避免每次列表
    都把全部明细（单叶实测 17.2s / 92MB）搬一遍。

    chain_lookup 非空时做跨叶扇出：newapi 叶子按小时分库，同一条对话在每小时目录里
    各有一条 session 行（同一个 chain_lookup）。只按 filename 定位到的那一个叶子取，
    合并行的「关联列表」就只列出最后一个小时的文件，与 file_count 对不上。这里再拿
    该 key 落到过的其它叶子，用同一个 chain_lookup 把各段的 trace 都取回来合并。
    """
    path = _export_view_find_file(roots, env_dir, filename, api_key=api_key)
    if not path:
        return {"items": [], "error": "file not found"}
    leaf = os.path.dirname(path)
    if _root_format(leaf) != "newapi":
        return {"items": [], "error": "unsupported format"}
    try:
        session_key = nidb.session_key_for_file(leaf, filename)
    except Exception:  # noqa: BLE001
        session_key = None
    if not session_key:
        return {"items": [], "error": "session not found"}

    def _traces_in(leaf_dir: str, sk: str) -> List[Dict[str, Any]]:
        try:
            return nidb.get_traces_for_session(leaf_dir, sk)
        except Exception:  # noqa: BLE001
            return []

    traces = _traces_in(leaf, session_key)
    if not chain_lookup:
        return {"items": traces, "session_key": session_key}

    # 跨叶扇出：只看该 key 落到过的叶子，不做全量扫盘。
    leaves: List[str] = []
    try:
        leaves = _export_key_leaves(roots, env_dir, api_key) if api_key else []
    except Exception:  # noqa: BLE001
        leaves = []
    if leaf not in leaves:
        leaves = [leaf] + list(leaves)

    def _seg(leaf_dir: str):
        if os.path.normpath(leaf_dir) == os.path.normpath(leaf):
            return []           # 已取过
        try:
            if _root_format(leaf_dir) != "newapi":
                return []
        except Exception:  # noqa: BLE001
            return []
        out: List[Dict[str, Any]] = []
        for sk in nidb.get_session_keys_by_chain(leaf_dir, chain_lookup):
            out.extend(_traces_in(leaf_dir, sk))
        return out

    if len(leaves) > 1:
        with ThreadPoolExecutor(max_workers=min(_AGG_LEAF_WORKERS, len(leaves))) as ex:
            for part in ex.map(_seg, leaves):
                if part:
                    traces.extend(part)

    # 按文件名去重后按时间升序，与单叶内的排序口径一致。
    seen = set()
    uniq: List[Dict[str, Any]] = []
    for t in traces:
        fn = t.get("filename") or ""
        if fn and fn in seen:
            continue
        if fn:
            seen.add(fn)
        uniq.append(t)
    uniq.sort(key=lambda t: str(t.get("ts") or ""))
    return {"items": uniq, "session_key": session_key}


# ---------------------------------------------------------------------------
# 导出浏览 · 流式聚合任务
#
# /export/view/api/aggregate 现在按叶子串行收齐（41 个 newapi 叶子约 25s）才返回，
# 前端要等全部读完才能渲染。这里改成「后台任务 + 增量队列」：
#   start()  解析叶子、起后台线程，逐叶 list_sessions + count + 组装 items，
#            每读完一个叶子就把该叶子的 items 追加进任务队列（mtime 新的叶子在前，
#            所以最新对话最先到）。
#   poll()   前端每 300ms 拉一次增量，拿到新 items 就 append 渲染，done 后停。
# 两种性质：total 用 COUNT 精确求和（与分页下推一致），items 不设上限——export
# 视图是一次拉全的浏览场景，滚动加载由前端就地追加，不做 offset 分页。
# ---------------------------------------------------------------------------
_AGG_STREAM_LOCK = threading.Lock()
_AGG_STREAM_TASKS: Dict[str, Dict[str, Any]] = {}
_AGG_STREAM_SEQ = 0
_AGG_STREAM_TTL = 300  # 秒；超过未再被 poll 则清理

# 流式收集的叶子并发度。单叶耗时几乎与叶子大小无关（建连 + 索引寻址），逐叶串行
# 时 52 叶要 31s；实测 16 并发 2.45s。用滑动窗口限制在途数量，内存不随叶子总数增长。
_AGG_LEAF_WORKERS = 16

# 收集完成的结果缓存：键 (api_key, model, min_messages)。同 key+model 再次打开
# 直接命中缓存，跳过对数万会话的重新收集。TTL 比任务长很多。
_AGG_CACHE_LOCK = threading.Lock()
_AGG_CACHE: Dict[Tuple[str, str, int], Dict[str, Any]] = {}
_AGG_CACHE_TTL = 900  # 秒；收集结果缓存有效期

# 叶子签名缓存：(叶子集) -> (计算时刻, 签名)。全量视图 4500+ 叶子逐个 stat 冷态
# 4.1s、热态 0.1s，故加 30s TTL；滞后只影响「新数据多久让缓存失效」，而缓存本身
# 900s，可忽略。
_LEAF_SIG_LOCK = threading.Lock()
_LEAF_SIG_CACHE: Dict[tuple, tuple] = {}
_LEAF_SIG_TTL = 30  # 秒

# 导出浏览读链路的耗时打点（阶段累计 + 最慢叶子），只进日志与 poll payload，
# 不新增 DB / 缓存结构。
_export_view_logger = logging.getLogger("export-view")


def _agg_cache_key(api_key: str, model: str, min_messages: int,
                   scope: str = "") -> Tuple[str, str, int, str]:
    """缓存键。scope 区分「读哪些叶子」：内部页选中时间目录时只有 1 个叶子，
    公开页是全量/按 key 的叶子集；两者结果不同，不能共用同一条缓存。"""
    return (api_key or "", model or "", int(min_messages or 1), scope or "")


def _leaf_sig(leaves: List[str]) -> Tuple[Tuple[str, int, int], ...]:
    """叶子集签名：每个叶子的 index.db（缺省回落到叶子目录）的 (路径, mtime_ns, size)。

    watcher 每同步出一个新会话就会写 index.db，mtime/size 随之变化。用它校验缓存
    是否仍然描述当前磁盘状态，比「任何叶子有写入就清空整表」精确得多：写 A 叶子
    不会让 B 叶子的视图失效。

    结果按叶子集做 30s 进程内缓存：全量视图有 4500+ 叶子，逐个 stat 冷态实测 4.1s，
    热态 0.1s。30s 的滞后只意味着「刚写入的数据最多晚 30s 才让缓存失效」，而缓存本身
    TTL 是 900s，这个代价可以忽略。
    """
    cache_key = tuple(leaves)
    now = time.time()
    with _LEAF_SIG_LOCK:
        hit = _LEAF_SIG_CACHE.get(cache_key)
        if hit and now - hit[0] < _LEAF_SIG_TTL:
            return hit[1]
    sig: List[Tuple[str, int, int]] = []
    for leaf in leaves:
        path = os.path.join(leaf, "index.db")
        if not os.path.exists(path):
            path = leaf
        try:
            st = os.stat(path)
            sig.append((leaf, st.st_mtime_ns, st.st_size))
        except OSError:
            sig.append((leaf, 0, 0))
    sig.sort()
    result = tuple(sig)
    with _LEAF_SIG_LOCK:
        # 只保留最近若干组叶子的签名，避免无界增长
        if len(_LEAF_SIG_CACHE) > 32:
            _LEAF_SIG_CACHE.clear()
        _LEAF_SIG_CACHE[cache_key] = (now, result)
    return result


def _leaves_scope(leaves: List[str]) -> str:
    """叶子集摘要，用作聚合缓存的 scope。

    内部页选中时间目录时只有 1 个叶子，公开页是全量/按 key 的叶子集 —— 两者结果
    完全不同，不能共用同一条缓存条目。取长度 + 首尾叶子路径做区分即可（叶子集
    本身变长变短或换目录都会命中不同的 scope）。
    """
    if not leaves:
        return "0"
    return "%d|%s|%s" % (len(leaves), leaves[0], leaves[-1])


def _agg_cache_get(api_key: str, model: str, min_messages: int,
                   leaves: Optional[List[str]] = None,
                   scope: str = "") -> Optional[Dict[str, Any]]:
    """命中且未过期则返回缓存条目（含 items/total/known_keys/known_models），否则 None。

    leaves 给定时还要求叶子签名与收集时一致：期间任何叶子有新数据落库即视为未命中，
    由调用方重新收集。这样缓存不必靠 watcher 主动清空就能保持正确。
    """
    now = time.time()
    ck = _agg_cache_key(api_key, model, min_messages, scope)
    with _AGG_CACHE_LOCK:
        # 顺手清理过期缓存
        stale = [k for k, v in _AGG_CACHE.items() if now - v.get("_ts", 0) > _AGG_CACHE_TTL]
        for k in stale:
            _AGG_CACHE.pop(k, None)
        entry = _AGG_CACHE.get(ck)
    if entry is None:
        return None
    if leaves is not None:
        if entry.get("_sig") != _leaf_sig(leaves):
            with _AGG_CACHE_LOCK:
                _AGG_CACHE.pop(ck, None)
            return None
    return entry


def _agg_cache_put(api_key: str, model: str, min_messages: int, entry: Dict[str, Any],
                   leaves: Optional[List[str]] = None, scope: str = "") -> None:
    ck = _agg_cache_key(api_key, model, min_messages, scope)
    entry = dict(entry)
    entry["_ts"] = time.time()
    if leaves is not None:
        entry["_sig"] = _leaf_sig(leaves)
    with _AGG_CACHE_LOCK:
        _AGG_CACHE[ck] = entry


def invalidate_agg_cache(leaf_dir: str = "") -> int:
    """叶子有新数据落库后，丢弃「覆盖到该叶子」的聚合结果缓存。

    缓存条目记录了收集时的叶子签名（_sig）。给定 leaf_dir 时只淘汰签名里含该叶子的
    条目，其它叶子/其它 key 的视图继续可用；不给定则整表清空。签名校验本身已经能
    发现过期（_agg_cache_get），这里只做一次即时淘汰，省掉一次「先命中再校验失败」
    的往返。

    由 utils/newapi_watcher 在叶子同步完成后调用。返回清掉的条目数。
    """
    target = os.path.normpath(leaf_dir) if leaf_dir else ""
    with _AGG_CACHE_LOCK:
        if not target:
            n = len(_AGG_CACHE)
            _AGG_CACHE.clear()
        else:
            drop = [k for k, v in _AGG_CACHE.items()
                    if any(os.path.normpath(s[0]) == target for s in (v.get("_sig") or ()))]
            for k in drop:
                _AGG_CACHE.pop(k, None)
            n = len(drop)
    if n:
        _export_view_logger.info("聚合缓存已失效 %d 项（叶子有新数据：%s）", n, leaf_dir)
    return n


def export_view_aggregate_stream_start(roots, env_dir: str, min_messages: int = 1,
                                       api_key: str = "", model: str = "", search: str = "",
                                       q1search: str = "", refresh: bool = False,
                                       leaves: Optional[List[str]] = None) -> str:
    """开启一个聚合流式任务，立即返回 task_id。后台线程在收集，前端轮询 poll()。

    leaves 给定时直接用它当叶子集（内部页选中某个时间目录时只读那一个叶子，
    不必走全量枚举）；不给则按 api_key 解析。
    """
    global _AGG_STREAM_SEQ
    _leaf_tm = StageTimer()
    with _leaf_tm.stage("叶子解析"):
        if leaves is None:
            leaves = _export_key_leaves(roots, env_dir, api_key)
    # 查结果缓存：命中则直接造一个「已完成」任务，poll 一次即拿全量，免重复收集。
    # 叶子解析要先做——缓存条目按叶子签名校验，签名的对象正是这批叶子。
    # scope 用叶子集的摘要：内部页（单叶）与公开页（全量/按 key）结果不同，必须分开。
    scope = _leaves_scope(leaves)
    if not refresh and not search and not q1search:
        with StageTimer() as _cache_tm:
            cached = _agg_cache_get(api_key, model, min_messages, leaves, scope)
        if cached is not None:
            with _AGG_STREAM_LOCK:
                _AGG_STREAM_SEQ += 1
                tid = "agg-%d-%d" % (os.getpid(), _AGG_STREAM_SEQ)
                _AGG_STREAM_TASKS[tid] = {
                    "status": "done",
                    "items": list(cached.get("items", [])),
                    "total": cached.get("total", 0),
                    "cursor": 0,
                    "known_keys": set(cached.get("known_keys", [])),
                    "known_models": set(cached.get("known_models", [])),
                    "leaves_total": cached.get("leaves_total", 0),
                    "leaves_done": cached.get("leaves_total", 0),
                    "cached": True,
                    "_ts": time.time(),
                    "_t0": time.time(),
                    "_timings": {"缓存命中": _cache_tm.elapsed(),
                                 "叶子解析": _leaf_tm.elapsed(), "读取会话": 0.0, "组装 items": 0.0},
                    "_slow_leaves": [],
                }
            _export_view_logger.info(
                "export-view 缓存命中 key=%s model=%s min=%s items=%s 用时 %.3fs（叶子解析 %.3fs）",
                api_key[-8:] if api_key else "", model, min_messages,
                cached.get("total", 0), _cache_tm.elapsed(), _leaf_tm.elapsed())
            return tid
    if not leaves:
        # 无叶子可读：直接给一个已完成的任务，避免前端空转。
        with _AGG_STREAM_LOCK:
            _AGG_STREAM_SEQ += 1
            tid = "agg-%d-%d" % (os.getpid(), _AGG_STREAM_SEQ)
            _AGG_STREAM_TASKS[tid] = {
                "status": "done", "items": [], "total": 0, "cursor": 0,
                "known_keys": set(), "known_models": set(),
                "leaves_total": 0, "leaves_done": 0, "_ts": 0,
                "_t0": time.time(),
                "_timings": {"叶子解析": _leaf_tm.elapsed()},
                "_slow_leaves": [],
            }
        _export_view_logger.info("export-view 无叶子 key=%s model=%s 叶子解析 %.3fs",
                                 api_key[-8:] if api_key else "", model, _leaf_tm.elapsed())
        return tid

    with _AGG_STREAM_LOCK:
        _AGG_STREAM_SEQ += 1
        tid = "agg-%d-%d" % (os.getpid(), _AGG_STREAM_SEQ)
        task = {
            "status": "running", "items": [], "total": 0, "cursor": 0,
            "known_keys": set(), "known_models": set(), "leaves": leaves,
            "leaves_total": len(leaves), "leaves_done": 0,
            "_ts": time.time(), "_env": env_dir, "_min": min_messages,
            "_key": api_key, "_model": model, "_search": search, "_q1": q1search,
            "_refresh": refresh,
            # 耗时打点：_timings 为阶段累计（秒），_slow_leaves 只留最慢 5 个叶子，
            # 避免大 key（90+ 叶子）时任务字典膨胀。前端 poll 里带 elapsed_s 出去。
            "_t0": time.time(),
            "_timings": {"叶子解析": _leaf_tm.elapsed(), "读取会话": 0.0, "组装 items": 0.0},
            "_slow_leaves": [],
        }
        _AGG_STREAM_TASKS[tid] = task
    _export_view_logger.info(
        "export-view 流式开始 key=%s model=%s 叶子=%d 叶子解析 %.3fs",
        api_key[-8:] if api_key else "", model, len(leaves), _leaf_tm.elapsed())

    def _collect_leaf(leaf: str) -> Tuple[str, float, float, List[Dict[str, Any]], List[str]]:
        """读单个叶子并组装 items。纯读、无共享状态，供线程池并发调用。

        返回 (leaf, 读会话耗时, 组装耗时, items, 该叶出现过的 model)。
        """
        try:
            _refresh_state("anthropic", leaf, force=task["_refresh"])
        except Exception:  # noqa: BLE001
            pass
        sel_tm = StageTimer()
        try:
            backend = _read_backend(leaf)
            with sel_tm.stage("读取会话"):
                sels = backend.list_sessions(leaf, api_key=task["_key"], model=task["_model"],
                                             min_msg_count=task["_min"])
        except Exception:  # noqa: BLE001
            return leaf, sel_tm.elapsed(), 0.0, [], []
        models: List[str] = []
        for s in sels:
            for m in s.get("models", []):
                if m:
                    models.append(m)
        try:
            models.extend(backend.get_known_models(leaf))
        except Exception:  # noqa: BLE001
            pass
        build_tm = StageTimer()
        with build_tm.stage("组装 items"):
            pairs = [(s, leaf) for s in sels]
            if pairs:
                _stamp_chain_lookup(pairs)
            items = _build_agg_items(pairs) if sels else []
        return leaf, sel_tm.elapsed(), build_tm.elapsed(), items, models

    def _run():
        # 叶子已按 mtime 倒序（_export_key_leaves 里 sorted(reverse=True)），新到旧，
        # 所以先推给前端的是最新会话。
        #
        # 并发收集：逐叶串行时 52 个叶子要 31s，瓶颈是每个叶子都要建连 + 走索引，
        # 与叶子大小无关（实测 16 并发 2.45s，12.7x）。用滑动窗口把在途任务限制在
        # _AGG_LEAF_WORKERS 内，内存只随并发数增长，与叶子总数无关。
        #
        # 结果仍按叶子顺序消费（deque 队首阻塞等待），保证前端看到的顺序与串行时
        # 一致；先发出去的叶子先到，最新数据依然最先显示。
        workers = _AGG_LEAF_WORKERS
        from collections import deque
        pending: deque = deque()
        leaf_iter = iter(task["leaves"])
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="agg-leaf") as pool:
            def _fill():
                while len(pending) < workers:
                    try:
                        nxt = next(leaf_iter)
                    except StopIteration:
                        return
                    pending.append(pool.submit(_collect_leaf, nxt))
            _fill()
            while pending:
                leaf, _sel_s, _build_s, items, models = pending.popleft().result()
                _fill()
                with _AGG_STREAM_LOCK:
                    if items:
                        task["items"].extend(items)
                        task["total"] = len(task["items"])  # 已收集到的 item 数
                    for m in models:
                        task["known_models"].add(m)
                    for it in items:
                        if it.get("api_key"):
                            task["known_keys"].add(it["api_key"])
                    task["leaves_done"] += 1
                    task["_ts"] = time.time()
                    _tm = task["_timings"]
                    _tm["读取会话"] = _tm.get("读取会话", 0.0) + _sel_s
                    _tm["组装 items"] = _tm.get("组装 items", 0.0) + _build_s
                    task["_slow_leaves"].append((round(_sel_s + _build_s, 3), leaf))
                    task["_slow_leaves"].sort(reverse=True)
                    del task["_slow_leaves"][5:]
        with _AGG_STREAM_LOCK:
            # 收集完成：把跨小时叶子的同一条对话合并掉。合并会让条数变少，所以
            # total 用合并后的条数——否则前端滚动到底会以为还有内容没加载。
            raw_items = list(task["items"])
        if not task["_search"] and not task["_q1"] and len(task["leaves"]) > 1:
            merged_items = _merge_chain_segments(raw_items)
        else:
            # 单叶（内部页选中某个时间目录）不做跨叶合并；搜索路径也保持原始段，
            # 因为命中是按文件内容判定的，合并会把某段命中的信息摊给整条对话。
            merged_items = raw_items
        with _AGG_STREAM_LOCK:
            task["items"] = merged_items
            task["total"] = len(merged_items)
            if merged_items is not raw_items:
                # 合并把已推给前端的原始段整体替换掉了，下次 poll 需要下发全量重绘。
                task["_merged"] = True
                task["cursor"] = 0
            task["leaves_done"] = task["leaves_total"]
            task["status"] = "done"
            task["_ts"] = time.time()
            _elapsed = time.time() - task.get("_t0", task["_ts"])
            snapshot = {
                "items": merged_items,
                "total": task["total"],
                "known_keys": sorted(task["known_keys"]),
                "known_models": sorted(task["known_models"]),
                "leaves_total": task["leaves_total"],
            }
            _leaf_n = task["leaves_total"]
            _items_n = task["total"]
            _parts = list(task["_timings"].items())
            _slow = list(task["_slow_leaves"])
        # 收集完成：写入结果缓存，供同 key+model 再次打开时秒开（search/q1search 不缓存：
        # 它们是过滤后的子集，与全量视图不是同一份结果）。refresh 现在也写缓存——它是
        # 「用户主动要最新数据」的意思，不是「别缓存」，跳过一次命中就足够了。
        if not task["_search"] and not task["_q1"]:
            _agg_cache_put(task["_key"], task["_model"], task["_min"], snapshot,
                           task["leaves"], _leaves_scope(task["leaves"]))
        _export_view_logger.info(
            "export-view 流式完成 key=%s model=%s 叶子=%d items=%d 总耗时 %.2fs | %s | 最慢叶子: %s",
            task["_key"][-8:] if task["_key"] else "", task["_model"], _leaf_n, _items_n, _elapsed,
            " + ".join("%s %.2fs" % (n, v) for n, v in _parts),
            "; ".join("%.2fs %s" % (s, l) for s, l in _slow) or "无")

    t = threading.Thread(target=_run, daemon=True, name="agg-stream-" + tid)
    t.start()
    return tid


def export_view_aggregate_stream_poll(task_id: str) -> Dict[str, Any]:
    """取 task 自上次 poll 以来的增量 items，并做 TTL 清理。"""
    now = time.time()
    with _AGG_STREAM_LOCK:
        # TTL 清理：顺手清掉超时未 poll 的已完成任务。
        stale = [k for k, v in _AGG_STREAM_TASKS.items()
                 if now - v.get("_ts", 0) > _AGG_STREAM_TTL]
        for k in stale:
            _AGG_STREAM_TASKS.pop(k, None)
        task = _AGG_STREAM_TASKS.get(task_id)
        if not task:
            return {"status": "gone", "items": [], "total": 0,
                    "leaves_total": 0, "leaves_done": 0, "cached": False,
                    "known_keys": [], "known_models": []}
        items = task["items"][task["cursor"]:]
        task["cursor"] = len(task["items"])
        task["_ts"] = now
        # 合并发生在收集结束时，会把之前的原始段整体替换成合并结果。此时不能用
        # 游标只推增量——前端拿到的是「合并列表的尾巴」，而不是整份新列表。
        # 标记 replace=true 并下发全量，前端据此重建 aggChains。
        replaced = bool(task.get("_merged"))
        if replaced:
            items = list(task["items"])
            task["_merged"] = False   # 只替换一次，之后仍按游标增量（若还会增长）
        return {
            "status": task["status"],
            "items": items,
            "replace": replaced,
            "total": task["total"],
            "leaves_total": task.get("leaves_total", 0),
            "leaves_done": task.get("leaves_done", 0),
            "cached": task.get("cached", False),
            "known_keys": sorted(task["known_keys"]),
            "known_models": sorted(task["known_models"]),
            # 已用时间（秒）+ 阶段累计耗时，前端显示「已用 xx.xs」，便于判断是
            # 收集慢还是推送慢。任务字典里没有 _t0 的（旧任务/被修剪）给 0。
            "elapsed_s": round(max(0.0, now - task.get("_t0", now)), 2),
            "timings": {k: round(v, 2) for k, v in (task.get("_timings") or {}).items()},
        }


def export_view_list_payload(roots, env_dir: str, min_messages: int = 1, offset: int = 0, limit: int = 50,
                             api_key: str = "", model: str = "", search: str = "", q1search: str = "",
                             refresh: bool = False) -> Dict[str, Any]:
    """导出浏览「单文件列表」：把每 session 的 trace 展开为文件级条目，分页返回。

    叶子集来自 _export_key_leaves（build_stats_multi 的 key→mtime 分布），只加载
    该 key 落到的叶子。先聚合会话、对 session 做 q1search/search 过滤（与
    export_view_aggregate_payload 同口径），再把命中的 session 展开成文件行；
    文件行保留所属 session_key 供 trace 归属。
    """
    leaves = _export_key_leaves(roots, env_dir, api_key)
    _ltm = StageTimer()
    sessions_by_leaf: List[Tuple[dict, str]] = []
    known_keys: set = set()
    known_models: set = set()

    with _ltm.stage("读取会话"):
        for leaf in leaves:
            try:
                _refresh_state("anthropic", leaf, force=refresh)
            except Exception:  # noqa: BLE001
                pass
            try:
                backend = _read_backend(leaf)
                sels = backend.list_sessions(leaf, api_key=api_key, model=model, min_msg_count=min_messages)
            except Exception:  # noqa: BLE001
                continue
            for s in sels:
                sessions_by_leaf.append((s, leaf))
                if s.get("api_key"):
                    known_keys.add(s["api_key"])
                for m in s.get("models", []):
                    if m:
                        known_models.add(m)

    q1search = (q1search or "").strip()
    if q1search:
        needle = q1search.lower()
        sessions_by_leaf = [p for p in sessions_by_leaf if needle in (p[0].get("q1", "") or "").lower()]

    search = (search or "").strip()
    if search:
        def _hit(pair):
            s, ld = pair
            fn = s.get("latest_file", "")
            ok = bool(fn) and _match_messages_content(ld, fn, search, _root_format(ld) == "newapi")
            return pair if ok else None
        workers = min(32, (os.cpu_count() or 4) * 4)
        with _ltm.stage("内容搜索"):
            with ThreadPoolExecutor(max_workers=workers) as ex:
                sessions_by_leaf = [p for p in ex.map(_hit, sessions_by_leaf) if p is not None]

    # 按叶子批量取 trace，展开为文件行
    from collections import defaultdict
    by_leaf: Dict[str, List[str]] = defaultdict(list)
    for s, ld in sessions_by_leaf:
        by_leaf[ld].append(s["session_key"])
    traces_map: Dict[str, Dict[str, List]] = {}
    with _ltm.stage("批量取 trace"):
        for ld, keys in by_leaf.items():
            try:
                traces_map[ld] = _read_backend(ld).get_traces_batch(keys)
            except Exception:  # noqa: BLE001
                traces_map[ld] = {}

    raw: List[Dict[str, Any]] = []
    for s, ld in sessions_by_leaf:
        for t in traces_map.get(ld, {}).get(s["session_key"], []):
            fn = t.get("filename", "")
            if not fn:
                continue
            raw.append({
                "filename": fn,
                "message_count": t.get("msg_count", 0),
                "model": t.get("model", ""),
                "api_key": s.get("api_key", ""),
                "_ts": t.get("ts", "") or s.get("last_ts", ""),
                "_session_key": s["session_key"],
                "_leaf": ld,
            })

    raw.sort(key=lambda r: r.get("_ts", ""), reverse=True)
    total = len(raw)
    paged = raw[offset:offset + limit] if limit > 0 else raw[offset:]
    items = [{k: v for k, v in r.items() if not k.startswith("_")} for r in paged]
    _export_view_logger.info(
        "export-view list key=%s model=%s 叶子=%d 文件行=%d %s",
        api_key[-8:] if api_key else "", model, len(leaves), total, _ltm.summary_line("list 耗时"))
    return {"items": items, "total": total, "known_keys": sorted(known_keys), "known_models": sorted(known_models)}


def _export_view_find_file(roots, env_dir: str, filename: str, api_key: str = "") -> Optional[str]:
    """在导出浏览覆盖的叶子下查找文件，返回绝对路径或 None。

    传 api_key 时按该 key 的 mtime 分布（_export_key_leaves）限定加载的叶子；
    不传则回退全量 _export_view_leaves。
    """
    leaves = _export_key_leaves(roots, env_dir, api_key) if api_key else _export_view_leaves(roots, env_dir)
    for ld in leaves:
        p = os.path.join(ld, filename)
        if os.path.isfile(p):
            return p
    return None


def _load_conversation_file(path: str) -> Optional[dict]:
    """读取请求-响应合并文件，返回与「对话浏览」一致的 payload（含 _usage）。

    path 为叶子内绝对路径。newapi 单文件用 build_preview_payload 拆解；
    native 三元组则加载 -req.json 并把响应内容作为 assistant 消息追加。
    解析失败返回 None。
    """
    leaf = os.path.dirname(path)
    filename = os.path.basename(path)
    is_newapi = _root_format(leaf) == "newapi"
    if (filename.endswith(".json") and not filename.endswith("-req.json")
            and "/" not in filename and "\\" not in filename and ".." not in filename):
        if is_newapi:
            from utils.newapi_format import build_preview_payload
            return build_preview_payload(Path(path))
        return None
    if not filename.endswith("-req.json") or "/" in filename or "\\" in filename or ".." in filename:
        return None
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    res_path = Path(path).with_name(filename.replace("-req.json", "-res.json"))
    res_content = _extract_anthropic_res_content(res_path)
    if res_content is not None and isinstance(data.get("messages"), list):
        data["messages"].append({"role": "assistant", "content": res_content, "_from_res": True})
    elif isinstance(data.get("messages"), list):
        openai_content = _extract_openai_res_content(res_path)
        if openai_content is not None:
            data["messages"].append({**openai_content, "_from_res": True})
    res_data = _load_json(res_path)
    usage = extract_res_usage(res_data) if res_data else None
    if usage:
        data["_usage"] = usage
    return data


def _find_file_in_all_dirs(env_dir: str, filename: str) -> Optional[str]:
    env_path = Path(env_dir)
    if not env_path.is_dir():
        return None
    for sub in sorted(env_path.iterdir(), key=lambda d: d.name, reverse=True):
        if sub.is_dir():
            candidate = sub / filename
            if candidate.is_file():
                return str(candidate)
    return None


# ---------------------------------------------------------------------------
# 后台预热线程 — 进程启动后自动预热所有 mtime 目录的 session 缓存
# ---------------------------------------------------------------------------

_warmer_thread = None
_warmer_stop = threading.Event()

_logger = logging.getLogger("session-warmer")


def _warmer_loop(env_dir: str, current_log_dir: str, interval: float = 30.0) -> None:
    env_path = Path(env_dir)
    if env_path.is_dir():
        subs = sorted(env_path.iterdir())
        _logger.info("session-warmer: 开始预热 %d 个目录", len(subs))
        for sub in subs:
            if _warmer_stop.is_set():
                return
            if not sub.is_dir():
                continue
            with _CACHE_LOCK:
                _refresh_state("anthropic", str(sub), force=True)
        _logger.info("session-warmer: 预热完成")

    while not _warmer_stop.is_set():
        _warmer_stop.wait(interval)
        if _warmer_stop.is_set():
            break
        with _CACHE_LOCK:
            _refresh_state("anthropic", current_log_dir, force=True)


def start_session_cache_warmer(env_dir: str, current_log_dir: str) -> None:
    global _warmer_thread
    if _warmer_thread is not None and _warmer_thread.is_alive():
        return
    _warmer_stop.clear()
    _warmer_thread = threading.Thread(
        target=_warmer_loop,
        args=(env_dir, current_log_dir),
        daemon=True,
        name="session-cache-warmer",
    )
    _warmer_thread.start()


def register_log_routes(app: FastAPI) -> None:
    _current_log_dir = get_log_dir("logs_all")
    _env_dir = str(Path(_current_log_dir).parent)

    def _all_roots() -> list:
        """活跃 env_dir + 配置的历史路径。"""
        from utils.logs_config import get_stats_roots
        return get_stats_roots(_env_dir)

    def unified_log_dir() -> str:
        return _current_log_dir

    def _leaf_by_hour(root: str, rel: str) -> str:
        """在 root 的日期层里找裸小时目录：<root>/<day>/<rel>。

        dir_key 有时只带 8 位小时段（如 26102720），而真实布局是
        <root>/261027/26102720 —— resolve_leaf 只会试 <root>/<rel> 与
        <root>/<rel>/<末段>，都命中不了。这里补一层目录扫描。
        只扫 root 的直接子目录（通常几十个），命中即返回。
        """
        if not rel or "/" in rel or os.sep in rel:
            return ""
        try:
            children = os.listdir(root)
        except OSError:
            return ""
        for day in children:
            cand = os.path.join(root, day, rel)
            try:
                if os.path.isdir(cand):
                    return cand
            except OSError:
                continue
        return ""

    def resolve_log_dir(log_dir: str = "") -> str:
        if not log_dir:
            return _current_log_dir
        # log_dir 形如 "26072520" 或跨 root 时 "<root_id>::<rel_key>"
        # root_part 现为稳定 root_id；旧链接可能是 basename/normpath，保留回退匹配。
        rel_hint = log_dir
        if "::" in log_dir:
            from utils.logs_config import get_root_id
            from utils.log_scan import resolve_leaf
            root_part, _, rel = log_dir.partition("::")
            rel_hint = rel
            for root in _all_roots():
                if (get_root_id(root, _env_dir) == root_part
                        or os.path.basename(os.path.normpath(root)) == root_part
                        or os.path.normpath(root) == root_part):
                    candidate = str(resolve_leaf(root, rel))
                    if os.path.isdir(candidate):
                        return candidate
                    # 该 root 下没直接命中：继续用 rel 在本 root 的日期层里找
                    # （dir_key 只带小时段、丢了日期层的情形，见下方兜底）。
                    found = _leaf_by_hour(root, rel)
                    if found:
                        return found
        # 用 rel_hint 继续兜底：带 "::" 的整串拼路径必然落空，必须剥掉前缀。
        log_dir = rel_hint
        # 兼容旧格式：同 env-key 下的时间戳子目录
        candidate = os.path.join(_env_dir, log_dir)
        if os.path.isdir(candidate):
            return candidate
        # 再在所有 root 下按相对路径找
        for root in _all_roots():
            c = os.path.join(root, log_dir)
            if os.path.isdir(c):
                return c
        # 兼容旧报告：new-api 布局 <root>/<day>/<hour>，但旧报告只存了裸 <hour>
        # （历史 bug：log_dir=Path(src_dir).name 丢了日期层）。这里在每个 root
        # 下一层里搜同名子目录补回。
        if os.sep not in log_dir and "/" not in log_dir:
            for root in _all_roots():
                try:
                    for day in os.listdir(root):
                        c = os.path.join(root, day, log_dir)
                        if os.path.isdir(c):
                            return c
                except OSError:
                    continue
        return _current_log_dir

    def anthropic_log_dir() -> str:
        return get_log_dir("logs_anthropic")

    def openai_log_dir() -> str:
        return get_log_dir("logs_openai")

    @app.get("/logs/dirs")
    def logs_dirs():
        """列出所有配置 root 下的日志目录，当前活跃目录排在第一个。

        数据源优先用 leaf_status（同步阶段已落库的 dir_key + entry_count）：
        实测与 refresh_index 重算逐条一致，但 0.018s vs 0.821s。只有 leaf_status
        尚未覆盖的 root（本地目录 / 未同步源）才回退到即时扫描。
        """
        from utils.stats_index import refresh_index, get_dir_counts
        from utils.token_index import refresh_token_index
        from utils.logs_config import get_path_name, get_root_id
        import utils.logdir_store as _lds

        current_tag = Path(_current_log_dir).name
        roots = _all_roots()
        multi = len(roots) > 1
        dirs = []
        current_name = current_tag

        # 一次读出全部 root 的目录列表（内部有签名缓存，命中时零成本）
        try:
            known = _lds.dir_list() if _lds._ready() else {}
        except Exception:  # noqa: BLE001
            known = {}

        for root in roots:
            root_path = Path(root)
            if not root_path.is_dir():
                continue
            # 多 root 时用稳定 root_id 作前缀区分来源（消除同 basename 冲突）
            root_id = get_root_id(root, _env_dir)
            # 活跃 env_dir 名称固定 default；历史根取配置名称
            is_active_root = (os.path.normpath(root) == os.path.normpath(_env_dir))
            root_label = "default" if is_active_root else get_path_name(root)

            entries = known.get(root_id) or []
            if entries:
                # 快路径：直接用同步产物。历史目录的形状与计数不会变，无需重算。
                pairs = [(e["dir_key"], e["count"]) for e in entries]
                # 当前小时的叶子通常还没被同步进 leaf_status（watcher 有延迟）。
                # 这里只补一条占位（count=None），**不能**为它触发 refresh_index：
                # 那是整个 root 的重扫（实测冷态 3.7s）。当前小时本来也还在增长，
                # 显示精确条数没有意义，前端在 count 为空时就不显示数字。
                if current_tag not in {k for k, _ in pairs}:
                    pairs.append((current_tag, None))
            else:
                # 回退：该 root 还没同步过，只能即时扫（本地目录等少量场景）
                index = refresh_index(root_path)
                tok_index = refresh_token_index(root)
                pairs = list(get_dir_counts(index, tok_index).items())

            for rel_key, count in pairs:
                is_current = (is_active_root and rel_key == current_tag)
                name = f"{root_id}::{rel_key}" if multi else rel_key
                if is_current:
                    current_name = name
                dirs.append({
                    "name": name,
                    "current": is_current,
                    "count": count,
                    "root": root_id,
                    "root_label": root_label,
                })
        # 当前目录优先，其余按名称倒序
        dirs.sort(key=lambda d: (not d["current"], d["name"]), reverse=False)
        dirs[1:] = sorted(dirs[1:], key=lambda d: d["name"], reverse=True)
        return JSONResponse({"dirs": dirs, "current": current_name})

    # --- 统一路由（新） ---

    @app.get("/logs/list")
    def logs_list(min_messages: int = 10, offset: int = 0, limit: int = 50, api_key: str = "", refresh: bool = False, model: str = "", log_dir: str = ""):
        return JSONResponse(_list_payload("anthropic", resolve_log_dir(log_dir), min_messages, offset, limit, api_key, refresh, model))

    @app.get("/logs/file")
    def logs_file(filename: str, log_dir: str = ""):
        # new-api：合并文件（文件名以 .json 结尾但非 -req.json），拆解为 messages + assistant
        if (filename.endswith(".json") and not filename.endswith("-req.json")
                and "/" not in filename and "\\" not in filename and ".." not in filename):
            target_dir = resolve_log_dir(log_dir)
            if _root_format(target_dir) != "newapi":
                # 没指定 log_dir（例如只带了 source）时，resolve_log_dir 会回落成本服务
                # 自己的活跃目录（native 格式），合并文件在那边根本不存在。这里按文件名
                # 在全部来源里找一次，避免点开条目就 400 / 404。
                found = _export_view_find_file(_all_roots(), _env_dir, filename)
                if found:
                    target_dir = os.path.dirname(found)
            if _root_format(target_dir) == "newapi":
                from utils.newapi_format import build_preview_payload
                path = os.path.join(target_dir, filename)
                if not os.path.isfile(path):
                    return JSONResponse({"error": "file not found"}, status_code=404)
                payload = build_preview_payload(Path(path))
                if payload is None:
                    return JSONResponse({"error": "parse failed"}, status_code=500)
                return JSONResponse(payload)
        if not filename.endswith("-req.json") or "/" in filename or "\\" in filename or ".." in filename:
            return JSONResponse({"error": "invalid filename"}, status_code=400)
        if log_dir == "__ALL__":
            found = _find_file_in_all_dirs(_env_dir, filename)
            if not found:
                return JSONResponse({"error": "file not found"}, status_code=404)
            path = found
        else:
            target_dir = resolve_log_dir(log_dir)
            path = os.path.join(target_dir, filename)
            if not os.path.isfile(path):
                # fallback: 在当前目录和 legacy 目录中搜索
                for search_dir in [unified_log_dir(), anthropic_log_dir(), openai_log_dir()]:
                    alt = os.path.join(search_dir, filename)
                    if os.path.isfile(alt):
                        path = alt
                        break
                else:
                    return JSONResponse({"error": "file not found"}, status_code=404)
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        res_path = Path(path).with_name(filename.replace("-req.json", "-res.json"))
        res_content = _extract_anthropic_res_content(res_path)
        if res_content is not None and isinstance(data.get("messages"), list):
            data["messages"].append({"role": "assistant", "content": res_content, "_from_res": True})
        elif isinstance(data.get("messages"), list):
            openai_content = _extract_openai_res_content(res_path)
            if openai_content is not None:
                data["messages"].append({**openai_content, "_from_res": True})
        res_data = _load_json(res_path)
        usage = extract_res_usage(res_data) if res_data else None
        if usage:
            data["_usage"] = usage
        return JSONResponse(data)

    @app.get("/logs/file/download")
    def logs_file_download(filename: str, log_dir: str = ""):
        if "/" in filename or "\\" in filename or ".." in filename:
            return JSONResponse({"error": "invalid filename"}, status_code=400)
        # new-api 合并文件：文件名以 .json 结尾但非三元组后缀（同 /logs/file、/logs/file/raw 的处理）。
        # 只在确认目标是 new-api 叶子时才放行，避免放宽普通目录下任意 .json 的下载范围。
        if (not any(filename.endswith(s) for s in _RAW_SUFFIXES)
                and filename.endswith(".json")):
            if log_dir == "__ALL__":
                found = _export_view_find_file(_all_roots(), _env_dir, filename)
                if not found:
                    return JSONResponse({"error": "file not found"}, status_code=404)
                return FileResponse(found, filename=filename, media_type="application/json")
            target_dir = resolve_log_dir(log_dir)
            if _root_format(target_dir) == "newapi":
                path = os.path.join(target_dir, filename)
                if not os.path.isfile(path):
                    return JSONResponse({"error": "file not found"}, status_code=404)
                return FileResponse(path, filename=filename, media_type="application/json")
            return JSONResponse({"error": "invalid filename"}, status_code=400)
        if not filename.endswith("-req.json"):
            return JSONResponse({"error": "invalid filename"}, status_code=400)
        if log_dir == "__ALL__":
            found = _find_file_in_all_dirs(_env_dir, filename)
            if not found:
                return JSONResponse({"error": "file not found"}, status_code=404)
            path = found
        else:
            target_dir = resolve_log_dir(log_dir)
            path = os.path.join(target_dir, filename)
            if not os.path.isfile(path):
                for search_dir in [unified_log_dir(), anthropic_log_dir(), openai_log_dir()]:
                    alt = os.path.join(search_dir, filename)
                    if os.path.isfile(alt):
                        path = alt
                        break
                else:
                    return JSONResponse({"error": "file not found"}, status_code=404)
        return FileResponse(path, filename=filename, media_type="application/json")

    _RAW_SUFFIXES = ("-req.json", "-headers.json", "-res.json")

    @app.get("/logs/file/raw")
    def logs_file_raw(filename: str, log_dir: str = ""):
        """返回原始 JSON 文件内容（支持 req / headers / res 三种后缀，及 new-api 合并文件）。"""
        if "/" in filename or "\\" in filename or ".." in filename:
            return JSONResponse({"error": "invalid filename"}, status_code=400)
        # new-api 合并文件：文件名以 .json 结尾但非三元组后缀，限定 new-api root
        if (not any(filename.endswith(s) for s in _RAW_SUFFIXES)
                and filename.endswith(".json")):
            target_dir = resolve_log_dir(log_dir)
            if _root_format(target_dir) == "newapi":
                path = os.path.join(target_dir, filename)
                if not os.path.isfile(path):
                    return JSONResponse({"error": "file not found"}, status_code=404)
                with open(path, "r", encoding="utf-8") as f:
                    return JSONResponse(json.load(f))
            return JSONResponse({"error": "invalid filename suffix"}, status_code=400)
        if not any(filename.endswith(s) for s in _RAW_SUFFIXES):
            return JSONResponse({"error": "invalid filename suffix"}, status_code=400)
        if log_dir == "__ALL__":
            req_name = filename
            for s in _RAW_SUFFIXES:
                if filename.endswith(s):
                    req_name = filename[:-len(s)] + "-req.json"
                    break
            found = _find_file_in_all_dirs(_env_dir, req_name)
            if found:
                path = os.path.join(os.path.dirname(found), filename)
            else:
                return JSONResponse({"error": "file not found"}, status_code=404)
        else:
            target_dir = resolve_log_dir(log_dir)
            path = os.path.join(target_dir, filename)
            if not os.path.isfile(path):
                for search_dir in [unified_log_dir(), anthropic_log_dir(), openai_log_dir()]:
                    alt = os.path.join(search_dir, filename)
                    if os.path.isfile(alt):
                        path = alt
                        break
        if not os.path.isfile(path):
            return JSONResponse({"error": "file not found"}, status_code=404)
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return JSONResponse(data)

    @app.get("/logs/aggregate")
    def logs_aggregate(min_messages: int = 1, offset: int = 0, limit: int = 50, api_key: str = "", refresh: bool = False, model: str = "", log_dir: str = "", search: str = "", q1search: str = ""):
        return JSONResponse(_aggregate_payload("anthropic", resolve_log_dir(log_dir), min_messages, offset, limit, api_key, refresh, model, search, q1search))

    # 内部「对话浏览」页的流式聚合：与公开导出浏览共用同一套后台任务 + 结果缓存。
    # 不提供这条路由的话，内部页只能走上面的同步 /logs/aggregate，每次重新收集、
    # 且享受不到 900s 缓存（单叶实测 3.6s，多叶线性累加）。
    def _inner_leaves(log_dir: str, source: str = "") -> List[str]:
        """内部页的叶子集。

        任何情形都**不会**枚举一个来源的全部叶子（那会把默认视图放大几个数量级，
        且每个新请求重开一个全量任务，把进程 CPU/内存拖满）：
          - log_dir 指定了具体小时目录 → 只读那一个叶子
          - 只给了 source → 读该来源**最新的一小时**
          - 都没给 → 空集，前端显示「请选择时间目录」
        """
        if log_dir:
            target = resolve_log_dir(log_dir)
            return [target] if Path(target).is_dir() else []
        if source:
            newest = _newest_leaf(source)
            return [newest] if newest else []
        return []

    def _newest_leaf(source: str) -> str:
        """该来源下 dir_key 最新（末段小时最大）的一个叶子。

        页面默认落点是「最新一小时」而不是整个来源：后者动辄上千个叶子，
        一次收集要几分钟且把 CPU 打满。
        """
        leaves = _source_leaves(source)
        if not leaves:
            return ""
        def _hour(p: str) -> str:
            return os.path.basename(os.path.normpath(p))
        try:
            return max(leaves, key=lambda p: (_hour(p), p))
        except ValueError:
            return ""

    def _source_leaves(source: str) -> List[str]:
        """按 root_id 取一个来源下所有已登记的、磁盘上存在的叶子。"""
        import utils.logdir_store as _lds
        from utils.logs_config import get_root_id
        rid = source
        # 兼容老链接里传的是 basename / 绝对路径
        for root in _all_roots():
            if (get_root_id(root, _env_dir) == source
                    or os.path.basename(os.path.normpath(root)) == source
                    or os.path.normpath(root) == source):
                rid = get_root_id(root, _env_dir)
                break
        try:
            rows = _lds.bulk_get(rid)
        except Exception:  # noqa: BLE001
            return []
        leaves: List[str] = []
        for row in rows:
            lp = row.get("leaf_path") or ""
            if lp and Path(lp).is_dir():
                leaves.append(lp)
        return leaves

    @app.get("/logs/aggregate/stream/start")
    def logs_aggregate_stream_start(min_messages: int = 1, api_key: str = "", model: str = "",
                                    refresh: bool = False, log_dir: str = "", source: str = ""):
        tid = export_view_aggregate_stream_start(
            _all_roots(), _env_dir, min_messages, api_key, model,
            refresh=refresh, leaves=_inner_leaves(log_dir, source))
        return JSONResponse({"task_id": tid})

    @app.get("/logs/aggregate/stream/{task_id}")
    def logs_aggregate_stream_poll(task_id: str):
        return JSONResponse(export_view_aggregate_stream_poll(task_id))

    @app.get("/logs/session-traces")
    def logs_session_traces(filename: str = "", api_key: str = "", log_dir: str = "",
                           chain_lookup: str = ""):
        """按会话的某个请求文件名取该会话的 trace 明细（「关联列表」）。

        列表接口只回 file_count；前端点开会话后才调这里，避免每次列表都把全部
        明细搬一遍。指定 log_dir 时直接在该叶子内查找，省掉跨叶子枚举。
        """
        if not filename:
            return JSONResponse({"items": [], "error": "missing filename"}, status_code=400)
        if log_dir:
            target = resolve_log_dir(log_dir)
            if _root_format(target) == "newapi":
                sk = nidb.session_key_for_file(target, filename)
                if sk:
                    return JSONResponse({
                        "items": nidb.get_traces_for_session(target, sk),
                        "session_key": sk,
                    })
        return JSONResponse(session_traces_payload(_all_roots(), _env_dir, api_key, filename,
                                                   chain_lookup=chain_lookup))

    # --- 旧路由（别名，向后兼容） ---

    @app.get("/logs/anthropic/list")
    def logs_anthropic_list(min_messages: int = 10, offset: int = 0, limit: int = 50, api_key: str = "", refresh: bool = False, model: str = ""):
        return JSONResponse(_list_payload("anthropic", unified_log_dir(), min_messages, offset, limit, api_key, refresh, model))

    @app.get("/logs/anthropic/file")
    def logs_anthropic_file(filename: str):
        return logs_file(filename)

    @app.get("/logs/anthropic/aggregate")
    def logs_anthropic_aggregate(min_messages: int = 1, offset: int = 0, limit: int = 50, api_key: str = "", refresh: bool = False, model: str = "", search: str = "", q1search: str = ""):
        return JSONResponse(_aggregate_payload("anthropic", unified_log_dir(), min_messages, offset, limit, api_key, refresh, model, search, q1search))

    @app.get("/logs/openai/list")
    def logs_openai_list(min_messages: int = 10, offset: int = 0, limit: int = 50, api_key: str = "", refresh: bool = False, model: str = ""):
        return JSONResponse(_list_payload("anthropic", unified_log_dir(), min_messages, offset, limit, api_key, refresh, model))

    @app.get("/logs/openai/file")
    def logs_openai_file(filename: str):
        return logs_file(filename)

    @app.get("/logs/openai/aggregate")
    def logs_openai_aggregate(min_messages: int = 1, offset: int = 0, limit: int = 50, api_key: str = "", refresh: bool = False, model: str = "", search: str = "", q1search: str = ""):
        return JSONResponse(_aggregate_payload("anthropic", unified_log_dir(), min_messages, offset, limit, api_key, refresh, model, search, q1search))

    # start_session_cache_warmer(_env_dir, _current_log_dir)
