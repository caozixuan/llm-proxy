"""
utils/session_store.py — Session 持久化存储（SQLite）

表结构：
  sessions      — session 摘要（q1/first_ts/last_ts/models/latest_file/msg_count 等）
  traces        — 每次请求的 trace 记录（filename/model/msg_count/ts/success 等）
  chain_index   — lookup_key → session_key 映射（完整 key，不截断）
  index_progress — root_dir → byte_offset/line_count（index.jsonl 消费进度）

所有写操作在 _lock 内执行，WAL 模式支持并发读。
"""

import json
import os
import sqlite3
import threading
from typing import Any, Dict, List, Optional, Tuple

_lock = threading.Lock()
_conn: Optional[sqlite3.Connection] = None
_db_path: str = ""

# traces 批量查询的 IN 分批大小。SQLITE_MAX_VARIABLE_NUMBER 在 3.32+ 为 32767、
# 更早版本仅 999；全量导出时 session_keys 可能上十万，故按低于下限的固定批切分。
_TRACES_IN_CHUNK = 500


def init_db(db_dir: str = "data") -> None:
    global _conn, _db_path
    _db_path = os.path.join(db_dir, "session_cache.db")
    os.makedirs(db_dir, exist_ok=True)
    _conn = sqlite3.connect(_db_path, check_same_thread=False)
    _conn.row_factory = sqlite3.Row
    _conn.execute("PRAGMA journal_mode=WAL")
    _conn.execute("PRAGMA synchronous=NORMAL")
    _conn.execute("PRAGMA busy_timeout=5000")
    with _lock:
        _conn.executescript("""
            CREATE TABLE IF NOT EXISTS sessions (
                session_key      TEXT PRIMARY KEY,
                root_dir         TEXT NOT NULL,
                api_key          TEXT NOT NULL DEFAULT '',
                q1               TEXT NOT NULL DEFAULT '',
                first_ts         TEXT NOT NULL DEFAULT '',
                last_ts          TEXT NOT NULL DEFAULT '',
                models           TEXT NOT NULL DEFAULT '[]',
                latest_file      TEXT NOT NULL DEFAULT '',
                msg_count        INTEGER NOT NULL DEFAULT 0,
                max_real_turns   INTEGER NOT NULL DEFAULT 0,
                best_req_count   INTEGER NOT NULL DEFAULT 0
            );

            CREATE INDEX IF NOT EXISTS sessions_root_dir ON sessions(root_dir);
            CREATE INDEX IF NOT EXISTS sessions_last_ts  ON sessions(last_ts);
            CREATE INDEX IF NOT EXISTS sessions_api_key  ON sessions(api_key);

            CREATE TABLE IF NOT EXISTS traces (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                session_key    TEXT NOT NULL,
                root_dir       TEXT NOT NULL,
                filename       TEXT NOT NULL,
                model          TEXT NOT NULL DEFAULT '',
                msg_count      INTEGER NOT NULL DEFAULT 0,
                ts             TEXT NOT NULL DEFAULT '',
                success        INTEGER NOT NULL DEFAULT 1,
                total_attempts INTEGER NOT NULL DEFAULT 1,
                debug_file     TEXT NOT NULL DEFAULT ''
            );

            CREATE INDEX IF NOT EXISTS traces_session_key ON traces(session_key);
            CREATE INDEX IF NOT EXISTS traces_root_dir    ON traces(root_dir);
            -- 覆盖索引：get_session_stats 的 (root_dir GROUP BY session_key) 计数走它，
            -- 免 TEMP B-TREE 且不碰主数据页。大 traces 表（数百万行）上把
            -- 导出/统计页的按叶子聚合从数十秒降到 ~1s，是「导出一直加载中」的根治。
            CREATE INDEX IF NOT EXISTS traces_root_sess    ON traces(root_dir, session_key);

            CREATE TABLE IF NOT EXISTS chain_index (
                lookup_key  TEXT PRIMARY KEY,
                session_key TEXT NOT NULL,
                root_dir    TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS chain_index_root_dir ON chain_index(root_dir);

            CREATE TABLE IF NOT EXISTS index_progress (
                root_dir    TEXT PRIMARY KEY,
                byte_offset INTEGER NOT NULL DEFAULT 0,
                line_count  INTEGER NOT NULL DEFAULT 0
            );
        """)
        _conn.commit()


def _get_conn() -> sqlite3.Connection:
    if _conn is None:
        raise RuntimeError("session_store not initialized, call init_db() first")
    return _conn


# ---------------------------------------------------------------------------
# index_progress
# ---------------------------------------------------------------------------

def get_progress(root_dir: str) -> Tuple[int, int]:
    """返回 (byte_offset, line_count)。"""
    conn = _get_conn()
    row = conn.execute(
        "SELECT byte_offset, line_count FROM index_progress WHERE root_dir = ?",
        (root_dir,),
    ).fetchone()
    if row:
        return row["byte_offset"], row["line_count"]
    return 0, 0


def get_all_progress() -> Dict[str, int]:
    """返回 {root_dir: byte_offset}，供批量判定叶子是否已「追平」index.jsonl。

    「数据管理」同步（newapi_backfill.sync_leaves）用它一次性拿到该库所有叶子的
    消费进度，避免逐叶 get_progress 的 N 次查询。未初始化（_conn is None）时返回
    空 dict —— 软失败：调用方据此把叶子一律判为「未追平/未 built」，不抛异常拖垮同步。
    """
    if _conn is None:
        return {}
    with _lock:
        rows = _conn.execute(
            "SELECT root_dir, byte_offset FROM index_progress"
        ).fetchall()
    return {r["root_dir"]: r["byte_offset"] for r in rows}



def set_progress(root_dir: str, byte_offset: int, line_count: int) -> None:
    conn = _get_conn()
    with _lock:
        conn.execute(
            """INSERT INTO index_progress(root_dir, byte_offset, line_count)
               VALUES(?, ?, ?)
               ON CONFLICT(root_dir) DO UPDATE SET
                 byte_offset = excluded.byte_offset,
                 line_count  = excluded.line_count""",
            (root_dir, byte_offset, line_count),
        )
        conn.commit()


# ---------------------------------------------------------------------------
# chain_index
# ---------------------------------------------------------------------------

def get_session_key_by_lookup(root_dir: str, lookup_key: str) -> Optional[str]:
    conn = _get_conn()
    row = conn.execute(
        "SELECT session_key FROM chain_index WHERE root_dir = ? AND lookup_key = ?",
        (root_dir, lookup_key),
    ).fetchone()
    return row["session_key"] if row else None


def set_chain_index(root_dir: str, lookup_key: str, session_key: str) -> None:
    conn = _get_conn()
    with _lock:
        conn.execute(
            """INSERT INTO chain_index(lookup_key, session_key, root_dir)
               VALUES(?, ?, ?)
               ON CONFLICT(lookup_key) DO UPDATE SET
                 session_key = excluded.session_key""",
            (lookup_key, session_key, root_dir),
        )
        conn.commit()


def get_all_chain_index(root_dir: str) -> Dict[str, str]:
    """返回 {lookup_key: session_key}，用于启动时重建内存 chain_map。"""
    conn = _get_conn()
    rows = conn.execute(
        "SELECT lookup_key, session_key FROM chain_index WHERE root_dir = ?",
        (root_dir,),
    ).fetchall()
    return {r["lookup_key"]: r["session_key"] for r in rows}


# ---------------------------------------------------------------------------
# sessions
# ---------------------------------------------------------------------------

def get_session(session_key: str) -> Optional[Dict[str, Any]]:
    conn = _get_conn()
    row = conn.execute(
        "SELECT * FROM sessions WHERE session_key = ?", (session_key,)
    ).fetchone()
    return _row_to_session(row) if row else None


def create_session(root_dir: str, session_key: str, data: Dict[str, Any]) -> None:
    conn = _get_conn()
    with _lock:
        conn.execute(
            """INSERT OR IGNORE INTO sessions
               (session_key, root_dir, api_key, q1, first_ts, last_ts,
                models, latest_file, msg_count, max_real_turns, best_req_count)
               VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
            (
                session_key,
                root_dir,
                data.get("api_key", ""),
                data.get("q1", ""),
                data.get("first_ts", ""),
                data.get("last_ts", ""),
                json.dumps(data.get("models", []), ensure_ascii=False),
                data.get("latest_file", ""),
                data.get("msg_count", 0),
                data.get("_max_real_turns", 0),
                data.get("_best_req_count", 0),
            ),
        )
        conn.commit()


def update_session(session_key: str, data: Dict[str, Any]) -> None:
    conn = _get_conn()
    with _lock:
        conn.execute(
            """UPDATE sessions SET
               last_ts        = ?,
               models         = ?,
               latest_file    = ?,
               msg_count      = ?,
               max_real_turns = ?,
               best_req_count = ?
               WHERE session_key = ?""",
            (
                data.get("last_ts", ""),
                json.dumps(data.get("models", []), ensure_ascii=False),
                data.get("latest_file", ""),
                data.get("msg_count", 0),
                data.get("_max_real_turns", 0),
                data.get("_best_req_count", 0),
                session_key,
            ),
        )
        conn.commit()


def list_sessions(
    root_dir: str,
    api_key: str = "",
    model: str = "",
    min_msg_count: int = 0,
    offset: int = 0,
    limit: int = 0,
) -> List[Dict[str, Any]]:
    conn = _get_conn()
    sql = "SELECT * FROM sessions WHERE root_dir = ?"
    params: list = [root_dir]
    if api_key:
        sql += " AND api_key = ?"
        params.append(api_key)
    if min_msg_count > 0:
        sql += " AND msg_count >= ?"
        params.append(min_msg_count)
    if model:
        # models 是 JSON 数组，用 json_each 做精确匹配
        sql += " AND EXISTS (SELECT 1 FROM json_each(models) WHERE value = ?)"
        params.append(model)
    sql += " ORDER BY last_ts DESC"
    if limit > 0:
        sql += " LIMIT ? OFFSET ?"
        params.extend([limit, offset])
    rows = conn.execute(sql, params).fetchall()
    return [_row_to_session(r) for r in rows]


def count_sessions(
    root_dir: str,
    api_key: str = "",
    model: str = "",
    min_msg_count: int = 0,
) -> int:
    conn = _get_conn()
    sql = "SELECT COUNT(*) AS c FROM sessions WHERE root_dir = ?"
    params: list = [root_dir]
    if api_key:
        sql += " AND api_key = ?"
        params.append(api_key)
    if min_msg_count > 0:
        sql += " AND msg_count >= ?"
        params.append(min_msg_count)
    if model:
        sql += " AND EXISTS (SELECT 1 FROM json_each(models) WHERE value = ?)"
        params.append(model)
    row = conn.execute(sql, params).fetchone()
    return row["c"] if row else 0


def get_known_models(root_dir: str) -> List[str]:
    """返回该目录下所有出现过的 model 列表（去重排序）。"""
    conn = _get_conn()
    rows = conn.execute(
        "SELECT DISTINCT value FROM sessions, json_each(sessions.models) WHERE sessions.root_dir = ? AND value != '' ORDER BY value",
        (root_dir,),
    ).fetchall()
    return [r["value"] for r in rows]


def get_session_count_by_root(root_dir: str) -> int:
    conn = _get_conn()
    row = conn.execute(
        "SELECT COUNT(*) AS c FROM sessions WHERE root_dir = ?", (root_dir,)
    ).fetchone()
    return row["c"] if row else 0


def get_all_session_counts() -> Dict[str, int]:
    """返回 {root_dir: session 数}，一次 GROUP BY 拿全库各叶子的会话数。

    与 get_all_progress() 配套：「数据管理」同步时批量回填 leaf_status.sessions，
    避免逐叶 get_session_count_by_root 的 N 次查询。未初始化返回空 dict（软失败）。
    """
    if _conn is None:
        return {}
    with _lock:
        rows = _conn.execute(
            "SELECT root_dir, COUNT(*) AS c FROM sessions GROUP BY root_dir"
        ).fetchall()
    return {r["root_dir"]: r["c"] for r in rows}


def get_model_stats_by_key(root_dir: str = "") -> Dict[str, Dict[str, int]]:
    """返回 {api_key: {model: session_count}}，供 Key 列表展示「主要模型」分布。

    一个 session 的 models 是 JSON 数组（一次会话可能跨多 model），用 json_each
    展开后按 (api_key, model) 计数。空串 model 跳过。与 mtime 分布同口径（按 session 计）。

    root_dir 省略时聚合当前 DB 里所有叶子（DB 已按 service 绑定，Key 列表也是跨叶子的），
    传入具体叶子路径则只统计该叶子。
    """
    conn = _get_conn()
    sql = (
        "SELECT sessions.api_key AS api_key, je.value AS model, COUNT(*) AS c "
        "FROM sessions, json_each(sessions.models) je "
        "WHERE je.value != ''"
    )
    params: list = []
    if root_dir:
        sql += " AND sessions.root_dir = ?"
        params.append(root_dir)
    sql += " GROUP BY sessions.api_key, je.value"
    rows = conn.execute(sql, params).fetchall()
    out: Dict[str, Dict[str, int]] = {}
    for r in rows:
        ak = r["api_key"] or "(empty)"
        out.setdefault(ak, {})[r["model"]] = r["c"]
    return out


def delete_root(root_dir: str) -> None:
    """删除某 root_dir 的全部聚合行（sessions/traces/chain_index/index_progress）。

    用于回填前清空重建，保证幂等（避免 traces 裸 INSERT 重复插入）。
    """
    conn = _get_conn()
    with _lock:
        conn.execute("DELETE FROM sessions WHERE root_dir = ?", (root_dir,))
        conn.execute("DELETE FROM traces WHERE root_dir = ?", (root_dir,))
        conn.execute("DELETE FROM chain_index WHERE root_dir = ?", (root_dir,))
        conn.execute("DELETE FROM index_progress WHERE root_dir = ?", (root_dir,))
        conn.commit()


def _row_to_session(row: sqlite3.Row) -> Dict[str, Any]:
    d = dict(row)
    try:
        d["models"] = json.loads(d.get("models") or "[]")
    except (json.JSONDecodeError, TypeError):
        d["models"] = []
    d["_max_real_turns"] = d.pop("max_real_turns", 0)
    d["_best_req_count"] = d.pop("best_req_count", 0)
    return d


# ---------------------------------------------------------------------------
# traces
# ---------------------------------------------------------------------------

def append_trace(root_dir: str, session_key: str, trace: Dict[str, Any]) -> None:
    conn = _get_conn()
    with _lock:
        conn.execute(
            """INSERT INTO traces
               (session_key, root_dir, filename, model, msg_count, ts,
                success, total_attempts, debug_file)
               VALUES(?,?,?,?,?,?,?,?,?)""",
            (
                session_key,
                root_dir,
                trace.get("filename", ""),
                trace.get("model", ""),
                trace.get("msg_count", 0),
                trace.get("ts", ""),
                0 if trace.get("success") is False else 1,
                trace.get("total_attempts", 1),
                trace.get("debug_file", ""),
            ),
        )
        conn.commit()


def get_traces(session_key: str) -> List[Dict[str, Any]]:
    conn = _get_conn()
    rows = conn.execute(
        "SELECT * FROM traces WHERE session_key = ? ORDER BY ts ASC, id ASC",
        (session_key,),
    ).fetchall()
    return [_row_to_trace(r) for r in rows]


def get_traces_batch(session_keys: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    """批量获取多个 session 的 trace_list，减少查询次数。"""
    if not session_keys:
        return {}
    conn = _get_conn()
    # 按固定批大小切分：单条 SQL 的变量数受 SQLITE_MAX_VARIABLE_NUMBER 限制
    # （3.32+ 为 32767，更早为 999），全量导出时 session_keys 可能上十万，
    # 一次性拼 IN 会抛 "too many SQL variables"。
    rows = []
    for i in range(0, len(session_keys), _TRACES_IN_CHUNK):
        chunk = session_keys[i:i + _TRACES_IN_CHUNK]
        placeholders = ",".join("?" * len(chunk))
        rows.extend(conn.execute(
            f"SELECT * FROM traces WHERE session_key IN ({placeholders}) ORDER BY ts ASC, id ASC",
            chunk,
        ).fetchall())
    result: Dict[str, List[Dict[str, Any]]] = {k: [] for k in session_keys}
    for row in rows:
        result[row["session_key"]].append(_row_to_trace(row))
    return result


def _row_to_trace(row: sqlite3.Row) -> Dict[str, Any]:
    d = dict(row)
    t: Dict[str, Any] = {
        "filename": d["filename"],
        "model": d["model"],
        "msg_count": d["msg_count"],
        "ts": d["ts"],
    }
    if not d["success"]:
        t["success"] = False
        t["total_attempts"] = d["total_attempts"]
    if d["debug_file"]:
        t["debug_file"] = d["debug_file"]
    return t


# ---------------------------------------------------------------------------
# 批量写入（用于历史迁移 / compact）
# ---------------------------------------------------------------------------

def bulk_insert(root_dir: str, sessions_data: List[Dict[str, Any]]) -> None:
    """从内存 state 批量写入 DB，用于首次迁移或测试。sessions_data 每项含 trace_list。"""
    conn = _get_conn()
    with _lock:
        for s in sessions_data:
            session_key = s.get("_key") or s.get("first_ts", "")
            if not session_key:
                continue
            conn.execute(
                """INSERT OR REPLACE INTO sessions
                   (session_key, root_dir, api_key, q1, first_ts, last_ts,
                    models, latest_file, msg_count, max_real_turns, best_req_count)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    session_key,
                    root_dir,
                    s.get("api_key", ""),
                    s.get("q1", ""),
                    s.get("first_ts", ""),
                    s.get("last_ts", ""),
                    json.dumps(s.get("models", []), ensure_ascii=False),
                    s.get("latest_file", ""),
                    s.get("msg_count", 0),
                    s.get("_max_real_turns", 0),
                    s.get("_best_req_count", 0),
                ),
            )
            for trace in s.get("trace_list", []):
                conn.execute(
                    """INSERT INTO traces
                       (session_key, root_dir, filename, model, msg_count, ts,
                        success, total_attempts, debug_file)
                       VALUES(?,?,?,?,?,?,?,?,?)""",
                    (
                        session_key,
                        root_dir,
                        trace.get("filename", ""),
                        trace.get("model", ""),
                        trace.get("msg_count", 0),
                        trace.get("ts", ""),
                        0 if trace.get("success") is False else 1,
                        trace.get("total_attempts", 1),
                        trace.get("debug_file", ""),
                    ),
                )
        conn.commit()


# ---------------------------------------------------------------------------
# Session 统计（供 stats_index 替换 _scan_session_cache）
# ---------------------------------------------------------------------------

def get_session_stats(root_dir: str, threshold: int = 5) -> Dict[str, Dict[str, int]]:
    """
    返回 {bucket_key: {total, qualified}}，bucket_key = "{api_key}|{date}"。
    替代 stats_index._scan_session_cache 直接读文件的做法。
    """
    conn = _get_conn()
    rows = conn.execute(
        """SELECT api_key, first_ts,
                  COUNT(*) OVER (PARTITION BY api_key, SUBSTR(first_ts, 1, 8)) AS dir_total,
                  session_key
           FROM sessions WHERE root_dir = ?""",
        (root_dir,),
    ).fetchall()

    # 用 Python 做聚合（避免复杂 SQL，兼容性更好）
    from collections import defaultdict
    buckets: Dict[str, Dict[str, int]] = defaultdict(lambda: {"total": 0, "qualified": 0})
    session_trace_counts: Dict[str, int] = {}

    # 批量获取 trace 数量
    count_rows = conn.execute(
        "SELECT session_key, COUNT(*) AS c FROM traces WHERE root_dir = ? GROUP BY session_key",
        (root_dir,),
    ).fetchall()
    for r in count_rows:
        session_trace_counts[r["session_key"]] = r["c"]

    session_rows = conn.execute(
        "SELECT session_key, api_key, first_ts FROM sessions WHERE root_dir = ?",
        (root_dir,),
    ).fetchall()

    for r in session_rows:
        api_key = r["api_key"] or "(empty)"
        ts = r["first_ts"] or ""
        date_str = ts.split("_")[0] if ts else "unknown"
        bk = f"{api_key}|{date_str}"
        trace_count = session_trace_counts.get(r["session_key"], 0)
        buckets[bk]["total"] += 1
        if trace_count >= threshold:
            buckets[bk]["qualified"] += 1

    return dict(buckets)


# ---------------------------------------------------------------------------
# 读取全部 sessions（供导出，替代 _read_session_cache）
# ---------------------------------------------------------------------------

def export_sessions(root_dir: str, api_key: str = "") -> List[Dict[str, Any]]:
    """
    返回完整 session 列表（含 trace_list），格式与原 .session_cache.jsonl 行兼容。
    供 export_sync.export_session_index 使用。

    api_key 非空时下推到 SQL，只取该 key 的 session 及其 traces（与 new-api 分支同语义）。
    """
    sessions = list_sessions(root_dir, api_key=api_key)
    if not sessions:
        return []
    session_keys = [s["session_key"] for s in sessions]
    traces_by_key = get_traces_batch(session_keys)
    result = []
    for s in sessions:
        sk = s["session_key"]
        out: Dict[str, Any] = {
            "_key": sk,
            "api_key": s["api_key"],
            "q1": s["q1"],
            "first_ts": s["first_ts"],
            "last_ts": s["last_ts"],
            "models": s["models"],
            "latest_file": s["latest_file"],
            "msg_count": s["msg_count"],
            "trace_list": traces_by_key.get(sk, []),
        }
        result.append(out)
    return result


def _fmt_ts(ts: str) -> Optional[str]:
    """下划线时间戳 → 'YYYY-MM-DD HH:MM:SS'；解析失败返回 None。"""
    if not ts:
        return None
    try:
        from utils.eval.eval import _parse_folder_ts
        dt = _parse_folder_ts(ts)
        return dt.strftime("%Y-%m-%d %H:%M:%S") if dt else None
    except Exception:
        return None


def to_unified_record(entry: Dict[str, Any]) -> Dict[str, Any]:
    """把 export_sessions 的一条记录转成统一 schema（version 2，兼容超集）。

    - 规范键：session/start_time/end_time/total_messages/api_call_count/
      models(list)/api_key/q1/latest_file/trace_list
    - 兼容别名：_key/first_ts/last_ts/msg_count（导出侧旧读取方仍依赖）
    - 评估字段：纯导出不填（由质检链填充）；trace_list.ts 保留下划线格式

    见 doc/examples/README_unified_format.md。
    """
    first_ts = entry.get("_key") or entry.get("first_ts", "")
    last_ts = entry.get("last_ts", "")
    trace_list = entry.get("trace_list", []) or []
    msg_count = entry.get("msg_count", 0)
    models = entry.get("models", []) or []
    return {
        # 规范键
        "session": first_ts,
        "api_key": entry.get("api_key", ""),
        "q1": entry.get("q1", ""),
        "models": models,
        "latest_file": entry.get("latest_file", ""),
        "start_time": _fmt_ts(first_ts),
        "end_time": _fmt_ts(last_ts),
        "total_messages": msg_count,
        "api_call_count": len(trace_list) or 1,
        "trace_list": trace_list,
        # 兼容别名（导出侧旧读取方）
        "_key": first_ts,
        "first_ts": first_ts,
        "last_ts": last_ts,
        "msg_count": msg_count,
    }
