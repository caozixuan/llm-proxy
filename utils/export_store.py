"""
utils/export_store.py — Session 导出记录持久化

SQLite 后端，记录每次导出操作的状态和结果。
数据库路径：{service_log_dir}/export_session_record.db
"""

import os
import socket
import sqlite3
import threading
import json
import time
from pathlib import Path
from typing import Optional

_lock = threading.Lock()
_conn: Optional[sqlite3.Connection] = None
_db_path: str = ""

# ── worker 归属 / 心跳 ─────────────────────────────────────────────────────
# 每条 running/pending 记录标记「哪个 worker 领的」+「最近一次心跳的 unix 秒」。
# 启动清理（cancel_interrupted）据此只清理「所属 worker 已不存活」的遗留任务，
# 而不是像以前那样把全表 running/queued/pending 一并取消 —— 后者在起第 2 个
# worker 时会误杀第 1 个正在跑的任务。
#
# owner 标识 = "<hostname>:<pid>:<单调启动随机串>"。hostname + pid 足以判断
# 「本机这个 worker 进程是否还活着」（os.kill(pid, 0)）；随机串用于防止 pid 复用
# 造成的误判（重启后新 worker 换了 pid 或同一 pid 但不同实例）。
_worker_id: str = ""

# 心跳超过这个秒数即视为「所属 worker 已死」（进程被 SIGKILL、机器异常重启等
# 无法走优雅退出的场景）。取 3 倍心跳间隔，容忍一次丢拍。
HEARTBEAT_TIMEOUT = float(os.getenv("EXPORT_WORKER_HEARTBEAT_TIMEOUT", "180"))


def _make_worker_id() -> str:
    import uuid
    return f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"


def set_worker_id(worker_id: str = "") -> str:
    """登记本进程的 worker 标识（进程内全局）。空串时自动生成。

    worker 进程启动时调用一次；之后 claim_next_queued() 把该标识写进记录的
    owner_worker，heartbeat() 刷新 owner_heartbeat。
    """
    global _worker_id
    _worker_id = worker_id or _make_worker_id()
    return _worker_id


def get_worker_id() -> str:
    if not _worker_id:
        set_worker_id()
    return _worker_id


def _pid_alive(pid: int) -> bool:
    """本机进程是否存活。EPERM 表示存在但无权限 —— 仍算存活。"""
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _owner_alive(owner: str, heartbeat_ts: float, timeout_sec: float = HEARTBEAT_TIMEOUT) -> bool:
    """判断某条记录的 owner 是否仍存活。

    判据：
      1) owner 指向本机：**以进程是否存在为准**（owner 是 claim 时写入、从不转手，
         进程没了任务就不可能还在跑）。owner 就是本 worker 自然也算存活。
         pid 复用只会导致「误判为存活 → 不清理」，不会误杀，方向安全。
      2) owner 不是本机（共享存储上的多机部署）：无法探活，退化为心跳超时判定。
    """
    if not owner:
        return False
    parts = owner.split(":", 2)
    host = parts[0] if parts else ""
    try:
        pid = int(parts[1]) if len(parts) > 1 else 0
    except ValueError:
        pid = 0
    if host == socket.gethostname():
        if owner == _worker_id:
            return True
        return _pid_alive(pid)
    # 跨机：只能看心跳新鲜度
    return (time.time() - float(heartbeat_ts or 0)) <= timeout_sec


def _get_export_log_dir() -> Path:
    """返回外部日志目录路径，与数据库同目录"""
    if _db_path:
        return Path(_db_path).parent / "export_log"
    return Path("logs") / "export_log"


def _write_external_log(record_id: int, field_name: str, content: str) -> str:
    """将字段内容写入外部文件，返回 file:// 路径标记"""
    log_dir = _get_export_log_dir()
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{record_id}.{field_name}.log"
    log_path.write_text(content, encoding="utf-8")
    return f"file://{log_path}"


def _read_field_content(value: str) -> str:
    """读取字段内容：如果是 file:// 路径则从文件读取，否则直接返回（兼容旧数据）"""
    if not value:
        return ""
    if value.startswith("file://"):
        file_path = Path(value[7:])  # 去掉 file:// 前缀
        if file_path.is_file():
            return file_path.read_text(encoding="utf-8")
        # 相对路径：尝试从数据库所在目录解析
        if _db_path and not file_path.is_absolute():
            abs_path = Path(_db_path).parent / file_path
            if abs_path.is_file():
                return abs_path.read_text(encoding="utf-8")
        return ""
    # 旧数据：直接存储的内容
    return value


def _externalize_field(record_id: int, field_name: str, content: str) -> str:
    """默认写入外部文件并返回路径标记"""
    return _write_external_log(record_id, field_name, content)


def init_db(db_dir: str):
    global _conn, _db_path
    _db_path = os.path.join(db_dir, "export_session_record.db")
    os.makedirs(db_dir, exist_ok=True)
    _conn = sqlite3.connect(_db_path, check_same_thread=False)
    _conn.execute("PRAGMA journal_mode=WAL")
    _conn.execute("PRAGMA busy_timeout=5000")
    _conn.row_factory = sqlite3.Row
    with _lock:
        _conn.execute("""
            CREATE TABLE IF NOT EXISTS export_records (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                api_key         TEXT NOT NULL DEFAULT '',
                key_slot        TEXT NOT NULL DEFAULT 'all',
                mtime_dirs      TEXT NOT NULL DEFAULT '[]',
                status          TEXT NOT NULL DEFAULT 'pending',
                error_message   TEXT NOT NULL DEFAULT '',
                total_sessions  INTEGER NOT NULL DEFAULT 0,
                files_uploaded  INTEGER NOT NULL DEFAULT 0,
                files_skipped   INTEGER NOT NULL DEFAULT 0,
                obs_dst         TEXT NOT NULL DEFAULT '',
                local_copy_dir  TEXT NOT NULL DEFAULT '',
                progress_log    TEXT NOT NULL DEFAULT '[]',
                created_at      TEXT NOT NULL DEFAULT (datetime('now','localtime')),
                started_at      TEXT,
                finished_at     TEXT
            )
        """)
        _conn.execute("CREATE INDEX IF NOT EXISTS idx_export_key_slot ON export_records(key_slot)")
        _conn.execute("CREATE INDEX IF NOT EXISTS idx_export_status ON export_records(status)")
        for col_def in [
            "progress_log TEXT NOT NULL DEFAULT '[]'",
            "eval_status TEXT NOT NULL DEFAULT ''",
            "eval_report_path TEXT NOT NULL DEFAULT ''",
            "mode TEXT NOT NULL DEFAULT 'export'",
            "analysis_json TEXT NOT NULL DEFAULT ''",
            "source_export_id INTEGER",
            "in_manage INTEGER NOT NULL DEFAULT 0",
            "manage_name TEXT NOT NULL DEFAULT ''",
            "key_name TEXT NOT NULL DEFAULT ''",
            "workers INTEGER NOT NULL DEFAULT 0",
            "dir_workers INTEGER NOT NULL DEFAULT 0",
            "leaves_cache TEXT NOT NULL DEFAULT ''",
            # 任务执行参数（JSON），供独立 export_worker 进程从 DB 重建执行上下文，
            # 不再依赖 app 进程内的内存闭包。见 utils/export_jobs.py。
            "task_json TEXT NOT NULL DEFAULT ''",
            # 软删除标记：删除任务=置 1，列表默认过滤掉。行与 obs_dst/local_copy_dir
            # 等指向云端/本地产物的元数据都保留，误删可恢复。见 delete_record()。
            "is_delete INTEGER NOT NULL DEFAULT 0",
            # 任务归属的 worker 标识（owner_worker）+ 最近心跳时间（owner_heartbeat，
            # unix 秒）。用于「多 worker 启动时只清理各自已死进程的遗留任务」：
            # 老库/老记录该列为空串/0，由 cancel_interrupted() 的兼容分支处理。
            # 见 claim_next_queued() / heartbeat() / cancel_interrupted()。
            "owner_worker TEXT NOT NULL DEFAULT ''",
            "owner_heartbeat REAL NOT NULL DEFAULT 0",
            # 进度：done/total 当前阶段已完成/总数（session 或目录，看 phase），
            # phase 是阶段名（生成索引 / 重构 / 上传 …）。供前端画进度条。
            # 由 update_progress() 节流写入，任务结束不清零，便于回看各阶段规模。
            "progress_done INTEGER NOT NULL DEFAULT 0",
            "progress_total INTEGER NOT NULL DEFAULT 0",
            "progress_phase TEXT NOT NULL DEFAULT ''",
            # 跨小时节点聚合开关：把同一条对话在多个小时目录里的段合并成一条产物。
            # 草稿任务也要记住它——否则「创建草稿 → 再启动」会丢掉这个选择。
            "merge_hours INTEGER NOT NULL DEFAULT 0",
        ]:
            try:
                _conn.execute(f"ALTER TABLE export_records ADD COLUMN {col_def}")
            except sqlite3.OperationalError:
                pass
        _conn.commit()


def _get_conn() -> sqlite3.Connection:
    if _conn is None:
        raise RuntimeError("export_store not initialized, call init_db() first")
    return _conn


def create_record(
    api_key: str,
    key_slot: str,
    mtime_dirs: str,
    obs_dst: str = "",
    local_copy_dir: str = "",
    mode: str = "export",
    source_export_id: int | None = None,
    key_name: str = "",
    workers: int = 0,
    dir_workers: int = 0,
    merge_hours: bool = False,
) -> int:
    conn = _get_conn()
    with _lock:
        cur = conn.execute(
            """INSERT INTO export_records
               (api_key, key_slot, mtime_dirs, obs_dst, local_copy_dir, mode, source_export_id, key_name, workers, dir_workers, merge_hours)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (api_key, key_slot, mtime_dirs, obs_dst, local_copy_dir, mode, source_export_id, key_name, workers, dir_workers, 1 if merge_hours else 0),
        )
        conn.commit()
    return cur.lastrowid


def update_progress(record_id: int, phase: str = "", done: int = 0, total: int = 0) -> None:
    """更新进度列（供前端进度条）。

    与 append_log 分开：append_log 每次都要读改写整段 JSON（O(n²)），而进度需要
    高频更新（每完成一个 session 一次）。这里只做一次单行 UPDATE，代价固定。
    调用方负责节流（见 export_routes._ProgressReporter）。
    """
    conn = _get_conn()
    with _lock:
        conn.execute(
            "UPDATE export_records SET progress_phase = ?, progress_done = ?, "
            "progress_total = ? WHERE id = ?",
            (phase or "", int(done or 0), int(total or 0), record_id),
        )
        conn.commit()


def update_status(record_id: int, status: str, **kwargs):
    conn = _get_conn()
    fields = ["status = ?"]
    values = [status]
    if status == "running":
        fields.append("started_at = datetime('now','localtime')")
    elif status in ("success", "failed", "cancelled"):
        fields.append("finished_at = datetime('now','localtime')")
    for k, v in kwargs.items():
        if k in ("error_message", "total_sessions", "files_uploaded", "files_skipped",
                 "obs_dst", "local_copy_dir", "eval_status", "eval_report_path",
                 "analysis_json", "key_name", "leaves_cache"):
            fields.append(f"{k} = ?")
            values.append(v)
    values.append(record_id)
    with _lock:
        conn.execute(f"UPDATE export_records SET {', '.join(fields)} WHERE id = ?", values)
        conn.commit()


def delete_record(record_id: int, *, purge: bool = False, purge_local: bool = False) -> Optional[dict]:
    """删除一条导出记录。

    返回被删记录（删前快照）；不存在返回 None。调用方负责状态守卫
    （running 记录不应删，避免删掉 worker 正在写的行）。

    默认软删除：仅置 is_delete=1，列表默认过滤掉。行与 obs_dst / local_copy_dir
    等元数据全部保留，误删可 restore_record() 恢复，也不会丢掉指向云端产物的唯一索引。

    purge=True 为永久删除（硬删）：真正 DELETE 行 + 清外置日志文件；再叠加
    purge_local=True 时递归删本地产物目录 local_copy_dir。硬删不可逆，需调用方显式请求。
    OBS 云端产物本函数一律不动。
    """
    conn = _get_conn()
    rec = get_record(record_id)
    if not rec:
        return None
    if not purge:
        # 软删除：改状态，元数据与产物一律保留
        with _lock:
            conn.execute("UPDATE export_records SET is_delete = 1 WHERE id = ?", (record_id,))
            conn.commit()
        return rec
    # ---- 以下为永久删除（硬删）路径 ----
    # 1) DB 行
    with _lock:
        conn.execute("DELETE FROM export_records WHERE id = ?", (record_id,))
        conn.commit()
    # 2) 外置日志文件：progress_log / analysis_json 等大字段以 file:// 落在 export_log/
    log_dir = _get_export_log_dir()
    if log_dir.is_dir():
        for p in log_dir.glob(f"{record_id}.*.log"):
            try:
                p.unlink()
            except OSError:
                pass
    # 3) 可选：本地产物目录（不可逆，默认不动）
    if purge_local:
        local = (rec.get("local_copy_dir") or "").strip()
        if local and os.path.isdir(local):
            import shutil
            try:
                shutil.rmtree(local)
            except OSError:
                pass
    return rec


def restore_record(record_id: int) -> Optional[dict]:
    """撤销软删除：is_delete 置 0。不存在返回 None。"""
    conn = _get_conn()
    rec = get_record(record_id)
    if not rec:
        return None
    with _lock:
        conn.execute("UPDATE export_records SET is_delete = 0 WHERE id = ?", (record_id,))
        conn.commit()
    return rec


def save_leaves_cache(record_id: int, leaves: list, warnings: list | None = None) -> None:
    """把某记录的节点分布结果落库（JSON），供之后展开直接读、免重复扫盘。"""
    payload = json.dumps({"leaves": leaves, "warnings": warnings or []}, ensure_ascii=False)
    conn = _get_conn()
    with _lock:
        conn.execute("UPDATE export_records SET leaves_cache = ? WHERE id = ?", (payload, record_id))
        conn.commit()


def get_leaves_cache(record_id: int) -> Optional[dict]:
    """读回缓存的节点分布；无缓存或解析失败返回 None。"""
    conn = _get_conn()
    with _lock:
        row = conn.execute("SELECT leaves_cache FROM export_records WHERE id = ?", (record_id,)).fetchone()
    if not row:
        return None
    raw = row["leaves_cache"] if "leaves_cache" in row.keys() else ""
    if not raw:
        return None
    try:
        return json.loads(raw)
    except (ValueError, TypeError):
        return None


def set_task_json(record_id: int, task_json: str) -> None:
    """持久化任务执行参数（JSON 字符串），供独立 export_worker 进程重建执行上下文。"""
    conn = _get_conn()
    with _lock:
        conn.execute("UPDATE export_records SET task_json = ? WHERE id = ?", (task_json, record_id))
        conn.commit()


def claim_next_queued() -> Optional[dict]:
    """worker 领取下一条待执行任务：取最老的 queued 记录，原子置为 pending 并返回该行。

    用 UPDATE ... WHERE id=(SELECT ... LIMIT 1) + changes() 判定是否抢到，配合共享连接
    的 _lock 保证同进程内单次领取；跨进程（多 worker）下 SQLite 行级串行 + 领取后立刻
    改状态，二次领取会落空（该行已非 queued）。返回 None 表示暂无待执行任务。

    置 pending（而非直接 running）：running 由 _run_task_inner 真正开跑时再置，
    使「已领取待跑」与「执行中」可区分；cancel_interrupted 会把二者一并清理。
    """
    conn = _get_conn()
    with _lock:
        row = conn.execute(
            "SELECT * FROM export_records WHERE status = 'queued' AND is_delete = 0 ORDER BY id LIMIT 1"
        ).fetchone()
        if not row:
            return None
        rec = dict(row)
        cur = conn.execute(
            "UPDATE export_records SET status = 'pending', owner_worker = ?, "
            "owner_heartbeat = ? WHERE id = ? AND status = 'queued'",
            (get_worker_id(), time.time(), rec["id"]),
        )
        conn.commit()
        if cur.rowcount == 0:
            return None  # 被其他 worker 抢先
    rec["status"] = "pending"
    rec["owner_worker"] = _worker_id
    return rec


def heartbeat(record_id: int, worker_id: str = "") -> None:
    """刷新该记录（及本 worker 持有的其它在途记录）的心跳时间。

    长任务（大叶导出可达几十分钟）期间必须持续续命，否则别的 worker 会用
    「心跳超时」把这条正在跑的任务判成遗留并取消。调用方（export_routes 的
    _run_task_inner 循环）在每个目录/检查点调一次即可，开销是一次点更新。

    worker_id 省略时用本进程标识；带上它是为了兼容「同一进程同时执行多条任务」
    的回退模式（EXPORT_QUEUE_IN_PROCESS=1 的 N 个 runner 线程）。
    """
    wid = worker_id or get_worker_id()
    conn = _get_conn()
    with _lock:
        conn.execute(
            "UPDATE export_records SET owner_heartbeat = ? "
            "WHERE id = ? AND owner_worker = ?",
            (time.time(), record_id, wid),
        )
        conn.commit()


def ensure_owner(record_id: int, worker_id: str = "") -> None:
    """把记录归属到自己名下（若尚未归属或已归属自己），并刷新心跳。

    覆盖两种「不经过 claim_next_queued」的执行路径：
      - EXPORT_QUEUE_IN_PROCESS=1 的回退模式（app 进程内 runner 直接调 _run_task）；
      - upload_retry（记录本来就存在，worker 直接把它置 running 后重传）。
    这两种情况若不登记 owner，别的 worker 启动时会把这条 running 记录当作
    「无归属的老记录」清掉。
    """
    wid = worker_id or get_worker_id()
    conn = _get_conn()
    with _lock:
        conn.execute(
            "UPDATE export_records SET owner_worker = ?, owner_heartbeat = ? "
            "WHERE id = ? AND (owner_worker = '' OR owner_worker = ?)",
            (wid, time.time(), record_id, wid),
        )
        conn.commit()


def heartbeat_owner(worker_id: str = "") -> int:
    """把本 worker 名下所有非终态记录的心跳一次性续上，返回受影响行数。

    用于长任务里的批量续命（进程内可能有多条 pending/running），比逐条点更新
    省事务。终态记录（success/failed/cancelled）不动，避免把已完成的行也拖着走。
    """
    wid = worker_id or get_worker_id()
    conn = _get_conn()
    with _lock:
        cur = conn.execute(
            "UPDATE export_records SET owner_heartbeat = ? "
            "WHERE owner_worker = ? AND status IN ('running', 'pending')",
            (time.time(), wid),
        )
        conn.commit()
    return cur.rowcount


def get_record(record_id: int) -> Optional[dict]:
    conn = _get_conn()
    # 共享连接（check_same_thread=False）：读也必须持锁，否则与并发写交错会触发
    # sqlite3 "another row available"（同一连接上有未取完的语句时再 execute）。
    with _lock:
        row = conn.execute("SELECT * FROM export_records WHERE id = ?", (record_id,)).fetchone()
    return dict(row) if row else None


def get_record_resolved(record_id: int) -> Optional[dict]:
    """获取记录并自动解析外部文件内容（仅展开 progress_log，不读 analysis_json）"""
    rec = get_record(record_id)
    if not rec:
        return None
    if rec.get("progress_log"):
        rec["progress_log"] = _read_field_content(rec["progress_log"])
    return rec


def get_record_with_analysis(record_id: int) -> Optional[dict]:
    """获取记录并展开 analysis_json（体积可能极大，仅在需要重新渲染报告时使用）"""
    rec = get_record(record_id)
    if not rec:
        return None
    if rec.get("progress_log"):
        rec["progress_log"] = _read_field_content(rec["progress_log"])
    if rec.get("analysis_json"):
        rec["analysis_json"] = _read_field_content(rec["analysis_json"])
    return rec


def list_records(limit: int = 100) -> list:
    conn = _get_conn()
    with _lock:
        rows = conn.execute(
            "SELECT * FROM export_records WHERE is_delete = 0 ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()
    return [dict(r) for r in rows]


def list_records_by_key(key_slot: str, limit: int = 50) -> list:
    conn = _get_conn()
    with _lock:
        rows = conn.execute(
            "SELECT * FROM export_records WHERE key_slot = ? AND is_delete = 0 ORDER BY created_at DESC LIMIT ?",
            (key_slot, limit),
        ).fetchall()
    return [dict(r) for r in rows]


def latest_quality_record(source_export_id: int) -> Optional[dict]:
    with _lock:
        row = _get_conn().execute(
            """SELECT * FROM export_records
               WHERE mode='eval' AND source_export_id=?
               ORDER BY id DESC LIMIT 1""",
            (source_export_id,),
        ).fetchone()
    return dict(row) if row else None


# 列表页用的精简列。started_at/finished_at/files_skipped 必须在内：模板
# templates/export.html 的「开始 / 完成（耗时）/ 跳过」子行直接渲染这三个字段，
# 少任何一个都会静默不显示。
_SLIM_COLS = "id, key_slot, api_key, key_name, mtime_dirs, status, mode, created_at, started_at, finished_at, total_sessions, files_uploaded, files_skipped, obs_dst, local_copy_dir, error_message, manage_name, progress_done, progress_total, progress_phase"

_DS_COLS = "id, key_slot, api_key, key_name, status, mode, created_at, total_sessions, obs_dst, error_message, local_copy_dir, in_manage, manage_name"


def list_records_for_datasets(limit: int = 1000, in_manage: Optional[bool] = None) -> list:
    """列出数据集候选记录。

    in_manage=None: 全部；True: 仅已加入管理列表；False: 仅未加入。
    """
    conn = _get_conn()
    sql = f"SELECT {_DS_COLS} FROM export_records WHERE is_delete = 0"
    params: list = []
    if in_manage is not None:
        sql += " AND in_manage = ?"
        params.append(1 if in_manage else 0)
    sql += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)
    with _lock:
        rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def set_in_manage(record_id: int, flag: bool) -> None:
    """设置单条记录是否在 Session 管理列表中（仅解绑/加入，不删数据）。"""
    conn = _get_conn()
    with _lock:
        conn.execute("UPDATE export_records SET in_manage = ? WHERE id = ?",
                     (1 if flag else 0, record_id))
        conn.commit()


def set_manage_name(record_id: int, name: str) -> None:
    """设置该记录在管理列表中的自定义显示名（留空表示回退原 key_slot）。"""
    conn = _get_conn()
    with _lock:
        conn.execute("UPDATE export_records SET manage_name = ? WHERE id = ?",
                     (name or "", record_id))
        conn.commit()


def set_in_manage_bulk(record_ids: list, flag: bool) -> int:
    """批量设置 in_manage，返回受影响行数。"""
    ids = [int(x) for x in (record_ids or [])]
    if not ids:
        return 0
    conn = _get_conn()
    placeholders = ",".join("?" for _ in ids)
    with _lock:
        cur = conn.execute(
            f"UPDATE export_records SET in_manage = ? WHERE id IN ({placeholders})",
            [1 if flag else 0, *ids],
        )
        conn.commit()
    return cur.rowcount


def get_records_summary() -> tuple:
    """返回 (max_id, count, running_count)，用于快速判断 export_records 是否有变化。
    running_count 用于检测 status 从 running/queued → success/failed 的变化。"""
    conn = _get_conn()
    with _lock:
        row = conn.execute(
            "SELECT MAX(id), COUNT(*), SUM(CASE WHEN status IN ('running','queued') THEN 1 ELSE 0 END) FROM export_records WHERE is_delete = 0"
        ).fetchone()
    return (row[0] or 0, row[1] or 0, row[2] or 0)


def list_records_all_slim(limit_per_key: int = 10) -> dict:
    """一次查询所有 records（排除 progress_log 等大字段），按 key_slot 分组返回。"""
    conn = _get_conn()
    with _lock:
        rows = conn.execute(
            f"SELECT {_SLIM_COLS} FROM export_records WHERE is_delete = 0 ORDER BY created_at DESC"
        ).fetchall()
    grouped: dict = {}
    for r in rows:
        d = dict(r)
        slot = d.pop("key_slot", "all")
        if slot not in grouped:
            grouped[slot] = []
        if len(grouped[slot]) < limit_per_key:
            grouped[slot].append(d)
    return grouped

def mark_interrupted():
    """启动时将遗留的 running/queued 记录标记为 failed/cancelled。

    已被 requeue_interrupted() 取代（自动重新入队而非直接判失败）；保留供无法
    重建任务的场景兜底。当前不再由启动流程调用。
    """
    conn = _get_conn()
    with _lock:
        conn.execute(
            "UPDATE export_records SET status = 'failed', error_message = '服务重启中断', "
            "finished_at = datetime('now','localtime') WHERE status = 'running'"
        )
        conn.execute(
            "UPDATE export_records SET status = 'cancelled', error_message = '服务重启取消', "
            "finished_at = datetime('now','localtime') WHERE status = 'queued'"
        )
        conn.commit()


def cancel_interrupted(reason: str = "服务重启，队列已清空") -> int:
    """启动时清理**所属 worker 已不存活**的遗留任务，返回受影响行数。

    这是多 worker 部署的关键：以前是「一把梭」把全表 running/queued/pending 全标
    cancelled，起第 2 个 worker 就会杀掉第 1 个正在跑的任务。现在按记录的
    owner_worker / owner_heartbeat 逐条判活，只清理孤儿：

      - running/pending 且 owner 为空（老库/老记录、或升级瞬间在途）：
        没有归属信息，无法证明有活进程在跑它 → 视为遗留，清理（保持旧的保守语义）。
      - running/pending 且有 owner：
        owner 对应的进程仍存在（或心跳未超时）→ **保留**，不动；
        进程已死且心跳已过期 → 清理。
      - queued：**不清**。queued 从未被任何 worker 领取（claim 时才写 owner），
        也没有「归属进程」可言；队列是跨 worker 共享的，任何存活 worker 都能领取。
        旧版启动即清空队列的语义在多 worker 下不成立（会误删别的 worker 刚入队的
        任务），故明确改为保留。老版本遗留的 queued 记录若缺 task_json，worker
        领取时会标 failed 并提示重建（见 export_jobs.load_params），不会静默执行。
      - draft / 终态：不动。

    兼容性约定（明确且安全）：
      * 老库第一次升级上来，所有老记录的 owner_worker 都是 ''；running/pending
        会被清理，行为与旧版一致，不会因为「没法判活」而把队列卡住。
    """
    conn = _get_conn()
    cancelled = 0
    with _lock:
        rows = conn.execute(
            "SELECT id, status, owner_worker, owner_heartbeat FROM export_records "
            "WHERE status IN ('running', 'pending') AND is_delete = 0"
        ).fetchall()

        to_cancel = []
        for r in rows:
            owner = r["owner_worker"] or ""
            if not owner:
                to_cancel.append(r["id"])          # 老记录：无归属信息，保守清理
            elif not _owner_alive(owner, r["owner_heartbeat"] or 0):
                to_cancel.append(r["id"])          # 所属 worker 已死，清理其遗留

        if to_cancel:
            ph = ",".join("?" for _ in to_cancel)
            cur = conn.execute(
                "UPDATE export_records SET status = 'cancelled', "
                "error_message = ?, finished_at = datetime('now','localtime') "
                f"WHERE id IN ({ph}) AND status IN ('running', 'pending')",
                [reason, *to_cancel],
            )
            cancelled = cur.rowcount
        conn.commit()
    return cancelled


def take_interrupted() -> list:
    """启动时取出被重启打断的记录（running/queued），置回 pending 待重新入队。

    返回完整记录行（dict）列表，供上层用其持久化字段重建任务再入队。
    置为 pending（而非直接 failed）：这样即使重建/入队失败，记录也停在 pending
    而非误报成功；成功重新入队后 _enqueue_task 会把它推进为 queued。

    已被 cancel_interrupted() 取代（重启即清空队列而非重建）；保留供需要
    「重启后自动续跑」的场景切回使用。当前不再由启动流程调用。
    """
    conn = _get_conn()
    with _lock:
        rows = conn.execute(
            "SELECT * FROM export_records WHERE status IN ('running', 'queued') "
            "ORDER BY id"
        ).fetchall()
        recs = [dict(r) for r in rows]
        if recs:
            conn.execute(
                "UPDATE export_records SET status = 'pending', error_message = '', "
                "started_at = NULL, finished_at = NULL "
                "WHERE status IN ('running', 'queued')"
            )
            conn.commit()
    return recs


def append_log(record_id: int, message: str):
    """追加一条进度日志到 progress_log JSON 数组。"""
    import json
    from datetime import datetime as _dt

    conn = _get_conn()
    entry = {"ts": _dt.now().strftime("%H:%M:%S"), "msg": message}
    with _lock:
        row = conn.execute("SELECT progress_log FROM export_records WHERE id = ?", (record_id,)).fetchone()
        if row:
            current_log = _read_field_content(row[0] or "[]")  # 支持外部文件和旧数据
            try:
                logs = json.loads(current_log)
            except (json.JSONDecodeError, TypeError):
                logs = []
            logs.append(entry)
            new_content = json.dumps(logs, ensure_ascii=False)
            # 默认写入外部文件
            stored_value = _externalize_field(record_id, "progress_log", new_content)
            conn.execute("UPDATE export_records SET progress_log = ? WHERE id = ?",
                         (stored_value, record_id))
            conn.commit()
