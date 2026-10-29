"""
utils/backup_store.py — 备份同步状态持久化

SQLite 后端，记录每个 mtime 目录的 OBS 同步状态和同步日志。
数据库路径：{service_log_dir}/backup_sync.db
"""

import os
import re
import sqlite3
import threading
from datetime import datetime
from typing import List, Optional

_lock = threading.Lock()
_conn: Optional[sqlite3.Connection] = None
_db_path: str = ""


def init_db(db_dir: str):
    global _conn, _db_path
    _db_path = os.path.join(db_dir, "backup_sync.db")
    os.makedirs(db_dir, exist_ok=True)
    _conn = sqlite3.connect(_db_path, check_same_thread=False)
    _conn.execute("PRAGMA journal_mode=WAL")
    _conn.execute("PRAGMA busy_timeout=5000")
    _conn.row_factory = sqlite3.Row
    with _lock:
        _conn.execute("""
            CREATE TABLE IF NOT EXISTS backup_dirs (
                dir_path    TEXT PRIMARY KEY,
                env_name    TEXT NOT NULL DEFAULT '',
                mtime_tag   TEXT NOT NULL DEFAULT '',
                file_count  INTEGER NOT NULL DEFAULT 0,
                synced      INTEGER NOT NULL DEFAULT 0,
                status      TEXT NOT NULL DEFAULT 'pending',
                sync_time   TEXT,
                obs_path    TEXT NOT NULL DEFAULT '',
                error_msg   TEXT NOT NULL DEFAULT '',
                created_at  TEXT NOT NULL DEFAULT (datetime('now','localtime')),
                updated_at  TEXT NOT NULL DEFAULT (datetime('now','localtime'))
            )
        """)
        _conn.execute("""
            CREATE TABLE IF NOT EXISTS backup_logs (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                dir_path    TEXT NOT NULL,
                level       TEXT NOT NULL DEFAULT 'info',
                message     TEXT NOT NULL,
                created_at  TEXT NOT NULL DEFAULT (datetime('now','localtime'))
            )
        """)
        _conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_backup_logs_dir ON backup_logs(dir_path)"
        )
        _conn.execute("""
            CREATE TABLE IF NOT EXISTS backup_failed_files (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                dir_path   TEXT NOT NULL,
                local_path TEXT NOT NULL,
                obs_path   TEXT NOT NULL,
                error      TEXT NOT NULL DEFAULT '',
                resolved   INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT (datetime('now','localtime'))
            )
        """)
        _conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_failed_files_dir ON backup_failed_files(dir_path)"
        )
        # backup_queue —— 手动上传的持久化 FIFO 队列（id 自增即先后顺序）
        _conn.execute("""
            CREATE TABLE IF NOT EXISTS backup_queue (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                dir_path    TEXT NOT NULL,
                job_type    TEXT NOT NULL DEFAULT 'sync',    -- 'sync' | 'resync'
                state       TEXT NOT NULL DEFAULT 'queued',  -- 'queued' | 'running'
                enqueued_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
                started_at  TEXT
            )
        """)
        # 同一目录同时只允许一个活动队列项 —— DB 层去重
        _conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_backup_queue_active "
            "ON backup_queue(dir_path) WHERE state IN ('queued','running')"
        )
        # backup_clear_queue —— 本地清理队列（与上传队列**完全分离**）
        #
        # 为什么要单独一条队列：上传和删除的失败模式完全不同。上传是网络/OBS
        # 问题，失败可重试；删除是本地 NFS 问题，可能无限阻塞。混在一条队列里时，
        # 一个卡住的删除会堵死整条上传流水线，也让「传了多少」和「删了多少」
        # 无法分开统计。现在 backup_worker 只管上传，clear_worker 只管删除。
        _conn.execute("""
            CREATE TABLE IF NOT EXISTS backup_clear_queue (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                dir_path      TEXT NOT NULL,
                local_path    TEXT NOT NULL,           -- 已解析的绝对路径（精准删除的依据）
                state         TEXT NOT NULL DEFAULT 'queued',  -- queued|running|failed
                attempts      INTEGER NOT NULL DEFAULT 0,
                last_error    TEXT NOT NULL DEFAULT '',
                enqueued_at   TEXT NOT NULL DEFAULT (datetime('now','localtime')),
                started_at    TEXT,
                finished_at   TEXT
            )
        """)
        _conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_clear_queue_active "
            "ON backup_clear_queue(dir_path) WHERE state IN ('queued','running')"
        )
        for col, definition in [
            ("sync_pid", "INTEGER DEFAULT NULL"),
            ("port",     "TEXT NOT NULL DEFAULT ''"),
            ("has_local","INTEGER NOT NULL DEFAULT 1"),
            ("has_obs",  "INTEGER NOT NULL DEFAULT 0"),
            # 上传成功 / 本地清理完成的时间戳。自动删除后审计用：
            # 只有 uploaded_at 非空且 deleted_at 非空，才说明「确实传上去了才删的」。
            ("uploaded_at", "TEXT NOT NULL DEFAULT ''"),
            ("deleted_at",  "TEXT NOT NULL DEFAULT ''"),
            # 上传时 obsutil 报告的真实字节数（`Succeed bytes:`）。
            # 本地不预先统计：逐个 iterdir 数文件在 NFS 上代价过高。
            # -1 表示尚未上传过、没有数据。
            ("size_bytes",  "INTEGER NOT NULL DEFAULT -1"),
        ]:
            try:
                _conn.execute(f"ALTER TABLE backup_dirs ADD COLUMN {col} {definition}")
            except sqlite3.OperationalError:
                pass

        # backup_logs.event：结构化操作事件（upload_done/verify_ok/deleted/...）。
        # 「最近备份操作」只渲染带事件的日志，避免 obsutil 横幅淹没真实信息；
        # 历史行 event 为空，查询侧按关键字过滤后仍可展示。
        log_cols = {row[1] for row in _conn.execute("PRAGMA table_info(backup_logs)")}
        if "event" not in log_cols:
            _conn.execute(
                "ALTER TABLE backup_logs ADD COLUMN event TEXT NOT NULL DEFAULT ''"
            )
        if "idx_backup_logs_event" not in {
            row[0] for row in _conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index'")
        }:
            _conn.execute(
                "CREATE INDEX idx_backup_logs_event ON backup_logs(event)"
            )

        # 迁移旧 source 字段 -> has_local / has_obs
        cols = {row[1] for row in _conn.execute("PRAGMA table_info(backup_dirs)")}
        if "source" in cols:
            _conn.execute("""
                UPDATE backup_dirs SET
                    has_local = CASE WHEN source IN ('local','both') THEN 1 ELSE 0 END,
                    has_obs   = CASE WHEN source IN ('obs','both') OR obs_path != '' THEN 1 ELSE 0 END
                WHERE has_local = 1 AND has_obs = 0 AND source IS NOT NULL
            """)
            # 对于 backed_up 状态（本地已删）本地标记清零
            _conn.execute("""
                UPDATE backup_dirs SET has_local = 0
                WHERE status = 'backed_up'
            """)

        _conn.commit()


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def migrate_env_prefix(mapping: dict):
    """把存量记录里旧的 env 标识（basename）改写为新的 root_id。

    mapping: {old_env_name: new_env_name}。幂等——已是新前缀的记录不受影响
    （old==new 时跳过，且新前缀不在 mapping 键中）。仅改写 env_name 与
    dir_path 的第一段前缀；obs_path 保留原值（存量 OBS 对象不搬动）。
    """
    if not _conn or not mapping:
        return
    pairs = [(o, n) for o, n in mapping.items() if o and n and o != n]
    if not pairs:
        return
    with _lock:
        for old, new in pairs:
            like = old + "/%"
            # backup_dirs：env_name + dir_path 前缀
            _conn.execute(
                "UPDATE OR IGNORE backup_dirs SET env_name=?, "
                "dir_path=? || substr(dir_path, ?) "
                "WHERE env_name=? AND dir_path LIKE ?",
                (new, new, len(old) + 1, old, like),
            )
            # 关联表：仅 dir_path 前缀
            for tbl in ("backup_logs", "backup_failed_files", "backup_queue"):
                _conn.execute(
                    f"UPDATE OR IGNORE {tbl} SET dir_path=? || substr(dir_path, ?) "
                    "WHERE dir_path LIKE ?",
                    (new, len(old) + 1, like),
                )
        _conn.commit()


def upsert_dir(dir_path: str, env_name: str, mtime_tag: str, file_count: int = 0,
               port: str = "", has_local: bool = True, has_obs: bool = False):
    with _lock:
        existing = _conn.execute(
            "SELECT has_local, has_obs, port FROM backup_dirs WHERE dir_path = ?", (dir_path,)
        ).fetchone()
        if existing:
            merged_local = existing["has_local"] or int(has_local)
            merged_obs   = existing["has_obs"]   or int(has_obs)
            merged_port  = existing["port"] or port
            fields = ["has_local = ?", "has_obs = ?", "port = ?", "updated_at = ?"]
            params = [merged_local, merged_obs, merged_port, _now()]
            if file_count > 0:
                fields.insert(0, "file_count = ?")
                params.insert(0, file_count)
            params.append(dir_path)
            _conn.execute(
                f"UPDATE backup_dirs SET {', '.join(fields)} WHERE dir_path = ?",
                params,
            )
        else:
            _conn.execute("""
                INSERT INTO backup_dirs
                    (dir_path, env_name, mtime_tag, file_count, port, has_local, has_obs, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (dir_path, env_name, mtime_tag, file_count, port,
                  int(has_local), int(has_obs), _now(), _now()))
        _conn.commit()


def set_upload_stats(dir_path: str, size_bytes=None, file_count=None):
    """记录上传时 obsutil 报告的真实数据量与文件数。

    数据量只有上传过程能拿到（obsutil 的 `Succeed bytes:`），本地不预先统计
    —— 逐个 iterdir 数文件在 NFS 上代价过高（72 个叶子几十秒）。
    只写非 None 的字段，避免把已有值覆盖成 0。
    """
    fields, params = [], []
    if size_bytes is not None and int(size_bytes) >= 0:
        fields.append("size_bytes = ?")
        params.append(int(size_bytes))
    if file_count is not None and int(file_count) > 0:
        fields.append("file_count = ?")
        params.append(int(file_count))
    if not fields:
        return
    fields.append("updated_at = ?")
    params.append(_now())
    params.append(dir_path)
    with _lock:
        _conn.execute(
            f"UPDATE backup_dirs SET {', '.join(fields)} WHERE dir_path = ?", params)
        _conn.commit()


def update_sync_status(dir_path: str, status: str, obs_path: str = "", error_msg: str = ""):
    fields = ["status = ?", "updated_at = ?"]
    params: list = [status, _now()]
    if obs_path:
        fields.append("obs_path = ?")
        fields.append("has_obs = 1")
        params.append(obs_path)
    if error_msg:
        fields.append("error_msg = ?")
        params.append(error_msg)
    else:
        fields.append("error_msg = ''")
    if status == "done":
        fields.append("synced = 1")
        fields.append("sync_time = ?")
        params.append(_now())
    if status in ("done", "error", "pending", "partial", "pending_delete"):
        fields.append("sync_pid = NULL")
    if status == "backed_up":
        fields.append("has_local = 0")
        fields.append("has_obs = 1")
    params.append(dir_path)
    with _lock:
        _conn.execute(
            f"UPDATE backup_dirs SET {', '.join(fields)} WHERE dir_path = ?",
            params,
        )
        _conn.commit()


def list_dirs(env_name: Optional[str] = None) -> List[dict]:
    with _lock:
        if env_name:
            rows = _conn.execute(
                "SELECT * FROM backup_dirs WHERE env_name = ? ORDER BY env_name, mtime_tag",
                (env_name,),
            ).fetchall()
        else:
            rows = _conn.execute(
                "SELECT * FROM backup_dirs ORDER BY env_name, mtime_tag"
            ).fetchall()
    return [dict(r) for r in rows]


def list_backup_candidates(env_name: str) -> List[dict]:
    """自动备份候选：某 env 下未备份（pending 且本地存在）的目录，最旧优先。"""
    with _lock:
        rows = _conn.execute(
            "SELECT * FROM backup_dirs "
            "WHERE env_name = ? AND status = 'pending' AND has_local = 1 "
            "ORDER BY mtime_tag ASC",
            (env_name,),
        ).fetchall()
    return [dict(r) for r in rows]


def list_pending_delete(env_name: Optional[str] = None) -> List[dict]:
    """已成功上传、等待人工确认删除的目录。"""
    with _lock:
        if env_name:
            rows = _conn.execute(
                "SELECT * FROM backup_dirs WHERE status = 'pending_delete' AND env_name = ? "
                "ORDER BY env_name, mtime_tag",
                (env_name,),
            ).fetchall()
        else:
            rows = _conn.execute(
                "SELECT * FROM backup_dirs WHERE status = 'pending_delete' "
                "ORDER BY env_name, mtime_tag"
            ).fetchall()
    return [dict(r) for r in rows]


def count_pending_delete(env_name: Optional[str] = None) -> int:
    with _lock:
        if env_name:
            row = _conn.execute(
                "SELECT COUNT(*) AS c FROM backup_dirs WHERE status='pending_delete' AND env_name=?",
                (env_name,),
            ).fetchone()
        else:
            row = _conn.execute(
                "SELECT COUNT(*) AS c FROM backup_dirs WHERE status='pending_delete'"
            ).fetchone()
    return row["c"] if row else 0


def list_env_names() -> List[str]:
    with _lock:
        rows = _conn.execute(
            "SELECT DISTINCT env_name FROM backup_dirs WHERE env_name != '' ORDER BY env_name"
        ).fetchall()
    return [r["env_name"] for r in rows]


def get_dir(dir_path: str) -> Optional[dict]:
    with _lock:
        row = _conn.execute(
            "SELECT * FROM backup_dirs WHERE dir_path = ?", (dir_path,)
        ).fetchone()
    return dict(row) if row else None


def mark_backed_up(dir_path: str):
    with _lock:
        _conn.execute(
            "UPDATE backup_dirs SET status='backed_up', has_local=0, has_obs=1, updated_at=? WHERE dir_path=?",
            (_now(), dir_path),
        )
        _conn.commit()


def set_uploaded(dir_path: str):
    """记上传完成时间（内容已确认在 OBS 上）。"""
    with _lock:
        _conn.execute(
            "UPDATE backup_dirs SET uploaded_at = ?, has_obs = 1, updated_at = ? WHERE dir_path = ?",
            (_now(), _now(), dir_path),
        )
        _conn.commit()


def set_deleted(dir_path: str):
    """记本地清理完成时间。只有它非空才代表本地确实删了。"""
    with _lock:
        _conn.execute(
            "UPDATE backup_dirs SET deleted_at = ?, has_local = 0, updated_at = ? WHERE dir_path = ?",
            (_now(), _now(), dir_path),
        )
        _conn.commit()


def update_sync_pid(dir_path: str, pid: Optional[int]):
    with _lock:
        _conn.execute(
            "UPDATE backup_dirs SET sync_pid = ?, updated_at = ? WHERE dir_path = ?",
            (pid, _now(), dir_path),
        )
        _conn.commit()


def get_live_syncing_dirs() -> List[dict]:
    with _lock:
        rows = _conn.execute(
            "SELECT * FROM backup_dirs WHERE status = 'live_syncing'"
        ).fetchall()
    return [dict(r) for r in rows]


def has_any_dirs(env_name: str = None) -> bool:
    with _lock:
        if env_name:
            row = _conn.execute(
                "SELECT 1 FROM backup_dirs WHERE env_name = ? LIMIT 1", (env_name,)
            ).fetchone()
        else:
            row = _conn.execute("SELECT 1 FROM backup_dirs LIMIT 1").fetchone()
    return row is not None


def remove_dir(dir_path: str):
    with _lock:
        _conn.execute("DELETE FROM backup_dirs WHERE dir_path = ?", (dir_path,))
        _conn.execute("DELETE FROM backup_logs WHERE dir_path = ?", (dir_path,))
        _conn.execute("DELETE FROM backup_failed_files WHERE dir_path = ?", (dir_path,))
        _conn.commit()


def append_log(dir_path: str, message: str, level: str = "info", event: str = ""):
    """写一条备份日志。

    event: 结构化事件名（upload_done / verify_ok / verify_fail / deleted /
        upload_fail / enqueue）。带事件的行会出现在「最近备份操作」里，
        空事件表示原始输出（obsutil 横幅、进度条等），只在详情抽屉展示。
    """
    with _lock:
        _conn.execute(
            "INSERT INTO backup_logs (dir_path, level, message, event, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (dir_path, level, message, event, _now()),
        )
        _conn.commit()


def get_logs(dir_path: str, limit: int = 200) -> List[dict]:
    with _lock:
        rows = _conn.execute(
            "SELECT * FROM backup_logs WHERE dir_path = ? ORDER BY id DESC LIMIT ?",
            (dir_path, limit),
        ).fetchall()
    return [dict(r) for r in rows]


# obsutil 每次任务都会打印大量横幅、进度条和统计行：
#   CheckpointDir: /root/.obsutil_checkpoint
#   [---------------] 68% tps:0.00 ?/s 0/? 0B/? ?
#   Metrics [max cost:150 ms, min cost:5 ms, average cost:...]
#   Succeed count: 5   Failed count: 0 / Succeed bytes: 7.92GB
# 实测 564 个叶子各重复一遍，「最近备份操作」里没有信息量，只在详情抽屉保留原文。
_NOISE_PREFIXES = (
    "CheckpointDir:", "OutputDir:", "Parallel:", "Threshold:",
    "VerifyLength:", "VerifyMd5:", "Task id", "Start at ",
    "Listing objects", "Succeed count:", "Failed count:",
    "Succeed bytes:", "average tps:", "Metrics [", "执行: ",
    "Upload successfully", "Download successfully", "Copy successfully",
)
# 进度条形如 `[---------] 68% tps:...` 或 `[________] ?% ...`
_NOISE_RE = re.compile(r"^\[[-_ ]*\]|^\[.*\]\s*\d*%?\s*tps:")
# 「最近备份操作」只认业务事件；老数据没有 event 标签，用这份白名单兜底。
# 不在白名单里的一律视为 obsutil 原始输出，不展示。
_LEGACY_EVENT_HINTS = (
    "同步完成", "校验通过", "校验失败", "开始同步", "补同步",
    "本地目录已删除", "已自动清理", "人工确认删除", "自动备份",
    "上传失败", "同步失败", "部分失败", "转入待确认删除",
)


def _is_noise(message: str) -> bool:
    """是否是 obsutil 的原始输出（不该进「最近备份操作」）。"""
    m = (message or "").strip()
    if not m:
        return True
    if _NOISE_RE.match(m):
        return True
    return any(m.startswith(p) for p in _NOISE_PREFIXES)


def _is_legacy_event(message: str) -> bool:
    """没有 event 标签的历史行，是否属于业务事件（白名单）。"""
    m = (message or "").strip()
    return any(h in m for h in _LEGACY_EVENT_HINTS)


def daily_sync_counts() -> List[dict]:
    """按**天**统计「备份完成」的叶子数，供概览页趋势图（近 N 天视图）。

    与 hourly_sync_counts 同源（backup_dirs.sync_time），只是粒度不同。
    返回 [{day: 'YYYY-MM-DD', leaves: n}, ...]，按时间升序。
    """
    with _lock:
        rows = _conn.execute(
            "SELECT substr(sync_time, 1, 10) AS d, COUNT(*) AS n FROM backup_dirs "
            "WHERE sync_time IS NOT NULL AND sync_time != '' "
            "GROUP BY d ORDER BY d"
        ).fetchall()
    return [{"day": r["d"], "leaves": int(r["n"])} for r in rows if r["d"]]


def hourly_sync_counts() -> List[dict]:
    """按小时统计「备份完成」的叶子数，供概览页的趋势图。

    数据源是 backup_dirs.sync_time（上传成功时盖的时间戳），所以这是
    「什么时候真正备份了多少叶子」的真实记录，而不是按数据时间派生的估计。
    返回 [{hour: 'YYYY-MM-DD HH', leaves: n}, ...]，按时间升序。
    """
    with _lock:
        rows = _conn.execute(
            "SELECT substr(sync_time, 1, 13) AS h, COUNT(*) AS n FROM backup_dirs "
            "WHERE sync_time IS NOT NULL AND sync_time != '' "
            "GROUP BY h ORDER BY h"
        ).fetchall()
    return [{"hour": r["h"], "leaves": int(r["n"])} for r in rows if r["h"]]


def _recent_logs_where(level: str, dir_prefix: str) -> tuple:
    """「最近备份操作」的 WHERE 子句 + 参数，供列表与计数共用。

    直接在 SQL 里筛业务事件，不能「取最后 N 条再内存过滤」：一次普通上传会写
    几十行 obsutil 原始输出（Task id / 进度条 / Metrics / Succeed bytes …），
    600 行窗口里可能一条业务事件都没有，过滤后就是空列表 —— 表现为「最近备份
    操作没有日志了」。
      - 新数据：event 非空
      - 老数据：无 event 标签，靠 LEGACY 关键字白名单命中
    """
    conds, params = [], []
    if level:
        conds.append("level = ?")
        params.append(level)
    if dir_prefix:
        conds.append("dir_path LIKE ?")
        params.append(dir_prefix + "%")
    ev_terms = list(_LEGACY_EVENT_HINTS)
    ev_sql = "event != '' OR " + " OR ".join("message LIKE ?" for _ in ev_terms)
    conds.append(f"({ev_sql})")
    params.extend(f"%{t}%" for t in ev_terms)
    where = (" WHERE " + " AND ".join(conds)) if conds else ""
    return where, params


def count_recent_logs(level: str = "", dir_prefix: str = "") -> int:
    """符合「最近备份操作」过滤条件的总行数，供前端分页算总页。

    与 get_recent_logs 共用同一 WHERE。注意这是 SQL 层的计数，不含老数据
    二次噪音过滤（_is_noise）剔除的那几条，所以可能比逐页累加略多；分页足够用。
    """
    where, params = _recent_logs_where(level, dir_prefix)
    with _lock:
        row = _conn.execute(
            "SELECT COUNT(*) AS n FROM backup_logs" + where, params
        ).fetchone()
    return int(row["n"]) if row else 0


def get_recent_logs(limit: int = 30, level: str = "", dir_prefix: str = "",
                    offset: int = 0) -> List[dict]:
    """「最近备份操作」：只返回**有意义**的操作事件，支持分页。

    优先取带 event 的结构化行（新数据）；event 为空的历史行按关键字过滤掉
    obsutil 噪音后仍可展示，因此不需要回填旧数据。

    走 id 倒序（自增即时间序），比 ORDER BY created_at 稳定（同秒多条不抖动）。
    返回的每条带 `event` 字段，前端据此选图标与文案。
    """
    where, params = _recent_logs_where(level, dir_prefix)
    sql = ("SELECT id, dir_path, level, message, event, created_at FROM backup_logs"
           + where + " ORDER BY id DESC LIMIT ? OFFSET ?")
    params = params + [int(limit), max(0, int(offset))]
    with _lock:
        rows = _conn.execute(sql, params).fetchall()

    out = []
    for r in rows:
        row = dict(r)
        ev = row.get("event") or ""
        msg = row.get("message") or ""
        # 老数据没有 event 标签，还可能命中白名单里的噪音词（如「执行: ...」
        # 含「同步」），再过一遍噪音过滤。
        if ev or not _is_noise(msg):
            out.append(row)
    return out


def clear_logs(dir_path: str):
    with _lock:
        _conn.execute("DELETE FROM backup_logs WHERE dir_path = ?", (dir_path,))
        _conn.commit()


# ---------------------------------------------------------------------------
# backup_failed_files — obsutil 上传部分失败的文件清单，供"补同步"逐个重传
# ---------------------------------------------------------------------------

def record_failed_files(dir_path: str, items: List[tuple]):
    """记录失败文件清单。items = [(local_path, obs_path, error), ...]。
    先清除该目录已有的未 resolved 记录，避免重复。"""
    with _lock:
        _conn.execute(
            "DELETE FROM backup_failed_files WHERE dir_path = ? AND resolved = 0",
            (dir_path,),
        )
        _conn.executemany(
            """INSERT INTO backup_failed_files (dir_path, local_path, obs_path, error, created_at)
               VALUES (?, ?, ?, ?, ?)""",
            [(dir_path, local, obs, error or "", _now()) for local, obs, error in items],
        )
        _conn.commit()


def list_failed_files(dir_path: str, only_unresolved: bool = True) -> List[dict]:
    with _lock:
        if only_unresolved:
            rows = _conn.execute(
                "SELECT * FROM backup_failed_files WHERE dir_path = ? AND resolved = 0 ORDER BY id",
                (dir_path,),
            ).fetchall()
        else:
            rows = _conn.execute(
                "SELECT * FROM backup_failed_files WHERE dir_path = ? ORDER BY id",
                (dir_path,),
            ).fetchall()
    return [dict(r) for r in rows]


def mark_file_resolved(dir_path: str, obs_path: str):
    with _lock:
        _conn.execute(
            "UPDATE backup_failed_files SET resolved = 1 WHERE dir_path = ? AND obs_path = ?",
            (dir_path, obs_path),
        )
        _conn.commit()


def count_unresolved_failed(dir_path: str) -> int:
    with _lock:
        row = _conn.execute(
            "SELECT COUNT(*) FROM backup_failed_files WHERE dir_path = ? AND resolved = 0",
            (dir_path,),
        ).fetchone()
    return row[0] if row else 0


def clear_failed_files(dir_path: str):
    """清空该目录的失败文件记录（用于目录整体同步成功后）。"""
    with _lock:
        _conn.execute("DELETE FROM backup_failed_files WHERE dir_path = ?", (dir_path,))
        _conn.commit()


# ---------------------------------------------------------------------------
# backup_queue — 手动上传的持久化串行队列，dispatcher 逐个消费
# ---------------------------------------------------------------------------

def enqueue_job(dir_path: str, job_type: str = "sync") -> bool:
    """把目录加入上传队列。已在队列/运行中（唯一部分索引冲突）返回 False。"""
    with _lock:
        try:
            _conn.execute(
                "INSERT INTO backup_queue (dir_path, job_type, state, enqueued_at) "
                "VALUES (?, ?, 'queued', ?)",
                (dir_path, job_type, _now()),
            )
            _conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False


# ---------------------------------------------------------------------------
# backup_clear_queue — 本地清理队列（只由 clear_worker 消费，与上传完全隔离）
# ---------------------------------------------------------------------------

def enqueue_clear(dir_path: str, local_path: str) -> bool:
    """把「待删除本地目录」加入清理队列。重复入队返回 False。

    local_path 是**入队时就解析好的绝对路径**，worker 只按它删，不再自己推导 ——
    删除是不可逆操作，路径必须在能看见完整上下文的主进程里算准。
    """
    with _lock:
        try:
            _conn.execute(
                "INSERT INTO backup_clear_queue (dir_path, local_path, state, enqueued_at) "
                "VALUES (?, ?, 'queued', ?)",
                (dir_path, local_path, _now()),
            )
            _conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False


def claim_next_clear() -> Optional[dict]:
    """clear_worker 领取一个待删任务（原子领取，同 claim_next_job）。"""
    with _lock:
        try:
            _conn.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError:
            return None
        try:
            row = _conn.execute(
                "SELECT * FROM backup_clear_queue WHERE state = 'queued' "
                "ORDER BY id ASC LIMIT 1").fetchone()
            if not row:
                _conn.rollback()
                return None
            cur = _conn.execute(
                "UPDATE backup_clear_queue SET state = 'running', started_at = ?, "
                "attempts = attempts + 1 WHERE id = ? AND state = 'queued'",
                (_now(), row["id"]))
            if cur.rowcount == 0:
                _conn.rollback()
                return None
            _conn.commit()
        except Exception:
            try:
                _conn.rollback()
            except sqlite3.Error:
                pass
            raise
        job = dict(row)
        job["state"] = "running"
        job["attempts"] = int(job.get("attempts") or 0) + 1
        return job


def finish_clear(job_id: int, ok: bool, error: str = ""):
    """结束一个清理任务。成功则移除队列行；失败保留为 failed 供排查。"""
    with _lock:
        if ok:
            _conn.execute("DELETE FROM backup_clear_queue WHERE id = ?", (job_id,))
        else:
            _conn.execute(
                "UPDATE backup_clear_queue SET state = 'failed', last_error = ?, "
                "finished_at = ? WHERE id = ?", (error or "", _now(), job_id))
        _conn.commit()


def reset_clear_on_startup() -> int:
    """把上次中断的 running 清理任务降回 queued（clear_worker 启动时调用）。"""
    with _lock:
        cur = _conn.execute(
            "UPDATE backup_clear_queue SET state = 'queued', started_at = NULL "
            "WHERE state = 'running'")
        _conn.commit()
        return cur.rowcount or 0


def list_clear_queue() -> List[dict]:
    with _lock:
        rows = _conn.execute(
            "SELECT * FROM backup_clear_queue WHERE state IN ('queued','running') "
            "ORDER BY id ASC").fetchall()
    return [dict(r) for r in rows]


def clear_queue_stats() -> dict:
    """清理队列概况。paths 供前端在队列表格里标出「正在清理」的行。"""
    with _lock:
        rows = _conn.execute(
            "SELECT dir_path, state FROM backup_clear_queue "
            "WHERE state IN ('queued','running') ORDER BY id").fetchall()
        failed = _conn.execute(
            "SELECT COUNT(*) AS n FROM backup_clear_queue WHERE state='failed'").fetchone()["n"]
    running = next((r["dir_path"] for r in rows if r["state"] == "running"), "")
    return {
        "queued": sum(1 for r in rows if r["state"] == "queued"),
        "running": running,
        "failed": int(failed),
        # 前端用来判断某一行是否正在清理（含排队中）
        "paths": [r["dir_path"] for r in rows],
    }


def claim_next_job() -> Optional[dict]:
    """领取下一个待处理任务：全局同时只允许一个 running。

    **必须原子**：这个函数会被 web 进程内的 dispatcher 线程和独立的
    backup_worker 进程同时调用。旧实现是「先 SELECT 有没有 running，再
    SELECT+UPDATE 领取」，两步之间没有条件约束，跨进程时会双领同一条任务
    （同一个叶子被两个进程同时上传 + 同时 rmtree）。

    这里用 BEGIN IMMEDIATE 拿写锁，把「查 running」和「条件领取」放进同一个
    事务；配合 `UPDATE ... WHERE id=? AND state='queued'` + rowcount 判定，
    跨进程也不会重复领取。
    """
    with _lock:
        try:
            _conn.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError:
            return None  # 拿不到写锁（别的进程正在写），下轮再试
        try:
            running = _conn.execute(
                "SELECT 1 FROM backup_queue WHERE state = 'running' LIMIT 1"
            ).fetchone()
            if running:
                _conn.rollback()
                return None
            row = _conn.execute(
                "SELECT * FROM backup_queue WHERE state = 'queued' ORDER BY id ASC LIMIT 1"
            ).fetchone()
            if not row:
                _conn.rollback()
                return None
            cur = _conn.execute(
                "UPDATE backup_queue SET state = 'running', started_at = ? "
                "WHERE id = ? AND state = 'queued'",
                (_now(), row["id"]),
            )
            if cur.rowcount == 0:
                _conn.rollback()
                return None  # 被另一个进程抢先
            _conn.commit()
        except Exception:
            try:
                _conn.rollback()
            except sqlite3.Error:
                pass
            raise
        job = dict(row)
        job["state"] = "running"
        return job


def finish_job(job_id: int):
    """任务完成后出队。"""
    with _lock:
        _conn.execute("DELETE FROM backup_queue WHERE id = ?", (job_id,))
        _conn.commit()


def list_queue() -> List[dict]:
    """按入队顺序返回全部活动队列项（queued + running）。"""
    with _lock:
        rows = _conn.execute(
            "SELECT * FROM backup_queue WHERE state IN ('queued','running') ORDER BY id ASC"
        ).fetchall()
    return [dict(r) for r in rows]


def reset_running_on_startup(owner: str = "web"):
    """把上次中断的 running 降回 queued，供 worker 重新领取。

    **只能由持有队列的进程调用**：web 进程启动时若无条件重置，会把正在被
    worker 正常执行的任务（哪怕它跑了几十分钟的大叶子）打回 queued ——
    结果是同一条任务被「排队中」显示、而 worker 仍在跑它，看起来像卡住。
    所以 web 侧只在 worker 没在跑时调用（见 register_backup_routes）。
    """
    with _lock:
        n = 0
        try:
            cur = _conn.execute(
                "UPDATE backup_queue SET state = 'queued', started_at = NULL "
                "WHERE state = 'running'")
            n = cur.rowcount or 0
        except sqlite3.Error:
            n = 0
        _conn.commit()
    return n


def requeue_stale_running(max_age_sec: int = 7200) -> int:
    """把「running 但已开始太久」的任务降回 queued（防进程被杀留下僵尸任务）。

    与 reset_running_on_startup 的区别：按 started_at 判断年龄，不动正在正常
    推进的任务。返回重排条数。
    """
    from datetime import datetime, timedelta
    cutoff = (datetime.now() - timedelta(seconds=max_age_sec)).strftime("%Y-%m-%d %H:%M:%S")
    with _lock:
        try:
            cur = _conn.execute(
                "UPDATE backup_queue SET state = 'queued', started_at = NULL "
                "WHERE state = 'running' AND (started_at IS NULL OR started_at < ?)",
                (cutoff,))
            n = cur.rowcount or 0
        except sqlite3.Error:
            n = 0
        _conn.commit()
    return n

