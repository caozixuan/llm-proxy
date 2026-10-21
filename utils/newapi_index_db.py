"""
utils/newapi_index_db.py — new-api 叶子的「富 index」+ 会话聚合（每叶子一个 index.db）

背景：new-api 上游写的 index.jsonl 只有 ts/model/api_key/req_file/usage，缺聚合所需的
q1_hash/msg_count/user_turns，导致历史/导出聚合必须逐个打开 ~1MB 合并文件（单叶子可达数万
文件 / 数十 GB）。本模块给每个叶子在其目录内建一个 index.db（与 index.jsonl 同级、共享盘、
跨实例共享），把「贵的原文件解析」固化成一次性、可并行、可断点续跑的批量补 meta。

三段式，全增量：
  Stage 1 ingest        —— 从 index.jsonl 按 ingest_offset 只读新增行，写 requests（meta 列留空）
  Stage 2 enrich        —— SELECT q1_hash IS NULL 的行，叶子内分片多进程解析合并文件回填 meta
  Stage 3 rebuild_sessions —— requests(meta 齐全) 按 rowid(=时间序) 内存聚合，写 sessions/traces/chain_index

视图只读本库（count_sessions/list_sessions/get_traces_batch/...）；写仅经批量构建路径。
会话切分口径对齐 utils/newapi_consumer._process_entry。
"""

import json
import logging
import multiprocessing as mp
import os
import sqlite3
import sys
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from concurrent.futures.process import BrokenProcessPool
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from utils.log_scan import iter_index_dirs

logger = logging.getLogger(__name__)

# enrich 进程池用 spawn 启动，而非默认 fork。
# 本进程是多线程（Flask + 后台调度 + asyncio 线程池），fork worker 会继承父进程
# 在 fork 那一刻处于加锁状态的锁（stdout flush 锁 / sqlite 锁等），worker 一 flush
# 便永久阻塞在 _flush_std_streams，进而 ProcessPoolExecutor.__exit__ 的 join 死等——
# 回填卡死不前。spawn 每个 worker 全新解释器、不继承父进程锁，彻底规避此类挂死。
# （Python 3.14 起 fork 多线程程序即弃用；此处提前显式指定 spawn。）
_MP_CTX = mp.get_context("spawn")


class LeafStalledError(Exception):
    """enrich 某叶子的进程池长时间无进展（多为 NFS 读被 hard 挂载死等阻塞）。
    上层据此「跳过该叶子 + 预警」，让整轮回填继续，而非被单个卡死叶子拖住。"""

    def __init__(self, leaf_dir: str, done: int, total: int, elapsed: float):
        self.leaf_dir = leaf_dir
        self.done = done
        self.total = total
        self.elapsed = elapsed
        super().__init__(
            f"leaf enrich stalled: {leaf_dir} done={done}/{total} elapsed={elapsed:.0f}s"
        )


def _reap_pool_async(pool: "ProcessPoolExecutor") -> None:
    """后台收尸卡死的进程池：不阻塞主流程。
    残留 worker 多处于 D（不可中断）状态，SIGKILL 也未必能立刻杀掉，需等 NFS
    读 RPC 返回后才退出——故此处在 daemon 线程里 shutdown(wait=True) 慢慢等，
    调用方已 shutdown(wait=False, cancel_futures=True) 抛弃它、继续往下跑。"""
    def _reap():
        try:
            pool.shutdown(wait=True)
        except Exception:  # noqa: BLE001 — 收尸尽力而为，失败不影响主流程
            pass

    t = threading.Thread(target=_reap, name="newapi-enrich-pool-reaper", daemon=True)
    t.start()

_INDEX_NAME = "index.jsonl"
_DB_NAME = "index.db"

# traces 批量查询的 IN 分批大小。SQLite 的 SQLITE_MAX_VARIABLE_NUMBER 在 3.32+ 是 32767、
# 更早版本仅 999；整叶导出（单叶常 3-10w session）会超限抛 "too many SQL variables"，
# 故按远低于下限的固定批大小切分。
_TRACES_IN_CHUNK = 500

# enrich 失败重试上限：合并文件「尚未落盘」是活跃叶子的常态瞬态失败，
# 保持 q1_hash=NULL 让后续构建重扫重试；累计到此上限仍失败才标死（q1_hash=''）永久跳过。
_MAX_ENRICH_ATTEMPTS = 3

# BrokenProcessPool 重试上限：worker 进程被 OS 终止（偶发崩溃/被 kill，非解析异常）时，
# 抛 BrokenProcessPool 且进程池不可复用。重建池续跑（enrich 逐片增量落库，重试自动断点续跑），
# 最多重试这么多次仍崩才 raise，交上层判叶子失败。可用 NEWAPI_ENRICH_BROKEN_RETRIES 覆盖。
try:
    _POOL_BROKEN_RETRIES = max(1, int(os.getenv("NEWAPI_ENRICH_BROKEN_RETRIES", "2")))
except ValueError:
    _POOL_BROKEN_RETRIES = 2

# enrich 分片粒度：每个进程池任务处理的合并文件数（固定，不随文件总数放大）。
# 关键——单 worker 常驻内存 ≈ 一个分片的解析结果 × 并行 worker 数；分片按 worker 均分时，
# 巨型叶子（如单小时 10w+ 文件）每片会攒上千条结果 ×8 并行，峰值溢出被 OOM kill。
# 固定小片让单 worker 内存与文件总数解耦：份数随文件数增长，每份始终 ~此值。
# 可用 NEWAPI_ENRICH_CHUNK_SIZE 覆盖。
try:
    _ENRICH_CHUNK_SIZE = max(1, int(os.getenv("NEWAPI_ENRICH_CHUNK_SIZE", "200")))
except ValueError:
    _ENRICH_CHUNK_SIZE = 200

# 每叶子一把写锁（同进程内串行写；跨实例互斥由上层 dispatcher + leader_lock 负责）
_leaf_locks: Dict[str, threading.Lock] = {}
_leaf_locks_guard = threading.Lock()


def db_path(leaf_dir: str) -> str:
    return os.path.join(leaf_dir, _DB_NAME)


def _leaf_lock(leaf_dir: str) -> threading.Lock:
    key = os.path.normpath(leaf_dir)
    with _leaf_locks_guard:
        lk = _leaf_locks.get(key)
        if lk is None:
            lk = threading.Lock()
            _leaf_locks[key] = lk
        return lk


# ── 连接 / schema ─────────────────────────────────────────────────────────

_SCHEMA = """
CREATE TABLE IF NOT EXISTS requests (
    req_file    TEXT PRIMARY KEY,
    ts          TEXT,
    model       TEXT,
    api_key     TEXT,
    usage_json  TEXT,
    success     INTEGER DEFAULT 0,
    -- Stage 2 补齐（q1_hash IS NULL 视作待补）：
    q1_hash     TEXT,
    msg_count   INTEGER,
    user_turns  INTEGER,
    q1_preview  TEXT,
    chain_key   TEXT,
    has_res     INTEGER,
    attempts    INTEGER DEFAULT 0   -- enrich 尝试次数；瞬态失败累计到 _MAX_ENRICH_ATTEMPTS 才标死
);
CREATE INDEX IF NOT EXISTS requests_pending ON requests(q1_hash) WHERE q1_hash IS NULL;
CREATE INDEX IF NOT EXISTS requests_lookup  ON requests(api_key, q1_hash);

CREATE TABLE IF NOT EXISTS sessions (
    session_key     TEXT PRIMARY KEY,
    api_key         TEXT DEFAULT '',
    q1              TEXT DEFAULT '',
    models          TEXT DEFAULT '[]',   -- JSON 数组，配合 json_each 过滤
    latest_file     TEXT DEFAULT '',
    msg_count       INTEGER DEFAULT 0,
    first_ts        TEXT DEFAULT '',
    last_ts         TEXT DEFAULT '',
    has_failure     INTEGER DEFAULT 0,
    best_req_count  INTEGER DEFAULT 0,
    max_real_turns  INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS sessions_last_ts ON sessions(last_ts);
CREATE INDEX IF NOT EXISTS sessions_api_key ON sessions(api_key);

CREATE TABLE IF NOT EXISTS traces (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    session_key  TEXT,
    filename     TEXT,
    model        TEXT,
    msg_count    INTEGER,
    ts           TEXT,
    success      INTEGER DEFAULT 1
);
CREATE INDEX IF NOT EXISTS traces_sk ON traces(session_key);

CREATE TABLE IF NOT EXISTS chain_index (
    lookup_key   TEXT PRIMARY KEY,
    session_key  TEXT
);

CREATE TABLE IF NOT EXISTS meta (
    k TEXT PRIMARY KEY,
    v TEXT
);
"""


def _connect(leaf_dir: str, write: bool = False) -> sqlite3.Connection:
    path = db_path(leaf_dir)
    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")
    if write:
        conn.executescript(_SCHEMA)
        _migrate(conn)
    return conn


def _migrate(conn: sqlite3.Connection) -> None:
    """对既有 db 做轻量列迁移（CREATE TABLE IF NOT EXISTS 不会给旧表补列）。"""
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(requests)")}
    if "attempts" not in cols:
        conn.execute("ALTER TABLE requests ADD COLUMN attempts INTEGER DEFAULT 0")
        conn.commit()


def _db_exists(leaf_dir: str) -> bool:
    return os.path.isfile(db_path(leaf_dir))

def _meta_get(conn: sqlite3.Connection, k: str, default: str = "") -> str:
    row = conn.execute("SELECT v FROM meta WHERE k=?", (k,)).fetchone()
    return row["v"] if row else default


def _meta_set(conn: sqlite3.Connection, k: str, v: str) -> None:
    conn.execute(
        "INSERT INTO meta(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
        (k, str(v)),
    )


# ── Stage 1：摄取 index.jsonl 新增行 ──────────────────────────────────────

def _iter_index_lines(leaf_dir: str, start_offset: int) -> Tuple[List[dict], int]:
    """从 index.jsonl 的 start_offset 处读到末尾，返回 (entries, end_offset)。
    只解析非 _meta 行；解析失败的行跳过。"""
    index_file = os.path.join(leaf_dir, _INDEX_NAME)
    entries: List[dict] = []
    try:
        with open(index_file, "rb") as f:
            f.seek(start_offset)
            data = f.read()
            end_offset = f.tell()
    except OSError:
        return [], start_offset
    for raw in data.split(b"\n"):
        line = raw.decode("utf-8", errors="ignore").strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        if entry.get("_meta"):
            continue
        entries.append(entry)
    return entries, end_offset


def ingest(leaf_dir: str) -> int:
    """Stage 1：把 index.jsonl 新增行插入 requests（meta 列留 NULL）。幂等、增量。

    截断/轮转（当前文件大小 < 记录偏移）→ 清空重摄取。返回本次新增行数。
    """
    index_file = os.path.join(leaf_dir, _INDEX_NAME)
    if not os.path.isfile(index_file):
        return 0
    with _leaf_lock(leaf_dir):
        conn = _connect(leaf_dir, write=True)
        try:
            offset = int(_meta_get(conn, "ingest_offset", "0") or "0")
            try:
                cur_size = os.path.getsize(index_file)
            except OSError:
                cur_size = 0
            if cur_size < offset:
                # 文件被截断/轮转：全量重摄取
                conn.execute("DELETE FROM requests")
                offset = 0
            elif cur_size == offset:
                conn.close()
                return 0

            entries, end_offset = _iter_index_lines(leaf_dir, offset)
            n = 0
            for e in entries:
                req_file = (e.get("req_file") or "").strip()
                if not req_file:
                    continue
                usage = e.get("usage") if isinstance(e.get("usage"), dict) else {}
                success = 1 if (usage.get("token_out") or 0) > 0 else 0
                conn.execute(
                    "INSERT OR IGNORE INTO requests(req_file, ts, model, api_key, usage_json, success) "
                    "VALUES(?,?,?,?,?,?)",
                    (
                        req_file,
                        str(e.get("ts") or ""),
                        str(e.get("model") or ""),
                        str(e.get("api_key") or ""),
                        json.dumps(usage, ensure_ascii=False),
                        success,
                    ),
                )
                n += 1
            _meta_set(conn, "ingest_offset", end_offset)
            _meta_set(conn, "updated_at", str(int(time.time())))
            if n:
                _meta_set(conn, "sessions_dirty", "1")
            conn.commit()
            return n
        finally:
            conn.close()


# ── Stage 2：补 meta（叶子内分片多进程）────────────────────────────────────

def pending_count(leaf_dir: str) -> int:
    if not _db_exists(leaf_dir):
        return 0
    conn = _connect(leaf_dir)
    try:
        row = conn.execute("SELECT COUNT(*) AS c FROM requests WHERE q1_hash IS NULL").fetchone()
        return row["c"] if row else 0
    finally:
        conn.close()


def _worker_enrich(leaf_dir: str, req_files: List[str]) -> List[tuple]:
    """子进程：解析一批合并文件，算 meta，返回 (req_file, q1_hash, msg_count,
    user_turns, q1_preview, chain_key, has_res, success, model, ts) 列表。"""
    from utils.newapi_format import parse_combined_file, compute_session_fields
    from utils.newapi_consumer import _resolve_combined_path

    leaf = Path(leaf_dir)
    out: List[tuple] = []
    for req_file in req_files:
        path = _resolve_combined_path(leaf, req_file)
        if path is None:
            # 文件缺失/损坏：q1_hash='' 防重扫，msg_count=-1 作解析失败标记
            # （rebuild_sessions 会跳过，等价 _process_entry 的 path/parsed None → return）
            out.append((req_file, "", -1, 0, "", "", 0, 0, "", ""))
            continue
        parsed = parse_combined_file(path)
        if parsed is None:
            out.append((req_file, "", -1, 0, "", "", 0, 0, "", ""))
            continue
        messages = parsed["messages"]
        fields = compute_session_fields(messages)
        has_res = 1 if parsed["assistant_content"] is not None else 0
        out.append((
            req_file,
            fields["q1_hash"] or "",
            fields["msg_count"],
            fields["user_turns"],
            fields["q1_preview"],
            fields["chain_key"],
            has_res,
            1 if parsed["success"] else 0,
            parsed["model"] or "",
            parsed["ts"] or "",
        ))
    return out


def _chunk(items: List[Any], n: int) -> List[List[Any]]:
    if n <= 1 or len(items) <= 1:
        return [items] if items else []
    size = (len(items) + n - 1) // n
    return [items[i:i + size] for i in range(0, len(items), size)]


def enrich(leaf_dir: str, workers: int = 8, progress_cb=None,
           stall_timeout: float = 300.0) -> Dict[str, Any]:
    """Stage 2：把 requests 中 q1_hash IS NULL 的行补齐 meta。叶子内分片并行。

    progress_cb(done, total) 可选，供上层刷新进度。返回 {enriched, total}。
    stall_timeout：进程池「距上次有分片完成」超过此秒数仍有未完成分片，判定卡住
    （多为 NFS 读被 hard 挂载死等），抛弃池并 raise LeafStalledError，交上层跳过。
    """
    if not _db_exists(leaf_dir):
        return {"enriched": 0, "total": 0}
    conn = _connect(leaf_dir, write=True)
    try:
        pending = [r["req_file"] for r in conn.execute(
            "SELECT req_file FROM requests WHERE q1_hash IS NULL ORDER BY rowid"
        ).fetchall()]
    finally:
        conn.close()

    total = len(pending)
    if not total:
        return {"enriched": 0, "total": 0}

    workers = max(1, min(int(workers or 1), (os.cpu_count() or 4)))
    # 固定片大小切分：份数 = ceil(total / chunk_size)，至少 workers 份（保证并行用满）。
    # 每片 ~_ENRICH_CHUNK_SIZE 条，单 worker 常驻内存不随文件总数放大 → 巨型叶子不再 OOM。
    n_chunks = max(workers, (total + _ENRICH_CHUNK_SIZE - 1) // _ENRICH_CHUNK_SIZE)
    chunks = _chunk(pending, n_chunks)
    done = 0

    def _apply(rows: List[tuple]) -> None:
        nonlocal done
        ok_rows = [r for r in rows if r[2] >= 0]     # msg_count>=0：解析成功
        fail_rows = [r for r in rows if r[2] < 0]    # msg_count==-1：文件缺失/损坏
        with _leaf_lock(leaf_dir):
            w = _connect(leaf_dir, write=True)
            try:
                if ok_rows:
                    w.executemany(
                        "UPDATE requests SET q1_hash=?, msg_count=?, user_turns=?, q1_preview=?, "
                        "chain_key=?, has_res=?, success=?, model=COALESCE(NULLIF(?,''), model), "
                        "ts=COALESCE(NULLIF(?,''), ts) WHERE req_file=?",
                        [(r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9], r[0]) for r in ok_rows],
                    )
                if fail_rows:
                    # 瞬态失败（合并文件多为「尚未落盘」）：累加 attempts，q1_hash 仍留 NULL 下轮重扫重试；
                    # 累计到 _MAX_ENRICH_ATTEMPTS 才标死（q1_hash='' + msg_count=-1）永久跳过，避免无限重扫。
                    w.executemany(
                        "UPDATE requests SET attempts = COALESCE(attempts,0) + 1, "
                        "q1_hash = CASE WHEN COALESCE(attempts,0) + 1 >= ? THEN '' ELSE q1_hash END, "
                        "msg_count = CASE WHEN COALESCE(attempts,0) + 1 >= ? THEN -1 ELSE msg_count END "
                        "WHERE req_file = ?",
                        [(_MAX_ENRICH_ATTEMPTS, _MAX_ENRICH_ATTEMPTS, r[0]) for r in fail_rows],
                    )
                _meta_set(w, "sessions_dirty", "1")
                w.commit()
            finally:
                w.close()
        done += len(rows)
        if progress_cb:
            progress_cb(done, total)

    if workers == 1:
        for ch in chunks:
            _apply(_worker_enrich(leaf_dir, ch))
    else:
        # 手动持有池（不用 with：其 __exit__ 会 join 死等卡死的 D-worker）。
        # 盯「上次有分片完成」的时刻：一段 stall_timeout 内无任何分片完成，
        # 判定卡住 → 抛弃池（后台收尸）并 raise，交上层跳过该叶子。
        # BrokenProcessPool（worker 进程被 OS 终止）：重建池续跑。enrich 逐片增量
        # 落库（已完成的 q1_hash 已写回），重试重新查 q1_hash IS NULL 自然断点续跑。
        cur_workers = workers
        retries_left = _POOL_BROKEN_RETRIES
        while True:
            pool = ProcessPoolExecutor(max_workers=cur_workers, mp_context=_MP_CTX)
            clean = False
            try:
                # 每次进循环重查待处理行：重试时跳过已落库的分片（断点续跑）。
                conn = _connect(leaf_dir, write=True)
                try:
                    pending = [r["req_file"] for r in conn.execute(
                        "SELECT req_file FROM requests WHERE q1_hash IS NULL ORDER BY rowid"
                    ).fetchall()]
                finally:
                    conn.close()
                if not pending:
                    return {"enriched": done, "total": total}
                n_chunks = max(cur_workers,
                               (len(pending) + _ENRICH_CHUNK_SIZE - 1) // _ENRICH_CHUNK_SIZE)
                chunks = _chunk(pending, n_chunks)
                futs = {pool.submit(_worker_enrich, leaf_dir, ch): ch for ch in chunks}
                pending_set = set(futs)
                last_progress = time.monotonic()
                while pending_set:
                    finished, pending_set = wait(
                        pending_set, timeout=stall_timeout, return_when=FIRST_COMPLETED)
                    if not finished:
                        # 本轮超时窗口内一个分片都没完成 → 卡住
                        raise LeafStalledError(
                            leaf_dir, done, total, time.monotonic() - last_progress)
                    for fut in finished:
                        _apply(fut.result())
                    last_progress = time.monotonic()
                clean = True
            except BrokenProcessPool:
                # 池已废，立即抛弃（后台收尸）；重试前先重查 pending，跳过已完成的片。
                pool.shutdown(wait=False, cancel_futures=True)
                _reap_pool_async(pool)
                if retries_left <= 0:
                    raise
                retries_left -= 1
                # 降并发重试（半 worker），缓解偶发资源峰；下一轮循环重建池、续跑剩余片。
                cur_workers = max(1, cur_workers // 2)
                logger.warning(
                    "enrich worker 进程池 BrokenProcessPool（worker 被终止，多为偶发崩溃）；"
                    "重建进程池续跑剩余 %d 条（重试 %d/%d，并发 %d→%d）",
                    len(pending), _POOL_BROKEN_RETRIES - retries_left, _POOL_BROKEN_RETRIES,
                    max(1, cur_workers * 2), cur_workers)
                continue
            finally:
                if clean:
                    # 全部分片正常完成：worker 已空闲，join 立即返回
                    pool.shutdown(wait=True)
                elif not isinstance(sys.exc_info()[1], BrokenProcessPool):
                    # 卡死 or 其它异常：不 join（可能被 D-worker 拖死），抛弃 + 后台收尸。
                    # BrokenProcessPool 分支已在上方 shutdown+收尸，此处不再重复。
                    pool.shutdown(wait=False, cancel_futures=True)
                    _reap_pool_async(pool)
            break

    return {"enriched": done, "total": total}


# ── Stage 3：从 requests 聚合出 sessions（纯内存，口径对齐 _process_entry）──

def rebuild_sessions(leaf_dir: str) -> int:
    """Stage 3：清空 sessions/traces/chain_index，从 requests(meta 齐全) 全量重建。

    按 rowid（= 摄取顺序 = index.jsonl 追加顺序 = 时间序）遍历，复刻 newapi_consumer
    ._process_entry 的会话归并与切分逻辑。返回 session 数。
    """
    if not _db_exists(leaf_dir):
        return 0
    with _leaf_lock(leaf_dir):
        conn = _connect(leaf_dir, write=True)
        try:
            rows = conn.execute(
                "SELECT req_file, ts, model, api_key, q1_hash, msg_count, user_turns, "
                "q1_preview, chain_key, has_res, success FROM requests "
                "WHERE q1_hash IS NOT NULL AND msg_count >= 0 ORDER BY rowid"
            ).fetchall()

            sessions: Dict[str, Dict[str, Any]] = {}
            chain_map: Dict[str, str] = {}

            for r in rows:
                _agg_one(r, sessions, chain_map)

            conn.execute("DELETE FROM sessions")
            conn.execute("DELETE FROM traces")
            conn.execute("DELETE FROM chain_index")

            for sk, s in sessions.items():
                has_failure = 1 if any(not t.get("_ok", True) for t in s["trace_list"]) else 0
                conn.execute(
                    "INSERT INTO sessions(session_key, api_key, q1, models, latest_file, msg_count, "
                    "first_ts, last_ts, has_failure, best_req_count, max_real_turns) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        sk, s["api_key"], s["q1"], json.dumps(s["models"], ensure_ascii=False),
                        s["latest_file"], s["msg_count"], s["first_ts"], s["last_ts"],
                        has_failure, s["_best_req_count"], s["_max_real_turns"],
                    ),
                )
                conn.executemany(
                    "INSERT INTO traces(session_key, filename, model, msg_count, ts, success) "
                    "VALUES(?,?,?,?,?,?)",
                    [(sk, t["filename"], t["model"], t["msg_count"], t["ts"],
                      1 if t.get("_ok", True) else 0) for t in s["trace_list"]],
                )
            for lk, sk in chain_map.items():
                conn.execute(
                    "INSERT OR REPLACE INTO chain_index(lookup_key, session_key) VALUES(?,?)",
                    (lk, sk),
                )
            _meta_set(conn, "sessions_dirty", "0")
            _meta_set(conn, "sessions_built_at", str(int(time.time())))
            conn.commit()
            return len(sessions)
        finally:
            conn.close()


def _agg_one(r: sqlite3.Row, sessions: Dict[str, Dict[str, Any]],
             chain_map: Dict[str, str]) -> None:
    """处理一条 request 行，就地更新 sessions/chain_map。等价 _process_entry 内存分支。"""
    api_key = r["api_key"] or ""
    model = r["model"] or ""
    filename = os.path.basename(r["req_file"] or "")
    ts = r["ts"] or filename
    q1_hash = r["q1_hash"] or ""
    msg_count = r["msg_count"] or 0
    user_turns = r["user_turns"] or 0
    has_res = bool(r["has_res"])
    success = bool(r["success"])
    full_msg_count = msg_count + (1 if has_res else 0)

    lookup_key = f"{api_key}||{q1_hash}"
    sess_key = chain_map.get(lookup_key)
    session = sessions.get(sess_key) if sess_key else None

    # 新会话检测：user_turns 回落 ≤1 且低于峰值 → 另起 ##session_N
    if session is not None and user_turns <= 1 and \
       user_turns < session.get("_max_real_turns", 1):
        suffix = 1
        new_lookup = f"{lookup_key}##session_{suffix}"
        while new_lookup in chain_map:
            suffix += 1
            new_lookup = f"{lookup_key}##session_{suffix}"
        lookup_key = new_lookup
        session = None
        sess_key = None

    trace_entry = {"filename": filename, "model": model, "msg_count": full_msg_count,
                   "ts": ts, "_ok": success}

    if session is None:
        sess_key = ts
        while sess_key in sessions:
            sess_key = sess_key + "_x"
        q1 = r["q1_preview"] or (r["chain_key"] or "")[:200]
        sessions[sess_key] = {
            "api_key": api_key,
            "q1": q1,
            "models": [model] if model else [],
            "latest_file": filename,
            # 对齐 _new_session：新会话 msg_count 用 raw msg_count（更新分支才用 full_msg_count）
            "msg_count": msg_count,
            "first_ts": ts,
            "last_ts": ts,
            "_best_req_count": msg_count,
            "_max_real_turns": user_turns,
            "trace_list": [trace_entry],
        }
        chain_map[lookup_key] = sess_key
    else:
        models = session["models"]
        if model and model not in models:
            models.append(model)
        session["_max_real_turns"] = max(user_turns, session.get("_max_real_turns", 0))
        best = session.get("_best_req_count", 0)
        if msg_count > best or (msg_count == best and has_res):
            session["latest_file"] = filename
            session["msg_count"] = full_msg_count
            session["_best_req_count"] = msg_count
        session["last_ts"] = ts
        session["trace_list"].append(trace_entry)


# ── 一条龙（批量构建用）────────────────────────────────────────────────────

def build_leaf(leaf_dir: str, workers: int = 8, progress_cb=None,
               stall_timeout: float = 300.0) -> Dict[str, Any]:
    """ingest → enrich → rebuild_sessions 一条龙。返回汇总统计。
    enrich 卡住会 raise LeafStalledError（不吞），交上层 _run 跳过该叶子。"""
    ingested = ingest(leaf_dir)
    en = enrich(leaf_dir, workers=workers, progress_cb=progress_cb,
                stall_timeout=stall_timeout)
    n_sessions = rebuild_sessions(leaf_dir)
    return {
        "ingested": ingested,
        "enriched": en.get("enriched", 0),
        "sessions": n_sessions,
        "pending": pending_count(leaf_dir),
    }


def remove_db(leaf_dir: str) -> None:
    """删除叶子 index.db（含 WAL/SHM）。force 重建或修数据用。"""
    p = db_path(leaf_dir)
    for path in (p, p + "-wal", p + "-shm"):
        try:
            if os.path.isfile(path):
                os.remove(path)
        except OSError:
            pass


def needs_build(leaf_dir: str) -> bool:
    """叶子是否需要构建/重建：无 index.db / index.jsonl 有新增（offset 落后）/
    有待补 meta（pending>0）/ sessions 脏，任一为真即 True。

    与 leaf_completed 严格互补：对同一个有 index.jsonl 的叶子，
    leaf_completed 为真 ⇔ needs_build 为假。回填增量模式（_run 非 force）用它筛掉
    已完成的叶子，ensure_fresh 用它判断是否要真正重建，避免无谓开进程池。

    只读、轻量：一次 getsize + 一条 meta 读取 + 一条 COUNT，不开进程池、不写库。
    index.jsonl 不存在的不是有效叶子，返回 False（无从构建）。
    """
    index_file = os.path.join(leaf_dir, _INDEX_NAME)
    if not os.path.isfile(index_file):
        return False
    if not _db_exists(leaf_dir):
        return True  # 从未构建过 → 需要构建
    conn = _connect(leaf_dir)
    try:
        offset = int(_meta_get(conn, "ingest_offset", "0") or "0")
        pending = conn.execute(
            "SELECT COUNT(*) AS c FROM requests WHERE q1_hash IS NULL").fetchone()["c"]
        dirty = _meta_get(conn, "sessions_dirty", "1") == "1"
    except sqlite3.OperationalError:
        return True  # db 半成品（上次构建被 kill 等）：需要重建
    finally:
        conn.close()
    try:
        cur_size = os.path.getsize(index_file)
    except OSError:
        return False
    # index.jsonl 有新增（offset 落后）/ 有待补 meta / sessions 脏 → 需要构建
    return cur_size != offset or pending > 0 or dirty


def leaf_completed(leaf_dir: str) -> bool:
    """叶子是否「已完成跟上」：index.db 存在，且已追上 index.jsonl 当前大小，
    且无待补 meta（pending=0）、sessions 不脏。

    与 needs_build 互补：needs_build 回答「要不要重建」（无 db / 有新增 / 有待补 /
    sessions 脏任一即 True）；本函数回答「现在是否已是完成态」——needs_build 的
    反向精确判据，用于「数据管理」刷新时的主动核对：实时读每个叶子，而不是只看
    log_dir.db 里上次同步的记录。

    只读、轻量：一次 getsize + 一条 meta 读取 + 两条 COUNT，不开进程池、不写库，
    可安全放在请求热路径（配合上层 TTL 缓存使用）。index.jsonl 都不存在的不算
    有效叶子，返回 False。
    """
    index_file = os.path.join(leaf_dir, _INDEX_NAME)
    if not os.path.isfile(index_file):
        return False
    if not _db_exists(leaf_dir):
        return False
    conn = _connect(leaf_dir)
    try:
        offset = int(_meta_get(conn, "ingest_offset", "0") or "0")
        pending = conn.execute("SELECT COUNT(*) AS c FROM requests WHERE q1_hash IS NULL").fetchone()["c"]
        dirty = _meta_get(conn, "sessions_dirty", "1") == "1"
    except sqlite3.OperationalError:
        return False  # db 半成品（上次构建被 kill 等）：未完成
    finally:
        conn.close()
    try:
        cur_size = os.path.getsize(index_file)
    except OSError:
        return False
    return cur_size == offset and pending == 0 and not dirty


def ensure_fresh(leaf_dir: str, workers: int = 8) -> Dict[str, Any]:
    """把叶子 index.db 同步追平 index.jsonl 当前状态，供「导出/统计」确保数据完整。

    有变化才构建（needs_build 判据：无 db / index.jsonl 有新增 / 有待补 meta / sessions 脏）。
    对活跃叶子尾部「合并文件尚未落盘」的行，enrich 会标为瞬态失败并保留待补状态；这里按
    _MAX_ENRICH_ATTEMPTS 轮循环 ingest→enrich→rebuild（轮间短暂 sleep 给落盘时间），
    直到 pending 归零（含落盘成功补齐 + 持续缺失被标死）或达轮次上限。返回 status()。

    注意：这会开进程池做真正的解析补 meta，属「贵操作」，只应在后台任务（导出/回填）里同步调用，
    绝不放到请求视图热路径上。跨实例并发由 _leaf_lock + SQLite busy_timeout 兜底。
    """
    if not os.path.isfile(os.path.join(leaf_dir, _INDEX_NAME)):
        return status(leaf_dir)
    if not needs_build(leaf_dir):
        return status(leaf_dir)
    try:
        build_leaf(leaf_dir, workers=workers)  # 第 1 轮：ingest + enrich + rebuild
        rounds = 1
        while pending_count(leaf_dir) > 0 and rounds < _MAX_ENRICH_ATTEMPTS:
            time.sleep(0.5)          # 给活跃叶子未落盘的合并文件一点落盘时间
            ingest(leaf_dir)         # 期间 index.jsonl 可能又追加了新行
            enrich(leaf_dir, workers=workers)
            rebuild_sessions(leaf_dir)
            rounds += 1
    except Exception:
        pass
    return status(leaf_dir)


# ── 状态 / 读 API（供视图）─────────────────────────────────────────────────

def status(leaf_dir: str) -> Dict[str, Any]:
    """{built, ingested, pending, sessions, sessions_dirty}。built=有 sessions 且无待补。"""
    if not _db_exists(leaf_dir):
        return {"built": False, "ingested": 0, "pending": 0, "sessions": 0, "sessions_dirty": True}
    conn = _connect(leaf_dir)
    try:
        ingested = conn.execute("SELECT COUNT(*) AS c FROM requests").fetchone()["c"]
        pending = conn.execute("SELECT COUNT(*) AS c FROM requests WHERE q1_hash IS NULL").fetchone()["c"]
        n_sess = conn.execute("SELECT COUNT(*) AS c FROM sessions").fetchone()["c"]
        dirty = _meta_get(conn, "sessions_dirty", "1") == "1"
        updated_at = _meta_get(conn, "updated_at", "0") or "0"
    finally:
        conn.close()
    return {
        "built": (pending == 0 and n_sess > 0 and not dirty),
        "ingested": ingested,
        "pending": pending,
        "sessions": n_sess,
        "sessions_dirty": dirty,
        "updated_at": updated_at,
    }


def _row_to_session(row: sqlite3.Row) -> Dict[str, Any]:
    try:
        models = json.loads(row["models"] or "[]")
    except (json.JSONDecodeError, TypeError):
        models = []
    return {
        "session_key": row["session_key"],
        "api_key": row["api_key"] or "",
        "q1": row["q1"] or "",
        "models": models,
        "latest_file": row["latest_file"] or "",
        "msg_count": row["msg_count"] or 0,
        "first_ts": row["first_ts"] or "",
        "last_ts": row["last_ts"] or "",
        "has_failure": bool(row["has_failure"]),
    }


def list_sessions(leaf_dir: str, api_key: str = "", model: str = "",
                  min_msg_count: int = 0, offset: int = 0, limit: int = 0) -> List[Dict[str, Any]]:
    if not _db_exists(leaf_dir):
        return []
    conn = _connect(leaf_dir)
    try:
        sql = "SELECT * FROM sessions WHERE 1=1"
        params: list = []
        if api_key:
            sql += " AND api_key = ?"; params.append(api_key)
        if min_msg_count > 0:
            sql += " AND msg_count >= ?"; params.append(min_msg_count)
        if model:
            sql += " AND EXISTS (SELECT 1 FROM json_each(models) WHERE value = ?)"; params.append(model)
        sql += " ORDER BY last_ts DESC"
        if limit > 0:
            sql += " LIMIT ? OFFSET ?"; params.extend([limit, offset])
        rows = conn.execute(sql, params).fetchall()
        return [_row_to_session(r) for r in rows]
    finally:
        conn.close()


def count_sessions(leaf_dir: str, api_key: str = "", model: str = "",
                   min_msg_count: int = 0) -> int:
    if not _db_exists(leaf_dir):
        return 0
    conn = _connect(leaf_dir)
    try:
        sql = "SELECT COUNT(*) AS c FROM sessions WHERE 1=1"
        params: list = []
        if api_key:
            sql += " AND api_key = ?"; params.append(api_key)
        if min_msg_count > 0:
            sql += " AND msg_count >= ?"; params.append(min_msg_count)
        if model:
            sql += " AND EXISTS (SELECT 1 FROM json_each(models) WHERE value = ?)"; params.append(model)
        row = conn.execute(sql, params).fetchone()
        return row["c"] if row else 0
    finally:
        conn.close()


def get_traces_batch(leaf_dir: str, session_keys: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    if not session_keys or not _db_exists(leaf_dir):
        return {k: [] for k in session_keys}
    conn = _connect(leaf_dir)
    try:
        rows = []
        # 按固定批大小切分：单条 SQL 的变量数受 SQLITE_MAX_VARIABLE_NUMBER 限制
        # （3.32+ 为 32767，更早为 999），整叶导出动辄 3-10w session，
        # 一次性拼 IN 会抛 "too many SQL variables"。分批后语义不变。
        for i in range(0, len(session_keys), _TRACES_IN_CHUNK):
            chunk = session_keys[i:i + _TRACES_IN_CHUNK]
            ph = ",".join("?" * len(chunk))
            rows.extend(conn.execute(
                f"SELECT * FROM traces WHERE session_key IN ({ph}) ORDER BY ts ASC, id ASC",
                chunk,
            ).fetchall())
        result: Dict[str, List[Dict[str, Any]]] = {k: [] for k in session_keys}
        for row in rows:
            t: Dict[str, Any] = {
                "filename": row["filename"],
                "model": row["model"],
                "msg_count": row["msg_count"],
                "ts": row["ts"],
            }
            if not row["success"]:
                t["success"] = False
            result[row["session_key"]].append(t)
        return result
    finally:
        conn.close()


def get_known_models(leaf_dir: str) -> List[str]:
    if not _db_exists(leaf_dir):
        return []
    conn = _connect(leaf_dir)
    try:
        rows = conn.execute(
            "SELECT DISTINCT value FROM sessions, json_each(sessions.models) "
            "WHERE value != '' ORDER BY value"
        ).fetchall()
        return [r["value"] for r in rows]
    finally:
        conn.close()


def get_known_keys(leaf_dir: str) -> List[str]:
    if not _db_exists(leaf_dir):
        return []
    conn = _connect(leaf_dir)
    try:
        rows = conn.execute(
            "SELECT DISTINCT api_key FROM sessions WHERE api_key != '' ORDER BY api_key"
        ).fetchall()
        return [r["api_key"] for r in rows]
    finally:
        conn.close()


def export_sessions(leaf_dir: str) -> List[Dict[str, Any]]:
    """返回完整 session 列表（含 trace_list），行格式与 session_store.export_sessions 兼容，
    供 export_sync.export_session_index 生成 session_index.jsonl。"""
    sessions = list_sessions(leaf_dir)
    if not sessions:
        return []
    traces_by_key = get_traces_batch(leaf_dir, [s["session_key"] for s in sessions])
    result: List[Dict[str, Any]] = []
    for s in sessions:
        sk = s["session_key"]
        result.append({
            "_key": sk,
            "api_key": s["api_key"],
            "q1": s["q1"],
            "first_ts": s["first_ts"],
            "last_ts": s["last_ts"],
            "models": s["models"],
            "latest_file": s["latest_file"],
            "msg_count": s["msg_count"],
            "trace_list": traces_by_key.get(sk, []),
        })
    return result


def get_session_stats(leaf_dir: str, threshold: int = 5) -> Dict[str, Dict[str, int]]:
    """{ '{api_key}|{date}': {total, qualified} }，date=first_ts[:10]，
    qualified = trace 数 >= threshold 的 session。对齐 session_store.get_session_stats 语义，
    供 stats_index 的 new-api 分支读归并统计（date 用 ts[:10]，与 _sessions_from_index 一致）。"""
    if not _db_exists(leaf_dir):
        return {}
    conn = _connect(leaf_dir)
    try:
        tc = {r["session_key"]: r["c"] for r in conn.execute(
            "SELECT session_key, COUNT(*) AS c FROM traces GROUP BY session_key"
        ).fetchall()}
        rows = conn.execute("SELECT session_key, api_key, first_ts FROM sessions").fetchall()
    finally:
        conn.close()
    from collections import defaultdict
    buckets: Dict[str, Dict[str, int]] = defaultdict(lambda: {"total": 0, "qualified": 0})
    for r in rows:
        api_key = r["api_key"] or "(empty)"
        ts = r["first_ts"] or ""
        date_str = ts[:10] if len(ts) >= 10 else "unknown"
        bk = f"{api_key}|{date_str}"
        buckets[bk]["total"] += 1
        if tc.get(r["session_key"], 0) >= threshold:
            buckets[bk]["qualified"] += 1
    return dict(buckets)
