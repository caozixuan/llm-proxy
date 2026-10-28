"""
utils/logdir_store.py — 日志路径叶子（子节点）构建状态持久化

SQLite 后端，记录每个 new-api 源下每个叶子目录（小时节点）的索引构建状态。
数据库路径：{service_log_dir}/log_dir.db

替代旧的「进程内存 + 前端轮询」方案：状态落盘、重启不丢、粒度到叶子。
- 「同步」扫描源的叶子目录，把新增节点写入（state=pending），已存在的更新构建状态。
- 回填过程逐叶写入 building/done/error。
- 列表页读汇总（count_summary），不再实时全盘 stat。

sources 表（「数据管理」表 1）是日志路径列表的唯一数据源，无任何 YAML 导入/镜像：
增删改全部以本表为准；活跃目录行（root_id='default'）由启动时的
_refresh_active_row 刷新/补插。

主键 (root_id, dir_key)：root_id 来自 logs_config.get_root_id（消除同 basename 冲突），
dir_key 来自 log_scan.dir_key_for（稳定、已折叠冗余层、可逆）。
"""

import json as _json
import os
import sqlite3
import threading
import time
from datetime import datetime
from typing import List, Optional

_lock = threading.RLock()
_conn: Optional[sqlite3.Connection] = None
_db_path: str = ""

# models_by_leaf_path() 的结果缓存：(计算时刻, {leaf_path: set(model)})。
# 全表 4589 行 token_stats_json 解析约 0.09s，而调用方按叶子逐个调用，
# 不缓存就要付「叶子数 × 0.09s」。写侧变化（新叶子同步）最多滞后 _MODELS_TTL 被看到。
_MODELS_CACHE: Optional[tuple] = None
_MODELS_TTL = 60  # 秒

# dir_list() 的结果缓存：(失效签名, 计算时刻, {root_id: [...]})。
# 签名 = (leaf_status 行数, MAX(synced_at))，同步进程写入新行才变。
# 解析 4600 行 token_stats_json 约 0.018s，缓存在 TTL 内降到 0。
_DIR_LIST_CACHE: Optional[tuple] = None
_DIR_LIST_TTL = 300  # 秒


def init_db(db_dir: str):
    global _conn, _db_path
    _db_path = os.path.join(db_dir, "log_dir.db")
    os.makedirs(db_dir, exist_ok=True)
    _conn = sqlite3.connect(_db_path, check_same_thread=False)
    _conn.execute("PRAGMA journal_mode=WAL")
    _conn.execute("PRAGMA busy_timeout=5000")
    _conn.row_factory = sqlite3.Row
    with _lock:
        _conn.execute("""
            CREATE TABLE IF NOT EXISTS leaf_status (
                root_id     TEXT NOT NULL,
                dir_key     TEXT NOT NULL,
                root_path   TEXT NOT NULL DEFAULT '',
                leaf_path   TEXT NOT NULL DEFAULT '',
                built       INTEGER NOT NULL DEFAULT 0,
                ingested    INTEGER NOT NULL DEFAULT 0,
                pending     INTEGER NOT NULL DEFAULT 0,
                sessions    INTEGER NOT NULL DEFAULT 0,
                state       TEXT NOT NULL DEFAULT 'pending',  -- pending|building|done|error
                last_error  TEXT NOT NULL DEFAULT '',
                synced_at   TEXT NOT NULL DEFAULT (datetime('now','localtime')),
                created_at  TEXT NOT NULL DEFAULT (datetime('now','localtime')),
                PRIMARY KEY (root_id, dir_key)
            )
        """)
        _conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_leaf_status_root ON leaf_status(root_id)"
        )
        # 数据源表（「数据管理」表 1）：一源一行。root_id 主键，与 leaf_status 同口径。
        # templates 存 JSON 数组（多行层级模板）；leaf_count/built_count 同步时刷新。
        _conn.execute("""
            CREATE TABLE IF NOT EXISTS sources (
                root_id     TEXT PRIMARY KEY,
                root_path   TEXT NOT NULL DEFAULT '',
                name        TEXT NOT NULL DEFAULT 'default',
                format      TEXT NOT NULL DEFAULT '',
                templates   TEXT NOT NULL DEFAULT '',
                leaf_count  INTEGER NOT NULL DEFAULT 0,
                built_count INTEGER NOT NULL DEFAULT 0,
                synced_at   TEXT NOT NULL DEFAULT (datetime('now','localtime')),
                created_at  TEXT NOT NULL DEFAULT (datetime('now','localtime'))
            )
        """)
        # 回填请求表：把「哪个 root 要回填」从 app 进程内存（newapi_backfill._running）
        # 搬到 DB，供独立 backfill_worker 进程领取执行、app 进程查询运行态。
        # 运行态真相 = status='running' 的行；逐叶进度仍以 leaf_status(count_summary) 为准。
        _conn.execute("""
            CREATE TABLE IF NOT EXISTS backfill_requests (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                root        TEXT NOT NULL,
                workers     INTEGER NOT NULL DEFAULT 8,
                force       INTEGER NOT NULL DEFAULT 0,
                status      TEXT NOT NULL DEFAULT 'pending',  -- pending|running|done|failed
                created_at  TEXT NOT NULL DEFAULT (datetime('now','localtime')),
                started_at  TEXT,
                finished_at TEXT,
                error       TEXT NOT NULL DEFAULT ''
            )
        """)
        _conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_backfill_requests_status ON backfill_requests(status)"
        )
        # 迁移：老库 leaf_status 无 leaf_path 列则补齐（存折叠层已补回的真实叶子
        # 绝对路径，供 mtime→叶子解析直接查表，取代反复扫盘的兜底）。空值=未回填，
        # 解析侧走模板扫兜底；下次同步刷新即落库。
        cols = {r[1] for r in _conn.execute("PRAGMA table_info(leaf_status)").fetchall()}
        if "leaf_path" not in cols:
            _conn.execute(
                "ALTER TABLE leaf_status ADD COLUMN leaf_path TEXT NOT NULL DEFAULT ''"
            )
        # 迁移：把 leaf_status 当成 /sessions 统计的持久缓存。记录上次统计时
        # index.jsonl 的签名（size/mtime）+ 该叶聚合结果 JSON + 所用 threshold，
        # 让统计页免扫盘枚举、未变叶子零重算（详见 stats_index.refresh_index 的
        # leaf_status 分支）。默认值让老库补列后「首打即回填」。
        if "idx_size" not in cols:
            _conn.execute(
                "ALTER TABLE leaf_status ADD COLUMN idx_size INTEGER NOT NULL DEFAULT -1"
            )
        if "idx_mtime" not in cols:
            _conn.execute(
                "ALTER TABLE leaf_status ADD COLUMN idx_mtime REAL NOT NULL DEFAULT 0"
            )
        if "stats_json" not in cols:
            _conn.execute(
                "ALTER TABLE leaf_status ADD COLUMN stats_json TEXT NOT NULL DEFAULT ''"
            )
        if "stats_threshold" not in cols:
            _conn.execute(
                "ALTER TABLE leaf_status ADD COLUMN stats_threshold INTEGER NOT NULL DEFAULT 0"
            )
        if "stats_built_at" not in cols:
            _conn.execute(
                "ALTER TABLE leaf_status ADD COLUMN stats_built_at TEXT NOT NULL DEFAULT ''"
            )
        # 迁移：把 leaf_status 当成 /query token 统计的持久缓存。与 stats_json
        # 同款：记录每叶 index.jsonl 签名（idx_size/idx_mtime 共用）+ 该叶 token 聚合
        # 结果 JSON。让 /query 与 /sessions 一样从 DB 免扫盘读，数据源统一（看板用）。
        # 旧文件 .token_index.jsonl 保留但不更新；新数据一律落本列。
        if "token_stats_json" not in cols:
            _conn.execute(
                "ALTER TABLE leaf_status ADD COLUMN token_stats_json TEXT NOT NULL DEFAULT ''"
            )
        # 迁移：记录「stats_json 快照是基于哪个同步水位算出来的」。
        # stats_json 的失效签名只校验 index.jsonl 的 size+mtime，而 watcher 把新
        # session 折叠进 index.db 时并不改 index.jsonl —— 快照会「名义有效」却
        # 落后于 index.db（实测某叶 sessions 26 个 key、stats_json 只有 24 个）。
        # 在**写侧**（watcher 已打开叶子库，读 meta 近乎免费）记下当时的
        # sessions_synced_rowid，读侧只需比对本地库里的两个整数即可判过期。
        # 读侧若自己去叶子库读 meta，每叶约 73ms（NFS 打开 SQLite），4600 叶
        # 要 5 分钟，不可接受。
        if "stats_sync_rowid" not in cols:
            _conn.execute(
                "ALTER TABLE leaf_status ADD COLUMN stats_sync_rowid INTEGER NOT NULL DEFAULT -1"
            )
        if "sync_rowid" not in cols:
            _conn.execute(
                "ALTER TABLE leaf_status ADD COLUMN sync_rowid INTEGER NOT NULL DEFAULT -1"
            )
        _conn.commit()


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _ready() -> bool:
    return _conn is not None


def upsert_leaf(root_id: str, dir_key: str, root_path: str = "", *,
                leaf_path: str = "",
                built: Optional[bool] = None, ingested: Optional[int] = None,
                pending: Optional[int] = None, sessions: Optional[int] = None,
                state: Optional[str] = None, last_error: str = "",
                idx_size: Optional[int] = None, idx_mtime: Optional[float] = None,
                stats_json: Optional[str] = None, stats_threshold: Optional[int] = None,
                stats_built_at: Optional[str] = None,
                token_stats_json: Optional[str] = None,
                stats_sync_rowid: Optional[int] = None,
                sync_rowid: Optional[int] = None) -> str:
    """新增或更新一个叶子。返回 'added' | 'updated'。仅传入的字段被更新。

    leaf_path: 折叠层已补回的真实叶子绝对路径（含 index.jsonl 的那一层）。传入非空
        才更新，供 resolve_leaf_path 直接查表，取代 mtime→叶子的反复扫盘兜底。
    idx_size/idx_mtime/stats_json/stats_threshold/stats_built_at: /sessions 统计缓存，
        由 stats_index 写入（见 set_leaf_stats）。仅传入非 None 才更新。
    token_stats_json: /query token 统计缓存，由 token_index 写入（见
        set_leaf_token_stats）。仅传入非 None 才更新。共享 idx_size/idx_mtime 签名。
    """
    if not _ready():
        return "skipped"
    with _lock:
        existing = _conn.execute(
            "SELECT dir_key FROM leaf_status WHERE root_id = ? AND dir_key = ?",
            (root_id, dir_key),
        ).fetchone()
        if existing:
            fields = ["synced_at = ?"]
            params: list = [_now()]
            if root_path:
                fields.append("root_path = ?"); params.append(root_path)
            if leaf_path:
                fields.append("leaf_path = ?"); params.append(leaf_path)
            if built is not None:
                fields.append("built = ?"); params.append(int(built))
            if ingested is not None:
                fields.append("ingested = ?"); params.append(ingested)
            if pending is not None:
                fields.append("pending = ?"); params.append(pending)
            if sessions is not None:
                fields.append("sessions = ?"); params.append(sessions)
            if state is not None:
                fields.append("state = ?"); params.append(state)
            if idx_size is not None:
                fields.append("idx_size = ?"); params.append(idx_size)
            if idx_mtime is not None:
                fields.append("idx_mtime = ?"); params.append(idx_mtime)
            if stats_json is not None:
                fields.append("stats_json = ?"); params.append(stats_json)
            if stats_threshold is not None:
                fields.append("stats_threshold = ?"); params.append(stats_threshold)
            if stats_built_at is not None:
                fields.append("stats_built_at = ?"); params.append(stats_built_at)
            if token_stats_json is not None:
                fields.append("token_stats_json = ?"); params.append(token_stats_json)
            if stats_sync_rowid is not None:
                fields.append("stats_sync_rowid = ?"); params.append(stats_sync_rowid)
            if sync_rowid is not None:
                fields.append("sync_rowid = ?"); params.append(sync_rowid)
            fields.append("last_error = ?"); params.append(last_error)
            params.extend([root_id, dir_key])
            _conn.execute(
                f"UPDATE leaf_status SET {', '.join(fields)} WHERE root_id = ? AND dir_key = ?",
                params,
            )
            _conn.commit()
            return "updated"
        else:
            _conn.execute("""
                INSERT INTO leaf_status
                    (root_id, dir_key, root_path, leaf_path, built, ingested, pending, sessions,
                     state, last_error, synced_at, created_at,
                     idx_size, idx_mtime, stats_json, stats_threshold, stats_built_at,
                     token_stats_json, stats_sync_rowid, sync_rowid)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (root_id, dir_key, root_path, leaf_path, int(bool(built)),
                  ingested or 0, pending or 0, sessions or 0,
                  state or "pending", last_error, _now(), _now(),
                  -1 if idx_size is None else idx_size,
                  0 if idx_mtime is None else idx_mtime,
                  stats_json or "",
                  stats_threshold or 0,
                  stats_built_at or "",
                  token_stats_json or "",
                  -1 if stats_sync_rowid is None else stats_sync_rowid,
                  -1 if sync_rowid is None else sync_rowid))
            _conn.commit()
            return "added"


def set_leaf_state(root_id: str, dir_key: str, state: str, *,
                   leaf_path: str = "",
                   built: Optional[bool] = None, ingested: Optional[int] = None,
                   pending: Optional[int] = None, sessions: Optional[int] = None,
                   last_error: str = ""):
    """回填过程写单叶状态（building/done/error）。叶子不存在则插入。

    leaf_path 传入非空则一并落库（真实折叠叶子绝对路径），供后续解析查表。
    """
    if not _ready():
        return
    upsert_leaf(root_id, dir_key, leaf_path=leaf_path, built=built, ingested=ingested,
                pending=pending, sessions=sessions, state=state, last_error=last_error)


def resolve_leaf_path(root_id: str, dir_key: str) -> Optional[str]:
    """按 (root_id, dir_key) 直接查真实叶子绝对路径（同步时已折叠层补回并落库）。

    命中且非空返回路径；未同步 / 老库未回填（leaf_path 为空）返回 None，调用方
    据此退回模板扫兜底。这是取代「每次解析都反复扫盘 + 各处兜底」的查表快路径。
    """
    if not _ready():
        return None
    with _lock:
        row = _conn.execute(
            "SELECT leaf_path FROM leaf_status WHERE root_id = ? AND dir_key = ?",
            (root_id, dir_key),
        ).fetchone()
    if row and row[0]:
        return row[0]
    return None


def bulk_get(root_id: str) -> List[dict]:
    if not _ready():
        return []
    with _lock:
        rows = _conn.execute(
            "SELECT * FROM leaf_status WHERE root_id = ? ORDER BY dir_key", (root_id,)
        ).fetchall()
    return [dict(r) for r in rows]


def count_summary(root_id: str) -> dict:
    """{total, built, pending, building, error}；供列表页一次性读，无需全盘 stat。"""
    if not _ready():
        return {"total": 0, "built": 0, "pending": 0, "building": 0, "error": 0}
    with _lock:
        row = _conn.execute("""
            SELECT
                COUNT(*)                                          AS total,
                COALESCE(SUM(built), 0)                           AS built,
                COALESCE(SUM(state = 'pending'), 0)               AS pending,
                COALESCE(SUM(state = 'building'), 0)              AS building,
                COALESCE(SUM(state = 'error'), 0)                 AS error
            FROM leaf_status WHERE root_id = ?
        """, (root_id,)).fetchone()
    return {k: (row[k] or 0) for k in ("total", "built", "pending", "building", "error")}


def get_last_leaf_synced_at(root_id: str) -> Optional[str]:
    """返回该 root 最后一次叶子同步时间（max(synced_at)），用于检测是否长时间无更新。"""
    if not _ready():
        return None
    with _lock:
        row = _conn.execute(
            "SELECT MAX(synced_at) FROM leaf_status WHERE root_id = ?", (root_id,)
        ).fetchone()
    return row[0] if row and row[0] else None


def has_any(root_id: str) -> bool:
    """该源是否已同步过（DB 里是否有其叶子记录）。"""
    if not _ready():
        return False
    with _lock:
        row = _conn.execute(
            "SELECT 1 FROM leaf_status WHERE root_id = ? LIMIT 1", (root_id,)
        ).fetchone()
    return row is not None


def key_to_leaves() -> dict:
    """从 leaf_status.stats_json 建 {api_key: [leaf_path, ...]} 倒排索引（跨全部 root）。

    stats_json 是同步阶段按 index.jsonl 算好的「{api_key}|{date} -> {total,qualified}」，
    也就是「该 key 落在哪些叶子」的现成答案。相比 build_stats_multi 每次重算
    key→mtime 分布（实测 0.7~1.3s），这里 4591 行全读 + 建索引约 50ms。

    返回的 list 按叶子路径排序，供调用方直接当叶子集用（顺序不影响正确性）。
    """
    if not _ready():
        return {}
    with _lock:
        rows = _conn.execute(
            "SELECT leaf_path, stats_json FROM leaf_status "
            "WHERE leaf_path != '' AND stats_json != ''"
        ).fetchall()
    out: dict = {}
    for r in rows:
        lp = r[0]
        if not lp:
            continue
        try:
            buckets = _json.loads(r[1])
        except (ValueError, TypeError):
            continue
        if not isinstance(buckets, dict):
            continue
        for bucket_key in buckets:
            api_key = bucket_key.split("|", 1)[0]
            if not api_key:
                continue
            lst = out.get(api_key)
            if lst is None:
                out[api_key] = [lp]
            elif lst[-1] != lp:
                lst.append(lp)
    return out


def dir_list() -> dict:
    """从 leaf_status 建 {root_id: [{"dir_key", "leaf_path", "count"}, ...]}。

    目录列表的全部信息（哪些小时目录、各自多少条）在同步阶段就已落库：
    dir_key 是目录，token_stats_json.entry_count 是请求条数。实测两者与
    get_dir_counts()/refresh_index() 逐条相同（f1563a05 的 1385 个叶子 0 处差异），
    但读库 0.018s vs 重算索引 0.821s —— 所以列表页不该再走即时计算。

    count 取 entry_count，缺失时回落 sessions（未跑过 token 统计的叶子）。
    """
    if not _ready():
        return {}
    global _DIR_LIST_CACHE
    now = time.time()
    with _lock:
        # 用「行数 + 最大 synced_at」作失效键：同步进程写入新叶子或更新任何一行
        # 都会改变二者之一。历史目录的行不会被改写，故命中期不会被无谓重算。
        sig_row = _conn.execute(
            "SELECT COUNT(*), MAX(synced_at) FROM leaf_status"
        ).fetchone()
        sig = (sig_row[0], sig_row[1]) if sig_row else (0, None)
        if _DIR_LIST_CACHE is not None and _DIR_LIST_CACHE[0] == sig \
                and now - _DIR_LIST_CACHE[1] < _DIR_LIST_TTL:
            return _DIR_LIST_CACHE[2]
        rows = _conn.execute(
            "SELECT root_id, dir_key, leaf_path, sessions, token_stats_json "
            "FROM leaf_status WHERE dir_key != ''"
        ).fetchall()
    out: dict = {}
    for r in rows:
        root_id, dir_key, leaf_path, sess, raw = r[0], r[1], r[2] or "", r[3] or 0, r[4] or ""
        count = None
        if raw:
            try:
                data = _json.loads(raw)
                if isinstance(data, dict):
                    ec = data.get("entry_count")
                    if isinstance(ec, int):
                        count = ec
            except (ValueError, TypeError):
                pass
        if count is None:
            count = sess
        out.setdefault(root_id, []).append({
            "dir_key": dir_key,
            "leaf_path": leaf_path,
            "count": count,
        })
    for lst in out.values():
        # 按「末段小时」倒序，而不是整串：dir_key 有 '261027/26102720' 与
        # 'details/260730/26073018' 两种形态，按整串排会把 'details/...' 全部
        # 抬到 '26xxxx/...' 之前（'d' > '2'），导致「最新」取到最旧的目录。
        lst.sort(key=lambda d: d["dir_key"].rsplit("/", 1)[-1], reverse=True)
    with _lock:
        _DIR_LIST_CACHE = (sig, now, out)
    return out


def models_by_leaf_path() -> dict:
    """从 token_stats_json 取 {leaf_path: set(model)}，供列表页填模型下拉，免逐叶全表扫。

    token_stats_json.models 的键形如 "{model}|{date}"。值为空（未跑过 token 统计）
    的叶子不出现在返回值里，调用方据此决定是否回退到 SQL 扫描。

    结果按 TTL 缓存：本函数每次会把 4589 行的 token_stats_json 全读并 JSON 解析
    （实测 0.09s）。调用方是「每个叶子调一次」的形态（52 叶 → 4.7s），缓存后
    每个请求只付一次。
    """
    if not _ready():
        return {}
    global _MODELS_CACHE
    now = time.time()
    with _lock:
        if _MODELS_CACHE is not None and now - _MODELS_CACHE[0] < _MODELS_TTL:
            return _MODELS_CACHE[1]
        rows = _conn.execute(
            "SELECT leaf_path, token_stats_json FROM leaf_status "
            "WHERE leaf_path != '' AND token_stats_json != ''"
        ).fetchall()
    out: dict = {}
    for leaf_path, raw in rows:
        if not leaf_path:
            continue
        try:
            data = _json.loads(raw)
        except (ValueError, TypeError):
            continue
        models = (data or {}).get("models") or {}
        if not isinstance(models, dict):
            continue
        names = out.setdefault(leaf_path, set())
        for mk in models:
            name = mk.split("|", 1)[0]
            if name:
                names.add(name)
    with _lock:
        _MODELS_CACHE = (now, out)
    return out


def bulk_get_stats(root_id: str) -> List[dict]:
    """取该 root 全部叶子的统计缓存字段（供 /sessions 免扫盘枚举 + 签名判活）。

    只选统计需要的列，不走 bulk_get 的 SELECT *（省序列化）。返回每行 dict：
    dir_key, leaf_path, idx_size, idx_mtime, stats_json, stats_threshold, token_stats_json。
    """
    if not _ready():
        return []
    with _lock:
        rows = _conn.execute(
            "SELECT dir_key, leaf_path, idx_size, idx_mtime, stats_json, stats_threshold, "
            "sessions, token_stats_json, stats_sync_rowid, sync_rowid "
            "FROM leaf_status WHERE root_id = ? ORDER BY dir_key", (root_id,)
        ).fetchall()
    return [dict(r) for r in rows]


def stale_stats_leaves(limit: int = 100) -> List[dict]:
    """列出「统计快照不可信」的叶子，供 session_worker 后台补算。

    两种判定，任一成立即算：
      1) stats_sync_rowid != sync_rowid —— 快照对应的 index.db 同步水位落后于
         最新水位（watcher 又折叠进了新 session）。
      2) stats_json 的 total 之和 != leaf_status.sessions —— 快照内容与库里的
         真实会话数矛盾。

    第 2 条是必要的：`sync_rowid` 在引入水位之前同步过的叶子上恒为 -1，而
    `stats_sync_rowid` 默认也是 -1，两个 -1 相等会让判定 1 永远不成立。这类叶子
    里混着 index.db 尚未构建时写入的「裸计数近似」（每请求算 1 session，偏大
    5~9 倍，实测 1672 个叶子中招），只靠水位永远发现不了。

    代价：全表读 4709 行 + 解析 JSON 约 0.03s（本地库），每轮只做一次。

    排序：水位差距大的优先，其次按会话数规模。
    """
    if not _ready():
        return []
    with _lock:
        rows = _conn.execute(
            "SELECT root_id, dir_key, leaf_path, idx_size, idx_mtime, sessions, "
            "stats_json, stats_sync_rowid, sync_rowid "
            "FROM leaf_status WHERE leaf_path != ''"
        ).fetchall()

    out: List[dict] = []
    for r in rows:
        row = dict(r)
        raw = row.pop("stats_json", "") or ""
        stale = row.get("stats_sync_rowid", -1) != row.get("sync_rowid", -1)
        if not stale and raw:
            # 内容一致性：stats_json 的 total 之和应等于 sessions。
            try:
                d = _json.loads(raw)
                total = sum(v.get("total", 0) for v in d.values()) if isinstance(d, dict) else -1
            except (ValueError, TypeError):
                total = -1
            if total != (row.get("sessions") or 0):
                stale = True
        elif not raw:
            stale = True          # 没有快照，必须算一次
        if stale:
            out.append(row)

    out.sort(key=lambda x: (x.get("sync_rowid", -1) - x.get("stats_sync_rowid", -1)), reverse=True)
    return out[:int(limit)]


def count_stale_stats_leaves() -> int:
    """需补算叶子的总数，供 worker 启动日志/进度展示。"""
    if not _ready():
        return 0
    with _lock:
        rows = _conn.execute(
            "SELECT sessions, stats_json, stats_sync_rowid, sync_rowid "
            "FROM leaf_status WHERE leaf_path != ''"
        ).fetchall()
    n = 0
    for r in rows:
        if r["stats_sync_rowid"] != r["sync_rowid"]:
            n += 1
            continue
        raw = r["stats_json"] or ""
        if not raw:
            n += 1
            continue
        try:
            d = _json.loads(raw)
            total = sum(v.get("total", 0) for v in d.values()) if isinstance(d, dict) else -1
        except (ValueError, TypeError):
            total = -1
        if total != (r["sessions"] or 0):
            n += 1
    return n


def update_sync_rowid(leaf_path: str, rowid: int) -> int:
    """按叶子绝对路径更新 sync_rowid，返回受影响行数。

    写侧（watcher/回填）调用：它已经打开了叶子库，这里只是本地库里的一条
    UPDATE（4685 行全表约 5ms）。读侧 stats_index 用它判断 stats_json 快照是否
    落后于 index.db —— 若让读侧自己去叶子库读 meta，每叶约 73ms（NFS 打开
    SQLite），4600 叶要 5 分钟。
    """
    if not _ready() or not leaf_path:
        return 0
    key = os.path.normpath(leaf_path)
    with _lock:
        cur = _conn.execute(
            "UPDATE leaf_status SET sync_rowid = ? WHERE leaf_path = ?",
            (int(rowid or 0), key),
        )
        _conn.commit()
        return cur.rowcount or 0


def set_leaf_stats(root_id: str, dir_key: str, *, leaf_path: str = "",
                   idx_size: int, idx_mtime: float,
                   stats_json: str, stats_threshold: int,
                   stats_sync_rowid: Optional[int] = None) -> None:
    """写单叶 /sessions 统计缓存：index.jsonl 签名 + 聚合结果 + threshold。

    薄封装 upsert_leaf（叶子不存在则插入），一并盖 stats_built_at=now。不触碰
    built/state/sessions 等叶子管理字段（只写统计缓存列）。

    stats_sync_rowid: 这份快照对应的 index.db 同步水位（来自 leaf_status.sync_rowid）。
        读侧据此判断快照是否已落后于 index.db —— 只靠 index.jsonl 的 size+mtime
        判断不出来，因为 watcher 折叠新 session 不改 index.jsonl。
    """
    if not _ready():
        return
    upsert_leaf(root_id, dir_key, leaf_path=leaf_path,
                idx_size=idx_size, idx_mtime=idx_mtime,
                stats_json=stats_json, stats_threshold=stats_threshold,
                stats_built_at=_now(),
                stats_sync_rowid=stats_sync_rowid)


def set_leaf_token_stats(root_id: str, dir_key: str, *, leaf_path: str = "",
                         idx_size: int, idx_mtime: float,
                         token_stats_json: str) -> None:
    """写单叶 /query token 统计缓存：index.jsonl 签名 + 该叶 token 聚合结果。

    薄封装 upsert_leaf（叶子不存在则插入）。只写 token_stats_json 及共用签名
    idx_size/idx_mtime；不触碰 stats_json/stats_threshold（/sessions 缓存列）与
    built/state/sessions 等叶子管理字段。
    """
    if not _ready():
        return
    upsert_leaf(root_id, dir_key, leaf_path=leaf_path,
                idx_size=idx_size, idx_mtime=idx_mtime,
                token_stats_json=token_stats_json)


# ── 数据源表（「数据管理」表 1）───────────────────────────────────────────


def _templates_to_json(templates) -> str:
    """把模板（list 或多行字符串）规整为 JSON 数组字符串存库。"""
    if templates is None:
        return ""
    if isinstance(templates, str):
        items = [ln.strip() for ln in templates.splitlines() if ln.strip()]
    else:
        items = [str(t).strip() for t in templates if str(t).strip()]
    return _json.dumps(items, ensure_ascii=False)


def _templates_from_json(raw: str) -> list:
    """把库里的 templates 字段（JSON 数组字符串）反序列化为 list。"""
    if not raw:
        return []
    try:
        v = _json.loads(raw)
        return [str(x) for x in v] if isinstance(v, list) else []
    except (ValueError, TypeError):
        return []


def upsert_source(root_id: str, *, root_path: Optional[str] = None,
                  name: Optional[str] = None, format: Optional[str] = None,
                  templates=None, leaf_count: Optional[int] = None,
                  built_count: Optional[int] = None) -> str:
    """新增或更新一条数据源。返回 'added' | 'updated'。仅传入的字段被更新。"""
    if not _ready() or not root_id:
        return "skipped"
    with _lock:
        existing = _conn.execute(
            "SELECT root_id FROM sources WHERE root_id = ?", (root_id,)
        ).fetchone()
        if existing:
            fields = ["synced_at = ?"]
            params: list = [_now()]
            if root_path is not None:
                fields.append("root_path = ?"); params.append(root_path)
            if name is not None:
                fields.append("name = ?"); params.append(name)
            if format is not None:
                fields.append("format = ?"); params.append(format)
            if templates is not None:
                fields.append("templates = ?"); params.append(_templates_to_json(templates))
            if leaf_count is not None:
                fields.append("leaf_count = ?"); params.append(int(leaf_count))
            if built_count is not None:
                fields.append("built_count = ?"); params.append(int(built_count))
            params.append(root_id)
            _conn.execute(
                f"UPDATE sources SET {', '.join(fields)} WHERE root_id = ?", params
            )
            _conn.commit()
            return "updated"
        else:
            _conn.execute("""
                INSERT INTO sources
                    (root_id, root_path, name, format, templates,
                     leaf_count, built_count, synced_at, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (root_id, root_path or "", name or "default", format or "",
                  _templates_to_json(templates),
                  int(leaf_count or 0), int(built_count or 0), _now(), _now()))
            _conn.commit()
            return "added"


def get_source(root_id: str) -> Optional[dict]:
    """读一条数据源；templates 反序列化为 list。无则 None。"""
    if not _ready() or not root_id:
        return None
    with _lock:
        row = _conn.execute(
            "SELECT * FROM sources WHERE root_id = ?", (root_id,)
        ).fetchone()
    if not row:
        return None
    d = dict(row)
    d["templates"] = _templates_from_json(d.get("templates", ""))
    return d


def list_sources(active_env_dir: str = "") -> List[dict]:
    """数据源全量列表（「数据管理」列表页的唯一行来源）。

    活跃目录（root_id='default'）排第一，其余按 root_path 排序。
    templates 反序列化为 list；无活跃行时（空库 / 被删）返回全部。
    DB 未就绪或查询异常时返回空列表（调用方回退，不 500）。
    """
    if not _ready():
        return []
    try:
        with _lock:
            rows = _conn.execute(
                "SELECT * FROM sources ORDER BY root_id = 'default' DESC, root_path"
            ).fetchall()
    except Exception:  # noqa: BLE001
        return []
    out = [dict(r) for r in rows]
    for d in out:
        d["templates"] = _templates_from_json(d.get("templates", ""))
    return out


def get_source_by_path(path: str) -> Optional[dict]:
    """按路径取数据源行（root_id 由 get_root_id 计算）。无则 None。"""
    if not path:
        return None
    try:
        from utils.logs_config import get_root_id
    except Exception:  # noqa: BLE001
        return None
    return get_source(get_root_id(path))


def _refresh_active_row(active_env_dir: str) -> None:
    """把活跃目录行（root_id='default'）的 root_path 指向当前 env_dir。

    每次启动调用（app.py 启动流程）：env/base 变化时重指向；行不存在（新机器
    空库 / 被删）则补插 default 行——这是空库下唯一保证有活跃行的路径。
    顺带把该 root 下 leaf_status 的 root_path 批量更新，避免 /leaves 详情
    用旧路径拼接（leaf_status.root_path 存的是每叶快照）。
    """
    if not _ready() or not active_env_dir:
        return
    # import 必须在 _lock 之前（避免与另一线程的 `import utils.logdir_store`
    # 形成 importlib 可重入死锁，见旧 seed_sources_from_yaml 注释）
    from utils.log_scan import detect_format
    norm = os.path.normpath(active_env_dir)
    with _lock:
        existing = _conn.execute(
            "SELECT root_id FROM sources WHERE root_id = ?", ("default",)
        ).fetchone()
        if existing:
            _conn.execute(
                "UPDATE sources SET root_path = ?, synced_at = ? WHERE root_id = ?",
                (norm, _now(), "default"),
            )
        else:
            fmt = detect_format(norm)
            _conn.execute("""
                INSERT INTO sources
                    (root_id, root_path, name, format, templates,
                     leaf_count, built_count, synced_at, created_at)
                VALUES ('default', ?, 'default', ?, '', 0, 0, ?, ?)
            """, (norm, fmt, _now(), _now()))
        # 活跃行 root_path 变化时同步该 root 下已登记叶子的路径快照
        _conn.execute(
            "UPDATE leaf_status SET root_path = ? WHERE root_id = 'default'",
            (norm,),
        )
        _conn.commit()


def set_source_name(root_id: str, name: str) -> bool:
    """改数据源名字。行不存在返回 False。"""
    if not _ready() or not root_id:
        return False
    with _lock:
        cur = _conn.execute(
            "UPDATE sources SET name = ?, synced_at = ? WHERE root_id = ?",
            (name or "default", _now(), root_id),
        )
        _conn.commit()
        return cur.rowcount > 0


def set_source_templates(root_id: str, templates) -> bool:
    """改数据源层级模板。行不存在返回 False。"""
    if not _ready() or not root_id:
        return False
    with _lock:
        cur = _conn.execute(
            "UPDATE sources SET templates = ?, synced_at = ? WHERE root_id = ?",
            (_templates_to_json(templates), _now(), root_id),
        )
        _conn.commit()
        return cur.rowcount > 0


def delete_root(root_id: str):
    """移除某源时清掉其所有叶子记录（含数据源行）。"""
    if not _ready():
        return
    with _lock:
        _conn.execute("DELETE FROM leaf_status WHERE root_id = ?", (root_id,))
        _conn.execute("DELETE FROM sources WHERE root_id = ?", (root_id,))
        _conn.commit()


def reset_building_on_startup():
    """进程启动时把上次中断的 building 降回 pending（回填被打断，需重新构建）。"""
    if not _ready():
        return
    with _lock:
        _conn.execute(
            "UPDATE leaf_status SET state = 'pending' WHERE state = 'building'"
        )
        _conn.commit()


# ── 回填请求队列（backfill_requests 表）─────────────────────────────────
# app 侧入队 / 查询运行态；backfill_worker 侧领取 / 完成。跨进程靠此表 + WAL。
# 运行态真相 = status='running' 的行；逐叶进度仍读 count_summary(leaf_status)。

_BACKFILL_ACTIVE = ("pending", "running")


def enqueue_backfill(root: str, workers: int = 8, force: bool = False) -> int:
    """入队一条回填请求，返回新记录 id。调用方应先 has_active_backfill 去重。"""
    if not _ready():
        raise RuntimeError("logdir_store not initialized")
    root = os.path.normpath(root)
    with _lock:
        cur = _conn.execute(
            "INSERT INTO backfill_requests (root, workers, force, status) "
            "VALUES (?, ?, ?, 'pending')",
            (root, int(workers or 8), 1 if force else 0),
        )
        _conn.commit()
        return cur.lastrowid


def has_active_backfill(root: str) -> bool:
    """该 root 是否已有 pending/running 请求（同源去重，避免两份进程池打架）。"""
    if not _ready():
        return False
    root = os.path.normpath(root)
    with _lock:
        row = _conn.execute(
            "SELECT 1 FROM backfill_requests WHERE root = ? AND status IN ('pending','running') LIMIT 1",
            (root,),
        ).fetchone()
    return row is not None


def has_recent_backfill(root: str, seconds: int = 300) -> bool:
    """该 root 最近 seconds 秒内是否有已结束(pending/running 除外)的回填请求。

    供 watcher 决定「要不要替这个 root 再入队一次增量构建」：全新叶子会被 watcher
    转交回填，但若该 root 刚失败/刚跑完，立即重入队只会每 5s 刷一条请求，把
    backfill_requests 变成噪音。冷却窗口内一律不再入队。
    """
    if not _ready():
        return False
    root = os.path.normpath(root)
    seconds = max(0, int(seconds or 0))
    with _lock:
        row = _conn.execute(
            "SELECT 1 FROM backfill_requests WHERE root = ? "
            "AND status IN ('done','failed') "
            "AND finished_at IS NOT NULL "
            "AND finished_at >= datetime('now','localtime', ?) "
            "LIMIT 1",
            (root, f"-{seconds} seconds"),
        ).fetchone()
    return row is not None


def get_active_backfill_request(root: str) -> Optional[dict]:
    """返回该 root 的活跃请求详情(pending/running),供构建状态判断 status。"""
    if not _ready():
        return None
    root = os.path.normpath(root)
    with _lock:
        row = _conn.execute(
            "SELECT * FROM backfill_requests WHERE root = ? AND status IN ('pending','running') ORDER BY id LIMIT 1",
            (root,),
        ).fetchone()
    return dict(row) if row else None


def list_active_backfill() -> List[dict]:
    """所有 pending/running 请求行（按 id）。供 UI/导出页展示排队与运行。"""
    if not _ready():
        return []
    with _lock:
        rows = _conn.execute(
            "SELECT * FROM backfill_requests WHERE status IN ('pending','running') ORDER BY id"
        ).fetchall()
    return [dict(r) for r in rows]


def claim_next_backfill() -> Optional[dict]:
    """worker 领取下一条 pending 请求：最老的一条原子置 running 并返回。

    UPDATE ... WHERE id=? AND status='pending' + rowcount 判定抢占；多 worker/重复
    领取会落空（该行已非 pending）。返回 None 表示暂无待执行请求。
    """
    if not _ready():
        return None
    with _lock:
        row = _conn.execute(
            "SELECT * FROM backfill_requests WHERE status = 'pending' ORDER BY id LIMIT 1"
        ).fetchone()
        if not row:
            return None
        rec = dict(row)
        cur = _conn.execute(
            "UPDATE backfill_requests SET status = 'running', "
            "started_at = datetime('now','localtime') WHERE id = ? AND status = 'pending'",
            (rec["id"],),
        )
        _conn.commit()
        if cur.rowcount == 0:
            return None  # 被其他 worker 抢先
    rec["status"] = "running"
    return rec


def complete_backfill(req_id: int, ok: bool, error: str = "") -> None:
    """标记请求终态：ok→done，否则→failed（附 error）。"""
    if not _ready():
        return
    with _lock:
        _conn.execute(
            "UPDATE backfill_requests SET status = ?, error = ?, "
            "finished_at = datetime('now','localtime') WHERE id = ?",
            ("done" if ok else "failed", error or "", req_id),
        )
        _conn.commit()


def reset_running_backfill_on_startup() -> int:
    """worker 启动时把上次中断的 running/pending 请求标 failed（队列从零开始）。

    与 export_store.cancel_interrupted 同语义：崩溃/重启视为放弃在途请求，
    用户可在页面重新发起。返回受影响行数。leaf_status 的 building→pending 由
    reset_building_on_startup 单独处理。
    """
    if not _ready():
        return 0
    with _lock:
        cur = _conn.execute(
            "UPDATE backfill_requests SET status = 'failed', "
            "error = '服务重启，队列已清空', finished_at = datetime('now','localtime') "
            "WHERE status IN ('pending', 'running')"
        )
        _conn.commit()
    return cur.rowcount


# ── watch 队列（watch_queue 表）────────────────────────────────────────────
# 独立 watch worker 进程的核心调度表：发现线程入队、同步池消费。
# 与 backfill_requests 的区别：粒度是单个 leaf（而不是整个 root），带优先级。
# 部分唯一索引 watch_queue_active 保证同一 leaf 不会同时有两条 pending/running。

def _ensure_watch_tables() -> None:
    """确保 watch_queue / watch_source_stats 表存在（兼容老库）。"""
    if not _ready():
        return
    with _lock:
        _conn.executescript("""
            CREATE TABLE IF NOT EXISTS watch_queue (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                root_id       TEXT NOT NULL DEFAULT '',
                root_path     TEXT NOT NULL DEFAULT '',
                leaf_path     TEXT NOT NULL DEFAULT '',
                dir_key       TEXT NOT NULL DEFAULT '',
                priority      INTEGER NOT NULL DEFAULT 0,
                reason        TEXT NOT NULL DEFAULT '',
                idx_size      INTEGER NOT NULL DEFAULT 0,
                offset_at     INTEGER NOT NULL DEFAULT 0,
                status        TEXT NOT NULL DEFAULT 'pending',
                bytes_done    INTEGER NOT NULL DEFAULT 0,
                created_at    TEXT NOT NULL DEFAULT (datetime('now','localtime')),
                started_at    TEXT,
                finished_at   TEXT,
                took_ms       INTEGER NOT NULL DEFAULT 0,
                error         TEXT NOT NULL DEFAULT ''
            );
            CREATE UNIQUE INDEX IF NOT EXISTS watch_queue_active
                ON watch_queue(leaf_path) WHERE status IN ('pending','running');
            CREATE INDEX IF NOT EXISTS watch_queue_status
                ON watch_queue(status, priority DESC, id);
            CREATE TABLE IF NOT EXISTS watch_source_stats (
                root_id        TEXT PRIMARY KEY,
                bytes_per_sec  REAL NOT NULL DEFAULT 0,
                rows_per_sec   REAL NOT NULL DEFAULT 0,
                samples        INTEGER NOT NULL DEFAULT 0,
                updated_at     TEXT NOT NULL DEFAULT (datetime('now','localtime'))
            );
        """)
        _conn.commit()


def enqueue_watch(root_id: str, root_path: str, leaf_path: str, dir_key: str,
                  priority: int = 0, reason: str = "", idx_size: int = 0,
                  offset_at: int = 0) -> bool:
    """入队一个叶子同步任务。已有 pending/running 的同路径叶子则忽略（幂等）。
    返回 True 表示成功入队，False 表示因唯一索引冲突被忽略（已在队）。
    """
    _ensure_watch_tables()
    if not _ready():
        return False
    with _lock:
        cur = _conn.execute(
            "INSERT OR IGNORE INTO watch_queue"
            "(root_id, root_path, leaf_path, dir_key, priority, reason, idx_size, offset_at)"
            " VALUES (?,?,?,?,?,?,?,?)",
            (root_id or "", os.path.normpath(root_path) if root_path else "",
             leaf_path, dir_key, int(priority), reason or "", int(idx_size), int(offset_at)),
        )
        _conn.commit()
    return cur.rowcount > 0


def claim_next_watch() -> Optional[dict]:
    """同步池领取下一条 pending 任务（按 priority DESC, id ASC 排序）。
    原子置 running；返回 None 表示队空。
    """
    _ensure_watch_tables()
    if not _ready():
        return None
    with _lock:
        row = _conn.execute(
            "SELECT * FROM watch_queue WHERE status='pending' "
            "ORDER BY priority DESC, id LIMIT 1"
        ).fetchone()
        if not row:
            return None
        rec = dict(row)
        cur = _conn.execute(
            "UPDATE watch_queue SET status='running', "
            "started_at=datetime('now','localtime') WHERE id=? AND status='pending'",
            (rec["id"],),
        )
        _conn.commit()
        if cur.rowcount == 0:
            return None
    rec["status"] = "running"
    return rec


def touch_watch_size(leaf_path: str, idx_size: int) -> bool:
    """把该叶子活跃队列行（pending/running）的 idx_size 刷新为最新文件大小。

    发现线程每轮扫到增长都会调一次。目的有二：
      1. 看板的 remaining_bytes / ETA 跟着文件实时前进，不冻在入队那一刻；
      2. 返回 False 说明该叶子没有活跃行——调用方据此决定是否要入队。
    走 watch_queue_active 部分索引，一条 UPDATE，成本可忽略。
    """
    _ensure_watch_tables()
    if not _ready() or not leaf_path:
        return False
    with _lock:
        cur = _conn.execute(
            "UPDATE watch_queue SET idx_size=? "
            "WHERE leaf_path=? AND status IN ('pending','running')",
            (int(idx_size), leaf_path),
        )
        _conn.commit()
    return cur.rowcount > 0


def watch_active_counts() -> dict:
    """活跃任务计数 {pending, running}：供看板显示并发水位（pool.busy）。"""
    _ensure_watch_tables()
    if not _ready():
        return {"pending": 0, "running": 0}
    with _lock:
        row = _conn.execute(
            "SELECT COALESCE(SUM(status='pending'),0) AS pending, "
            "COALESCE(SUM(status='running'),0) AS running FROM watch_queue"
        ).fetchone()
    return {"pending": (row["pending"] if row else 0),
            "running": (row["running"] if row else 0)}


def finish_watch(wq_id: int, ok: bool, bytes_done: int = 0,
                 took_ms: int = 0, error: str = "") -> None:
    """标记任务终态（done / failed）并记录耗时/字节数。"""
    _ensure_watch_tables()
    if not _ready():
        return
    with _lock:
        _conn.execute(
            "UPDATE watch_queue SET status=?, bytes_done=?, took_ms=?, error=?, "
            "finished_at=datetime('now','localtime') WHERE id=?",
            ("done" if ok else "failed", int(bytes_done), int(took_ms), error or "", wq_id),
        )
        _conn.commit()


def watch_queue_stats() -> dict:
    """看板汇总：全局 + 按 root 分组的 {pending, running, done, failed, remaining_bytes}。"""
    _ensure_watch_tables()
    if not _ready():
        return {"totals": {}, "sources": []}
    with _lock:
        totals_row = _conn.execute("""
            SELECT
              COALESCE(SUM(status='pending'), 0) AS pending,
              COALESCE(SUM(status='running'), 0) AS running,
              COALESCE(SUM(status='done'),    0) AS done,
              COALESCE(SUM(status='failed'),  0) AS failed,
              COALESCE(SUM(CASE WHEN status IN ('pending','running') THEN MAX(idx_size - offset_at, 0) ELSE 0 END), 0) AS remaining_bytes
            FROM watch_queue
        """).fetchone()
        src_rows = _conn.execute("""
            SELECT root_id, root_path,
              COALESCE(SUM(status='pending'), 0) AS pending,
              COALESCE(SUM(status='running'), 0) AS running,
              COALESCE(SUM(status='done'),    0) AS done,
              COALESCE(SUM(status='failed'),  0) AS failed,
              COALESCE(SUM(CASE WHEN status IN ('pending','running') THEN MAX(idx_size - offset_at, 0) ELSE 0 END), 0) AS remaining_bytes
            FROM watch_queue
            GROUP BY root_id, root_path
        """).fetchall()
        stats_rows = _conn.execute(
            "SELECT * FROM watch_source_stats"
        ).fetchall()
    stats_by_rid = {r["root_id"]: dict(r) for r in stats_rows}
    sources = []
    for r in src_rows:
        d = dict(r)
        st = stats_by_rid.get(d["root_id"], {})
        bps = st.get("bytes_per_sec", 0)
        d["bytes_per_sec"] = bps
        rem = d.get("remaining_bytes") or 0
        d["eta_sec"] = int(rem / bps) if bps > 0 else None
        sources.append(d)
    totals = dict(totals_row) if totals_row else {}
    global_bps = sum(s["bytes_per_sec"] for s in sources if s["bytes_per_sec"])
    rem_total = totals.get("remaining_bytes") or 0
    totals["eta_sec"] = int(rem_total / global_bps) if global_bps > 0 else None
    return {"totals": totals, "sources": sources}


def update_watch_source_stats(root_id: str, bytes_done: int, took_ms: int,
                              rows_done: int = 0) -> None:
    """用 EMA（α=0.3）更新来源的历史吞吐率统计。"""
    _ensure_watch_tables()
    if not _ready() or took_ms <= 0 or bytes_done <= 0:
        return
    alpha = 0.3
    new_bps = bytes_done / (took_ms / 1000.0)
    new_rps = rows_done / (took_ms / 1000.0) if rows_done > 0 else 0.0
    with _lock:
        existing = _conn.execute(
            "SELECT bytes_per_sec, rows_per_sec, samples FROM watch_source_stats WHERE root_id=?",
            (root_id,),
        ).fetchone()
        if existing:
            old_bps = existing["bytes_per_sec"] or 0.0
            old_rps = existing["rows_per_sec"] or 0.0
            samples = (existing["samples"] or 0) + 1
            bps = old_bps * (1 - alpha) + new_bps * alpha if old_bps > 0 else new_bps
            rps = old_rps * (1 - alpha) + new_rps * alpha if old_rps > 0 else new_rps
            _conn.execute(
                "UPDATE watch_source_stats SET bytes_per_sec=?, rows_per_sec=?, samples=?, "
                "updated_at=datetime('now','localtime') WHERE root_id=?",
                (bps, rps, samples, root_id),
            )
        else:
            _conn.execute(
                "INSERT INTO watch_source_stats(root_id, bytes_per_sec, rows_per_sec, samples) "
                "VALUES (?,?,?,1)",
                (root_id, new_bps, new_rps),
            )
        _conn.commit()


def reset_watch_on_startup() -> int:
    """watch worker 启动时把上次中断的 running 复位为 pending（断点续跑）。"""
    _ensure_watch_tables()
    if not _ready():
        return 0
    with _lock:
        cur = _conn.execute(
            "UPDATE watch_queue SET status='pending', started_at=NULL "
            "WHERE status='running'"
        )
        _conn.commit()
    return cur.rowcount


def watch_queue_current() -> List[dict]:
    """当前正在处理的叶子列表（status=running），供看板「当前处理中」展示。"""
    _ensure_watch_tables()
    if not _ready():
        return []
    with _lock:
        rows = _conn.execute(
            "SELECT * FROM watch_queue WHERE status='running' ORDER BY started_at"
        ).fetchall()
    return [dict(r) for r in rows]


def watch_queue_recent(limit: int = 200, status: str = "", q: str = "") -> List[dict]:
    """最近的任务列表（按 id 倒序），供看板「最近同步任务」展示与前端筛选。

    status 为空表示不按状态过滤；q 对 dir_key 做 LIKE 模糊匹配。
    全部走索引/带 LIMIT —— watch_queue 从不 prune，必须限制返回行数。
    """
    _ensure_watch_tables()
    if not _ready():
        return []
    limit = max(1, min(int(limit or 200), 1000))
    sql = ("SELECT id, root_id, dir_key, priority, reason, status, idx_size, offset_at,"
           " bytes_done, created_at, started_at, finished_at, took_ms, error"
           " FROM watch_queue")
    where, args = [], []
    if status:
        where.append("status = ?")
        args.append(status)
    if q:
        where.append("dir_key LIKE ?")
        args.append(f"%{q}%")
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY id DESC LIMIT ?"
    args.append(limit)
    with _lock:
        rows = _conn.execute(sql, args).fetchall()
    return [dict(r) for r in rows]


def watch_throughput_buckets(minutes: int = 60, bucket_sec: int = 300) -> List[dict]:
    """吞吐趋势：把最近 N 分钟完成的任务按 bucket_sec 分桶，空桶补 0。

    返回等长序列 [{t, done, failed, bytes}]，t 为桶起始 epoch 秒。
    只统计终态（done/failed）—— finished_at 只有这两种状态才有值。
    """
    _ensure_watch_tables()
    minutes = max(1, min(int(minutes or 60), 24 * 60))
    bucket_sec = max(60, min(int(bucket_sec or 300), 3600))
    now = int(time.time())
    # 桶边界对齐，保证最后一桶是「当前」桶
    start = (now // bucket_sec) * bucket_sec - (minutes * 60 // bucket_sec) * bucket_sec
    n_buckets = max(1, (now - start) // bucket_sec + 1)
    out = [{"t": start + i * bucket_sec, "done": 0, "failed": 0, "bytes": 0}
           for i in range(n_buckets)]
    if not _ready():
        return out
    # finished_at 由 datetime('now','localtime') 写入，是本地时间字符串。
    # strftime('%s', x) 默认按 UTC 解析，会偏出时区差（东八区即 8 小时），
    # 必须加 'utc' 修饰符把它当本地时间换算回 epoch。
    _ts = "CAST(strftime('%s', finished_at, 'utc') AS INTEGER)"
    with _lock:
        rows = _conn.execute(
            "SELECT " + _ts + " AS ts, status, bytes_done"
            " FROM watch_queue"
            " WHERE status IN ('done','failed') AND finished_at IS NOT NULL"
            "   AND " + _ts + " >= ?",
            (start,),
        ).fetchall()
    for r in rows:
        ts = r["ts"]
        if ts is None:
            continue
        idx = (int(ts) - start) // bucket_sec
        if 0 <= idx < len(out):
            b = out[idx]
            b["done" if r["status"] == "done" else "failed"] += 1
            b["bytes"] += int(r["bytes_done"] or 0)
    return out


def sync_leaf_detail(limit: int = 200, only_pending: bool = True) -> List[dict]:
    """看板用：逐叶列出「还需同步 / 刚同步完」的明细。

    每行返回：
      root_id / dir_key / leaf_path
      state          leaf_status.state（pending|building|done|error）
      built          是否已建 index.db
      sessions       已折叠进 index.db 的会话数
      ingested       已摄取到 requests 表的行数
      pending_rows   尚未补 meta 的行数（requests.q1_hash IS NULL）
      new_rows       index.jsonl 已摄取、但还没折叠成 session 的行数
                     = entry_count(来自 token_stats_json) - ingested
      new_keys       该叶 index.jsonl 里出现过、但还没进 sessions 聚合的 key 数
                     （token_stats_json.api_keys_set 减 stats_json 已覆盖的 key）
      idx_size       本叶日志原始大小
      synced_at      最近一次同步时间

    only_pending=True 时只返回「还需要同步」的叶子：未构建、有 pending 行、
    或还有没折叠成 session 的新行。这类叶子正是看板上该被关注的对象。
    """
    if not _ready():
        return []
    with _lock:
        rows = _conn.execute(
            "SELECT root_id, dir_key, leaf_path, state, built, sessions, ingested, "
            "pending, idx_size, synced_at, stats_json, token_stats_json "
            "FROM leaf_status WHERE leaf_path != '' ORDER BY dir_key DESC"
        ).fetchall()
        # native 源（本项目格式）不走 newapi 的 ingest/enrich 流程，
        # leaf_status.ingested 对它们恒为 0。若照 newapi 口径算
        # new_rows = entry_count - ingested，会把整叶行数都当成「待同步」
        # （实测 native 源每叶虚报 30~60 万行，汇总后 3330 万）。
        try:
            fmt_by_root = {r[0]: (r[1] or "") for r in _conn.execute(
                "SELECT root_id, format FROM sources").fetchall()}
        except Exception:  # noqa: BLE001
            fmt_by_root = {}

    out: List[dict] = []
    for r in rows:
        ingested = int(r["ingested"] or 0)
        entry_count = None
        key_set: set = set()
        raw_t = r["token_stats_json"] or ""
        if raw_t:
            try:
                tj = _json.loads(raw_t)
                if isinstance(tj, dict):
                    ec = tj.get("entry_count")
                    if isinstance(ec, int):
                        entry_count = ec
                    ks = tj.get("api_keys_set")
                    if isinstance(ks, list):
                        key_set = {k for k in ks if k}
            except (ValueError, TypeError):
                pass

        known_keys: set = set()
        raw_s = r["stats_json"] or ""
        if raw_s:
            try:
                sj = _json.loads(raw_s)
                if isinstance(sj, dict):
                    for bk in sj:
                        k = bk.split("|", 1)[0]
                        if k:
                            known_keys.add(k)
            except (ValueError, TypeError):
                pass

        pending_rows = int(r["pending"] or 0)
        # entry_count 是 index.jsonl 的总行数；ingested 是已落 requests 的行数。
        # 差值为正说明还有没摄取的新行。仅对 newapi 源成立 —— native 源不经这条路。
        is_newapi = (fmt_by_root.get(r["root_id"]) == "newapi")
        if is_newapi and entry_count is not None:
            new_rows = max(0, entry_count - ingested)
        else:
            new_rows = 0
        new_keys = len(key_set - known_keys) if key_set else 0

        needs = (not r["built"]) or pending_rows > 0 or new_rows > 0
        if only_pending and not needs:
            continue

        out.append({
            "root_id": r["root_id"],
            "dir_key": r["dir_key"],
            "leaf_path": r["leaf_path"],
            "state": r["state"] or "",
            "built": bool(r["built"]),
            "sessions": int(r["sessions"] or 0),
            "ingested": ingested,
            "pending_rows": pending_rows,
            "new_rows": new_rows,
            "new_keys": new_keys,
            "idx_size": int(r["idx_size"] or 0),
            "synced_at": r["synced_at"] or "",
        })
    out.sort(key=lambda x: (x["new_rows"] + x["pending_rows"]), reverse=True)
    return out[:int(limit)]


def sync_leaf_summary() -> dict:
    """看板 KPI：全局「待同步叶子数 / 待摄取行数 / 待新增 key 数」。

    与 sync_leaf_detail 同口径，但不返回明细，只给总量。
    """
    rows = sync_leaf_detail(limit=10 ** 9, only_pending=True)
    return {
        "pending_leaves": len(rows),
        "pending_rows": sum(r["new_rows"] + r["pending_rows"] for r in rows),
        "pending_keys": sum(r["new_keys"] for r in rows),
        "pending_sessions": sum(r["sessions"] for r in rows),
    }

