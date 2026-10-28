"""
utils/watch_config.py — new-api 实时索引（watch）的全部可调参数

唯一配置入口：worker 进程、发现线程、看板接口都从这里读，避免同一项在
三处各写一遍默认值、改了 .env 却只生效一半。

约定：全部用环境变量（.env）覆盖，**不填就用下面的默认值**，不做强制校验；
非法值只告警并回退默认，绝不让配置写错把 worker 卡死。
"""

import logging
import os

logger = logging.getLogger("watch.config")


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return max(minimum, int(raw))
    except ValueError:
        logger.warning("%s=%r 不是整数，回退默认 %d", name, raw, default)
        return default


def _env_float(name: str, default: float, minimum: float = 1.0) -> float:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return max(minimum, float(raw))
    except ValueError:
        logger.warning("%s=%r 不是数字，回退默认 %s", name, raw, default)
        return default


# ── 调度 / 并发 ───────────────────────────────────────────────────────────

# 叶子间并发：同时处理多少个**不同的**叶子（1~8）。
# 看板的「并发 N/8」用的就是这个值。
CONCURRENCY = _env_int("WATCH_CONCURRENCY", 8)

# 发现线程轮询周期（秒）。每源一个线程，每轮只 stat index.jsonl。
POLL_SEC = _env_float("WATCH_POLL_SEC", 5.0)

# ── enrich（Stage 2，解析合并文件补 meta）─────────────────────────────────

# 单个叶子内部 enrich 的并发子进程数。
ENRICH_WORKERS = _env_int("WATCH_ENRICH_WORKERS", 4)

# pending 行数低于此阈值 → 用 workers=1 的进程内快路径（不起 spawn 池）。
# 只有 1~2 条新行的常态下，spawn 开销比干活本身还大。
ENRICH_INLINE_THRESHOLD = _env_int("WATCH_ENRICH_INLINE_THRESHOLD", 1000)

# 全局 enrich 子进程上限（跨所有并发任务共用，非阻塞分配）。
# 8 个任务 × 4 worker = 32 个 spawn 进程会压垮 NFS，这里兜底。
ENRICH_BUDGET = _env_int("WATCH_ENRICH_BUDGET", 12)

# ── 与回填的分工 ─────────────────────────────────────────────────────────

# 全新叶子（无 index.db）watcher 不冷建，转交回填的增量构建。若该 root 在这么多秒内
# 已经有过一次已结束的回填请求（done/failed），就不再重复入队——否则构建失败时
# watcher 每轮扫描都会追加一条请求，把 backfill_requests 刷成噪音。
BACKFILL_REQUEST_COOLDOWN = _env_int("WATCH_BACKFILL_REQUEST_COOLDOWN", 300, minimum=0)

# ── 兜底对账 ─────────────────────────────────────────────────────────────

# 低频对账：每 N 秒取一批「最久未同步」的叶子跑 needs_build()，为真的补入队。
# 为什么需要它：发现线程对「大小没变」的叶子会短路（否则每轮要打开 4700 个
# sqlite），所以「字节追平但数据脏」的叶子（enrich 留下的 NULL meta、待折叠的
# sessions）只靠发现永远碰不到。整源重扫的 auto_sync 太重，这里按叶子限速补漏。
RECONCILE_INTERVAL = _env_float("WATCH_RECONCILE_INTERVAL", 600.0)
RECONCILE_BATCH = _env_int("WATCH_RECONCILE_BATCH", 50)

# ── 队列保留 ─────────────────────────────────────────────────────────────

# watch_queue 里终态行（done/failed）保留多少天。这张表只增不减，一次叶子
# 同步就是一行（实测 4 天 5.2 万行）；不清理会让看板的累计计数只涨不跌。
WATCH_QUEUE_KEEP_DAYS = _env_float("WATCH_QUEUE_KEEP_DAYS", 7.0, minimum=0.0)

# ── 发现范围 / 优先级 ────────────────────────────────────────────────────

# 快扫只看最近 N 天的最近 M 小时叶子；其余靠全量扫描兜底。
FAST_SCAN_DAYS = _env_int("WATCH_FAST_SCAN_DAYS", 2)
FAST_SCAN_HOURS = _env_int("WATCH_FAST_SCAN_HOURS", 3)

# 全量扫描周期（秒）。快扫之外的叶子由它兜底发现。
FULL_SCAN_EVERY = _env_float("WATCH_FULL_SCAN_EVERY", 60.0)

# 队列优先级：越大越先处理。
# 当前小时的数据决定「实时可见」，必须最先同步；老叶子慢慢补，绝不挡新数据。
PRIO_NOW = _env_int("WATCH_PRIO_NOW", 100, minimum=0)
PRIO_RECENT = _env_int("WATCH_PRIO_RECENT", 50, minimum=0)
PRIO_OLD = _env_int("WATCH_PRIO_OLD", 0, minimum=0)
# 「最近」的口径：距今多少小时以内算 recent。
RECENT_HOURS = _env_int("WATCH_RECENT_HOURS", 3)

# ── 单叶同步（Stage 3b）──────────────────────────────────────────────────

# 一次 ingest 最多读多少行（分批提交，避免长事务长期占写锁）。
SYNC_BATCH = _env_int("WATCH_SYNC_BATCH", 500)

# enrich 进程池「多久没有分片完成就判定卡住」（多为 NFS 读死等）。
ENRICH_STALL_TIMEOUT = _env_float("WATCH_ENRICH_STALL_TIMEOUT", 60.0)


def summary() -> dict:
    """当前生效配置快照，供启动日志/看板自检。"""
    return {
        "concurrency": CONCURRENCY,
        "poll_sec": POLL_SEC,
        "enrich_workers": ENRICH_WORKERS,
        "enrich_inline_threshold": ENRICH_INLINE_THRESHOLD,
        "enrich_budget": ENRICH_BUDGET,
        "backfill_request_cooldown": BACKFILL_REQUEST_COOLDOWN,
        "reconcile_interval": RECONCILE_INTERVAL,
        "reconcile_batch": RECONCILE_BATCH,
        "queue_keep_days": WATCH_QUEUE_KEEP_DAYS,
        "fast_scan_days": FAST_SCAN_DAYS,
        "fast_scan_hours": FAST_SCAN_HOURS,
        "full_scan_every": FULL_SCAN_EVERY,
        "prio_now": PRIO_NOW,
        "prio_recent": PRIO_RECENT,
        "prio_old": PRIO_OLD,
        "recent_hours": RECENT_HOURS,
        "sync_batch": SYNC_BATCH,
        "enrich_stall_timeout": ENRICH_STALL_TIMEOUT,
    }
