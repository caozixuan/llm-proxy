"""
utils/newapi_watcher.py — new-api 叶子 index.db 的近实时增量同步

背景：new-api 格式叶子的视图只读 <leaf>/index.db。此前它只由小时级回填 worker
（src/backfill/worker.py，AUTO_SYNC_INTERVAL=3600）构建，新写入的请求最长要 1 小时
才出现在浏览器上。

本模块把「写入完成 → index.db 可查」压缩到秒级，且**不做全量重建**：
  1. 写入侧（utils/req_index.append_index）追加完 index.jsonl 后回调 mark_dirty()
  2. 去抖（默认 2s，合并突发写入）后，对脏叶子跑 ingest → enrich → sync_sessions
     三者都是增量的：ingest 只读 offset 之后的新行、enrich 只处理 q1_hash IS NULL
     的行、sync_sessions 只折叠 rowid 水位之后的行
  3. 全程持有一把 fcntl 文件锁（<leaf>/.indexer.lock）

为什么需要跨进程文件锁：newapi_index_db._leaf_lock 只是进程内的 threading.Lock，
而 app 进程与本仓库的 src/backfill/worker.py 会同时写同一个叶子的 index.db。
两把 SQLite 写事务撞上会退化为 'database is locked' 或长事务互相等待。

兜底：即使 hook 没接上（例如写入方是别的实例），watcher 也会每 POLL_INTERVAL
轮询一次活跃叶子，用 needs_build 判活。hook 是主路径，轮询只保底。

可用环境变量关闭：NEWAPI_WATCHER=0
"""

import fcntl
import logging
import os
import threading
import time
from pathlib import Path
from typing import Dict, Optional, Set

logger = logging.getLogger("newapi-watcher")

# 去抖窗口：写入后等这么久再同步，合并同一会话的连续几次请求
DEBOUNCE_SEC = float(os.getenv("NEWAPI_WATCHER_DEBOUNCE", "2.0"))
# 兜底轮询周期（秒）。真实 new-api 叶子由外部进程写入，hook 收不到，
# 所以这是主路径而不是兜底——它直接决定「写入到可见」的上限。
POLL_INTERVAL = float(os.getenv("NEWAPI_WATCHER_POLL", "5.0"))
# 单叶子每轮 enrich 的并发 worker 数。秒级路径上通常只有 1-2 条新行——
# workers=1 会走 enrich 的进程内快路径（不起 spawn 进程池，省 ~100ms 启动开销），
# 这对「写入到可见」的延迟是决定性的。突发大批量时才值得调大。
ENRICH_WORKERS = int(os.getenv("NEWAPI_WATCHER_WORKERS", "1"))

_LOCK_NAME = ".indexer.lock"

# 兜底轮询每个根只看最新的这几个 day/hour 目录（新数据只会落在最新的 hour 里）。
# 加大只会让每轮 readdir 变多，并不会让老叶子更新更快（老叶子由小时级回填补）。
_LOOKBACK_DAYS = 2
_LOOKBACK_HOURS = 3

_started = False
_start_guard = threading.Lock()
_stop_evt = threading.Event()

# dirty 叶子集合 + 唤醒事件（条件变量语义）
_dirty: Set[str] = set()
_dirty_guard = threading.Lock()
_wake = threading.Event()

# 已注册的根目录（env 目录），用于兜底轮询
_roots: Set[str] = set()
_roots_guard = threading.Lock()


def _enabled() -> bool:
    return os.getenv("NEWAPI_WATCHER", "1") not in ("0", "false", "False", "")


def mark_dirty(leaf_dir: str) -> None:
    """把叶子标脏并唤醒同步线程。可从任意线程调用（req_index 的 to_thread 线程池）。"""
    if not leaf_dir:
        return
    key = os.path.normpath(leaf_dir)
    with _dirty_guard:
        _dirty.add(key)
    _wake.set()


def register_root(env_dir: str) -> None:
    """登记需要兜底轮询的 env 目录。"""
    if env_dir:
        with _roots_guard:
            _roots.add(os.path.normpath(env_dir))


class _LeafLockError(Exception):
    pass


class _LeafFileLock:
    """跨进程叶子写锁（fcntl.flock，非阻塞探测 + 超时）。

    用独立锁文件而不是锁 index.db 本身：SQLite 自己会持有文件锁，
    再对同一文件 flock 语义会与之纠缠。
    """

    def __init__(self, leaf_dir: str, timeout: float = 30.0):
        self.path = os.path.join(leaf_dir, _LOCK_NAME)
        self.timeout = timeout
        self.fd: Optional[int] = None

    def __enter__(self):
        deadline = time.monotonic() + self.timeout
        self.fd = os.open(self.path, os.O_CREAT | os.O_RDWR, 0o644)
        while True:
            try:
                fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return self
            except OSError:
                if time.monotonic() >= deadline:
                    os.close(self.fd)
                    self.fd = None
                    raise _LeafLockError(f"leaf locked: {self.path}")
                time.sleep(0.05)

    def __exit__(self, *exc):
        if self.fd is not None:
            try:
                fcntl.flock(self.fd, fcntl.LOCK_UN)
            finally:
                os.close(self.fd)
                self.fd = None
        return False


def flush_leaf(leaf_dir: str, enrich_workers: int = 0,
               workers_fn=None) -> Dict[str, object]:
    """对单个叶子跑一轮增量同步。返回统计；失败只记日志不外抛。

    enrich_workers: enrich 的并发数；0 表示用模块默认 ENRICH_WORKERS。
    workers_fn:    可选回调，在 **ingest 之后** 调用并返回本次 enrich 并发数。
                   新叶子的积压行只有 ingest 之后才可见——若在 ingest 前按
                   pending 决策，会永远读到 0 而误走单进程慢路径。
    """
    from utils import newapi_index_db as nidb

    workers = enrich_workers if enrich_workers > 0 else ENRICH_WORKERS
    out: Dict[str, object] = {"leaf": leaf_dir, "ingested": 0,
                              "enriched": 0, "synced": 0, "ok": False}
    if not os.path.isfile(os.path.join(leaf_dir, "index.jsonl")):
        return out
    t0 = time.monotonic()
    if not nidb.needs_build(leaf_dir) and nidb._db_exists(leaf_dir):
        out["ok"] = True
        out["elapsed_ms"] = int((time.monotonic() - t0) * 1000)
        return out
    try:
        with _LeafFileLock(leaf_dir):
            try:
                out["ingested"] = nidb.ingest(leaf_dir)
            except Exception as ex:  # noqa: BLE001
                logger.warning("watcher ingest 失败 %s: %s", leaf_dir, ex)
            if workers_fn is not None:
                try:
                    workers = max(1, int(workers_fn() or 1))
                except Exception:  # noqa: BLE001
                    workers = ENRICH_WORKERS
            try:
                en = nidb.enrich(leaf_dir, workers=workers,
                                 stall_timeout=60.0)
                out["enriched"] = en.get("enriched", 0)
            except Exception as ex:  # noqa: BLE001
                logger.warning("watcher enrich 失败 %s: %s", leaf_dir, ex)
            try:
                sy = nidb.sync_sessions(leaf_dir)
                out["synced"] = sy.get("synced", 0)
            except Exception as ex:  # noqa: BLE001
                logger.warning("watcher sync_sessions 失败 %s: %s", leaf_dir, ex)
        out["ok"] = True
        # 有新会话真的落进 index.db 时，让导出浏览的聚合结果缓存失效，
        # 否则前端最长还要再吃 900s 的旧快照（用户点「刷新列表」也看不到）。
        if out["synced"]:
            try:
                from utils.log_routes import invalidate_agg_cache
                invalidate_agg_cache(leaf_dir)
            except Exception:  # noqa: BLE001
                pass
    except _LeafLockError as ex:
        # 另一个进程（如回填 worker）正在写这个叶子：跳过，等下轮
        logger.debug("watcher 跳过（%s）", ex)
    except Exception as ex:  # noqa: BLE001
        logger.warning("watcher flush 失败 %s: %s", leaf_dir, ex)
    finally:
        out["elapsed_ms"] = int((time.monotonic() - t0) * 1000)
    return out


def _active_leaves() -> Set[str]:
    """兜底轮询：只取每个根下**最新的** day/hour 叶子。

    关键约束：真实的 new-api 叶子是外部 new-api 进程写的，不会走本进程的
    append_index hook，所以轮询才是主路径。而全量 iter_index_dirs 在 NFS 上要
    遍历上千个目录（分钟级），绝不能放进 10s 循环。

    叶子布局是 <root>/<day>/<hour>/index.jsonl，新数据只会落在最新的 hour 目录，
    因此按名字排序取最后几个 day/hour 即可：每根 O(几) 次 readdir。
    """
    out: Set[str] = set()
    with _roots_guard:
        roots = list(_roots)
    for r in roots:
        try:
            root = Path(r)
            if not root.is_dir():
                continue
            days = sorted((d for d in root.iterdir() if d.is_dir()), key=lambda p: p.name)
            for day in days[-_LOOKBACK_DAYS:]:
                hours = sorted((h for h in day.iterdir() if h.is_dir()),
                               key=lambda p: p.name)
                for hour in hours[-_LOOKBACK_HOURS:]:
                    if (hour / "index.jsonl").is_file():
                        out.add(os.path.normpath(str(hour)))
        except OSError:
            continue
    return out


def _run_loop() -> None:
    from utils import newapi_index_db as nidb
    from utils.log_routes import _root_format

    last_poll = 0.0
    while not _stop_evt.is_set():
        _wake.wait(timeout=max(1.0, DEBOUNCE_SEC))
        if _stop_evt.is_set():
            break
        fired = _wake.is_set()
        _wake.clear()

        if fired:
            # 去抖：再等一个窗口，把突发写入合并成一次同步
            _stop_evt.wait(DEBOUNCE_SEC)
            if _stop_evt.is_set():
                break
            with _dirty_guard:
                batch = list(_dirty)
                _dirty.clear()
            for leaf in batch:
                try:
                    if _root_format(leaf) != "newapi":
                        continue
                    flush_leaf(leaf)
                except Exception as ex:  # noqa: BLE001
                    logger.warning("watcher 处理 %s 失败: %s", leaf, ex)

        # 兜底轮询：只在距上次足够久时做（枚举目录本身也有成本）
        now = time.monotonic()
        if now - last_poll >= POLL_INTERVAL:
            last_poll = now
            with _roots_guard:
                has_roots = bool(_roots)
            if has_roots:
                try:
                    for leaf in _active_leaves():
                        try:
                            if _root_format(leaf) != "newapi":
                                continue
                            if nidb.needs_build(leaf):
                                flush_leaf(leaf)
                        except Exception:  # noqa: BLE001
                            continue
                except Exception as ex:  # noqa: BLE001
                    logger.warning("watcher 轮询失败: %s", ex)


def start(env_dir: str = "") -> None:
    """启动 watcher 线程并注册根目录。幂等，可在每个 worker 里调用。

    每个 worker 都启动是安全的：真正的写互斥由 _LeafFileLock（跨进程 flock）保证，
    重复的一轮同步最多是空转（needs_build 已为假直接返回）。
    """
    if not _enabled():
        logger.info("newapi watcher 已禁用（NEWAPI_WATCHER=0）")
        return
    global _started
    with _start_guard:
        if env_dir:
            register_root(env_dir)
        if _started:
            return
        _started = True
    t = threading.Thread(target=_run_loop, name="newapi-watcher", daemon=True)
    t.start()
    logger.info("newapi watcher 已启动（debounce=%.1fs poll=%.1fs workers=%d）",
                DEBOUNCE_SEC, POLL_INTERVAL, ENRICH_WORKERS)


def stop() -> None:
    _stop_evt.set()
    _wake.set()


def pending_leaves() -> int:
    """当前待处理叶子数（自检/测试用）。"""
    with _dirty_guard:
        return len(_dirty)
