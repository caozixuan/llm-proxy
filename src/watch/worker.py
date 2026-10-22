"""
src/watch/worker.py — new-api 实时索引独立进程

架构：
  - 每个 newapi 源一个发现线程（src/watch/discover.py），只做 stat → 入 watch_queue
  - 一个 dispatcher 线程从 watch_queue 按优先级 claim → 提交到 8 线程同步池
  - 同步池每个任务：ingest → enrich → sync_sessions（复用 newapi_watcher.flush_leaf）
  - 完成后写 leaf_status + watch_source_stats（ETA 用）

与 backfill_worker 的分工：
  - watch worker  ：发现增量（日常运行，秒级）
  - backfill worker：手动触发的批量回填（力大砖飞，慢慢补）
  两者共用 flock 跨进程互斥，不会互相打架。
"""

import logging
import os
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from utils.log_paths import get_service_log_dir
import utils.logdir_store as logdir_store
import utils.logs_config as logs_config

# ── 配置 ──────────────────────────────────────────────────────────────────
# 全部来自 utils/watch_config（.env 可覆盖，不填用默认）。
from utils import watch_config as _cfg

WATCH_CONCURRENCY = _cfg.CONCURRENCY
WATCH_POLL_SEC = _cfg.POLL_SEC
WATCH_ENRICH_WORKERS = _cfg.ENRICH_WORKERS
WATCH_ENRICH_INLINE_THRESHOLD = _cfg.ENRICH_INLINE_THRESHOLD
WATCH_ENRICH_BUDGET = _cfg.ENRICH_BUDGET


class _EnrichBudget:
    """enrich 子进程槽位池。线程安全，按线程记录本次预留量以便释放。"""

    def __init__(self, total: int):
        self.total = max(1, total)
        self._free = self.total
        self._cv = threading.Condition()
        self._tl = threading.local()

    def acquire_for(self, want: int) -> int:
        """预留 1..want 个槽位，返回实际拿到的数量（至少 1）。阻塞直到有槽。

        保留给需要「一定拿到多并发」的场景；实时同步路径请用 try_acquire，
        避免 8 个同步线程互相堵死。
        """
        want = max(1, int(want))
        with self._cv:
            while self._free <= 0:
                self._cv.wait(timeout=1.0)
            got = min(want, self._free)
            self._free -= got
            self._tl.held = getattr(self._tl, "held", 0) + got
        return got

    def try_acquire(self, want: int) -> int:
        """非阻塞预留：能拿多少拿多少；一个都拿不到就返回 1（走进程内单进程路径）。

        为什么不能阻塞：8 个叶子并发时，若前几个占满全局预算，后面的线程会卡在
        wait 里白占一个同步槽位——本该并行推进的叶子被串行化了。返回 1 意味着
        该叶子用「不起 spawn 池」的进程内快路径，慢一点但绝不堵别人。
        """
        want = max(1, int(want))
        with self._cv:
            if self._free <= 0:
                return 1
            got = min(want, self._free)
            self._free -= got
            self._tl.held = getattr(self._tl, "held", 0) + got
        return got

    def release(self) -> None:
        """释放本线程当前持有的全部槽位。"""
        with self._cv:
            held = getattr(self._tl, "held", 0)
            if held:
                self._free = min(self.total, self._free + held)
                self._tl.held = 0
                self._cv.notify_all()


_ENRICH_BUDGET = _EnrichBudget(WATCH_ENRICH_BUDGET)

_logger = logging.getLogger("watch_worker")
_shutdown_requested = False
_start_ts = time.time()
_active_leaves: dict = {}     # wq_id -> leaf_path，供看板
_active_lock = threading.Lock()


def _handle_signal(signum, frame):
    global _shutdown_requested
    _shutdown_requested = True
    _logger.info("收到信号 %s，准备退出", signum)


def _sync_one(rec: dict) -> None:
    """同步池任务：处理一条 watch_queue 记录。"""
    wq_id = rec["id"]
    leaf_path = rec["leaf_path"]
    root_id = rec.get("root_id", "")
    root_path = rec.get("root_path", "")
    dir_key = rec.get("dir_key", "")
    t0 = time.time()

    with _active_lock:
        _active_leaves[wq_id] = leaf_path

    ok = False
    error = ""
    bytes_done = 0
    rows_done = 0
    try:
        from utils.newapi_watcher import flush_leaf
        from utils.newapi_index_db import pending_count, status as nidb_status
        from utils.log_scan import dir_key_for

        def _decide_workers() -> int:
            """在 ingest 之后决定 enrich 并发。

            关键：新叶子的积压行只有 ingest 写进 requests 后才可见。若在 ingest
            前按 pending_count 决策，新叶子永远读到 0，会误走单进程慢路径
            （实测 ~28 行/s，而 4 并发 ~300 行/s）。

            同时受全局预算约束：8 个同步线程各自开 4 个 worker 会起 32 个子进程，
            把 NFS 和内存打爆。用共享信号量按「每个子进程一个槽位」限流。
            """
            try:
                pend = pending_count(leaf_path)
            except Exception:
                pend = 0
            if pend < WATCH_ENRICH_INLINE_THRESHOLD:
                return 1
            return _ENRICH_BUDGET.try_acquire(WATCH_ENRICH_WORKERS)

        result = flush_leaf(leaf_path, workers_fn=_decide_workers)
        ok = result.get("ok", False)
        if not ok and result.get("enriched", 0) == 0 and result.get("ingested", 0) == 0:
            # 空叶或无需构建也算 ok
            ok = True

        # 写 leaf_status（不然「数据管理」页节点数会冻结）
        try:
            st = nidb_status(leaf_path)
            from utils.logs_config import get_root_id
            rid = root_id or get_root_id(root_path)
            dk = dir_key or dir_key_for(Path(root_path), Path(leaf_path))
            logdir_store.upsert_leaf(
                rid, dk, root_path=root_path, leaf_path=leaf_path,
                built=bool(st.get("built")),
                ingested=st.get("ingested", 0),
                pending=st.get("pending", 0),
                sessions=st.get("sessions", 0),
                state="done" if st.get("built") else "pending",
            )
        except Exception as ex:  # noqa: BLE001
            _logger.debug("写 leaf_status 失败 %s: %s", leaf_path, ex)

        # 计算字节吞吐
        try:
            idx_size = os.path.getsize(os.path.join(leaf_path, "index.jsonl"))
            offset_was = rec.get("offset_at") or 0
            bytes_done = max(0, idx_size - offset_was)
            rows_done = result.get("enriched", 0) + result.get("ingested", 0)
        except OSError:
            bytes_done = max(0, (rec.get("idx_size") or 0) - (rec.get("offset_at") or 0))

    except Exception as ex:  # noqa: BLE001
        ok = False
        error = str(ex)
        _logger.warning("flush_leaf 失败 %s: %s", leaf_path, ex)
    finally:
        took_ms = int((time.time() - t0) * 1000)
        with _active_lock:
            _active_leaves.pop(wq_id, None)
        # 释放 enrich 子进程槽位（_decide_workers 里预留的）
        try:
            _ENRICH_BUDGET.release()
        except Exception:  # noqa: BLE001
            pass
        logdir_store.finish_watch(wq_id, ok, bytes_done, took_ms, error)
        if ok and bytes_done > 0:
            try:
                logdir_store.update_watch_source_stats(
                    root_id, bytes_done, took_ms, rows_done)
            except Exception:
                pass
        _logger.info("LEAF %s %s took=%dms bytes=%d err=%s",
                     "OK" if ok else "FAIL", leaf_path, took_ms, bytes_done, error[:80])
        # 收尾权威复核：ingest 读的是「开始那一刻的 EOF」，读完到任务结束之间
        # 文件往往又长了。发现线程此时也会看到增长，但它的入队会被唯一索引忽略
        # （本行还是 running）——所以必须在这里补一条，否则那几行要等下次增长。
        # 这一步不依赖任何内存状态，是漏唤醒的最终兜底。
        if ok:
            try:
                _requeue_if_lagging(leaf_path, root_id, root_path, dir_key)
            except Exception as ex:  # noqa: BLE001
                _logger.debug("收尾复核失败 %s: %s", leaf_path, ex)


def _requeue_if_lagging(leaf_path: str, root_id: str, root_path: str,
                        dir_key: str) -> None:
    """任务完成后对比文件大小与 ingest_offset，仍落后就补一条入队。"""
    from utils.newapi_index_db import _connect, _meta_get, _db_exists
    idx = os.path.join(leaf_path, "index.jsonl")
    try:
        size = os.path.getsize(idx)
    except OSError:
        return
    offset = 0
    if _db_exists(leaf_path):
        try:
            c = _connect(leaf_path)
            offset = int(_meta_get(c, "ingest_offset", "0") or 0)
            c.close()
        except Exception:  # noqa: BLE001
            offset = 0
    if size <= offset:
        return
    from src.watch.discover import _leaf_priority
    added = logdir_store.enqueue_watch(
        root_id, root_path, leaf_path, dir_key,
        priority=_leaf_priority(os.path.basename(leaf_path.rstrip("/"))),
        reason="grown", idx_size=size, offset_at=offset,
    )
    if added:
        _logger.info("收尾复核补入队 %s size=%d offset=%d", dir_key, size, offset)


def _dispatcher(executor: ThreadPoolExecutor, stop_evt: threading.Event) -> None:
    """dispatcher：持续从 watch_queue 领取任务并提交到池。"""
    _logger.info("dispatcher 启动 concurrency=%d", WATCH_CONCURRENCY)
    # 追踪在途 futures 不超过 WATCH_CONCURRENCY
    futs = set()
    while not stop_evt.is_set():
        # 清理已完成
        done = {f for f in futs if f.done()}
        futs -= done

        # 填满池
        while len(futs) < WATCH_CONCURRENCY and not stop_evt.is_set():
            rec = logdir_store.claim_next_watch()
            if rec is None:
                break
            fut = executor.submit(_sync_one, rec)
            futs.add(fut)

        stop_evt.wait(0.5)

    # 关停：等在途任务完成
    _logger.info("dispatcher 关停，等待 %d 个在途任务…", len(futs))
    for f in futs:
        try:
            f.result(timeout=600)
        except Exception:
            pass


def _init_env():
    """初始化日志和 logdir_store。"""
    svc_dir = get_service_log_dir()
    os.makedirs(svc_dir, exist_ok=True)
    logdir_store.init_db(svc_dir)

    log_fmt = "%(asctime)s [%(levelname)s] %(message)s"
    log_file = os.path.join(svc_dir, "watch_worker.log")
    # 只挂 FileHandler：launcher（./app watch start）已把 stdout/stderr 重定向到
    # 同一个 watch_worker.log，再加 StreamHandler 会让每条日志写两遍。
    logging.basicConfig(
        level=logging.INFO,
        format=log_fmt,
        handlers=[logging.FileHandler(log_file, encoding="utf-8")],
    )
    _logger.info("watch_worker 启动 svc_dir=%s concurrency=%d poll=%.1fs",
                 svc_dir, WATCH_CONCURRENCY, WATCH_POLL_SEC)

    # 崩溃复位
    n = logdir_store.reset_watch_on_startup()
    if n:
        _logger.info("reset %d 个中断的 running 任务 → pending", n)


def main():
    global _shutdown_requested
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    _init_env()
    stop_evt = threading.Event()

    # 启动每源发现线程
    from src.watch.discover import start_discover_threads
    discover_threads = start_discover_threads(stop_evt, poll_sec=WATCH_POLL_SEC)
    _logger.info("已启动 %d 个发现线程", len(discover_threads))

    # 启动同步池 + dispatcher
    with ThreadPoolExecutor(max_workers=WATCH_CONCURRENCY,
                            thread_name_prefix="watch-sync-") as executor:
        disp = threading.Thread(
            target=_dispatcher, args=(executor, stop_evt),
            name="watch-dispatcher", daemon=False,
        )
        disp.start()

        while not _shutdown_requested:
            time.sleep(1)

        _logger.info("收到关停信号，通知所有线程退出…")
        stop_evt.set()
        disp.join(timeout=620)

    _logger.info("watch_worker 已退出")


if __name__ == "__main__":
    main()
