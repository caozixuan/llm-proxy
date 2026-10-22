#!/usr/bin/env python3
"""
src/backfill/worker.py — 回填独立 worker 进程

把 new-api index.db 回填的**执行**从 app 主进程（uvicorn）剥离到独立长驻进程，
与 web 事件循环彻底隔离 CPU/IO —— 7GB 级构建打满磁盘 IO / 进程池吃满 CPU 时，
不再拖垮代理请求。

职责：轮询 log_dir.db 的 backfill_requests 表，领取 status='pending' 的请求，
按其 (root, workers, force) 调 newapi_backfill._run 执行，完成后回写终态。
单进程 + 线程池 → 多任务并发（有几个任务就跑几个，默认 4 线程）；叶子内部
_run 仍开 ProcessPoolExecutor 并行。

与 export_worker.py / obs_sync.py 同款守护进程骨架：
  - sys.path 注入，保证 `python -m src.backfill.worker` 直接可 import
  - SIGTERM/SIGINT 优雅退出：置停机标志，正在跑的叶子在检查点自然收尾
  - 启动时清理上一轮遗留：reset_building_on_startup（叶子 building→pending）
    + reset_running_backfill_on_startup（请求 running/pending→failed）

用法：
    python -m src.backfill.worker            # 长驻，默认 4 线程
    python -m src.backfill.worker --once     # 把当前队列跑空后退出（调试/cron）
    python -m src.backfill.worker --workers 8  # 自定义并发线程数

环境变量与服务进程保持一致（决定 SERVICE_LOG_DIR / DB 路径 / 源解析）：
    PROXY_PORT, LOG_TASK_TAG, UPSTREAM_API_KEY, LOGS_DIR
必须在与 app 相同的工作目录、相同环境变量下运行，才能命中同一 log_dir.db。
"""

import argparse
import logging
import os
import signal
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# 允许 `python src/backfill/worker.py` 直接运行（把项目根加入 import 路径）
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from utils.log_paths import get_service_log_dir
import utils.logdir_store as logdir_store
import utils.newapi_backfill as newapi_backfill

logger = logging.getLogger("backfill_worker")

_shutdown_requested = False

# 常驻模式下周期跑 auto_sync 的间隔（秒）。
# 默认 0 = 关闭：new-api 叶子的实时增量已由独立进程 src/watch/worker.py 覆盖
# （每源发现线程 + 同步池，秒级），小时级全量 auto_sync 只会和它抢锁、白跑一遍
# sync_leaves 扫盘。需要恢复旧行为时设 BACKFILL_AUTO_SYNC_INTERVAL=3600。
try:
    AUTO_SYNC_INTERVAL = float(os.getenv("BACKFILL_AUTO_SYNC_INTERVAL", "0"))
except ValueError:
    AUTO_SYNC_INTERVAL = 0.0


def _handle_signal(signum, frame):
    global _shutdown_requested
    _shutdown_requested = True
    logger.info("收到信号 %s，准备优雅退出（正在跑的叶子将在检查点收尾）", signum)


def _interruptible_sleep(seconds: float) -> None:
    """分片 sleep，收到停机信号立即醒来。"""
    end = seconds
    step = 0.5
    while end > 0:
        if _shutdown_requested:
            break
        time.sleep(min(step, end))
        end -= step


def _setup_logging() -> None:
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
        logger.addHandler(sh)
        # 周期 auto_sync 用独立 logger 'backfill_auto_sync'（sync_leaves 各源刷盘、
        # 入队等日志走它）。默认没有 handler → 周期同步静默，排障看不到「入队 N 个
        # 回填任务」「自动 sync 失败 root=...」等行。挂同一个 stdout handler，让它和
        # worker 日志同流同格式，写进 backfill_worker.log。
        as_logger = logging.getLogger("backfill_auto_sync")
        if not as_logger.handlers:
            as_logger.setLevel(logging.INFO)
            as_logger.addHandler(sh)
            as_logger.propagate = False


def _init_env(svc_dir: str) -> None:
    """worker 进程独立初始化：DB 连接 + 回填专用日志 + 清理遗留队列。

    与 app 各自持有自己的 sqlite 连接（log_dir.db 为 WAL + busy_timeout，多进程
    读写安全）。启动即清空上一轮在途：请求行 running/pending→failed、叶子
    building→pending。原为 app 启动时执行，剥离后归 worker：app 重启不再误清
    worker 正在跑的请求。
    """
    logdir_store.init_db(svc_dir)
    newapi_backfill.init_backfill_logger(svc_dir)
    try:
        logdir_store.reset_building_on_startup()
    except Exception:
        logger.exception("启动清理 reset_building_on_startup 失败")
    try:
        n_req = logdir_store.reset_running_backfill_on_startup()
        if n_req:
            logger.info("启动清理：取消 %d 条遗留回填请求（队列重置）", n_req)
    except Exception:
        logger.exception("启动清理 reset_running_backfill_on_startup 失败")


def _run_one(rec: dict) -> None:
    """执行一条已领取（status=running）的回填请求。参数来自请求行。

    任务已解耦（每个 root 写自己的叶子状态到 DB,WAL+busy_timeout 多线程安全；
    叶内 index.db 有自己的文件锁），无需额外锁。
    """
    req_id = rec["id"]
    root = rec["root"]
    workers = int(rec.get("workers") or newapi_backfill._DEFAULT_WORKERS)
    force = bool(rec.get("force"))

    logger.info("开始回填 req=%s root=%s workers=%d force=%s", req_id, root, workers, force)
    try:
        newapi_backfill._run(root, workers=workers, force=force)
        logdir_store.complete_backfill(req_id, ok=True)
        logger.info("回填完成 req=%s root=%s", req_id, root)
    except Exception as e:
        logger.exception("回填执行崩溃 req=%s root=%s", req_id, root)
        logdir_store.complete_backfill(req_id, ok=False, error=str(e))


def _drain(once: bool, max_workers: int, svc_dir: str = "") -> None:
    """主循环：线程池并发领取 pending 请求 → 执行 → 回写终态。

    单进程多线程：ThreadPoolExecutor(max_workers) 并发执行多条请求，有几个任务
    就跑几个（不像原版单线程全局串行）。每个任务仍调用 newapi_backfill._run，
    其内部开 ProcessPoolExecutor 并行叶子构建。空闲时分片轮询。

    常驻模式（非 once）：每 AUTO_SYNC_INTERVAL 秒（默认 1h）在主线程调一次
    auto_sync.check_and_enqueue_stale —— 先 sync 各源到磁盘现状，再把
    built<total 的 newapi 源入队（入队的重活仍由本线程池领取执行）。启动即先
    跑一轮，不必等满 1h。once 模式不跑周期 auto_sync（保持纯队列消费语义）。
    """
    last_auto_sync = None  # None = 尚未跑过，启动即触发一次
    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="backfill-") as executor:
        futures = set()
        while not _shutdown_requested:
            # 周期性 auto_sync（仅常驻模式；扫盘+入队在主线程同步执行，不占线程池）
            # AUTO_SYNC_INTERVAL<=0 表示关闭（实时索引已由 watch worker 接管）。
            if not once and svc_dir and AUTO_SYNC_INTERVAL > 0:
                nowm = time.monotonic()
                if last_auto_sync is None or (nowm - last_auto_sync) >= AUTO_SYNC_INTERVAL:
                    try:
                        from src.backfill.auto_sync import check_and_enqueue_stale
                        n = check_and_enqueue_stale(svc_dir)
                        logger.info("周期 auto_sync 完成，入队 %d 个回填任务", n)
                    except Exception:
                        logger.exception("周期 auto_sync 执行失败")
                    last_auto_sync = time.monotonic()

            # 清理已完成的 future
            done = {f for f in futures if f.done()}
            for f in done:
                try:
                    f.result()  # 捕获线程内异常（_run_one 已 try/except,这里只是确保不漏）
                except Exception:
                    logger.exception("线程执行异常")
            futures -= done

            # 线程池未满时，领取新请求
            while len(futures) < max_workers and not _shutdown_requested:
                rec = logdir_store.claim_next_backfill()
                if rec is None:
                    break
                logger.info("领取回填请求 req=%s root=%s", rec["id"], rec["root"])
                fut = executor.submit(_run_one, rec)
                futures.add(fut)

            # once 模式：队列空 + 所有任务完成即退出
            if once and not futures:
                rec = logdir_store.claim_next_backfill()
                if rec is None:
                    return
                # 还有任务，继续循环
                logger.info("领取回填请求 req=%s root=%s", rec["id"], rec["root"])
                fut = executor.submit(_run_one, rec)
                futures.add(fut)

            # 空闲等待或停机等待
            _interruptible_sleep(0.5)

        # 停机：等待所有已提交任务完成
        if futures:
            logger.info("等待 %d 个正在跑的任务完成...", len(futures))
            for f in as_completed(futures):
                try:
                    f.result()
                except Exception:
                    logger.exception("任务执行异常")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="回填独立 worker 进程")
    parser.add_argument("--once", action="store_true",
                        help="把当前队列跑空后退出（调试 / cron），否则长驻。")
    parser.add_argument("--workers", type=int, default=4,
                        help="并发线程数（默认 4）。有几个任务就跑几个，不再全局串行。")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    _setup_logging()
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    svc_dir = get_service_log_dir()
    logger.info("backfill_worker 启动 SERVICE_LOG_DIR=%s once=%s workers=%d",
                svc_dir, args.once, args.workers)
    _init_env(svc_dir)

    try:
        _drain(once=args.once, max_workers=args.workers, svc_dir=svc_dir)
    except Exception:
        logger.exception("backfill_worker 主循环异常退出")
        return 1
    logger.info("backfill_worker 退出")
    return 0


if __name__ == "__main__":
    sys.exit(main())
