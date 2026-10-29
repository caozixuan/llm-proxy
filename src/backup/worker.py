#!/usr/bin/env python3
"""
src/backup/worker.py — 备份归档独立 worker 进程

把「叶子节点 → OBS」的归档执行从 web 进程剥离到独立长驻进程。此前上传跑在
app 进程的 `backup_routes._queue_dispatch_loop` 守护线程里，app 一重启就中断。

职责（对应需求 4 的完整闭环）：
  1. 从 backup_queue 领取任务（dir_path，如 `52e24556/260728/26072821`，
     对应 jumper-002 的 260728/26072821 叶子）；
  2. 上传到 OBS（tools/obs_upload.sh → obsutil cp -r -u -vlength）；
  3. 校验（obsutil 逐文件 -vlength + 哨兵对象大小比对）；
  4. 校验通过后按策略自动删除本地目录（受保留天数约束）；
  5. 登记 DB（backup_dirs.status=backed_up + uploaded_at/deleted_at）；
  6. 通知 session worker（leaf_status.archived=1，同库轮询）。

失败路径一律不删本地：上传失败→error/partial，校验失败→partial。

与其它 worker 的分工：
  - watch worker    ：发现增量并同步 index.db
  - backfill worker ：批量回填 index.db
  - session worker  ：把 index.db 的数据聚合成统计快照
  - backup worker   ：把冻结的叶子目录归档到 OBS 并清理本地（本进程）

用法：
    python -m src.backup.worker            # 长驻
    python -m src.backup.worker --once     # 把当前队列跑空后退出（调试/cron）

环境变量与 app 保持一致（决定 SERVICE_LOG_DIR / DB 路径 / 源解析）：
    PROXY_PORT, LOG_TASK_TAG, UPSTREAM_API_KEY, DATA_DIR, LOGS_DIR
"""

import argparse
import logging
import os
import signal
import sys
import time

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from utils.log_paths import get_service_log_dir
import utils.backup_store as backup_store
import utils.backup_routes as backup_routes
import utils.logdir_store as logdir_store

logger = logging.getLogger("backup_worker")

_shutdown_requested = False

# 队列空时的空转间隔（秒）。
IDLE_INTERVAL = float(os.getenv("BACKUP_WORKER_INTERVAL", "3") or 3)


def _handle_signal(signum, frame):
    global _shutdown_requested
    _shutdown_requested = True
    logger.info("收到信号 %s，准备优雅退出（当前叶子传完后退出）", signum)


def _sleep(seconds: float) -> None:
    """分片 sleep，收到停机信号立即醒来。"""
    end = seconds
    while end > 0:
        if _shutdown_requested:
            return
        time.sleep(min(0.5, end))
        end -= 0.5


def _setup_logging() -> None:
    fmt = "%(asctime)s [%(levelname)s] %(message)s"
    log_file = os.path.join(get_service_log_dir(), "backup_worker.log")
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    # 只挂 FileHandler：launcher 已把 stdout/stderr 重定向到同一文件，
    # 再加 StreamHandler 会让每条日志写两遍（与 watch/session worker 一致）。
    logging.basicConfig(level=logging.INFO, format=fmt,
                        handlers=[logging.FileHandler(log_file, encoding="utf-8")])


def _init_env() -> str:
    svc_dir = get_service_log_dir()
    backup_store.init_db(svc_dir)
    logdir_store.init_db(svc_dir)
    # 上次被 kill 时留在 running 的任务降回 queued，否则 claim_next_job
    # 看到「已有 running」会永远返回 None，队列就此卡死。
    backup_store.reset_running_on_startup()
    cfg = backup_routes._auto_cfg()
    logger.info(
        "backup_worker 启动 svc_dir=%s auto_delete=%s retain_days=%s obs_base=%s",
        svc_dir, cfg["auto_delete"], cfg["retain_days"], cfg["obs_base"] or "(未配置)",
    )
    if not cfg["auto_delete"]:
        logger.info("自动清理已关闭：校验通过后只转「待确认删除」，需人工确认")
    return svc_dir


def run_once() -> bool:
    """领取并执行一个任务。返回是否有任务被执行。"""
    job = backup_store.claim_next_job()
    if not job:
        return False

    dir_path = job["dir_path"]
    job_type = job.get("job_type", "sync")
    svc_dir = get_service_log_dir()
    logger.info("领取任务 #%s %s dir=%s", job.get("id"), job_type, dir_path)
    try:
        if job_type == "resync":
            backup_routes._run_resync_failed(dir_path)
        else:
            cfg = backup_routes._auto_cfg()
            if not cfg["obs_base"]:
                backup_store.update_sync_status(
                    dir_path, "error", error_msg="obs_base 未配置")
                backup_store.append_log(
                    dir_path, "同步失败: obs_base 未配置", level="error", event="upload_fail")
                return True
            # resolve_real_backup_dir 需要的是 SERVICE_LOG_DIR：它内部用
            # Path(logs_dir).parent.parent 反推 logs_all。
            backup_routes._run_sync_for_dir(
                dir_path,
                logs_all=None,        # 路径全部由 resolver 解析，不依赖 logs_all
                obs_base=cfg["obs_base"],
                workers=cfg["workers"],
                upload_script=cfg["upload_script"],
                resolver=lambda dp: backup_routes.resolve_real_backup_dir(dp, svc_dir),
                auto_delete=cfg["auto_delete"],
                retain_days=cfg["retain_days"],
            )
    except Exception:
        logger.exception("执行备份任务失败: %s", dir_path)
        try:
            backup_store.update_sync_status(dir_path, "error", error_msg="worker 执行异常")
            backup_store.append_log(
                dir_path, "worker 执行异常", level="error", event="upload_fail")
        except Exception:  # noqa: BLE001
            pass
    finally:
        backup_store.finish_job(job["id"])
        logger.info("任务完成 #%s dir=%s", job.get("id"), dir_path)
    return True


def maybe_auto_submit() -> int:
    """自动提交归档任务：开启后按「最远→最近」把候选目录批量入队。

    只在队列空时补货（run_once 返回 False 时调用），这样：
      - 不会把几千个任务一次性堆进 backup_queue；
      - 每个叶子都是「结算完一个再进下一批」，失败不污染后续批次；
      - 入队顺序即执行顺序（claim_next_job 是 ORDER BY id ASC），
        所以最旧的数据总被最先处理。

    返回本轮入队数量。未开启时直接返回 0。
    """
    cfg = backup_routes._auto_cfg()
    if not cfg.get("auto_submit") or not cfg["obs_base"]:
        return 0

    # 只自动提交「早于 N 天」的叶子：避免自动任务动到近期数据。
    # 未配置或为 0 时不限制（older_than 为空）。
    older_than = backup_routes._submit_age_cutoff(cfg.get("submit_min_age_days") or 0)

    try:
        chosen, matched, _skipped = backup_routes.select_archive_candidates(
            active_dir_path="",             # worker 不知道活跃目录，靠 status 过滤已足够
            envs=None,
            older_than=older_than,
            limit=cfg["submit_batch"],
            busy=set(),
        )
    except Exception:
        logger.exception("自动提交：挑选候选失败")
        return 0

    enqueued = 0
    for c in chosen:
        if backup_store.enqueue_job(c["dir_path"], "sync"):
            enqueued += 1
    if enqueued:
        logger.info("自动提交 %d 个归档任务（最远优先，剩余候选 %d，早于 %s）",
                    enqueued, max(0, matched - enqueued), older_than or "不限")
        try:
            backup_store.append_log(
                chosen[0]["dir_path"],
                f"自动提交 {enqueued} 个归档任务（按时间从远到近）",
                event="enqueue")
        except Exception:  # noqa: BLE001
            pass
    return enqueued


def main() -> int:
    parser = argparse.ArgumentParser(description="备份归档 worker（上传→校验→清理）")
    parser.add_argument("--once", action="store_true",
                        help="把当前队列跑空后退出（调试 / cron）")
    args = parser.parse_args()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    _setup_logging()
    _init_env()

    handled = 0
    try:
        while not _shutdown_requested:
            did = run_once()
            if did:
                handled += 1
                continue          # 有活就立刻继续，缩短总耗时
            # 队列空了：若开了「自动提交」，按最远→最近补一批
            if maybe_auto_submit():
                continue          # 补货成功，立刻回去干活
            if args.once:
                break
            _sleep(IDLE_INTERVAL)
    except Exception:
        logger.exception("backup_worker 主循环异常退出")
        return 1
    logger.info("backup_worker 退出（本次处理 %d 个任务）", handled)
    return 0


if __name__ == "__main__":
    sys.exit(main())
