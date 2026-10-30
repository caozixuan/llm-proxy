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
    n = backup_store.reset_running_on_startup()
    if n:
        logger.info("启动时把 %d 个中断的 running 任务降回 queued", n)
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
    requeue = False
    logger.info("领取任务 #%s %s dir=%s", job.get("id"), job_type, dir_path)
    try:
        if job_type == "resync":
            backup_routes._run_resync_failed(dir_path)
        elif job_type == "purge":
            # 人工确认删除：不走上传，直接校核 + 提交 clear
            backup_routes.run_purge_job(dir_path, svc_dir)
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
            res = backup_routes._run_sync_for_dir(
                dir_path,
                logs_all=None,        # 路径全部由 resolver 解析，不依赖 logs_all
                obs_base=cfg["obs_base"],
                workers=cfg["workers"],
                upload_script=cfg["upload_script"],
                resolver=lambda dp: backup_routes.resolve_real_backup_dir(dp, svc_dir),
                auto_delete=cfg["auto_delete"],
                retain_days=cfg["retain_days"],
                upload_jobs=cfg["upload_jobs"],
            )
            # 被停机信号打断（obsutil 退出码为负）：不算失败，重新排队等下次跑
            requeue = (res == "interrupted")
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
        # 必须等 finish_job 释放队列行之后才能重新入队：唯一部分索引会拒绝
        # 与仍在 running 的同名任务并存。
        if requeue and not _shutdown_requested:
            try:
                if backup_store.enqueue_job(dir_path, "sync"):
                    logger.info("已重新入队被中断的任务: %s", dir_path)
            except Exception:  # noqa: BLE001
                logger.exception("re-enqueue failed: %s", dir_path)
        logger.info("任务完成 #%s dir=%s", job.get("id"), dir_path)
    return True


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
            # 1) 手里有上传任务就先干完（run_once 内部已串行，一次一个）。
            did = run_once()
            if did:
                handled += 1
                continue

            # 2) 推进串行流水线一步：按叶子时间从远到近，
            #    遇到没备份的 → 入队上传（一次一个）；
            #    遇到已备份且到期的 → OBS 体积校核后提交删除。
            #
            #    **提交完立即推进下一个，不等 clear_worker 删完**：
            #    删除是 clear_worker 自己的队列（claim_next_clear 按 id ASC），
            #    只要提交顺序是按时间排的，执行顺序就也是按时间的。
            #    在这里干等只会让上传通道空转。
            cfg = backup_routes._auto_cfg()
            action, dp = backup_routes.run_pipeline_step(
                cfg["auto_delete"], cfg["retain_days"], cfg["submit_min_age_days"],
                auto_submit=cfg["auto_submit"])
            if action:
                logger.info("流水线推进 [%s] %s", action, dp)
                continue

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
