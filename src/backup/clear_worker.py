#!/usr/bin/env python3
"""
src/backup/clear_worker.py — 本地清理独立 worker 进程

**只做一件事：删本地目录。** 与 backup_worker（只负责上传）彻底分离。

为什么必须分开：
  - 失败模式不同。上传是网络/OBS 问题，失败可重试；删除是本地 NFS 问题，
    `unlink` 在 hard 挂载下可能**无限阻塞**。混在一条流水线里，一个卡住的删除
    会堵死后面所有上传（实测卡死过 3 小时，把整个 web 服务也拖进 D 状态）。
  - 统计口径不同。「传了多少」和「删了多少」需要各自可观测。

安全设计（删除不可逆，宁可删不掉也不能删错）：
  1. 路径在**主进程入队时**就解析成绝对路径并写入 backup_clear_queue.local_path，
     worker 只按这一列删，绝不自己推导路径。
  2. 删除前复查：路径必须存在、必须是一个以 YYMMDDHH 命名的叶子目录层级，
     且必须是 backup_dirs 里状态为 pending_delete 且 has_obs=1 的目录。
  3. 删除交给 `rm -rf` 子进程执行并带超时（默认 300s）。超时/失败就放弃，
     目录原样保留，状态落 failed —— 不重试、不递归硬删。
  4. 删除成功后：backup_dirs → backed_up（has_local=0, deleted_at），
     leaf_status.archived=1（这就是通知 session worker 的机制）。

用法：
    python -m src.backup.clear_worker            # 长驻
    python -m src.backup.clear_worker --once     # 跑空队列后退出（调试）

环境变量与 app 一致：PROXY_PORT, LOG_TASK_TAG, UPSTREAM_API_KEY, DATA_DIR, LOGS_DIR
"""

import argparse
import logging
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from utils.log_paths import get_service_log_dir
import utils.backup_store as backup_store
import utils.logdir_store as logdir_store

logger = logging.getLogger("clear_worker")

_shutdown_requested = False

IDLE_INTERVAL = float(os.getenv("CLEAR_WORKER_INTERVAL", "5") or 5)
# 单次删除的超时。NFS 上删一个 80 万文件的叶子实测要几分钟，
# 给足时间；超过就放弃（宁可留着也不能把 worker 卡死）。
# 单次删除的超时。
# 大叶子按文件数而非体积决定耗时：NFS 上每个 unlink 都是一次 RPC 往返，
# 实测 84 万文件的目录（45GB）串行删要按小时计。这里给足 4 小时，
# 宁可慢也不能因为超时把「正在正常推进的删除」判成失败。
# 超时仍然是兜底：真卡死了就放弃、保留目录，worker 继续处理下一个。
RM_TIMEOUT = int(os.getenv("CLEAR_WORKER_RM_TIMEOUT", "14400") or 14400)


def _handle_signal(signum, frame):
    global _shutdown_requested
    _shutdown_requested = True
    logger.info("收到信号 %s，准备退出（当前删除结束后）", signum)


def _sleep(seconds: float) -> None:
    end = seconds
    while end > 0:
        if _shutdown_requested:
            return
        time.sleep(min(0.5, end))
        end -= 0.5


def _setup_logging() -> None:
    fmt = "%(asctime)s [%(levelname)s] %(message)s"
    log_file = os.path.join(get_service_log_dir(), "clear_worker.log")
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    logging.basicConfig(level=logging.INFO, format=fmt,
                        handlers=[logging.FileHandler(log_file, encoding="utf-8")])


def _init_env() -> str:
    svc_dir = get_service_log_dir()
    backup_store.init_db(svc_dir)
    logdir_store.init_db(svc_dir)
    n = backup_store.reset_clear_on_startup()
    st = backup_store.clear_queue_stats()
    logger.info("clear_worker 启动 svc_dir=%s 重置中断任务=%d 队列=%d 超时=%ds",
                svc_dir, n, st["queued"], RM_TIMEOUT)
    return svc_dir


def _safe_to_delete(dir_path: str, local_path: str, root_hint: str = "") -> tuple:
    """删除前的最后一道校验。返回 (ok, reason)。

    删除不可逆，这里做四道检查，任一不满足就拒绝：
      1. 路径非空且是绝对路径；
      2. backup_dirs 里必须是 pending_delete + has_obs=1 + obs_path 非空
         （即：内容确实已上传到 OBS）；
      3. 路径最后一层必须是 YYMMDDHH 形态的时间目录（防误删到父目录/根）；
      4. 路径不能是文件系统根、不能只有一两层。
    """
    p = (local_path or "").strip()
    if not p or not os.path.isabs(p):
        return False, f"路径非法（需绝对路径）: {p!r}"
    if len(Path(p).parts) < 3:
        return False, f"路径层级过浅，拒绝删除: {p}"
    name = Path(p).name
    if not (len(name) == 8 and name.isdigit()):
        return False, f"末层目录名不是 YYMMDDHH（{name}），拒绝删除"

    info = backup_store.get_dir(dir_path)
    if not info:
        return False, "backup_dirs 无此记录"
    if info.get("status") != "pending_delete":
        return False, f"状态不是 pending_delete（当前 {info.get('status')}）"
    if not info.get("has_obs") or not info.get("obs_path"):
        return False, "未确认已上传 OBS（has_obs/obs_path 不满足）"
    return True, ""


def _do_rm(local_path: str) -> tuple:
    """用子进程删目录，返回 (ok, error)。

    绝不在本进程里用 shutil.rmtree：NFS hard 挂载下 unlink 会无限阻塞，
    把 worker 变成 D 状态僵尸。子进程 + 超时能保证 worker 永远活着。
    """
    try:
        r = subprocess.run(["rm", "-rf", "--", local_path],
                           capture_output=True, text=True, timeout=RM_TIMEOUT)
    except subprocess.TimeoutExpired:
        # 杀掉卡住的子进程，避免遗留 D 状态进程占资源
        return False, f"删除超时（>{RM_TIMEOUT}s），已放弃"
    except Exception as e:  # noqa: BLE001
        return False, f"{type(e).__name__}: {e}"
    if os.path.exists(local_path):
        return False, (r.stderr or "").strip() or "目录仍存在"
    return True, ""


def run_once() -> bool:
    """领取并执行一个清理任务。返回是否有任务被执行。"""
    job = backup_store.claim_next_clear()
    if not job:
        return False

    dir_path = job["dir_path"]
    local_path = job["local_path"]
    logger.info("领取清理任务 #%s dir=%s path=%s", job.get("id"), dir_path, local_path)

    ok, reason = _safe_to_delete(dir_path, local_path)
    if not ok:
        logger.warning("拒绝删除 #%s: %s", job.get("id"), reason)
        backup_store.append_log(dir_path, f"清理被拒绝: {reason}",
                                level="error", event="delete_fail")
        backup_store.finish_clear(job["id"], ok=False, error=reason)
        return True

    if not os.path.exists(local_path):
        # 已经被删掉了（人工或上次残留），直接收敛状态
        backup_store.mark_backed_up(dir_path)
        backup_store.set_deleted(dir_path)
        backup_store.append_log(dir_path, "本地目录已不存在，标记为已清理", event="deleted")
        _notify_session_worker(local_path)
        backup_store.finish_clear(job["id"], ok=True)
        return True

    ok, err = _do_rm(local_path)
    if ok:
        backup_store.mark_backed_up(dir_path)
        backup_store.set_deleted(dir_path)
        backup_store.append_log(dir_path, "本地目录已清理", event="deleted")
        _notify_session_worker(local_path)
        backup_store.finish_clear(job["id"], ok=True)
        logger.info("清理完成 #%s %s", job.get("id"), local_path)
    else:
        backup_store.append_log(dir_path, f"清理失败: {err}", level="error",
                                event="delete_fail")
        backup_store.finish_clear(job["id"], ok=False, error=err)
        logger.warning("清理失败 #%s %s: %s", job.get("id"), local_path, err)
    return True


def _notify_session_worker(local_path: str) -> None:
    """给 leaf_status 打 archived 标记 —— 这就是「通知 session worker」的机制。

    session_worker 每轮读同一个 WAL 库的 stale_stats_leaves()（已按
    archived=0 过滤），下一轮自然不再重试这个已删除的叶子。
    """
    try:
        if logdir_store._ready():
            logdir_store.mark_archived([local_path], True)
    except Exception:  # noqa: BLE001
        logger.exception("mark_archived failed for %s", local_path)


def main() -> int:
    parser = argparse.ArgumentParser(description="本地清理 worker（只删本地目录）")
    parser.add_argument("--once", action="store_true", help="跑空队列后退出")
    args = parser.parse_args()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    _setup_logging()
    _init_env()

    handled = 0
    try:
        while not _shutdown_requested:
            if run_once():
                handled += 1
                continue
            if args.once:
                break
            _sleep(IDLE_INTERVAL)
    except Exception:
        logger.exception("clear_worker 主循环异常退出")
        return 1
    logger.info("clear_worker 退出（本次处理 %d 个任务）", handled)
    return 0


if __name__ == "__main__":
    sys.exit(main())
