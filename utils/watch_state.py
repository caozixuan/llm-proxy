"""
utils/watch_state.py — watch worker 进程存活探测

给看板用：读取 launcher（./app watch start）写下的 pid 文件，判定进程是否在跑，
并给出 uptime。只做 os.kill(pid, 0) + /proc 读取，不碰 NFS、不碰 SQLite。
"""

import logging
import os
from typing import Tuple

logger = logging.getLogger("watch.state")

_PID_NAME = "watch_worker.pid"


def _pid_path() -> str:
    try:
        from utils.log_paths import get_service_log_dir
        return os.path.join(get_service_log_dir(), _PID_NAME)
    except Exception:  # noqa: BLE001
        return ""


def _is_running(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except PermissionError:
        return True          # 存在但属别的用户
    except OSError:
        return False


def read_pid() -> int:
    p = _pid_path()
    if not p:
        return 0
    try:
        with open(p, "r", encoding="utf-8") as f:
            return int((f.read() or "").strip() or 0)
    except (OSError, ValueError):
        return 0


def _uptime_sec(pid: int) -> int:
    """从 /proc/<pid>/stat 的 starttime 换算进程已运行秒数。拿不到返回 0。"""
    try:
        with open(f"/proc/{pid}/stat", "r", encoding="utf-8") as f:
            fields = f.read().split()
        # 第 22 个字段（1-based）是 starttime，单位 clock tick
        start_ticks = int(fields[21])
        with open("/proc/uptime", "r", encoding="utf-8") as f:
            sys_uptime = float(f.read().split()[0])
        hz = os.sysconf("SC_CLK_TCK") or 100
        return max(0, int(sys_uptime - start_ticks / hz))
    except (OSError, ValueError, IndexError):
        return 0


def worker_alive() -> Tuple[bool, int, int]:
    """返回 (是否在跑, pid, 已运行秒数)。"""
    pid = read_pid()
    if not pid or not _is_running(pid):
        return False, 0, 0
    return True, pid, _uptime_sec(pid)
