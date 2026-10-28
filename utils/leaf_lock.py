"""
utils/leaf_lock.py — new-api 叶子的跨进程写锁

一个叶子（`<root>/<day>/<hour>/`）的 index.db 有两条写入路径：

  - src/watch/worker.py   （实时增量，秒级，`newapi_watcher.flush_leaf`）
  - src/backfill/worker.py（批量增量构建，`newapi_backfill._run`）

两者是**不同进程**，而 newapi_index_db._leaf_lock 只是进程内的 threading.Lock，
拦不住对方。没有跨进程锁时两条路径撞上同一片叶子，只能靠 SQLite 的
busy_timeout=30s 兜底 —— 大叶子上一个写事务远超 30s，结果是一方
'database is locked' 报错，或者两边各拿一半进度互相覆盖水位。

锁文件用独立的 `<leaf>/.indexer.lock`，不是 index.db 本身：SQLite 自己持有
db 的文件锁，再对同一文件 flock 语义会与之纠缠。

用法：

    from utils.leaf_lock import leaf_lock, LeafLockBusy
    try:
        with leaf_lock(leaf_dir, timeout=5):
            ...
    except LeafLockBusy:
        ...  # 另一个进程正在写，跳过（不要重试，交给下一轮）
"""

import fcntl
import os
import time
from typing import Optional

LOCK_NAME = ".indexer.lock"


class LeafLockBusy(Exception):
    """在 timeout 内没拿到叶子的跨进程锁（另一个进程正在写它）。"""


class _LeafFileLock:
    """跨进程叶子写锁（fcntl.flock，非阻塞探测 + 超时）。"""

    def __init__(self, leaf_dir: str, timeout: float = 30.0):
        self.path = os.path.join(leaf_dir, LOCK_NAME)
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
                    raise LeafLockBusy(f"leaf locked: {self.path}")
                time.sleep(0.05)

    def __exit__(self, *exc):
        if self.fd is not None:
            try:
                fcntl.flock(self.fd, fcntl.LOCK_UN)
            finally:
                os.close(self.fd)
                self.fd = None
        return False


def leaf_lock(leaf_dir: str, timeout: float = 30.0) -> _LeafFileLock:
    """取叶子的跨进程写锁。拿不到抛 LeafLockBusy。"""
    return _LeafFileLock(leaf_dir, timeout=timeout)
