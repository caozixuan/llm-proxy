"""utils/eval/pool.py — 导出处理器的并行执行器选择。

背景：reformat / eval / reconstruct / jiuwen 这几条处理器链路的每个 task 都是
「读合并文件 → 多次 json.loads → 去重/回填 → json.dumps 落盘」。纯 Python 的
JSON 编解码**不释放 GIL**，所以 ThreadPoolExecutor 开再多线程也只吃一个核。

实测（reconstruct，同一批 60 个 session，page cache 已预热）：

    线程数  1     4     8     32
    墙钟   21.8  20.8  21.2  22.1 秒      cpu/wall 恒为 ~1.0

即线程数翻 32 倍，耗时纹丝不动。换 8 个进程后同一批降到 4.2 秒（≈5x），
产出文件逐字节一致。因此这里默认用进程池。

为什么不直接 fork：
    这些处理器是从 export_routes 的 dir_workers 线程池里调用的，此时父进程已有多条
    线程。直接 fork 会让子进程继承这些线程可能持有的锁（export_store / session_store
    的全局 Lock 等），有死锁风险。forkserver 从干净的单线程服务进程 fork，规避了
    这一点；同时不像 spawn 那样重新执行主模块（历史上 spawn 会重 import app.py 跑一遍
    模块级启动，刷屏 "leader_lock: another worker is leader"）。

可回退：EXPORT_PROCESS_POOL=0 强制退回线程池（排障 / 与旧行为对齐）。
进程池创建失败也自动退回线程池 —— 只是慢，不会让导出失败。
"""

from __future__ import annotations

import logging
import os
from typing import Tuple

logger = logging.getLogger(__name__)

# 设 0 / false 可全局退回线程池。
PROCESS_POOL_ENV = "EXPORT_PROCESS_POOL"

# 低于这个任务数时进程池不划算：建池 + 传参的固定开销会盖过并行收益，
# 而单任务的 JSON 解析在毫秒级。实测十几条以下线程池反而更快。
MIN_TASKS_FOR_PROCESS_POOL = 8


def process_pool_disabled() -> bool:
    return os.getenv(PROCESS_POOL_ENV, "1") in ("0", "false", "False")


def make_task_executor(workers: int, n_tasks: int = 0, label: str = ""):
    """为处理器创建并行执行器，返回 ``(executor, kind_label)``。

    workers 是「并行度」：进程池下每个进程同一时刻只跑一个 task，所以把并发降到
    task 数以下没有意义，取 ``min(workers, n_tasks)``。

    kind_label 供调用方写进进度日志，便于事后判断这一单到底跑在哪种池上。
    """
    from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

    workers = max(1, int(workers or 1))
    tag = f"{label}: " if label else ""

    if process_pool_disabled():
        return ThreadPoolExecutor(max_workers=workers), "线程池（按配置禁用进程池）"

    if n_tasks and n_tasks < MIN_TASKS_FOR_PROCESS_POOL:
        return (ThreadPoolExecutor(max_workers=workers),
                f"线程池（任务数 {n_tasks} < {MIN_TASKS_FOR_PROCESS_POOL}，进程池不划算）")

    try:
        import multiprocessing as mp

        procs = max(1, min(workers, n_tasks or workers))
        ctx = mp.get_context("forkserver")
        return ProcessPoolExecutor(max_workers=procs, mp_context=ctx), f"进程池({procs})"
    except Exception as exc:  # noqa: BLE001
        logger.warning("%s无法创建进程池（%s），退回线程池", tag, exc)
        return ThreadPoolExecutor(max_workers=workers), "线程池（进程池创建失败）"


def describe(executor_kind: str) -> str:
    """统一的日志文案，避免各处理器各写一套。"""
    return f"并行方式: {executor_kind}"
