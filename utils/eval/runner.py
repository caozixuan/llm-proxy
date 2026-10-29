"""
utils/eval/runner.py — 导出处理器的并行执行骨架

各处理器（reformat / reformat_full / reconstruct / reformat_jiuwen / jiuwenswarm /
分段导出）共用同一套执行模式：

    建任务 → 提交 → 完成一个收一个 → 查取消 → 调进度回调 → 累积结果

这段逻辑原本在 7 个文件里各写一遍。本模块把它收成 ``run_tasks``，与
``utils/eval/pool.py``（决定用进程池还是线程池）配合：pool 管「并行方式」，
runner 管「并行流程」。

两个设计要点：

1. **有界提交窗口**。一个大叶子可能有 20 万+ trace 文件，一次性 submit 全部 future
   会让「提交」阶段本身就耗掉几分钟，期间主循环一行进度都写不出去（worker 却在
   闷头产出），进度看起来像卡死，内存也会被进度信息撑高。这里只保持
   ``workers * window_factor`` 个在飞，完成一个补一个。

2. **worker_fn 必须是模块级函数**。进程池要 pickle 它，闭包/局部函数会失败。
   所有入参也应只含可 pickle 的基础类型。

并行度上限不在这里管：调用方传多少 worker 就用多少，保持各处理器原有设定不变。
"""

from __future__ import annotations

import logging
from concurrent.futures import FIRST_COMPLETED, wait
from typing import Any, Callable, Dict, List, Optional, Sequence

from utils.eval.pool import describe, make_task_executor

logger = logging.getLogger(__name__)

__all__ = ["RunResult", "run_tasks"]


class RunResult(dict):
    """返回结构，与各处理器历史上使用的 dict 兼容。"""


def run_tasks(
    tasks: Sequence[Any],
    worker_fn: Callable[[Any], Any],
    *,
    workers: int,
    label: str = "",
    progress_cb: Optional[Callable[[str], None]] = None,
    should_cancel: Optional[Callable[[], bool]] = None,
    on_task_done: Optional[Callable[[], None]] = None,
    window_factor: int = 4,
    progress_every: int = 100,
) -> RunResult:
    """并行执行 tasks，返回 ``{"results", "errors", "cancelled", "payload"}``。

    worker_fn(task) 的返回值放进 payload 列表（顺序不保证，与 as_completed 一致）。
    worker_fn 抛异常时记一条 ``"处理失败: <异常类型>"``，不影响其余任务。

    progress_every: 每完成这么多任务报一次进度；0 表示不报。
    取消是协作式的：一旦 should_cancel() 为真，停止收集、cancel_futures 丢弃
    尚未开始的任务，已在跑的任其自然收尾。
    """
    log = progress_cb or (lambda message: None)
    cancel = should_cancel or (lambda: False)

    result = RunResult(results=[], errors=[], cancelled=False, payload=[])
    n_tasks = len(tasks)
    if n_tasks == 0:
        return result

    if cancel():
        result["cancelled"] = True
        return result

    executor, pool_kind = make_task_executor(workers, n_tasks, label=label or "export")
    log(describe(pool_kind))

    window = max(8, max(1, int(workers)) * max(1, int(window_factor)))
    order = list(tasks)
    cursor = 0
    pending: set = set()
    done_count = 0

    def submit_one(item):
        try:
            return executor.submit(worker_fn, item)
        except Exception as exc:  # noqa: BLE001
            # 进程池崩坏（子进程被 OOM kill、forkserver 起不来等）时 submit 会抛
            # BrokenProcessPool。此时不能整批失败：记一条错误、停止提交，
            # 已提交的任务仍会被等待收尾，最终返回部分结果而不是抛异常。
            logger.warning("%s submit failed (%s): %s", label, type(exc).__name__, exc)
            result["errors"].append(f"进程池不可用: {type(exc).__name__}")
            result["pool_broken"] = True
            return None

    def add(item) -> bool:
        """提交一个任务；进程池不可用时返回 False 表示应停止提交。"""
        future = submit_one(item)
        if future is None:
            return False
        pending.add(future)
        return True

    try:
        while cursor < n_tasks and len(pending) < window:
            if not add(order[cursor]):
                break
            cursor += 1

        while pending:
            # wait 返回 (done, not_done)，两者都是集合；pending 随之变成 set。
            finished, pending = wait(pending, return_when=FIRST_COMPLETED)

            for future in finished:
                if cancel():
                    result["cancelled"] = True

                if result["cancelled"]:
                    continue

                try:
                    value = future.result()
                except Exception as exc:  # noqa: BLE001
                    logger.warning("%s task failed (%s)", label, type(exc).__name__)
                    result["errors"].append(f"处理失败: {type(exc).__name__}")
                else:
                    result["payload"].append(value)

                done_count += 1
                if on_task_done is not None:
                    try:
                        on_task_done()
                    except Exception:  # noqa: BLE001 进度回调失败不影响导出
                        pass
                if progress_every and done_count % progress_every == 0:
                    log(f"已完成 {done_count}/{n_tasks}")

                if not result["cancelled"] and cursor < n_tasks:
                    if not add(order[cursor]):
                        break
                    cursor += 1

            if result["cancelled"]:
                for future in pending:
                    future.cancel()
                break
    finally:
        executor.shutdown(wait=not result["cancelled"], cancel_futures=True)

    return result
