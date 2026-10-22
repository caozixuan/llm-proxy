"""utils/timing.py — 轻量分段耗时测量。

导出执行链（utils/export_routes.py）与 /export/view 读链路（utils/log_routes.py）
都需要回答「这一单到底慢在哪个阶段」。这里只提供打点工具，不做任何 IO ——
调用方决定把结果写进 progress_log 还是模块日志。

用 perf_counter()（单调钟），避免系统时间调整把耗时算成负数。

    t = StageTimer()
    with t.stage("索引"):
        ...
    with t.stage("上传"):
        ...
    logger.info(t.summary_line("导出耗时", total=True))

同一阶段名可以被多次 stage() 进入（多目录/多叶子循环），耗时自动累加。
"""

import threading
import time
from contextlib import contextmanager
from typing import Dict, List, Optional, Tuple

__all__ = ["StageTimer", "fmt_s", "summary_line"]


def fmt_s(sec: float) -> str:
    """0.234 → '0.23s'；83.4 → '1m23.4s'；3885 → '1h4m45s'。"""
    try:
        sec = float(sec)
    except (TypeError, ValueError):
        return "?"
    if sec < 0:
        sec = 0.0
    if sec < 60:
        return f"{sec:.2f}s"
    if sec < 3600:
        m, s = divmod(sec, 60)
        return f"{int(m)}m{s:.1f}s"
    h, rem = divmod(sec, 3600)
    m, s = divmod(rem, 60)
    return f"{int(h)}h{int(m)}m{int(s)}s"


def summary_line(label: str, parts: List[Tuple[str, float]], total: Optional[float] = None,
                 note: str = "") -> str:
    """拼一行可读汇总：`标签 总耗时 X = a 1s + b 2s + 其它 0.3s`。

    total 给定时，分项之和与它的差额记为「其它」；未给定则取分项之和。
    note 原样追加在末尾（如「含 dir_workers=8 并行」）。
    """
    shown = [(n, v) for n, v in parts if v is not None]
    acc = sum(v for _, v in shown)
    tail = ""
    if total is not None:
        rest = total - acc
        # 并行阶段之和可能大于墙钟总耗时，此时不写「其它」，避免出现负数
        if rest > 0.005:
            shown = shown + [("其它", rest)]
        tail = f"；总耗时 {fmt_s(total)}"
    body = " + ".join(f"{n} {fmt_s(v)}" for n, v in shown) or "无"
    line = f"[{label}] {body}{tail}"
    if note:
        line += f"（{note}）"
    return line


class StageTimer:
    """按阶段累计墙钟耗时。

    with t.stage("名字"): ...      # 可重复进入同名阶段，耗时累加
    t.add("名字", 1.23)            # 手工补一段（如已在别处测好）
    t.parts()                      # [("名字", 秒), ...]，按首次出现顺序
    """

    def __init__(self) -> None:
        self._t0 = time.perf_counter()
        self._acc: Dict[str, float] = {}
        self._order: List[str] = []
        self._lock = threading.Lock()

    @contextmanager
    def stage(self, name: str):
        start = time.perf_counter()
        try:
            yield
        finally:
            self.add(name, time.perf_counter() - start)

    def add(self, name: str, seconds: float) -> None:
        sec = max(0.0, float(seconds))
        with self._lock:
            if name not in self._acc:
                self._acc[name] = 0.0
                self._order.append(name)
            self._acc[name] += sec

    def get(self, name: str) -> float:
        with self._lock:
            return self._acc.get(name, 0.0)

    def elapsed(self) -> float:
        """从构造到现在的墙钟秒数。"""
        return time.perf_counter() - self._t0

    def parts(self) -> List[Tuple[str, float]]:
        with self._lock:
            return [(n, self._acc[n]) for n in self._order]

    def __enter__(self) -> "StageTimer":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def summary_line(self, label: str = "耗时", note: str = "") -> str:
        return summary_line(label, self.parts(), total=self.elapsed(), note=note)
