"""
utils/eval/reformat_full.py — 全量合并导出（无质检 / 无 analyze）

与 utils/eval/reformat.py 的 reformat-only 相比，本模块把每个 session 的
**全部 trace 文件**（trace_list 指向的每一次 API 调用）都合并落盘，而不是只处理
latest_file 一个。用于导出「特定 key 的某些 session 的全量文件」。

刻意保持纯粹：只做「读三元组 / new-api 合并单文件 → 合并成单 JSON → 落盘」，
不 import utils.eval.eval，不跑 analyze / 质检，不写 _eval 缓存。仅复用
reformat.py 的加载层纯函数（_resolve_triplet / _load_merged）与
session_store._fmt_ts（时间戳格式化），避免重复实现。

落盘布局与 reformat 一致：out_dir/<first_ts>/<stem>.json，同一 session 的多个
trace 各写一个文件，同名天然去重（同一 stem 的重复 trace 只保留一份）。

入口 full_reformat_export 与 reformat_and_analyze 的调用约定兼容，可直接接入
tools/offline_reformat_export.py 的 _run_one_export 框架（processor(**kwargs)）。
"""

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from utils.eval.pool import describe as _describe_pool, make_task_executor
from utils.eval.reformat import _load_merged, _resolve_triplet
from utils.session_store import _fmt_ts

logger = logging.getLogger(__name__)


def _stem_of(filename: str) -> str:
    """去掉 trace 文件名后缀，得到用于落盘的 stem。

    native 三元组：<stem>-req.json → <stem>；new-api 合并单文件：<stem>.json → <stem>。
    """
    if filename.endswith("-req.json"):
        return filename[:-len("-req.json")]
    if filename.endswith(".json"):
        return filename[:-len(".json")]
    return filename


def _write_one_trace(src_dir_s: str, out_dir_s: str, first_ts: str,
                     filename: str) -> Optional[str]:
    """合并单个 trace 文件并落盘。返回写出的 stem；无法加载返回 None。

    以 trace 的 filename 当作 reformat 的 latest_file 走 _load_merged
    （它按 stem 解析三元组或回退 new-api 合并单文件），落到
    out_dir/<first_ts>/<stem>.json（与 reformat 布局一致）。
    """
    if not filename:
        return None
    src_dir = Path(src_dir_s)
    out_dir = Path(out_dir_s)
    stem = _stem_of(filename)

    tri = _resolve_triplet(src_dir, stem)
    merged = _load_merged(src_dir, stem, filename, tri)
    if merged is None:
        return None

    session_dir = out_dir / first_ts
    session_dir.mkdir(parents=True, exist_ok=True)
    out_file = session_dir / f"{stem}.json"
    with open(out_file, "w", encoding="utf-8") as fh:
        json.dump(merged, fh, ensure_ascii=False, separators=(",", ":"))
    return stem


def _process_session(args: tuple) -> Dict[str, Any]:
    """处理单个 session 的全部 trace 文件，返回精简结果记录。

    args = (src_dir, out_dir, session_entry)。遍历 trace_list 逐个合并落盘；
    trace_list 为空时回退处理 latest_file 单文件。结果记录以 session 为单位，
    不含任何评估字段（无 analyze），供上层写 session_index.jsonl / 统计。
    """
    src_dir_s, out_dir_s, sess = args
    first_ts = sess.get("first_ts") or sess.get("_key", "")
    last_ts = sess.get("last_ts", "")
    api_key = sess.get("api_key", "")
    models = sess.get("models", []) or []
    latest_file = sess.get("latest_file", "")
    trace_list = sess.get("trace_list", []) or []

    # 收集要落盘的文件名：优先 trace_list 全量，空则回退 latest_file
    filenames = [t.get("filename", "") for t in trace_list if t.get("filename")]
    if not filenames and latest_file:
        filenames = [latest_file]

    written = 0
    for fn in filenames:
        try:
            if _write_one_trace(src_dir_s, out_dir_s, first_ts, fn):
                written += 1
        except Exception as e:
            logger.warning("full_reformat: write trace %s failed: %s", fn, e)

    return {
        "session": first_ts,
        "start_time": _fmt_ts(first_ts),
        "end_time": _fmt_ts(last_ts),
        "api_call_count": len(trace_list) if trace_list else 1,
        "latest_file": latest_file,
        "api_key": api_key,
        "models": models,
        "trace_list": trace_list,
        "_key": first_ts,
        "first_ts": first_ts,
        "last_ts": last_ts,
        "msg_count": sess.get("msg_count", 0),
        "_files_written": written,
    }


def full_reformat_export(
    src_dir: str,
    out_dir: str,
    session_entries: List[dict],
    api_key: Optional[str] = None,
    workers: int = 4,
    progress_cb: Optional[Callable[[str], None]] = None,
    log_dir: Optional[str] = None,
    should_cancel: Optional[Callable[[], bool]] = None,
    on_session_done: Optional[Callable[[], None]] = None,
) -> dict:
    """线程池并行：对每个 session 的 **全部 trace 文件** 做合并落盘（无 analyze）。

    与 reformat.reformat_and_analyze 调用约定兼容（同前若干形参 + 同返回 schema），
    但每个 session 落盘的是 trace_list 指向的全量文件，而非只有 latest_file。

    log_dir: 写入每个 result 的历史目录标识（相对 root 的 posix 路径）；缺省退回
        Path(src_dir).name。
    should_cancel: 协作式取消回调（同 reformat_and_analyze），取消即停止收集并
        cancel_futures 丢弃未开始任务。

    Returns:
        {"total_sessions": N, "total_files": M, "errors": [...], "results": [rec, ...], "cancelled": bool}
        其中 total_files = 实际写出的文件总数（各 session trace 文件数之和）。
    """
    _log = progress_cb or (lambda msg: None)
    _should_cancel = should_cancel or (lambda: False)
    workers = max(1, min(int(workers), 32))  # 线程数硬上限 32（同 reformat）
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    resolved_log_dir = log_dir if log_dir is not None else Path(src_dir).name

    entries = session_entries
    if api_key:
        entries = [s for s in entries if s.get("api_key") == api_key]

    total = len(entries)
    if total == 0:
        return {"total_sessions": 0, "total_files": 0, "errors": [], "results": [], "cancelled": False}

    # 建 tasks 前先看一眼取消：用户已终止时连合并都不必开始。
    if _should_cancel():
        _log("检测到取消，跳过本目录全量导出")
        return {"total_sessions": total, "total_files": 0, "errors": [], "results": [], "cancelled": True}

    tasks = []
    for sess in entries:
        first_ts = sess.get("first_ts") or sess.get("_key", "")
        if not first_ts:
            continue
        tasks.append((src_dir, out_dir, sess))

    results: List[dict] = []
    errors: List[str] = []
    total_files = 0

    # 并行方式：进程池优先，线程池兜底（低任务数 / 显式禁用 / 建池失败）。
    # 全量导出要把单个 mtime 下所有 trace 合并落盘，JSON 编解码不释放 GIL，
    # 线程池只能吃一个核（实测见 utils/eval/pool.py）。
    executor, _pool_kind = make_task_executor(workers, len(tasks), label="full_reformat")
    _log(_describe_pool(_pool_kind))
    cancelled = False
    try:
        futures = [executor.submit(_process_session, t) for t in tasks]
        for future in as_completed(futures):
            # 协作式取消：每收到一个结果查一次 DB 状态（上层已节流）。取消则停止收集。
            if _should_cancel():
                cancelled = True
                _log("检测到取消，停止全量导出剩余 session")
                break
            try:
                result = future.result()
                if result:
                    result["log_dir"] = resolved_log_dir
                    total_files += result.pop("_files_written", 0)
                    results.append(result)
                else:
                    errors.append("处理失败")
            except Exception as e:
                errors.append(str(e))
            finally:
                if on_session_done is not None:
                    try:
                        on_session_done()
                    except Exception:  # noqa: BLE001  进度回调失败不影响导出
                        pass
    finally:
        # cancel_futures=True：丢弃线程池里尚未开始的任务（已在跑的自然收尾）。
        executor.shutdown(wait=True, cancel_futures=True)

    return {
        "total_sessions": total,
        "total_files": total_files,
        "errors": errors,
        "results": results,
        "cancelled": cancelled,
    }
