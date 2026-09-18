"""
utils/eval/reformat_jiuwen.py — Openjiuwen 轨迹重构导出(reasoning_content 回填)

对每个 session 的 trace_list 指向的文件做 openjiuwen 回填重构：

  1. 把 trace_list 的每个文件（三元组 / new-api 合并单文件）用 reformat 的
     加载层合并成单 JSON（含 messages + response.content），得到内存 documents；
  2. 调用 src.export.jiuwen_traj.process_documents 对 documents 做核心重构：
     从早期响应索引 reasoning 块，按 content identity 匹配最终快照中的
     assistant 消息，回填缺失的 reasoning_content（thinking/reasoning/redacted_thinking）；
  3. process_documents 输出重构后的合并文件到 out_dir/<first_ts>/<last_stem>.json。

全程内存端到端：_load_merged 合并出 dict 列表后直接喂给 process_documents，
不落临时文件，避免 write→read→write 的多余 IO。

复用 reformat_and_analyze 的「session 迭代并行」框架（线程池，IO 密集），
与 reconstruct.py / reformat_full.py 同签名，可直接接入
tools/offline_reformat_export.py 的 _run_one_export 框架（processor(**kwargs)）。

与 hermes reconstruct 的区别：
  - jiuwen 重构不限定 new-api 格式——只要 trace 文件能被 _load_merged 合并成
    含 messages + response.content 的单 JSON，三元组和 new-api 两种格式都支持；
  - 核心逻辑（content identity 匹配 + reasoning gap 回填）全部在 jiuwen_traj 库中，
    本模块只负责「合并 trace → 调 process_documents → 收尾」。
"""

from __future__ import annotations

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from utils.eval.reformat import _load_merged, _resolve_triplet
from utils.session_store import _fmt_ts
from src.export.jiuwen_traj import GroupStats, TrajectoryError, process_documents

logger = logging.getLogger(__name__)


def _stem_of(filename: str) -> str:
    """去掉 trace 文件名后缀，得到用于落盘的 stem。

    native 三元组：<stem>-req.json → <stem>；new-api 合并单文件：<stem>.json → <stem>。
    """
    if filename.endswith("-req.json"):
        return filename[: -len("-req.json")]
    if filename.endswith(".json"):
        return filename[: -len(".json")]
    return filename


def _resolve_trace_filenames(sess: dict) -> List[str]:
    """收集 session 的全部 trace 文件名；trace_list 为空时回退 latest_file。"""
    trace_list = sess.get("trace_list") or []
    filenames = [
        t.get("filename", "")
        for t in trace_list
        if isinstance(t, dict) and t.get("filename")
    ]
    if not filenames:
        latest = sess.get("latest_file", "")
        if latest:
            filenames = [latest]
    return filenames


def _jiuwen_result_record(
    sess: dict,
    log_dir: Optional[str],
    stats: Optional[GroupStats],
    src_dir: Path,
) -> Dict[str, Any]:
    """统一 schema 记录：键集合对齐 _reformat_only_record（下游无需判 key 存在性）
    + jiuwen 重构特有字段（回填统计）。"""
    first_ts = sess.get("first_ts") or sess.get("_key", "")
    last_ts = sess.get("last_ts", "")
    api_key = sess.get("api_key", "")
    models = sess.get("models", []) or []
    trace_list = sess.get("trace_list", []) or []
    latest_file = sess.get("latest_file", "")

    restored_blocks = stats.restored_blocks if stats else 0
    restored_characters = stats.restored_characters if stats else 0
    matched_messages = stats.matched_messages if stats else 0
    changed_messages = stats.changed_messages if stats else 0

    return {
        "session": first_ts,
        "start_time": _fmt_ts(first_ts),
        "end_time": _fmt_ts(last_ts),
        "duration_s": None,
        "api_call_count": len(trace_list) if trace_list else 1,
        "api_errors": 0,
        "user_turns": 0,
        "total_messages": 0,
        "tool_use_count": 0,
        "tool_result_count": 0,
        "tool_success": 0,
        "tool_fail_flag": 0,
        "tool_fail_keyword": 0,
        "tool_fail_total": 0,
        "tool_success_rate": None,
        "model": models[0] if models else "",
        "q1": "",
        "latest_file": latest_file,
        "log_dir": log_dir or Path(src_dir).name,
        "tool_use_detail": {},
        "tool_success_detail": {},
        "tool_fail_detail": {},
        "skills_used": {},
        "api_key": api_key,
        "models": models,
        "trace_list": trace_list,
        "_key": first_ts,
        "first_ts": first_ts,
        "last_ts": last_ts,
        "msg_count": 0,
        "completed": 0,
        "completed_note": "",
        # jiuwen 重构特有
        "jiuwen_input_files": stats.input_files if stats else 0,
        "jiuwen_matched_messages": matched_messages,
        "jiuwen_changed_messages": changed_messages,
        "jiuwen_restored_blocks": restored_blocks,
        "jiuwen_restored_characters": restored_characters,
    }


def _process_one_jiuwen(args: tuple) -> Optional[dict]:
    """单个 session 的 jiuwen 轨迹重构（线程池 worker 可调用）。

    args: (src_dir_s, out_dir_s, sess, log_dir)

    流程：
      1. 把 trace_list 指向的每个文件用 _load_merged 合并成内存 dict
         （按 stem 排序，与 process_group 的文件名排序语义一致，确保末文件是最终快照）；
      2. 调用 jiuwen_traj.process_documents 做 reasoning_content 回填重构，
         直接输出到 out_dir/<first_ts>/<last_stem>.json（端到端，无临时文件）。

    无 trace 文件或全部合并失败时返回 None（严格失败，避免静默产出不完整数据）。
    """
    src_dir_s, out_dir_s, sess, log_dir = args
    src_dir = Path(src_dir_s)
    out_dir = Path(out_dir_s)

    first_ts = sess.get("first_ts") or sess.get("_key", "")
    if not first_ts:
        first_ts = "session"

    filenames = _resolve_trace_filenames(sess)
    if not filenames:
        return None

    # 按 stem 排序：与 process_group 的 sorted(glob, key=name) 语义一致，
    # 确保 documents[-1] 是时间最晚的最终快照。
    sorted_filenames = sorted(filenames, key=lambda fn: _stem_of(fn))

    documents: List[dict] = []
    last_stem = ""
    for fn in sorted_filenames:
        stem = _stem_of(fn)
        tri = _resolve_triplet(src_dir, stem)
        merged = _load_merged(src_dir, stem, fn, tri)
        if merged is None:
            logger.warning("jiuwen reformat: cannot merge %s/%s", src_dir, fn)
            continue
        documents.append(merged)
        last_stem = stem

    if not documents:
        return None

    # 端到端：内存 documents → process_documents → 写最终文件
    output_path = out_dir / first_ts / f"{last_stem}.json"
    try:
        stats = process_documents(
            documents,
            output_path=output_path,
            group_name=first_ts,
            selected_name=f"{last_stem}.json",
        )
    except TrajectoryError as exc:
        logger.warning("jiuwen reformat: trajectory error for session %s: %s", first_ts, exc)
        return None

    return _jiuwen_result_record(sess, log_dir, stats, src_dir)


def jiuwen_reformat_export(
    src_dir: str,
    out_dir: str,
    session_entries: List[dict],
    api_key: Optional[str] = None,
    workers: int = 4,
    progress_cb: Optional[Callable[[str], None]] = None,
    log_dir: Optional[str] = None,
    should_cancel: Optional[Callable[[], bool]] = None,
) -> dict:
    """线程池并行：对每个 session 的 trace_list 做 jiuwen 轨迹重构。

    与 reformat_and_analyze / reconstruct_and_export / full_reformat_export
    同签名（供上层按 mode 切换处理器，无需改调用处）：

    workers: 线程数，硬上限 32（IO 密集，与 reformat 一致）。
    log_dir: 写入每个 result 的历史目录标识（相对 root 的 posix 路径，
        如 "260728/26072813"）；缺省退回 Path(src_dir).name。
    should_cancel: 协作式取消回调（同 reformat_and_analyze），返回 True 表示用户已终止。
        drain 循环里逐个结果检查，取消即停止收集并 cancel_futures 丢弃未开始任务。

    Returns:
        {"total_sessions": N, "total_files": M, "errors": [...], "results": [...], "cancelled": bool}
        其中 total_files = 实际重构成功的 session 数（每 session 输出一个合并文件）。
    """
    _log = progress_cb or (lambda msg: None)
    _should_cancel = should_cancel or (lambda: False)
    workers = max(1, min(int(workers), 32))  # 线程数硬上限 32
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    resolved_log_dir = log_dir if log_dir is not None else Path(src_dir).name

    entries = session_entries
    if api_key:
        entries = [s for s in entries if s.get("api_key") == api_key]

    total = len(entries)
    if total == 0:
        return {"total_sessions": 0, "total_files": 0, "errors": [], "results": [], "cancelled": False}

    # 建 tasks 前先看一眼取消：用户在排队/前一目录期间已终止时，连重构都不必开始。
    if _should_cancel():
        _log("检测到取消，跳过本目录 jiuwen 重构")
        return {"total_sessions": total, "total_files": 0, "errors": [], "results": [], "cancelled": True}

    tasks = []
    for sess in entries:
        first_ts = sess.get("first_ts") or sess.get("_key", "")
        if not first_ts:
            continue
        tasks.append((src_dir, out_dir, sess, resolved_log_dir))

    results: List[dict] = []
    errors: List[str] = []
    n_tasks = len(tasks)
    if n_tasks == 0:
        return {"total_sessions": total, "total_files": 0, "errors": errors, "results": results, "cancelled": False}

    _log(f"进入 jiuwen 重构: {n_tasks} sessions, workers={workers}...")
    # 线程池并行（而非进程池）：纯 IO + 轻量聚合，理由同 reformat.py 注释
    # （spawn 会重 import app.py 跑模块级启动、worker 挂死变孤儿堆积等历史顽疾）。
    executor = ThreadPoolExecutor(max_workers=workers)
    cancelled = False
    try:
        futures = [executor.submit(_process_one_jiuwen, t) for t in tasks]
        for future in as_completed(futures):
            # 协作式取消：每收到一个结果查一次 DB 状态（上层已节流）。取消则停止收集。
            if _should_cancel():
                cancelled = True
                _log("检测到取消，停止 jiuwen 重构剩余 session")
                break
            try:
                result = future.result()
                if result:
                    # worker 内 log_dir 已由 resolved_log_dir 传入，无需再覆盖
                    results.append(result)
                else:
                    errors.append("处理失败")
            except Exception as e:  # noqa: BLE001
                errors.append(str(e))
    finally:
        # cancel_futures=True：丢弃线程池里尚未开始的任务（已在跑的自然收尾）。
        executor.shutdown(wait=True, cancel_futures=True)

    return {
        "total_sessions": total,
        "total_files": len(results),
        "errors": errors,
        "results": results,
        "cancelled": cancelled,
    }
