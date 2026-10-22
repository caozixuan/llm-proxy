"""
utils/eval/reconstruct.py — Hermes 轨迹重构导出（对齐 reformat 的并行框架）

对每个 session 的 trace_list 指向的 new-api 合并文件做 hermes 聚合重构：

  1) 按「最后一个 user 消息锚点」分组出前台/后台/恢复 run（last_user_anchor）；
  2) 组内去重精确重放、保留每个极大请求分支（select_branch_records），
     最后一条代理记录无条件保留；
  3) 从早期响应回填缺失的 reasoning_content（req / up_req 都修，
     response_registry + repair_nested_request）；
  4) 每个 session 输出重构后的合并文件 + _manifest.jsonl（可审计）。

复用 reformat_and_analyze 的「session 迭代并行」框架（线程池，IO 密集），
只是每 session 的处理从「只落盘 latest_file 一个文件」换成
「聚合该 session 的多个 trace 文件」。

注意：聚合器读的是 new-api 合并文件本身（trace.filename 是 basename，
Path(src_dir)/filename 定位，对齐 newapi_consumer._resolve_combined_path），
不依赖 trace_list 携带 req/resp 内容；native 三元组叶子没有合并文件，
reconstruct 不支持（上层应跳过非 new-api 目录）。
"""

from __future__ import annotations

import copy
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from src.export.hermes_traj import (
    atomic_write_json,
    atomic_write_text,
    manifest_json_lines,
    read_record,
    repair_nested_request,
    response_registry,
    select_branch_records,
)

logger = logging.getLogger(__name__)


def _resolve_trace_paths(src_dir: Path, trace_list: Any) -> List[Path]:
    """把 trace_list 里的 filename（basename）定位到叶子目录下的合并文件。"""
    paths: List[Path] = []
    if not isinstance(trace_list, list):
        return paths
    for trace in trace_list:
        if not isinstance(trace, dict):
            continue
        filename = trace.get("filename", "")
        if not isinstance(filename, str) or not filename.endswith(".json"):
            continue
        path = src_dir / filename
        if path.is_file():
            paths.append(path)
    return paths


def _reconstruct_result_record(
    src_dir: Path,
    first_ts: str,
    trace_list: Any,
    api_key: str,
    models: Any,
    run_groups: Any,
    selected_count: int,
    req_filled: int,
    up_req_filled: int,
    written_files: List[str],
    log_dir: Optional[str],
) -> Dict[str, Any]:
    """统一 schema 记录：键集合对齐 _reformat_only_record（下游无需判 key 存在性）
    + hermes 聚合特有字段（run/选分支/回填统计）。"""
    from utils.eval.eval import _parse_folder_ts
    start_ts = _parse_folder_ts(first_ts) if first_ts else None
    models_list = list(models) if models else []
    last_ts = ""
    if isinstance(trace_list, list) and trace_list:
        last_trace = trace_list[-1]
        if isinstance(last_trace, dict):
            last_ts = last_trace.get("ts", "") or ""
    return {
        "session": first_ts,
        "start_time": start_ts.strftime("%Y-%m-%d %H:%M:%S") if start_ts else None,
        "end_time": None,
        "duration_s": None,
        "api_call_count": len(trace_list) if isinstance(trace_list, list) else 0,
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
        "model": models_list[0] if models_list else "",
        "q1": "",
        "latest_file": written_files[-1] if written_files else "",
        "log_dir": log_dir or Path(src_dir).name,
        "tool_use_detail": {},
        "tool_success_detail": {},
        "tool_fail_detail": {},
        "skills_used": {},
        "api_key": api_key,
        "models": models_list,
        "trace_list": trace_list or [],
        "_key": first_ts,
        "first_ts": first_ts,
        "last_ts": last_ts,
        "msg_count": 0,
        "completed": 0,
        "completed_note": "",
        # hermes 聚合特有
        "hermes_run_count": len(run_groups),
        "hermes_selected_count": selected_count,
        "hermes_req_filled": req_filled,
        "hermes_up_req_filled": up_req_filled,
        "hermes_output_files": written_files,
    }


def _process_one_hermes(args: tuple) -> Optional[dict]:
    """单个 session 的 hermes 聚合重构（线程池 worker 可调用）。

    args: (src_dir_s, out_dir_s, first_ts, trace_list, api_key, models, log_dir)

    无 trace 文件或任一合并文件损坏时返回 None（严格失败，与
    hermes_traj.load_records 的 fail-fast 一致，避免静默产出不完整数据）。
    """
    src_dir_s, out_dir_s, first_ts, trace_list, api_key, models, log_dir = args

    src_dir = Path(src_dir_s)
    out_dir = Path(out_dir_s)

    paths = _resolve_trace_paths(src_dir, trace_list)
    if not paths:
        return None

    records = []
    for index, path in enumerate(paths):
        try:
            records.append(read_record(path, index))
        except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
            logger.warning("hermes reconstruct: cannot parse %s: %s", path, exc)
            return None

    run_groups, selected = select_branch_records(records)

    session_dir = out_dir / (first_ts or "session")
    session_dir.mkdir(parents=True, exist_ok=True)

    req_filled = 0
    up_req_filled = 0
    repair_counts: Dict[int, tuple] = {}
    written_files: List[str] = []
    for index in sorted(selected):
        record = records[index]
        output = copy.deepcopy(record.outer)
        registry = response_registry(records, before_index=index)
        req_count = repair_nested_request(output, "req", registry)
        up_req_count = repair_nested_request(output, "up_req", registry)
        req_filled += req_count
        up_req_filled += up_req_count
        repair_counts[index] = (req_count, up_req_count)
        atomic_write_json(session_dir / record.path.name, output)
        written_files.append(record.path.name)

    atomic_write_text(
        session_dir / "_manifest.jsonl",
        manifest_json_lines(src_dir, records, run_groups, repair_counts),
    )

    return _reconstruct_result_record(
        src_dir, first_ts, trace_list, api_key, models,
        run_groups, len(selected), req_filled, up_req_filled,
        written_files, log_dir,
    )


from utils.eval.pool import describe as _describe_pool, make_task_executor


def reconstruct_and_export(
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
    """
    线程池并行: 对每个 session 的 trace_list 做 hermes 聚合重构。

    与 reformat_and_analyze 同签名（供上层按 mode 切换处理器，无需改调用处）：

    workers: 线程数，硬上限 32（IO 密集，与 reformat 一致）。
    log_dir: 写入每个 result 的历史目录标识（相对 root 的 posix 路径，
        如 "260728/26072813"）；缺省退回 Path(src_dir).name。
    should_cancel: 协作式取消回调（同 reformat_and_analyze），返回 True 表示用户已终止。
        drain 循环里逐个结果检查，取消即停止收集并 cancel_futures 丢弃未开始任务。

    Returns:
        {"total_sessions": N, "total_files": M, "errors": [...], "results": [...], "cancelled": bool}
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

    # 建 tasks 前先看一眼取消：用户在排队/前一目录期间已终止时，连聚合都不必开始。
    if _should_cancel():
        _log("检测到取消，跳过本目录重构")
        return {"total_sessions": total, "total_files": 0, "errors": [], "results": [], "cancelled": True}

    tasks = []
    for sess in entries:
        first_ts = sess.get("first_ts") or sess.get("_key", "")
        trace_list = sess.get("trace_list", [])
        s_api_key = sess.get("api_key", "")
        s_models = sess.get("models", []) or []
        if not first_ts or not trace_list:
            continue
        tasks.append((src_dir, out_dir, first_ts, trace_list,
                      s_api_key, s_models, resolved_log_dir))

    results: List[dict] = []
    errors: List[str] = []
    n_tasks = len(tasks)
    if n_tasks == 0:
        return {"total_sessions": total, "total_files": 0,
                "errors": errors, "results": results, "cancelled": False}

    _log(f"进入 hermes 重构: {n_tasks} sessions, workers={workers}...")
    # 并行方式：进程池优先（GIL 决定线程池在这里只能吃一个核，见 utils/eval/pool.py），
    # 线程池兜底 —— 低任务数 / 显式禁用 / 建池失败时都退回线程池，行为与改造前一致。
    executor, _pool_kind = make_task_executor(workers, n_tasks, label="reconstruct")
    _log(_describe_pool(_pool_kind))
    cancelled = False
    try:
        futures = [executor.submit(_process_one_hermes, t) for t in tasks]
        for future in as_completed(futures):
            # 协作式取消：每收到一个结果查一次 DB 状态（上层已节流）。取消则停止收集。
            if _should_cancel():
                cancelled = True
                _log("检测到取消，停止重构剩余 session")
                break
            try:
                result = future.result()
                if result:
                    # worker 内 log_dir=Path(src_dir).name 会丢失日期层，统一覆盖
                    result["log_dir"] = resolved_log_dir
                    results.append(result)
                else:
                    errors.append("处理失败")
            except Exception as e:  # noqa: BLE001
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

    return {"total_sessions": total, "total_files": len(results),
            "errors": errors, "results": results, "cancelled": cancelled}
