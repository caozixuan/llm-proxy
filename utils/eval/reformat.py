"""
utils/eval/reformat.py — 格式重整 + 质检一体化

将三元组文件（req/headers/res）合并为单个 JSON，只处理 latest_file。
合并后立即跑 analyze_best_data，返回分析结果，省掉二次读取。
使用线程池并行处理（IO 密集 + 轻量本地质检，无需多进程）。
"""

import json
import logging
import os
from utils.atomic_write import safe_replace
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from utils.eval.eval import analyze_best_data, fmt_quality
from utils.eval.pool import describe as _describe_pool, make_task_executor
from utils.message_common import parse_response

logger = logging.getLogger(__name__)


def _load_json(path: Path) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _resolve_triplet(src_dir: Path, stem: str) -> Dict[str, Path]:
    tri: Dict[str, Path] = {}
    for suffix, key in [("-req.json", "req"), ("-headers.json", "headers"), ("-res.json", "res")]:
        p = src_dir / f"{stem}{suffix}"
        if p.is_file():
            tri[key] = p
    return tri


def _load_merged(src_dir: Path, stem: str, latest_file: str,
                 tri: Dict[str, Path]) -> Optional[dict]:
    """加载并组装 merged（messages + response + header）。

    优先三元组（本项目格式）；找不到 req 时回退 new-api 合并单文件
    （latest_file 是 .json 但非 -req.json），用 newapi_format 拆解。
    返回 None 表示无法加载。
    """
    if "req" in tri:
        try:
            merged = _load_json(tri["req"])
        except Exception:
            return None
        if "headers" in tri:
            try:
                merged["header"] = _load_json(tri["headers"])
            except Exception:
                merged["header"] = {}
        else:
            merged["header"] = {}
        if "res" in tri:
            try:
                merged["response"] = parse_response(_load_json(tri["res"]))
            except Exception:
                merged["response"] = {}
        else:
            merged["response"] = {}
        return merged

    # new-api：合并单文件
    if latest_file.endswith(".json") and not latest_file.endswith("-req.json"):
        combined = src_dir / latest_file
        if combined.is_file():
            try:
                from utils.newapi_format import build_merged_for_eval
                return build_merged_for_eval(combined)
            except Exception:
                return None
    return None


def _reformat_only_record(src_dir_s, first_ts, latest_file, stem,
                          trace_list, api_key, models) -> dict:
    """reformat-only：只合并落盘，不跑 analyze。

    返回统一 schema 记录，但所有评估字段用零值/空占位（键集合与
    _process_one 对齐，下游读取方无需判 key 存在性）。
    """
    from utils.eval.eval import _parse_folder_ts
    start_ts = _parse_folder_ts(first_ts)
    end_ts = _parse_folder_ts(stem)
    models_list = list(models) if models else []
    return {
        "session": first_ts,
        "start_time": start_ts.strftime("%Y-%m-%d %H:%M:%S") if start_ts else None,
        "end_time": end_ts.strftime("%Y-%m-%d %H:%M:%S") if end_ts else None,
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
        "model": models_list[0] if models_list else "",
        "q1": "",
        "latest_file": latest_file,
        "log_dir": Path(src_dir_s).name,
        "tool_use_detail": {},
        "tool_success_detail": {},
        "tool_fail_detail": {},
        "skills_used": {},
        "api_key": api_key,
        "models": models_list,
        "trace_list": trace_list or [],
        "_key": first_ts,
        "first_ts": first_ts,
        "last_ts": stem,
        "msg_count": 0,
        "completed": 0,
        "completed_note": "",
    }


def _process_one(args: tuple) -> Optional[dict]:
    """单个 session 的 reformat (+ 可选 analyze)，多进程可调用。

    args 末位 analyze=False 时只合并落盘、跳过 analyze（评估字段占位）。
    返回结果 dict（含 session/start_time 等字段），失败返回 None。
    """
    src_dir_s, out_dir_s, first_ts, latest_file, trace_list, api_key, models, analyze = args

    src_dir = Path(src_dir_s)
    out_dir = Path(out_dir_s)

    stem = latest_file
    if stem.endswith("-req.json"):
        stem = stem[:-len("-req.json")]
    elif stem.endswith(".json"):
        stem = stem[:-len(".json")]

    tri = _resolve_triplet(src_dir, stem)
    merged = _load_merged(src_dir, stem, latest_file, tri)
    if merged is None:
        return None

    session_dir = out_dir / first_ts
    session_dir.mkdir(parents=True, exist_ok=True)
    out_file = session_dir / f"{stem}.json"
    with open(out_file, "w", encoding="utf-8") as fh:
        json.dump(merged, fh, ensure_ascii=False, separators=(",", ":"))

    if not analyze:
        return _reformat_only_record(src_dir_s, first_ts, latest_file, stem,
                                     trace_list, api_key, models)

    # 直接分析，不再二次读取
    analyzed = analyze_best_data(merged)

    resp = merged.get("response") or {}
    api_errors = 0
    if isinstance(resp, dict) and isinstance(resp.get("status_code"), int):
        if resp["status_code"] >= 400:
            api_errors = 1

    api_call_count = len(trace_list) if trace_list else 1

    from utils.eval.eval import _parse_folder_ts
    start_ts = _parse_folder_ts(first_ts)
    end_ts = _parse_folder_ts(stem)
    duration_s = None
    if start_ts and end_ts and end_ts >= start_ts:
        duration_s = (end_ts - start_ts).total_seconds()

    # models 列表优先取传入值，退回 merged 的单值
    models_list = list(models) if models else ([merged.get("model")] if merged.get("model") else [])

    return {
        "session": first_ts,
        "start_time": start_ts.strftime("%Y-%m-%d %H:%M:%S") if start_ts else None,
        "end_time": end_ts.strftime("%Y-%m-%d %H:%M:%S") if end_ts else None,
        "duration_s": duration_s,
        "api_call_count": api_call_count,
        "api_errors": api_errors,
        "user_turns": analyzed["user_turns"],
        "total_messages": analyzed["total_messages"],
        "tool_use_count": analyzed["tool_use_count"],
        "tool_result_count": analyzed["tool_result_count"],
        "tool_success": analyzed["tool_success"],
        "tool_fail_flag": analyzed["tool_fail_flag"],
        "tool_fail_keyword": analyzed["tool_fail_keyword"],
        "tool_fail_total": analyzed["tool_fail_total"],
        "tool_success_rate": analyzed["tool_success_rate"],
        "model": merged.get("model", ""),
        "q1": analyzed["q1"],
        "latest_file": latest_file,
        "log_dir": Path(src_dir_s).name,
        "tool_use_detail": analyzed["tool_use_detail"],
        "tool_success_detail": analyzed["tool_success_detail"],
        "tool_fail_detail": analyzed["tool_fail_detail"],
        "skills_used": analyzed["skills_used"],
        # 统一 schema 导出字段 + 兼容别名
        "api_key": api_key,
        "models": models_list,
        "trace_list": trace_list or [],
        "_key": first_ts,
        "first_ts": first_ts,
        "last_ts": stem,
        "msg_count": analyzed["total_messages"],
        **dict(zip(("completed", "completed_note"), fmt_quality(analyzed["quality_errors"]))),
    }


def _copy_session_to_outdir(args: tuple) -> None:
    """缓存命中时，仍需将合并后的 session JSON 写到 out_dir。"""
    src_dir_s, out_dir_s, first_ts, latest_file, trace_list, api_key, models = args
    src_dir = Path(src_dir_s)
    out_dir = Path(out_dir_s)

    stem = latest_file
    if stem.endswith("-req.json"):
        stem = stem[:-len("-req.json")]
    elif stem.endswith(".json"):
        stem = stem[:-len(".json")]

    tri = _resolve_triplet(src_dir, stem)
    merged = _load_merged(src_dir, stem, latest_file, tri)
    if merged is None:
        return

    session_dir = out_dir / first_ts
    session_dir.mkdir(parents=True, exist_ok=True)
    out_file = session_dir / f"{stem}.json"
    with open(out_file, "w", encoding="utf-8") as fh:
        json.dump(merged, fh, ensure_ascii=False, separators=(",", ":"))


def _rebuild_from_cache(entry: dict) -> Optional[dict]:
    ev = entry.get("_eval")
    if not ev or not isinstance(ev, dict):
        return None
    first_ts = entry.get("_key") or entry.get("first_ts", "")
    if not first_ts:
        return None
    from utils.eval.eval import _parse_folder_ts
    start_ts = _parse_folder_ts(first_ts)
    models = entry.get("models", []) or []
    total_messages = ev.get("total_messages", entry.get("msg_count", 0))
    return {
        "session": first_ts,
        "latest_file": entry.get("latest_file", ""),
        "start_time": start_ts.strftime("%Y-%m-%d %H:%M:%S") if start_ts else None,
        "end_time": ev.get("end_time"),
        "duration_s": ev.get("duration_s"),
        "api_call_count": ev.get("api_call_count", len(entry.get("trace_list", []) or []) or 1),
        "api_errors": ev.get("api_errors", 0),
        "user_turns": ev.get("user_turns", 0),
        "total_messages": total_messages,
        "tool_use_count": ev.get("tool_use_count", 0),
        "tool_result_count": ev.get("tool_result_count", 0),
        "tool_success": ev.get("tool_success", 0),
        "tool_fail_flag": ev.get("tool_fail_flag", 0),
        "tool_fail_keyword": ev.get("tool_fail_keyword", 0),
        "tool_fail_total": ev.get("tool_fail_total", 0),
        "tool_success_rate": ev.get("tool_success_rate"),
        "model": ev.get("model", models[0] if models else ""),
        "q1": ev.get("q1", entry.get("q1", "")),
        "tool_use_detail": ev.get("tool_use_detail", {}),
        "tool_success_detail": ev.get("tool_success_detail", {}),
        "tool_fail_detail": ev.get("tool_fail_detail", {}),
        "skills_used": ev.get("skills_used", {}),
        "completed": ev.get("completed", 0),
        "completed_note": ev.get("completed_note", ""),
        # 统一 schema 导出字段 + 兼容别名
        "api_key": entry.get("api_key", ""),
        "models": models,
        "trace_list": entry.get("trace_list", []) or [],
        "_key": first_ts,
        "first_ts": first_ts,
        "last_ts": entry.get("last_ts", ""),
        "msg_count": total_messages,
    }


def write_eval_to_cache(logs_dir: str, results: List[dict]) -> None:
    cache_path = Path(logs_dir) / ".session_cache.jsonl"
    if not cache_path.is_file() or not results:
        return
    result_map = {}
    for r in results:
        sid = r.get("session", "")
        if sid:
            result_map[sid] = r
    if not result_map:
        return

    lines = []
    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
    except OSError:
        return

    from datetime import datetime as _dt
    now_str = _dt.now().strftime("%Y-%m-%dT%H:%M:%S")
    new_lines = []
    for line in lines:
        stripped = line.strip()
        if not stripped:
            new_lines.append(line)
            continue
        try:
            obj = json.loads(stripped)
        except json.JSONDecodeError:
            new_lines.append(line)
            continue
        key = obj.get("_key", "")
        if key and key in result_map:
            r = result_map[key]
            obj["_eval"] = {
                "ts": now_str,
                "completed": r.get("completed", 0),
                "completed_note": r.get("completed_note", ""),
                "tool_use_count": r.get("tool_use_count", 0),
                "tool_result_count": r.get("tool_result_count", 0),
                "tool_success": r.get("tool_success", 0),
                "tool_fail_flag": r.get("tool_fail_flag", 0),
                "tool_fail_keyword": r.get("tool_fail_keyword", 0),
                "tool_fail_total": r.get("tool_fail_total", 0),
                "tool_success_rate": r.get("tool_success_rate"),
                "user_turns": r.get("user_turns", 0),
                "total_messages": r.get("total_messages", 0),
                "duration_s": r.get("duration_s"),
                "end_time": r.get("end_time"),
                "api_call_count": r.get("api_call_count", 0),
                "api_errors": r.get("api_errors", 0),
                "model": r.get("model", ""),
                "q1": r.get("q1", ""),
                "tool_use_detail": r.get("tool_use_detail", {}),
                "tool_success_detail": r.get("tool_success_detail", {}),
                "tool_fail_detail": r.get("tool_fail_detail", {}),
                "skills_used": r.get("skills_used", {}),
            }
            new_lines.append(json.dumps(obj, ensure_ascii=False) + "\n")
        else:
            new_lines.append(line)

    import os
    tmp = str(cache_path) + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            f.writelines(new_lines)
        safe_replace(tmp, str(cache_path))
    except OSError:
        pass


def reformat_and_analyze(
    src_dir: str,
    out_dir: str,
    session_entries: List[dict],
    api_key: Optional[str] = None,
    workers: int = 4,
    progress_cb: Optional[Callable[[str], None]] = None,
    log_dir: Optional[str] = None,
    analyze: bool = True,
    should_cancel: Optional[Callable[[], bool]] = None,
    on_session_done: Optional[Callable[[], None]] = None,
) -> dict:
    """
    线程池并行: 对每个 session 的 latest_file 做 reformat (+ 可选 analyze_best_data)。

    workers: 线程数，硬上限 32（IO 密集，超过收益递减且徒增争用）。

    analyze=False: reformat-only，只把三元组合并成单 JSON 落盘（+上传由上层做），
        跳过 analyze/质检；结果记录评估字段占位为空，且不走 _eval 缓存复用。

    log_dir: 写入每个 result 的历史目录标识（相对 root 的 posix 路径，
        如 "260728/26072813"）；供报告生成 /history 链接用。缺省时退回
        Path(src_dir).name（仅在单级 native 布局下正确）。

    should_cancel: 协作式取消回调，返回 True 表示用户已终止。上层（export_routes）
        透传一个「查 DB 状态」的节流回调进来：这里在 drain 循环里每收到一个结果就查一次，
        一旦取消就停止收集、cancel_futures 丢弃尚未开始的任务并提前返回已完成部分。
        缺省 None（如离线 CLI）时永不取消，行为与旧版一致。

    Returns:
        {"total_sessions": N, "total_files": M, "errors": [...], "results": [analyzed_dict, ...], "cancelled": bool}
    """
    _log = progress_cb or (lambda msg: None)
    _should_cancel = should_cancel or (lambda: False)
    workers = max(1, min(int(workers), 32))  # 线程数硬上限 32
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    # 统一的历史目录标识：new-api 布局为 "<day>/<hour>"，native 为 "<hour>"。
    # worker 内只能拿到 Path(src_dir).name（丢日期层），故在此统一覆盖。
    resolved_log_dir = log_dir if log_dir is not None else Path(src_dir).name

    entries = session_entries
    if api_key:
        entries = [s for s in entries if s.get("api_key") == api_key]

    total = len(entries)
    if total == 0:
        return {"total_sessions": 0, "total_files": 0, "errors": [], "results": [], "cancelled": False}

    # 建 tasks 前先看一眼取消：用户在排队/前一目录期间已终止时，连合并落盘都不必开始。
    if _should_cancel():
        _log("检测到取消，跳过本目录处理")
        return {"total_sessions": total, "total_files": 0, "errors": [], "results": [], "cancelled": True}

    tasks = []
    cached_results = []
    copy_tasks = []
    for sess in entries:
        first_ts = sess.get("first_ts") or sess.get("_key", "")
        latest_file = sess.get("latest_file", "")
        trace_list = sess.get("trace_list", [])
        s_api_key = sess.get("api_key", "")
        s_models = sess.get("models", []) or []
        if not first_ts or not latest_file:
            continue
        # reformat-only 不复用 analyze 缓存（缓存里是 analyze 结果，与本次意图不符）
        cached = _rebuild_from_cache(sess) if analyze else None
        if cached:
            cached["log_dir"] = resolved_log_dir
            cached_results.append(cached)
            copy_tasks.append((src_dir, out_dir, first_ts, latest_file, trace_list, s_api_key, s_models))
        else:
            tasks.append((src_dir, out_dir, first_ts, latest_file, trace_list, s_api_key, s_models, analyze))

    for ct in copy_tasks:
        _copy_session_to_outdir(ct)

    results = list(cached_results)
    if cached_results:
        _log(f"缓存命中 {len(cached_results)} sessions, 需处理 {len(tasks)}")
    errors = []
    n_tasks = len(tasks)

    if n_tasks == 0:
        return {"total_sessions": total, "total_files": len(results), "errors": errors, "results": results, "cancelled": False}

    # 并行方式：进程池优先，线程池兜底（低任务数 / 显式禁用 / 建池失败）。
    #
    # 原注释认为这里是「IO 密集 + 轻量 CPU，线程足矣」，实测不成立：合并落盘前后
    # 要对每个文件做多次 json.loads / json.dumps，而纯 Python 的 JSON 编解码不释放
    # GIL，线程池开再多线程也只吃一个核（详见 utils/eval/pool.py 的实测数据）。
    # 改用 forkserver 进程池，既拿到多核，又避开 spawn 的两个历史顽疾：
    #   1) spawn 子进程会重新 import app.py，跑一遍模块级启动（init_db/leader 选举/
    #      load_index…），刷屏 "leader_lock: another worker is leader" 且浪费初始化；
    #   2) worker 挂死变孤儿（PPID=1）堆积吃内存 —— forkserver 的子进程随池关闭，
    #      无需 pid 快照 + SIGKILL 看门狗。
    # 逐 session 进度不再打点（曾按 5% 一档，仍会在多 mtime 时刷屏日志抽屉）；
    # 每个 mtime 的起止由上层 export_routes 记录，本函数只在全部完成时汇总一条。
    executor, _pool_kind = make_task_executor(workers, n_tasks, label="reformat")
    _log(_describe_pool(_pool_kind))
    cancelled = False
    try:
        futures = [executor.submit(_process_one, t) for t in tasks]
        for future in as_completed(futures):
            # 协作式取消：每收到一个结果查一次 DB 状态（上层已节流）。取消则停止收集，
            # 交给 finally 的 shutdown(cancel_futures=True) 丢弃尚未开始的任务，提前收尾。
            if _should_cancel():
                cancelled = True
                _log("检测到取消，停止处理剩余 session")
                break
            try:
                result = future.result()
                if result:
                    # worker 内 log_dir=Path(src_dir).name 会丢失日期层，统一覆盖
                    result["log_dir"] = resolved_log_dir
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
            # 逐 session 不打点：进度/完成汇总统一由上层 export_routes 每 mtime 输出一条，
            # 避免这里再刷屏或与上层日志重复。
    finally:
        # cancel_futures=True：丢弃线程池里尚未开始的任务（已在跑的自然收尾）。
        executor.shutdown(wait=True, cancel_futures=True)

    return {"total_sessions": total, "total_files": len(results), "errors": errors, "results": results, "cancelled": cancelled}
