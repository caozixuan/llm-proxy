#!/usr/bin/env python3
"""
tools/offline_reformat_export.py — 离线批量导出（对齐 Web 版）

对当前所有 key，逐个 key 做「合并导出（reformat）」：
  1) build_stats_multi 拿到每个 key 的 full api_key + 全部 mtime 目录；
  2) 逐 key create_record(mode="reformat") 并走与 Web 完全一致的流程：
       export_session_index → _load_session_index 按 key 过滤
       → reformat_and_analyze(analyze=False) 合并三元组落本地
       → 写 session_index.jsonl → 整目录 run_upload_cmd 上传 OBS
       → update_status("success", ...) 回写记录；
  3) 记录字段（obs_dst / local_copy_dir / total_sessions / files_uploaded /
     progress_log / status）与 Web 版 _run_task_inner 完全对齐，
     写入同一个 export_session_record.db，网页刷新即显示为「成功」。

即：网页版 reformat 导出的离线跑版本。

另支持 --mode reconstruct（Hermes 轨迹重构，仅 new-api 合并文件）：
  与 reformat 共享同一「session 迭代并行」框架，但每个 session 不是只取
  latest_file 合并成单文件，而是用 hermes_traj 聚合其 trace_list 指向的
  多个合并文件（按最后 user 锚点分组 → 保留极大分支 → 去重精确重放 →
  回填 reasoning_content），输出重构后的合并文件 + 每 session 的
  _manifest.jsonl。本地落 logs_session_reconstruct/，OBS 走 session_reconstruct/
  前缀；非 new-api 叶子（native 三元组）没有合并文件，直接跳过（warning）。

另支持 --mode full_reformat（全量合并导出，无质检）：
  与 reformat 共享同一「session 迭代并行」框架，但每个 session 不是只取
  latest_file 合并成单文件，而是把 trace_list 指向的**全部** trace 文件都合并
  落盘（out_dir/<first_ts>/<stem>.json，一 trace 一文件）。用于导出特定 key 的
  某些 session 的全量文件。纯合并落盘、不跑 analyze/质检；本地落
  logs_session_analysis_full/，OBS 走 session_analysis_full/ 前缀；三元组 /
  new-api 两种格式都支持。

用法（在项目根目录 /mnt/llm-proxy-main 下运行）：
  python3 tools/offline_reformat_export.py                 # 所有 key，全部 mtime，上传 OBS
  python3 tools/offline_reformat_export.py --no-obs        # 只本地导出，不传 OBS
  python3 tools/offline_reformat_export.py --key sk-xxxxxx # 只导某个 key（完整 key）
  python3 tools/offline_reformat_export.py --key Kjfu      # 只导后4位为 Kjfu 的 key（或 --key key-Kjfu）
  python3 tools/offline_reformat_export.py --mtime 260803  # 只导匹配的 mtime 目录（可多次，子串匹配）
  python3 tools/offline_reformat_export.py --threshold 5   # qualified 阈值（同 Web，默认 5）
  python3 tools/offline_reformat_export.py --mode reconstruct  # Hermes 轨迹重构导出（仅 new-api）
  python3 tools/offline_reformat_export.py --mode full_reformat # 全量合并导出（导 trace_list 全部文件，无质检）
  python3 tools/offline_reformat_export.py --mode full_reformat --key Kjfu --mtime 260803  # 特定 key/mtime 全量导出
  python3 tools/offline_reformat_export.py --dry-run       # 只打印计划，不执行
  python3 tools/offline_reformat_export.py \
      --service-log-dir logs/port8084/env-99oR \
      --env-dir logs_all/env-99oR                          # 手动指定（一般可自动探测）

自动探测：不传 --service-log-dir / --env-dir 时，脚本会
  - 扫描 logs/port*/ 找含 export_session_record.db 的目录作为 SERVICE_LOG_DIR；
  - 读 logs/app-meta-port*.json 的 logs_dir 推出 ENV_DIR = dirname(logs_dir)。
从而与正在运行的服务指向同一套 DB / 同一 logs 根，无需任何环境变量或凭据。
"""

import argparse
import glob
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

# 确保能 import 项目内的 utils.*（脚本位于 tools/ 下）
_PROJ_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJ_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJ_ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("offline_reformat")

# 导出模式注册表（与 Web 共用同一份 settings/export_modes.yaml）
from utils.export_modes import get_config as get_export_modes


# ---------------------------------------------------------------------------
# 自动探测 SERVICE_LOG_DIR / ENV_DIR（与运行中的服务对齐）
# ---------------------------------------------------------------------------
def _autodetect_service_log_dir() -> str:
    """扫描 logs/port*/ 找含 export_session_record.db 的服务日志目录。

    优先取带真实 env-key 段（形如 env-xxxx，非 nokey）的目录。
    """
    candidates = []
    for db in glob.glob(str(_PROJ_ROOT / "logs" / "port*" / "*" / "export_session_record.db")):
        d = os.path.dirname(db)
        candidates.append(d)
    if not candidates:
        return ""
    # 优先非 nokey 段
    non_nokey = [c for c in candidates if os.path.basename(c) != "nokey"]
    pick = (non_nokey or candidates)
    # 若多个，取 session_cache.db 最大的（数据最多的活跃服务）
    def _score(d):
        sc = Path(d) / "session_cache.db"
        try:
            return sc.stat().st_size if sc.is_file() else 0
        except OSError:
            return 0
    pick.sort(key=_score, reverse=True)
    return pick[0]


def _autodetect_env_dir(env_key_hint: str = "") -> str:
    """从 logs/app-meta-port*.json 的 logs_dir 推出 ENV_DIR = dirname(logs_dir)。

    logs_dir 形如 "logs_all/env-99oR/26080317" → ENV_DIR = "logs_all/env-99oR"。
    有 env_key_hint 时优先匹配对应 env 的 meta。
    """
    metas = sorted(glob.glob(str(_PROJ_ROOT / "logs" / "app-meta-port*.json")))
    best = ""
    for m in metas:
        try:
            data = json.loads(Path(m).read_text(encoding="utf-8"))
        except Exception:
            continue
        logs_dir = data.get("logs_dir", "")
        if not logs_dir:
            continue
        env_dir = os.path.dirname(logs_dir)  # 去掉 STARTUP_DATE_TAG 层
        if env_key_hint and os.path.basename(env_dir) == env_key_hint:
            return env_dir
        if not best:
            best = env_dir
    return best


def _mode_choices() -> tuple:
    """CLI 可选模式 = 配置里 pipeline="reformat" 的模式。

    排除 pipeline="copy"（原始三元组复制）与 pipeline="eval"（需跑 evaluate_sessions
    出质检报告，本脚本的收尾链路没有这一段，暴露出来只会静默少出报告）。
    """
    try:
        ids = tuple(m.id for m in get_export_modes().all() if m.pipeline == "reformat")
        return ids or ("reformat",)
    except Exception:
        return ("reformat", "reconstruct", "full_reformat", "jiuwen_reformat")


def _default_cli_mode() -> str:
    """CLI 默认模式：配置里 pipeline=reformat 的首个模式。"""
    return _mode_choices()[0]


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser(description="离线批量 reformat 导出（对齐 Web 版）")
    ap.add_argument("--service-log-dir", default="",
                    help="服务日志目录（含各 .db）。默认自动探测 logs/port*/<env>")
    ap.add_argument("--env-dir", default="",
                    help="ENV_DIR，形如 logs_all/env-99oR。默认从 app-meta 自动探测")
    ap.add_argument("--threshold", type=int, default=5, help="qualified 阈值（同 Web，默认 5）")
    ap.add_argument("--mode", default=_default_cli_mode(),
                    choices=list(_mode_choices()),
                    help="导出方式，取值来自 settings/export_modes.yaml 中 "
                         "pipeline=reformat 的模式："
                         "reformat=合并导出（默认，只导 latest_file）; "
                         "reconstruct=Hermes 轨迹重构（仅 new-api 合并文件）; "
                         "full_reformat=全量合并导出（导 trace_list 的全部文件，无质检）; "
                         "jiuwen_reformat=九问轨迹重构（reasoning_content 回填）。"
                         "新增模式只需改 YAML，无需改本脚本")
    ap.add_argument("--key", action="append", default=[],
                    help="只导指定 key（可多次）；支持完整 api_key / key-XXXX slot / 后4位；缺省=所有 key")
    ap.add_argument("--mtime", action="append", default=[],
                    help="只导 mtime 目录 key 含此子串的（可多次）；缺省=该 key 全部 mtime")
    ap.add_argument("--no-obs", action="store_true", help="只本地导出，不上传 OBS")
    ap.add_argument("--obs-prefix", default="",
                    help="OBS 前缀，默认取 obs_base.yaml 的 obs_base")
    ap.add_argument("--dry-run", action="store_true", help="只打印计划，不执行")
    args = ap.parse_args()

    # --- 定位并初始化各存储（与 app.py 启动顺序一致） ---
    service_log_dir = args.service_log_dir or _autodetect_service_log_dir()
    if not service_log_dir or not Path(service_log_dir).is_dir():
        logger.error("无法定位 SERVICE_LOG_DIR（含 export_session_record.db 的目录）；"
                     "请用 --service-log-dir 指定")
        return 2
    env_key_name = os.path.basename(os.path.normpath(service_log_dir))
    env_dir = args.env_dir or _autodetect_env_dir(env_key_name)
    if not env_dir:
        logger.error("无法自动探测 ENV_DIR；请用 --env-dir 指定（如 logs_all/%s）", env_key_name)
        return 2
    env_dir_p = Path(env_dir).resolve()

    logger.info("SERVICE_LOG_DIR = %s", service_log_dir)
    logger.info("ENV_DIR         = %s", env_dir_p)
    logger.info("env_key_name    = %s", env_key_name)

    from utils.key_store import init_db as init_key_db
    from utils.export_store import init_db as init_export_db
    from utils.session_store import init_db as init_session_db
    from utils.logdir_store import init_db as init_logdir_db
    init_session_db(service_log_dir)   # 供 export_session_index / build_stats_multi
    init_export_db(service_log_dir)    # 记录读写（export_session_record.db）
    init_key_db(service_log_dir)       # key 元数据（可选，用于名称展示）
    # logdir_store 也必须初始化（与 export_worker._init_env 同款）：build_stats_multi /
    # _resolve_mt 解析 mtime key 里的 <root_id>/ 前缀要读 sources 表
    # （get_stats_roots → _list_sources → lds.list_sources）。未 init 时 _ready()=False
    # 使 _list_sources 返回 []，历史 new-api 源（如 de3d5938=jumper）全部丢失 →
    # get_stats_roots 只剩活跃 env_dir 一个 root，静默漏导那些数据源。
    init_logdir_db(service_log_dir)    # 数据源注册表（log_dir.db 的 sources 表）

    # 延迟导入（依赖上面的 init）
    from utils.stats_index import build_stats_multi
    from utils.logs_config import get_stats_roots, get_root_id
    from utils.export_store import create_record, update_status, append_log, get_record
    from utils.export_sync import export_session_index, sync_session_index, _load_session_index
    from utils.obs_utils import run_upload_cmd, load_obs_base, load_sync_config

    # 处理器与落盘目录全部由 settings/export_modes.yaml 决定：新增模式只改 YAML。
    # 各模式共享同一「session 迭代并行」框架（_run_one_export），只是每 session
    # 的处理不同（合并 latest_file / 聚合多个 trace / 全量合并 / reason 回填）。
    mode = args.mode
    _mcfg = get_export_modes().get(mode)
    if _mcfg is None:
        logger.error("未知导出模式 %r（未在 settings/export_modes.yaml 注册）", mode)
        return 2
    _processor = get_export_modes().resolve_processor(mode)
    if _processor is None:
        logger.error("模式 %r 未配置可用处理器（检查 settings/export_modes.yaml 的 processor）", mode)
        return 2
    _mode_label = _mcfg.mode_label

    # --- 复刻 export_routes 里的 mtime 解析辅助（多 root 前缀感知） ---
    def _all_roots():
        return get_stats_roots(str(env_dir_p))

    def _existing_roots():
        return [r for r in _all_roots() if Path(r).is_dir()]

    def _resolve_mt(mt: str):
        """把 mtime key（<root_id>/<rel> 或裸 <rel>）解析为叶子目录绝对路径。

        以 leaf_status.leaf_path 为准：逐 root 拆前缀得 rel_key，DB 查表命中即返回；
        均未命中再退回「拼 root/rel_key 并 is_dir 校验」定位归属 root（未同步/裸 native）。
        与 export_routes._resolve_mt_for 同源。
        """
        from utils.logdir_store import resolve_leaf_path
        roots = _existing_roots()
        multi = len(roots) > 1
        cands = []  # (root, rel_key)
        for root in roots:
            rid = get_root_id(root, str(env_dir_p))
            base = os.path.basename(os.path.normpath(root))
            matched = False
            for pfx in (rid, base):
                if multi and mt.startswith(pfx + "/"):
                    cands.append((root, rid, mt[len(pfx) + 1:])); matched = True
            if not matched:
                cands.append((root, rid, mt))
        # 1) DB 查表优先
        for root, rid, rel_key in cands:
            try:
                hit = resolve_leaf_path(rid, rel_key)
            except Exception:
                hit = None
            if hit:
                return hit
        # 2) 兜底：拼 root/rel_key 并 is_dir 校验
        for root, rid, rel_key in cands:
            cand = Path(root) / rel_key
            if cand.is_dir():
                return str(cand)
        cand = env_dir_p / mt
        return str(cand) if cand.is_dir() else None

    def _log_dir_key(mt_src: str) -> str:
        from utils.log_scan import dir_key_for
        p = Path(mt_src)
        for root in _existing_roots():
            rp = Path(root)
            try:
                if p == rp or rp in p.parents:
                    return dir_key_for(rp, p)
            except (OSError, ValueError):
                continue
        return p.name

    def _key_slot(api_key: str) -> str:
        if not api_key:
            return "all"
        return "key-" + api_key[-4:]

    # --- 收集所有 key + 其 mtime 目录 ---
    roots = _existing_roots()
    if not roots:
        logger.error("没有可用的 stats root（env_dir 不存在？）")
        return 2
    logger.info("统计 roots: %s", roots)
    stats = build_stats_multi(roots, args.threshold, active_env_dir=str(env_dir_p))
    rows = stats.get("rows", [])
    if not rows:
        logger.warning("build_stats_multi 无数据行，无可导出的 key")
        return 0

    # --key 过滤：支持三种写法（可混用、可多次），命中任一即导出该 key——
    #   1) 完整 api_key（精确匹配）；
    #   2) key slot（形如 key-Kjfu，取后 4 位比对）；
    #   3) 后 4 位（如 Kjfu，与 slot 一致的短写法）。
    # 后两种按「最后 4 位相同」匹配，与网页卡片 key-XXXX 的分组口径一致。
    def _key_matches(api_key: str, pat: str) -> bool:
        if api_key == pat:                       # 完整 key 精确匹配
            return True
        suf = pat[-4:] if len(pat) >= 4 else pat  # key-Kjfu / Kjfu → Kjfu
        return bool(api_key) and api_key != "(empty)" and api_key.endswith(suf)

    key_filter = list(args.key)
    plan = []  # [(api_key, [mtime_dirs...])]
    for row in rows:
        api_key = row["api_key"]
        if key_filter and not any(_key_matches(api_key, p) for p in key_filter):
            continue
        mt_keys = sorted(row.get("mtime_cells", {}).keys(), reverse=True)
        if args.mtime:
            mt_keys = [m for m in mt_keys if any(sub in m for sub in args.mtime)]
        if not mt_keys:
            continue
        plan.append((api_key, mt_keys))

    if not plan:
        logger.warning("过滤后无可导出的 key/mtime")
        return 0

    obs_prefix = "" if args.no_obs else (args.obs_prefix.strip().rstrip("/") or load_obs_base())
    sync_cfg = load_sync_config()
    workers = sync_cfg.get("workers", 4)
    upload_script = sync_cfg.get("upload_script") or None

    logger.info("计划导出 %d 个 key；OBS=%s", len(plan),
                (obs_prefix or "(不上传)"))
    for api_key, mt_keys in plan:
        logger.info("  key=...%s  slot=%s  mtime_dirs=%d 个",
                    api_key[-8:] if api_key and api_key != "(empty)" else api_key,
                    _key_slot(api_key if api_key != "(empty)" else ""), len(mt_keys))
    if args.dry_run:
        logger.info("dry-run，结束。")
        return 0

    # --- 逐 key 执行导出（复刻 _run_task_inner 的 reformat/reconstruct 分支） ---
    ok_cnt = fail_cnt = 0
    for api_key, mt_keys in plan:
        rc = _run_one_export(
            api_key=api_key, mtime_dirs=mt_keys,
            env_dir_p=env_dir_p, env_key_name=env_key_name,
            obs_prefix=obs_prefix, workers=workers, upload_script=upload_script,
            mode=mode, _mcfg=_mcfg, _mode_label=_mode_label, processor=_processor,
            _key_slot=_key_slot, _resolve_mt=_resolve_mt, _log_dir_key=_log_dir_key,
            create_record=create_record, update_status=update_status,
            append_log=append_log, get_record=get_record,
            export_session_index=export_session_index,
            _load_session_index=_load_session_index,
            run_upload_cmd=run_upload_cmd,
        )
        if ok_cnt is not None and rc:
            ok_cnt += 1
        else:
            fail_cnt += 1

    logger.info("完成：成功 %d，失败 %d，共 %d 个 key", ok_cnt, fail_cnt, len(plan))
    return 0 if fail_cnt == 0 else 1


def _run_one_export(*, api_key, mtime_dirs, env_dir_p, env_key_name,
                      obs_prefix, workers, upload_script,
                      mode, _mcfg, _mode_label, processor,
                      _key_slot, _resolve_mt, _log_dir_key,
                      create_record, update_status, append_log, get_record,
                      export_session_index, _load_session_index,
                      run_upload_cmd) -> bool:
    """对单个 key 执行导出，写记录并回写状态。返回 True=success。

    严格对齐 utils/export_routes.py::_run_task_inner 的 reformat 分支：
    obs_sub / local_base / obs_dst 路径规则、日志文案、上传方式、字段回写全部一致。

    mode: "reformat"（合并导出，对齐 Web）或 "reconstruct"（Hermes 轨迹重构）。
    processor: 每 session 的处理函数（reformat_and_analyze / reconstruct_and_export，
        两者同签名，仅每 session 处理方式不同）。reconstruct 仅在 new-api 叶子生效，
        非 new-api（native 三元组）叶子直接跳过（warning，不判失败）。
    """
    _api_key = "" if api_key == "(empty)" else api_key
    slot = _key_slot(_api_key)
    from utils.export_jobs import make_now_tag
    now_tag = make_now_tag()

    # 建记录（与 Web 的 /api/export/run 一致：reformat / reconstruct）
    record_id = create_record(
        api_key=_api_key, key_slot=slot,
        mtime_dirs=json.dumps(mtime_dirs),
        obs_dst="", local_copy_dir="",
        mode=mode,
    )
    _log = lambda msg: append_log(record_id, msg)

    try:
        update_status(record_id, "running")

        # local_base / obs_dst 规则完全对齐 _run_task_inner，且与 Web 同读一份配置：
        # 落盘根优先级 EXPORT_CACHE_DIR(.env) > yaml 的 temp_path > env_dir.parent.parent。
        _temp_root = get_export_modes().resolved_temp_path(env_dir_p.parent.parent)
        local_base = (_temp_root / _mcfg.local_dir /
                      env_key_name / slot / f"ex-{now_tag}").resolve()
        obs_sub = _mcfg.obs_sub
        obs_dst = f"{obs_prefix}/{obs_sub}/{env_key_name}/{slot}/ex-{now_tag}/" if obs_prefix else ""
        local_base.mkdir(parents=True, exist_ok=True)
        update_status(record_id, "running", obs_dst=obs_dst, local_copy_dir=str(local_base))

        _log(f"开始{_mode_label}: key_slot={slot}, mtime_dirs={mtime_dirs}")
        _log(f"本地目录: {local_base}")
        if obs_dst:
            _log(f"OBS 目标: {obs_dst}")

        errors = []
        warnings = []
        total_sessions = 0
        total_uploaded = 0
        all_results = []
        all_entries = []

        for mt in mtime_dirs:
            mt_src = _resolve_mt(mt) or str(env_dir_p / mt)
            try:
                # new-api：索引未构建则跳过（与 Web 一致，非致命 warning）
                try:
                    from utils.log_scan import detect_format as _df
                    if _df(mt_src) == "newapi":
                        import utils.newapi_index_db as _nidb
                        if _nidb.needs_build(mt_src):
                            _log(f"[{mt}] 索引未构建/不完整，跳过；请先在数据管理界面手动构建索引后再导出")
                            warnings.append(f"{mt}: 索引未构建/不完整，已跳过")
                            continue
                except Exception as _pe:
                    _log(f"[{mt}] 索引未就绪（检查异常: {_pe}），跳过")
                    warnings.append(f"{mt}: 索引未构建/不完整，已跳过（检查异常: {_pe}）")
                    continue

                # 格式守卫来自配置：reconstruct 只认 new-api 合并文件（native 三元组叶子
                # 没有合并文件，hermes 聚合器读不到顶层 req），跳过（warning 非致命，
                # 与索引未构建同语义）。
                if _mcfg.formats is not None:
                    from utils.log_scan import detect_format as _df_fmt
                    fmt = _df_fmt(mt_src)
                    if not _mcfg.supports_format(fmt):
                        _log(f"[{mt}] 目录格式 {fmt}，{_mode_label}仅支持 {'/'.join(_mcfg.formats)} 合并文件，跳过")
                        warnings.append(f"{mt}: 非 {'/'.join(_mcfg.formats)} 格式（{fmt}），已跳过")
                        continue

                _log(f"[{mt}] 生成 session_index...")
                exp_result = export_session_index(mt_src, force=False)
                _log(f"[{mt}] session_index: {exp_result.get('total_sessions', 0)} sessions"
                     + (" (已是最新，跳过)" if exp_result.get("skipped") else ""))

                session_entries = _load_session_index(mt_src)
                total_before = len(session_entries)
                if _api_key:
                    session_entries = [s for s in session_entries if s.get("api_key") == _api_key]
                if not session_entries:
                    _log(f"[{mt}] 共 {total_before} sessions, 按 key 过滤后 0, 跳过")
                    continue
                if _api_key:
                    _log(f"[{mt}] 共 {total_before} sessions, 按 key 过滤后 {len(session_entries)}")

                _log(f"[{mt}] 进入{_mode_label}: {len(session_entries)} sessions, workers={workers}...")
                processor_kwargs = dict(
                    src_dir=mt_src, out_dir=str(local_base),
                    session_entries=session_entries, api_key=_api_key, workers=workers,
                    progress_cb=lambda msg, _mt=mt: _log(f"[{_mt}] {msg}"),
                    log_dir=_log_dir_key(mt_src),
                )
                # analyze 三态：None = 处理器签名里没有这个形参（reconstruct /
                # full_reformat / jiuwen_reformat），不传。reformat 配置里是 false
                # （与 Web reformat-only 一致）；eval 是 true。
                if _mcfg.analyze is not None:
                    processor_kwargs["analyze"] = _mcfg.analyze
                ra_result = processor(**processor_kwargs)
                all_results.extend(ra_result["results"])
                all_entries.extend(session_entries)
                total_sessions += len(ra_result["results"])
                _log(f"[{mt}] {_mode_label}返回: results={ra_result['total_files']}, "
                     f"errors={len(ra_result.get('errors', []))}")
            except Exception as e:
                _log(f"[{mt}] 错误: {e}")
                errors.append(f"{mt}: {e}")
                logger.exception("%s failed for %s", mode, mt)

        # 收尾：写 session_index.jsonl 清单 + 整目录上传（对齐 Web）
        if all_results:
            idx_path = local_base / "session_index.jsonl"
            with open(idx_path, "w", encoding="utf-8") as f:
                for entry in all_entries:
                    f.write(json.dumps(entry, ensure_ascii=False) + "\n")
            if obs_dst:
                _log(f"同步到 OBS: {obs_dst}")
                obs_parent = obs_dst.rstrip("/").rsplit("/", 1)[0] + "/"
                ok, msg = run_upload_cmd(str(local_base), obs_parent, upload_script, log_cb=_log)
                if ok:
                    _log("上传成功")
                    total_uploaded = len(all_results)
                else:
                    _log(f"上传失败: {msg}")
                    errors.append(f"OBS upload: {msg}")
        else:
            _log("无 session 数据")

        # 判定（与 _run_task_inner 完全一致）
        no_output = not all_results
        warn_suffix = f"（{len(warnings)} 个目录跳过：{'; '.join(warnings)}）" if warnings else ""
        if errors or no_output:
            if no_output and not errors:
                reason = f"无 session 数据{warn_suffix}" if warnings else "无 session 数据"
            else:
                reason = "; ".join(errors) + warn_suffix
            _log(f"{_mode_label}失败: {reason}")
            update_status(record_id, "failed",
                          error_message=reason,
                          total_sessions=total_sessions,
                          files_uploaded=total_uploaded,
                          files_skipped=0)
            logger.warning("record %s (slot=%s) FAILED: %s", record_id, slot, reason)
            return False
        else:
            msg = f"{_mode_label}完成: {total_sessions} sessions"
            if warnings:
                msg += f"，{len(warnings)} 个目录跳过（索引未构建）"
            _log(msg)
            update_status(record_id, "success",
                          error_message=(f"部分目录跳过：{'; '.join(warnings)}" if warnings else ""),
                          total_sessions=total_sessions,
                          files_uploaded=total_uploaded,
                          files_skipped=0)
            logger.info("record %s (slot=%s) SUCCESS: %s", record_id, slot, msg)
            return True

    except Exception as exc:
        logger.exception("_run_one_export crashed (record=%s)", record_id)
        try:
            _log(f"任务异常终止: {exc}")
        except Exception:
            pass
        update_status(record_id, "failed", error_message=str(exc))
        return False


if __name__ == "__main__":
    raise SystemExit(main())
