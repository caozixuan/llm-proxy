"""
utils/session_routes.py — Session 会话统计 API

使用 stats_index 增量索引 + 进程级内存缓存。
"""

import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from utils.log_paths import get_service_log_dir
from utils.stats_index import (
    refresh_index,
    build_stats_from_index,
    build_stats_multi,
    get_last_refresh_ts,
    start_stats_warmer,
    QUALIFIED_THRESHOLD_DEFAULT,
)
from utils.logs_config import get_registered_roots


def _leaf_stats_total(row: dict) -> Optional[int]:
    """取单叶 stats_json 的 session 总数;无缓存/解析失败返回 None。"""
    sj = row.get("stats_json") or ""
    if not sj:
        return None
    try:
        import json
        data = json.loads(sj)
    except (ValueError, TypeError):
        return None
    if not isinstance(data, dict):
        return None
    return sum(v.get("total", 0) for v in data.values() if isinstance(v, dict))


# 热力图横轴最小天数：避免只覆盖 1-2 天的 source 把列拉得异常宽。
# 以全部 source 日期并集为准，再向下取整到至少这么多天(缺失天补空)。
MIN_AXIS_DAYS = 14


def _axis_dates(source_dates: List[str]) -> List[str]:
    """计算所有 tab 共享的横轴日期序列。

    取各 source 日期并集的最小/最大范围，再向前扩展到至少 MIN_AXIS_DAYS
    天(缺的天补空)。这样覆盖天数少的 source(如 IP-1-95-199-64 只有 2 天)
    也保持正常列宽，缺失天以空 cell 呈现。返回升序 YYYY-MM-DD 列表。
    """
    if not source_dates:
        return []
    lo = min(source_dates)
    hi = max(source_dates)
    start = datetime.strptime(lo, "%Y-%m-%d")
    end = datetime.strptime(hi, "%Y-%m-%d")
    span = (end - start).days + 1
    if span < MIN_AXIS_DAYS:
        start = start - timedelta(days=MIN_AXIS_DAYS - span)
    out = []
    cur = start
    while cur <= end:
        out.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return out


def _build_stats_json(env_dir: Path, threshold: int = QUALIFIED_THRESHOLD_DEFAULT) -> dict:
    """兼容接口：供外部调用。"""
    index = refresh_index(env_dir, threshold)
    return build_stats_from_index(index, threshold, env_dir=env_dir)


def register_session_routes(app: FastAPI, logs_dir: str) -> None:
    env_dir = Path(logs_dir).parent

    from fastapi.templating import Jinja2Templates
    _templates = Jinja2Templates(directory="templates")

    @app.get("/sessions")
    async def sessions_page(request: Request):
        return _templates.TemplateResponse(request, "sessions.html", context={
            "active_page": "sessions",
            "user_role": request.session.get("monitor_role", "user"),
            "user_name": request.session.get("monitor_user", ""),
            "user_permissions": [p.strip() for p in (request.session.get("monitor_permissions") or "").split(",") if p.strip()],
        })

    @app.get("/sessions/stats")
    def sessions_stats(threshold: int = QUALIFIED_THRESHOLD_DEFAULT, refresh: bool = False,
                       include_rows: bool = False):
        """session 统计。默认 slim：不返回 rows（key×date / key×mtime 明细矩阵）。

        rows 占整个响应 ~99.9%（本机实测 2.8MB 中 2.84MB），而 /、/sessions、
        /overview 三个页面只读 total_sessions / qualified_sessions / threshold /
        _dir / last_refresh_ts，从不读 rows。真正需要 rows 的服务端调用方（导出
        浏览的 key→mtime 分布）走的是 build_stats_multi 本身，不经过本端点。
        需要明细时显式传 include_rows=1。
        """
        roots = get_registered_roots(str(env_dir))
        existing = [r for r in roots if Path(r).is_dir()]
        if not existing:
            return JSONResponse({"error": f"directory not found: {env_dir}"}, status_code=404)

        # 「刷新」按钮 = bust_ttl（跳过 10s 内存 TTL、强制重扫 index.jsonl 签名），
        # 但仍 force=False 享受签名短路（未变叶子命中 leaf_status 缓存、不重算）。
        # 真正的「全量重建」保留在「数据管理」的 backfill 路径，不由统计页触发。
        # include_rows 下传到构建层，slim 时根本不构建 rows 明细（省 ~0.45s/请求）。
        stats = build_stats_multi(existing, threshold, force=False, bust_ttl=refresh,
                                  active_env_dir=str(env_dir), include_rows=include_rows)

        stats["_dir"] = " + ".join(existing)
        stats["_roots"] = existing
        stats["_threshold"] = threshold
        stats["last_refresh_ts"] = get_last_refresh_ts()

        return JSONResponse(stats)

    @app.get("/sessions/heatmap")
    def sessions_heatmap():
        """天 × 2小时档 session 热力图数据(汇总 tab + 各 source 分 tab)。

        直接从 leaf_status.stats_json 推导,复用已落库的统计缓存:
        每叶 dir_key 末段为 8 位 YYMMDDHH 编码「天+小时」,stats_json 的
        sum(total) 即该叶 session 数;按 root_id 累加进 (date, hh//2) 得该
        source 的天×12档矩阵。零扫盘/零重算,亚秒级。

        返回的 sources[0] 为全量汇总(total,所有 source 求和),随后是各
        source;每个 source 的 dates 都是共享的补全横轴(见 _axis_dates),
        覆盖天数少的 source 不拉宽列,缺失天以空 cell 呈现。axis 亦单独返回。
        """
        roots = get_registered_roots(str(env_dir))
        existing = [r for r in roots if Path(r).is_dir()]
        if not existing:
            return JSONResponse({"error": f"directory not found: {env_dir}"}, status_code=404)

        import re
        import utils.logdir_store as lds
        from utils.logs_config import get_root_id

        HOUR8 = re.compile(r"^(\d{2})(\d{2})(\d{2})(\d{2})$")
        sources = []
        seen_rid = set()
        global_max = 0
        all_source_dates: List[str] = []

        for root in existing:
            rid = get_root_id(root, active_env_dir=str(env_dir))
            if rid in seen_rid:
                continue
            seen_rid.add(rid)

            rows = lds.bulk_get_stats(rid)
            if not rows:
                continue
            cells: Dict[str, List[int]] = {}
            maxv = 0
            has_data = False
            for row in rows:
                dk = row.get("dir_key") or ""
                leaf_total = _leaf_stats_total(row)
                if leaf_total is None:
                    continue
                m = HOUR8.match(dk.split("/")[-1])
                if not m:
                    continue
                y, mo, dd, hh = map(int, m.groups())
                date = f"{2000 + y:04d}-{mo:02d}-{dd:02d}"
                blk = hh // 2
                bucket = cells.setdefault(date, [0] * 12)
                bucket[blk] += leaf_total
                if bucket[blk] > maxv:
                    maxv = bucket[blk]
                has_data = True

            if not has_data:
                continue

            name = (lds.get_source(rid) or {}).get("name") or rid
            sources.append({
                "root_id": rid,
                "name": name,
                "root_path": root,
                "cells": {d: vals for d, vals in sorted(cells.items())},
                "bins": 12,
                "max": maxv,
            })
            all_source_dates.extend(cells.keys())
            if maxv > global_max:
                global_max = maxv

        if not sources:
            return JSONResponse({
                "sources": [],
                "global_max": 0,
                "threshold": QUALIFIED_THRESHOLD_DEFAULT,
                "built_at": lds._now(),
            })

        # 共享横轴：所有 source 日期并集范围，向前补齐到至少 MIN_AXIS_DAYS 天。
        axis = _axis_dates(all_source_dates)

        # 汇总 tab：对所有 source 的同 (date, bin) 求和。
        total_cells: Dict[str, List[int]] = {}
        total_max = 0
        for s in sources:
            for date, vals in s["cells"].items():
                bucket = total_cells.setdefault(date, [0] * 12)
                for blk in range(12):
                    bucket[blk] += vals[blk]
                    if bucket[blk] > total_max:
                        total_max = bucket[blk]
        total_entry = {
            "root_id": "total",
            "name": "total",
            "root_path": "",
            "cells": {d: vals for d, vals in sorted(total_cells.items())},
            "bins": 12,
            "max": total_max,
        }

        # 每个 source 及 total 都挂上共享 axis 作为 dates；前端按 axis 渲染，
        # cellVal 对缺失天返回空 → 无数据天呈 tier-0 空 cell。
        final = []
        for s in [total_entry] + sources:
            s["dates"] = axis
            final.append(s)

        return JSONResponse({
            "sources": final,
            "global_max": global_max,
            "axis": axis,
            "threshold": QUALIFIED_THRESHOLD_DEFAULT,
            "built_at": lds._now(),
        })

    start_stats_warmer(str(env_dir))
    # token 索引原先没有任何预热，冷成本（~4.4s，主要是 4592 叶的冷 NFS stat）
    # 每 600s TTL 到期后由用户请求全价支付，见 utils/token_index.start_token_warmer。
    try:
        from utils.token_index import start_token_warmer
        start_token_warmer(str(env_dir))
    except Exception:  # noqa: BLE001 — 预热启动失败不影响路由注册
        import logging
        logging.getLogger("session-routes").warning("token warmer 启动失败", exc_info=True)
