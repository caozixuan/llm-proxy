"""
utils/token_index.py — Token 用量增量索引

持久化文件: {env_dir}/.token_index.jsonl (JSONL, 第一行 meta)
进程级内存缓存: 10 分钟 TTL
frozen 机制: index.jsonl 文件 mtime/size 不变时标记 frozen，下次跳过

扫描 {env_dir}/{mtime}/index.jsonl，按 (model|date, status) / (api_key|date) /
(channel_key|date) 三个维度预聚合。查询时从内存中按日期范围过滤汇总。
"""

import hashlib
import json
import os
from utils.atomic_write import safe_replace
import time
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

from utils.log_scan import iter_index_dirs, dir_key_for, normalize_entry

_INDEX_FILE = ".token_index.jsonl"
_VERSION = 4  # v4: 多格式归一化（new-api usage.token_in / 无 success 字段）
_lock = threading.Lock()

_MEM_TTL = 600  # 10 分钟：与前端「10 分钟刷新」对齐，避免打开页面短时间内重复重扫
# 按 root 缓存，避免多目录轮询时单槽缓存互相驱逐
_mem_cache: Dict[str, dict] = {}      # root_str -> index dict
_mem_cache_ts: Dict[str, float] = {}  # root_str -> 最后刷新时间

# B：合并结果缓存——/query 三个端点并发（Promise.all）共享同一次刷新。
# 窗口比 _MEM_TTL 长，一次构建可服务后续端点；「刷新」按钮 force 绕过。
_COMBINED_TTL = 600  # 10 分钟：单个 3 端点并发窗口，与前端刷新节奏对齐
_combined_cache: Dict[tuple, dict] = {}     # root 签名(归一化 tuple) -> merged dirs
_combined_ts: Dict[tuple, float] = {}       # root 签名 -> 构建时间
_combined_lock = threading.Lock()

# 项目内缓存目录（当 root 不可写，如只读挂载的 new-api 目录时使用）
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_FALLBACK_CACHE_DIR = _PROJECT_ROOT / "logs" / ".token_index_cache"


def _resolve_cache_path(env_dir: Path) -> str:
    """索引缓存文件路径。root 可写则存 root 内（兼容 logs_all 旧行为），
    否则退回项目内 logs/.token_index_cache/{hash}.jsonl。"""
    in_root = env_dir / _INDEX_FILE
    try:
        if env_dir.is_dir() and os.access(str(env_dir), os.W_OK):
            return str(in_root)
    except OSError:
        pass
    h = hashlib.md5(os.path.normpath(str(env_dir)).encode()).hexdigest()[:16]
    try:
        os.makedirs(str(_FALLBACK_CACHE_DIR), exist_ok=True)
    except OSError:
        pass
    return str(_FALLBACK_CACHE_DIR / f"{h}.jsonl")


def _index_path(env_dir: Path) -> str:
    return _resolve_cache_path(env_dir)


def _load_index(env_dir: Path) -> dict:
    path = _index_path(env_dir)
    if not os.path.isfile(path):
        return {"version": _VERSION, "dirs": {}, "updated_at": 0}
    try:
        dirs = {}
        meta = None
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if obj.get("_meta"):
                    meta = obj
                else:
                    dir_key = obj.get("dir", "")
                    if dir_key:
                        dirs[dir_key] = obj
        version = meta.get("version", 0) if meta else 0
        if version != _VERSION:
            return {"version": _VERSION, "dirs": {}, "updated_at": 0}
        return {
            "version": _VERSION,
            "dirs": dirs,
            "updated_at": meta.get("updated_at", 0) if meta else 0,
        }
    except OSError:
        return {"version": _VERSION, "dirs": {}, "updated_at": 0}


def _save_index(env_dir: Path, data: dict) -> None:
    path = _index_path(env_dir)
    try:
        os.makedirs(str(env_dir), exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            meta = {"_meta": True, "version": _VERSION, "updated_at": data.get("updated_at", 0)}
            f.write(json.dumps(meta, ensure_ascii=False) + "\n")
            for dir_key, dir_info in sorted(data.get("dirs", {}).items()):
                row = dict(dir_info)
                row["dir"] = dir_key
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
        safe_replace(tmp, path)
    except OSError:
        pass


def _scan_index_file(index_file: Path, offset: int = 0,
                     prev: Optional[dict] = None) -> dict:
    """读一个 index.jsonl，预聚合为 models/keys/channels 三个维度（按日期分桶）。"""
    if prev and offset > 0:
        models = {k: dict(v) for k, v in prev.get("models", {}).items()}
        keys = {k: dict(v) for k, v in prev.get("keys", {}).items()}
        channels = {k: dict(v) for k, v in prev.get("channels", {}).items()}
        channel_keys_set = set(prev.get("channel_keys_set", []))
        api_keys_set = set(prev.get("api_keys_set", []))
        dates_set = set(prev.get("dates", []))
        entry_count = prev.get("entry_count", 0)
    else:
        models = {}
        keys = {}
        channels = {}
        channel_keys_set = set()
        api_keys_set = set()
        dates_set = set()
        entry_count = 0

    new_entries = 0
    end_offset = offset

    try:
        with open(index_file, "r", encoding="utf-8") as f:
            if offset > 0:
                f.seek(offset)
            for line in f:
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    entry = json.loads(stripped)
                except json.JSONDecodeError:
                    continue

                entry = normalize_entry(entry)

                new_entries += 1
                entry_count += 1
                date_str = entry["date"]
                dates_set.add(date_str)

                model = entry["model"]
                tok_in = entry["tok_in"]
                tok_out = entry["tok_out"]
                is_success = entry["success"]

                # models dimension
                if model:
                    mk = f"{model}|{date_str}"
                    if mk not in models:
                        models[mk] = {
                            "s_count": 0, "s_tok_in": 0, "s_tok_out": 0,
                            "e_count": 0, "e_tok_in": 0,
                        }
                    if is_success:
                        models[mk]["s_count"] += 1
                        models[mk]["s_tok_in"] += tok_in
                        models[mk]["s_tok_out"] += tok_out
                    else:
                        models[mk]["e_count"] += 1
                        models[mk]["e_tok_in"] += tok_in

                # keys dimension
                raw_key = entry["api_key"]
                if raw_key:
                    api_keys_set.add(raw_key)
                kk = f"{raw_key}|{date_str}"
                if kk not in keys:
                    keys[kk] = {"count": 0, "tok_in": 0, "tok_out": 0, "sessions": 0}
                keys[kk]["count"] += 1
                keys[kk]["tok_in"] += tok_in
                keys[kk]["tok_out"] += tok_out
                sess_id = entry["q1_hash"] or entry["chain_key"]
                if sess_id:
                    keys[kk]["sessions"] += 1

                # channels dimension
                ch_key = entry["channel_key"]
                if ch_key:
                    channel_keys_set.add(ch_key)
                ck = f"{ch_key or '(default)'}|{date_str}"
                if ck not in channels:
                    channels[ck] = {"count": 0, "tok_in": 0, "tok_out": 0, "sessions": 0}
                channels[ck]["count"] += 1
                channels[ck]["tok_in"] += tok_in
                channels[ck]["tok_out"] += tok_out
                if sess_id:
                    channels[ck]["sessions"] += 1

            end_offset = f.tell()

    except OSError:
        pass

    return {
        "entry_count": entry_count,
        "new_entries": new_entries,
        "scan_offset": end_offset,
        "models": models,
        "keys": keys,
        "channels": channels,
        "channel_keys_set": sorted(channel_keys_set),
        "api_keys_set": sorted(api_keys_set),
        "dates": sorted(dates_set),
    }


def _leaf_subs(env_path: Path):
    """免扫盘枚举叶子：已同步 root 走 leaf_status DB（镜像 stats_index），
    未同步/本地 root 回退 iter_index_dirs 全盘 walk。

    返回 (rid, [(sub: Path, row: dict|None), ...])。rid 供写回 leaf_status
    （token 聚合落 token_stats_json）；未同步 root 时 rid 为 None 且不写回。
    row 供签名判活；token 聚合产物经 set_leaf_token_stats 落 leaf_status。
    """
    import utils.logdir_store as _lds
    from utils.stats_index import _resolve_rid, _leaf_status_subs

    rid = _resolve_rid(env_path)
    if rid and _lds.has_any(rid):
        return rid, _leaf_status_subs(env_path, rid)
    return None, [(sub, None) for sub in iter_index_dirs(env_path)]


def refresh_token_index(env_dir: str, force: bool = False) -> dict:
    """增量刷新单个 root 的 token 索引。

    root 下用可变深度扫描找到所有含 index.jsonl 的叶子目录，
    dir_key 为叶子相对 root 的路径（如 '26072520' 或 '260727/26072717'）。
    每个 root 独立缓存，避免多目录轮询时互相驱逐。
    """
    env_path = Path(env_dir)
    env_dir_str = os.path.normpath(str(env_path))
    now = time.time()

    cached = _mem_cache.get(env_dir_str)
    if (not force and cached is not None
            and (now - _mem_cache_ts.get(env_dir_str, 0)) < _MEM_TTL):
        return cached

    with _lock:
        cached = _mem_cache.get(env_dir_str)
        if (not force and cached is not None
                and (time.time() - _mem_cache_ts.get(env_dir_str, 0)) < _MEM_TTL):
            return cached

        index = cached if cached else _load_index(env_path)
        dirs_cache = index.get("dirs", {})
        changed = False

        rid, leaf_iter = _leaf_subs(env_path)
        use_leaf_db = rid is not None
        # dir_key → (sub, leaf_row)：供循环后按签名回写 leaf_status token 缓存。
        sub_by_dir: Dict[str, tuple] = {}

        current_dirs = set()
        for leaf, leaf_row in leaf_iter:
            dir_key = dir_key_for(env_path, leaf)
            current_dirs.add(dir_key)
            sub_by_dir[dir_key] = (leaf, leaf_row)

            prev = dirs_cache.get(dir_key)

            # 未同步 root（全盘 walk，无 leaf_status DB 缓存）：沿用 frozen 快路径
            # 先跳过，避免每次刷新对每个叶子重复 stat/重扫。已同步 root 走下方 DB
            # 签名短路（同样跳过，且 force 也命中），无需此预检。
            if not use_leaf_db and not force and prev and prev.get("frozen"):
                continue

            index_file = leaf / "index.jsonl"
            try:
                st = index_file.stat()
                f_mtime = st.st_mtime
                f_size = st.st_size
            except OSError:
                continue

            # ── leaf_status token 缓存签名短路 ──────────────────────────────
            # 一次 stat index.jsonl：签名未变且该叶 DB 已有 token_stats_json →
            # 命中，直接用 DB 聚合结果组装 dir_info，跳过下方磁盘重扫。force
            # 也命中（用户要求统计过没变化就不要重复统计）。
            if (use_leaf_db and leaf_row is not None
                    and leaf_row.get("token_stats_json")
                    and leaf_row.get("idx_size") == f_size
                    and leaf_row.get("idx_mtime") == f_mtime):
                try:
                    db_info = json.loads(leaf_row["token_stats_json"])
                except (ValueError, TypeError):
                    db_info = None
                if db_info is not None:
                    entry = dict(prev) if prev else {}
                    entry.update(db_info)
                    entry["index_mtime"] = f_mtime
                    entry["index_size"] = f_size
                    entry["frozen"] = True  # 签名命中即视为稳定（活跃叶 row=None 走不到这）
                    dirs_cache[dir_key] = entry
                    continue

            # ── 文件/内存缓存签名命中（未同步 root 的持久来源，或迁库前旧数据）──
            if (prev
                    and prev.get("index_mtime") == f_mtime
                    and prev.get("index_size") == f_size):
                if not prev.get("frozen"):
                    prev["frozen"] = True
                    changed = True
                continue

            prev_offset = prev.get("scan_offset", 0) if prev else 0
            if (prev
                    and prev_offset > 0
                    and f_size > prev.get("index_size", 0)):
                # 文件增长、且已记录上次扫描偏移 → 增量读新追加部分
                scan_result = _scan_index_file(index_file, offset=prev_offset, prev=prev)
            else:
                scan_result = _scan_index_file(index_file)
            dirs_cache[dir_key] = {
                "index_mtime": f_mtime,
                "index_size": f_size,
                "scan_offset": scan_result.get("scan_offset", f_size),
                "frozen": False,
                **{k: v for k, v in scan_result.items() if k not in ("new_entries", "scan_offset")},
            }
            changed = True

        # 只删除非 frozen 的消失目录；frozen 代表数据已稳定，本地删除后仍保留统计
        removed = set(dirs_cache.keys()) - current_dirs
        for r in removed:
            if not dirs_cache[r].get("frozen"):
                del dirs_cache[r]
                changed = True

        if changed:
            index["dirs"] = dirs_cache
            index["updated_at"] = time.time()
            _save_index(env_path, index)

        # ── 回写 leaf_status token 缓存 ──────────────────────────────────────
        # 只在走 leaf_db 枚举（已同步 root）时回写。只回写已在表里的叶子；
        # 活跃小时叶子（leaf_row is None）还没进表、每请求重算、永不冻结，
        # 不落库污染叶子管理计数。签名相符且已有 token_stats_json → 跳过
        # （签名短路命中那批）；只有真正重算/首建或旧文件迁过来的叶子才写。
        if use_leaf_db:
            import utils.logdir_store as _lds
            for dir_key, (leaf, leaf_row) in sub_by_dir.items():
                if leaf_row is None:
                    continue
                entry = dirs_cache.get(dir_key)
                if not entry:
                    continue
                payload = {k: entry[k] for k in (
                    "scan_offset", "entry_count", "models", "keys", "channels",
                    "channel_keys_set", "api_keys_set", "dates") if k in entry}
                if not payload:
                    continue
                idx_size = entry.get("index_size")
                idx_mtime = entry.get("index_mtime")
                if idx_size is None or idx_mtime is None:
                    continue
                # 与库中记录一致（签名相符且已有 token_stats_json）→ 无需重复写
                if (leaf_row.get("idx_size") == idx_size
                        and leaf_row.get("idx_mtime") == idx_mtime
                        and leaf_row.get("token_stats_json")):
                    continue
                try:
                    _lds.set_leaf_token_stats(
                        rid, dir_key, leaf_path=str(leaf),
                        idx_size=idx_size, idx_mtime=idx_mtime,
                        token_stats_json=json.dumps(payload, ensure_ascii=False),
                    )
                except Exception:
                    pass

        _mem_cache[env_dir_str] = index
        _mem_cache_ts[env_dir_str] = time.time()

        return index


# ---------------------------------------------------------------------------
# 查询接口（从索引中按参数过滤聚合）
# ---------------------------------------------------------------------------

def _mask_api_key(key: str) -> str:
    if not key or len(key) <= 8:
        return key or "(empty)"
    return f"{key[:4]}...{key[-4:]}"


def _as_roots(env_dir) -> List[str]:
    """把 env_dir 归一化为 root 列表（接受 str 或 list）。"""
    if env_dir is None:
        return []
    if isinstance(env_dir, (str, os.PathLike)):
        return [str(env_dir)]
    return [str(x) for x in env_dir if x]


def _combined_index(env_dir, force: bool = False) -> dict:
    """刷新一个或多个 root，合并为一个 index（dir_key 加 root 前缀防冲突）。

    B：按 root 签名做窗口内合并缓存——前端 3 个端点并发到达时只构建一次，
    force（页面「刷新」按钮）绕过缓存强制重建。
    """
    roots = _as_roots(env_dir)
    sig = tuple(os.path.normpath(r) for r in roots)
    now = time.time()

    cached = _combined_cache.get(sig)
    if (not force and cached is not None
            and (now - _combined_ts.get(sig, 0)) < _COMBINED_TTL):
        return {"dirs": cached}

    with _combined_lock:
        cached = _combined_cache.get(sig)
        if (not force and cached is not None
                and (time.time() - _combined_ts.get(sig, 0)) < _COMBINED_TTL):
            return {"dirs": cached}

        merged: Dict[str, Any] = {}
        for root in roots:
            idx = refresh_token_index(root, force=force)
            prefix = os.path.normpath(root)
            for dir_key, dir_info in idx.get("dirs", {}).items():
                merged[f"{prefix}::{dir_key}"] = dir_info
        _combined_cache[sig] = merged
        _combined_ts[sig] = time.time()
        return {"dirs": merged}


def query_token_stats(env_dir, model: str = '', date_start: str = '2000-01-01',
                      date_end: str = '9999-12-31', status: str = '',
                      channel_key: str = '', api_key: str = '',
                      force: bool = False) -> dict:
    """从索引中聚合 token 统计。env_dir 可为单个 root 或 root 列表。"""
    index = _combined_index(env_dir, force=force)

    model_filter = model.lower() if model else ""
    model_agg: Dict[str, Dict[str, Dict[str, int]]] = {}

    for dir_info in index.get("dirs", {}).values():
        dir_dates = dir_info.get("dates", [])
        if dir_dates and dir_dates[-1] < date_start:
            continue
        if dir_dates and dir_dates[0] > date_end:
            continue

        if channel_key and channel_key not in (dir_info.get("channel_keys_set") or []):
            continue

        if api_key and api_key not in (dir_info.get("api_keys_set") or []):
            continue

        for mk, counts in dir_info.get("models", {}).items():
            parts = mk.rsplit("|", 1)
            if len(parts) != 2:
                continue
            m_name, m_date = parts

            if not (date_start <= m_date <= date_end):
                continue
            if model_filter and model_filter not in m_name.lower():
                continue

            if m_name not in model_agg:
                model_agg[m_name] = {
                    "success": {"count": 0, "tok_in": 0, "tok_out": 0},
                    "error": {"count": 0, "tok_in": 0},
                }
            model_agg[m_name]["success"]["count"] += counts.get("s_count", 0)
            model_agg[m_name]["success"]["tok_in"] += counts.get("s_tok_in", 0)
            model_agg[m_name]["success"]["tok_out"] += counts.get("s_tok_out", 0)
            model_agg[m_name]["error"]["count"] += counts.get("e_count", 0)
            model_agg[m_name]["error"]["tok_in"] += counts.get("e_tok_in", 0)

    res_data = []
    for m_name, agg in model_agg.items():
        if status in ("", "全部", "success", "成功"):
            if agg["success"]["count"] > 0:
                res_data.append({
                    "model": m_name, "date_start": date_start, "date_end": date_end,
                    "status": "success", "count": agg["success"]["count"],
                    "input_token_num": agg["success"]["tok_in"],
                    "output_token_num": agg["success"]["tok_out"],
                })
        if status in ("", "全部", "error", "失败"):
            if agg["error"]["count"] > 0:
                res_data.append({
                    "model": m_name, "date_start": date_start, "date_end": date_end,
                    "status": "error", "count": agg["error"]["count"],
                    "input_token_num": agg["error"]["tok_in"],
                    "output_token_num": 0,
                })

    s_count = sum(d["count"] for d in res_data if d["status"] == "success")
    s_tok_in = sum(d["input_token_num"] for d in res_data if d["status"] == "success")
    s_tok_out = sum(d["output_token_num"] for d in res_data if d["status"] == "success")
    e_count = sum(d["count"] for d in res_data if d["status"] == "error")
    e_tok_in = sum(d["input_token_num"] for d in res_data if d["status"] == "error")

    summary = [
        {"status": "success", "count": s_count, "total_input": s_tok_in, "total_output": s_tok_out},
        {"status": "error", "count": e_count, "total_input": e_tok_in, "total_output": 0},
    ]

    return {"data": res_data, "summary": summary}


def query_key_stats(env_dir, date_start: str = '2000-01-01', date_end: str = '9999-12-31',
                    force: bool = False) -> dict:
    """从索引中聚合 key 统计。env_dir 可为单个 root 或 root 列表。"""
    index = _combined_index(env_dir, force=force)

    key_agg: Dict[str, Dict[str, Any]] = {}

    for dir_info in index.get("dirs", {}).values():
        dir_dates = dir_info.get("dates", [])
        if dir_dates and dir_dates[-1] < date_start:
            continue
        if dir_dates and dir_dates[0] > date_end:
            continue

        for kk, counts in dir_info.get("keys", {}).items():
            parts = kk.rsplit("|", 1)
            if len(parts) != 2:
                continue
            raw_key, k_date = parts

            if not (date_start <= k_date <= date_end):
                continue

            if not raw_key:
                raw_key = "(unknown)"
            if raw_key not in key_agg:
                key_agg[raw_key] = {"count": 0, "tok_in": 0, "tok_out": 0, "sessions": 0}
            key_agg[raw_key]["count"] += counts.get("count", 0)
            key_agg[raw_key]["tok_in"] += counts.get("tok_in", 0)
            key_agg[raw_key]["tok_out"] += counts.get("tok_out", 0)
            key_agg[raw_key]["sessions"] += counts.get("sessions", 0)

    keys_list = [
        {
            "key": _mask_api_key(k),
            "count": v["count"], "tok_in": v["tok_in"],
            "tok_out": v["tok_out"],
            "sessions": v["sessions"] or v["count"],
        }
        for k, v in key_agg.items()
    ]
    keys_list.sort(key=lambda x: x["count"], reverse=True)
    return {"keys": keys_list}


def query_channel_stats(env_dir, date_start: str = '2000-01-01', date_end: str = '9999-12-31',
                        force: bool = False) -> dict:
    """从索引中聚合 channel 统计。env_dir 可为单个 root 或 root 列表。"""
    index = _combined_index(env_dir, force=force)

    ch_agg: Dict[str, Dict[str, Any]] = {}

    for dir_info in index.get("dirs", {}).values():
        dir_dates = dir_info.get("dates", [])
        if dir_dates and dir_dates[-1] < date_start:
            continue
        if dir_dates and dir_dates[0] > date_end:
            continue

        for ck, counts in dir_info.get("channels", {}).items():
            parts = ck.rsplit("|", 1)
            if len(parts) != 2:
                continue
            ch_key, c_date = parts

            if not (date_start <= c_date <= date_end):
                continue

            if ch_key not in ch_agg:
                ch_agg[ch_key] = {"count": 0, "tok_in": 0, "tok_out": 0, "sessions": 0}
            ch_agg[ch_key]["count"] += counts.get("count", 0)
            ch_agg[ch_key]["tok_in"] += counts.get("tok_in", 0)
            ch_agg[ch_key]["tok_out"] += counts.get("tok_out", 0)
            ch_agg[ch_key]["sessions"] += counts.get("sessions", 0)

    ch_list = [
        {
            "key": _mask_api_key(k),
            "count": v["count"], "tok_in": v["tok_in"],
            "tok_out": v["tok_out"],
            "sessions": v["sessions"] or v["count"],
        }
        for k, v in ch_agg.items()
    ]
    ch_list.sort(key=lambda x: x["count"], reverse=True)
    return {"channels": ch_list}


def query_channel_keys(env_dir, force: bool = False) -> List[str]:
    """从索引中提取所有 channel key。env_dir 可为单个 root 或 root 列表。"""
    index = _combined_index(env_dir, force=force)
    keys: set = set()
    for dir_info in index.get("dirs", {}).values():
        for ck in dir_info.get("channel_keys_set", []):
            keys.add(ck)
    return sorted(keys)


def query_api_keys(env_dir, force: bool = False) -> List[str]:
    """从索引中提取所有 api key。env_dir 可为单个 root 或 root 列表。"""
    index = _combined_index(env_dir, force=force)
    keys: set = set()
    for dir_info in index.get("dirs", {}).values():
        for ak in dir_info.get("api_keys_set", []):
            keys.add(ak)
    return sorted(keys)


# ---------------------------------------------------------------------------
# 后台预热（与 stats_index._stats_warmer_loop 对齐）
# ---------------------------------------------------------------------------
# token 索引原先没有任何预热：refresh_token_index 只在请求里懒执行，而
# _MEM_TTL/_COMBINED_TTL 都是 600s，于是「重启后第一次」以及「每 10 分钟过期后
# 第一次」都由用户请求全价支付冷成本（实测 ~4.4s，其中 ~4.0s 是 4592 叶的
# 冷 NFS stat 扫描）。5 个 token 端点还会在 _combined_lock 上串行等同一次重建。
#
# 这个线程只做 force=False 的常规刷新：签名命中的叶子走 leaf_status 短路，
# 不扫 index.jsonl、不重写 .token_index.jsonl（changed=False 时 _save_index
# 不触发），把 NFS 属性缓存与内存缓存都维持热态，用户请求恒走无锁快路径。

_token_warmer_thread = None
_token_warmer_stop = threading.Event()
_token_logger = None


def _get_logger():
    global _token_logger
    if _token_logger is None:
        import logging
        _token_logger = logging.getLogger("token-warmer")
    return _token_logger


def _token_warmer_roots(env_dir: str) -> List[str]:
    """预热覆盖的 root 列表：全部已登记源。"""
    roots: List[str] = [env_dir]
    try:
        from utils.logs_config import get_registered_roots
        seen = {os.path.normpath(env_dir)}
        for r in get_registered_roots(env_dir):
            key = os.path.normpath(r)
            if key not in seen:
                seen.add(key)
                roots.append(r)
    except Exception:  # noqa: BLE001
        pass
    return [r for r in roots if os.path.isdir(r)]


def _token_warmer_loop(env_dir: str, interval: float = 120.0) -> None:
    log = _get_logger()
    roots = _token_warmer_roots(env_dir)
    log.info("token-warmer: 开始预热，覆盖 %d 个 root", len(roots))
    while not _token_warmer_stop.is_set():
        try:
            # 走 _combined_index 而非逐 root refresh：顺带把 _combined_cache 也
            # 建好，5 个端点并发时直接命中，不必抢 _combined_lock 重建。
            _combined_index(roots, force=False)
        except Exception:  # noqa: BLE001 — 预热失败绝不能影响服务
            log.warning("token-warmer: 预热失败", exc_info=True)
        _token_warmer_stop.wait(interval)


def start_token_warmer(env_dir: str, interval: float = 120.0) -> None:
    """启动 token 索引预热线程（幂等）。interval 需明显小于 _MEM_TTL(600s)，
    否则缓存会在两次预热之间过期。默认 120s 留足余量。"""
    global _token_warmer_thread
    if _token_warmer_thread is not None and _token_warmer_thread.is_alive():
        return
    _token_warmer_stop.clear()
    _token_warmer_thread = threading.Thread(
        target=_token_warmer_loop,
        args=(env_dir, interval),
        daemon=True,
        name="token-cache-warmer",
    )
    _token_warmer_thread.start()
