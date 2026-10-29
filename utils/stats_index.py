"""
utils/stats_index.py — 增量汇总索引

持久化文件:
  {env_dir}/.stats_index.json       — 目录级扫描索引
  {env_dir}/.session_key_cache.json  — key×date / key×mtime 预聚合缓存

进程级内存缓存: _MEM_TTL 秒内直接返回内存数据，跳过磁盘读取和 stat。

增量机制:
  refresh_index 扫描时对比 old/new sessions 生成 _changed_buckets；
  build_stats_from_index 根据 _changed_buckets 做 bucket 级定向更新，
  结果持久化到 .session_key_cache.json，进程重启后直接加载。
"""

import hashlib
import json
import logging
import os
from utils.atomic_write import safe_replace
import time
import threading
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_INDEX_FILE = ".stats_index.json"
_VERSION = 3
_lock = threading.Lock()

QUALIFIED_THRESHOLD_DEFAULT = 5

# ---------------------------------------------------------------------------
# 进程级内存缓存 — 目录扫描索引（按 env_dir 分槽，避免多目录轮询互相驱逐）
# ---------------------------------------------------------------------------
_MEM_TTL = 600  # 10 分钟：与前端「10 分钟刷新」对齐，避免打开页面短时间内重复重扫
_mem_index_map: Dict[str, dict] = {}       # env_dir_str -> index dict
_mem_index_ts_map: Dict[str, float] = {}   # env_dir_str -> 最后刷新时间

# ---------------------------------------------------------------------------
# 进程级内存缓存 — key 聚合表（按 env_dir 分槽）
# ---------------------------------------------------------------------------
_KEY_CACHE_FILE = ".session_key_cache.json"
_KEY_CACHE_VERSION = 2
_mem_key_cache_map: Dict[str, dict] = {}   # env_dir_str -> key cache dict
_current_key_cache_env: Optional[str] = None  # 最近一次 build 的 env（供 export 读取）

# 只读挂载的 root 无法在原地写缓存，退回项目内目录
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_FALLBACK_CACHE_DIR = _PROJECT_ROOT / "logs" / ".stats_index_cache"


def _writable_cache_path(env_dir: Path, filename: str) -> str:
    """缓存文件路径：env_dir 可写则存原地（兼容旧行为），否则退回项目内。"""
    try:
        if env_dir.is_dir() and os.access(str(env_dir), os.W_OK):
            return str(env_dir / filename)
    except OSError:
        pass
    h = hashlib.md5(os.path.normpath(str(env_dir)).encode()).hexdigest()[:16]
    try:
        os.makedirs(str(_FALLBACK_CACHE_DIR), exist_ok=True)
    except OSError:
        pass
    return str(_FALLBACK_CACHE_DIR / f"{h}-{filename}")


def _index_path(env_dir: Path) -> str:
    return _writable_cache_path(env_dir, _INDEX_FILE)


def _load_index(env_dir: Path) -> dict:
    path = _index_path(env_dir)
    if not os.path.isfile(path):
        return {"_version": _VERSION, "dirs": {}, "updated_at": 0}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if data.get("_version", 0) < _VERSION:
            old_dirs = data.get("dirs", {})
            for d in old_dirs.values():
                d.setdefault("frozen", False)
            return {"_version": _VERSION, "dirs": old_dirs, "updated_at": 0}
        return data
    except (OSError, json.JSONDecodeError):
        return {"_version": _VERSION, "dirs": {}, "updated_at": 0}


def _save_index(env_dir: Path, data: dict) -> None:
    path = _index_path(env_dir)
    try:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        safe_replace(tmp, path)
    except OSError:
        pass


def _scan_session_cache(cache_file: Path, threshold: int) -> dict:
    """读 session 统计数据，优先从 DB 查询，降级到读 .session_cache.jsonl 文件。"""
    root_dir = str(cache_file.parent)
    try:
        import utils.session_store as _ss
        count = _ss.get_session_count_by_root(root_dir)
        if count > 0:
            return _ss.get_session_stats(root_dir, threshold)
    except Exception:
        pass

    # 降级：直接读文件（DB 未初始化或该目录无数据时）
    buckets: Dict[str, Dict[str, int]] = {}
    try:
        with open(cache_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if obj.get("_meta"):
                    continue
                api_key = obj.get("api_key", "") or "(empty)"
                ts = obj.get("first_ts", "")
                date_str = ts.split("_")[0] if ts else "unknown"
                bucket_key = f"{api_key}|{date_str}"
                if bucket_key not in buckets:
                    buckets[bucket_key] = {"total": 0, "qualified": 0}
                buckets[bucket_key]["total"] += 1
                if len(obj.get("trace_list", [])) >= threshold:
                    buckets[bucket_key]["qualified"] += 1
    except OSError:
        pass
    return buckets


def _count_req_files(dir_path: Path) -> int:
    try:
        return sum(1 for f in dir_path.iterdir() if f.name.endswith("-req.json"))
    except OSError:
        return 0


def _sessions_from_index(leaf: Path) -> dict:
    """无会话链信息的 root（如 new-api）时，直接从 index.jsonl 按 api_key×date 聚合。

    每条请求算 1 个 session（total），qualified 无法判定 → 记 0。
    返回 {"{api_key}|{date}": {"total": n, "qualified": 0}}。
    """
    from utils.log_scan import normalize_entry
    buckets: Dict[str, Dict[str, int]] = {}
    index_file = leaf / "index.jsonl"
    try:
        with open(index_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if obj.get("_meta"):
                    continue
                e = normalize_entry(obj)
                api_key = e["api_key"] or "(empty)"
                bucket_key = f"{api_key}|{e['date']}"
                if bucket_key not in buckets:
                    buckets[bucket_key] = {"total": 0, "qualified": 0}
                buckets[bucket_key]["total"] += 1
    except OSError:
        pass
    return buckets


def _sub_is_newapi(sub: Path) -> bool:
    """叶子是否为 new-api 格式（无 -req.json 但有 index.jsonl）。用于统计分支择路。"""
    try:
        from utils.log_scan import detect_format
        return detect_format(str(sub)) == "newapi"
    except Exception:
        return False


def _sessions_lazy_backfill(leaf: Path, threshold: int) -> tuple:
    """new-api 叶子的统计：(sessions 字典, 是否为裸计数近似)。

    已构建（sessions>0）→ newapi_index_db.get_session_stats（真实归并值），第二项 False。
    未构建 → 返回裸计数（每请求算 1 session，近似偏大），第二项 True。

    第二项存在的意义：裸计数**绝不能**落进 leaf_status.stats_json。否则 index.db
    建好之后，index.jsonl 的 size/mtime 没变，签名断言「缓存有效」，这份偏大
    5~9 倍的估算就被永久固化（实测 1668 个叶子中招，如某叶 stats=208014 而真实
    sessions=22804）。调用方据此决定是否跳过回写。
    """
    try:
        import utils.newapi_index_db as nidb
        if nidb.status(str(leaf)).get("sessions", 0) > 0:
            return nidb.get_session_stats(str(leaf), threshold), False
    except Exception:
        pass
    # 未构建：裸计数近似（前端在未构建时会标「聚合中·近似」），不阻塞
    return _sessions_from_index(leaf), True


def _get_active_tag() -> str:
    try:
        from utils.log_paths import STARTUP_DATE_TAG
        return STARTUP_DATE_TAG
    except ImportError:
        return ""


def _resolve_rid(env_dir: Path) -> Optional[str]:
    """解析 env_dir 对应的真实存储 root_id（镜像 log_routes._export_view_leaves）。

    优先按归一化 root_path 查 sources 行的真实 root_id（活跃 env 目录的叶子挂在
    md5 哈希 root_id 下）；查不到再退 get_root_id(norm)——**不传** active_env_dir，
    否则活跃 env 会被折叠成 'default'，bulk_get('default') 恒空。
    """
    try:
        import utils.logdir_store as _lds
        from utils.logs_config import get_root_id
    except Exception:
        return None
    norm = os.path.normpath(str(env_dir))
    rid = None
    try:
        src = _lds.get_source_by_path(norm)
        if src:
            rid = src.get("root_id")
    except Exception:
        rid = None
    if not rid:
        try:
            rid = get_root_id(norm)
        except Exception:
            rid = None
    return rid


def _leaf_status_subs(env_dir: Path, rid: str):
    """从 leaf_status 免扫盘枚举叶子，返回 [(sub: Path, row: dict|None), ...]。

    row 为该叶的统计缓存行（含 idx_size/idx_mtime/stats_json/stats_threshold），
    供签名判活；活跃小时叶子（可能还没进 leaf_status）并入且 row=None。
    去重按叶子绝对路径归一化。

    不在枚举时逐叶 is_dir()：那是冷路径上最贵的一步（4595 次 NFS 元数据查询，
    属性缓存冷时约 0.25ms/次），而下游 refresh_index / refresh_token_index 对
    每叶的 `stat(index.jsonl)` 已经能证明目录存在，且都 try/except OSError。
    这里只保留"路径存在性"的轻量过滤交给下游，省掉一次往返/叶。
    """
    import utils.logdir_store as _lds
    norm = os.path.normpath(str(env_dir))
    out = []
    seen = set()
    for row in _lds.bulk_get_stats(rid):
        lp = row.get("leaf_path") or ""
        if not lp:
            continue
        # 相对 root 登记的 leaf_path 本身可能就是相对 root 的路径（如
        # 'logs_all/env-99oR/26081109'），此时不能再拼一次 root，否则得到
        # 'logs_all/env-99oR/logs_all/env-99oR/26081109'，叶子被静默丢弃。
        if os.path.isabs(lp):
            full = lp
        elif os.path.normpath(lp) == norm or lp.startswith(norm + os.sep):
            full = lp
        else:
            full = os.path.join(norm, lp)
        key = os.path.normpath(full)
        if key in seen:
            continue
        seen.add(key)
        out.append((Path(full), row))
    # 并入活跃小时叶子（新建的小时目录可能还没同步进 leaf_status，且它永不冻结）
    active_tag = _get_active_tag()
    if active_tag:
        ap = env_dir / active_tag
        akey = os.path.normpath(str(ap))
        if akey not in seen:
            seen.add(akey)
            out.append((ap, None))
    return out


def _diff_sessions(old_sessions: dict, new_sessions: dict, dir_name: str) -> list:
    """对比两个 sessions dict，返回 [(dir_name, bucket_key, old_counts, new_counts), ...]"""
    diffs = []
    all_keys = set(old_sessions) | set(new_sessions)
    for bk in all_keys:
        old_c = old_sessions.get(bk)
        new_c = new_sessions.get(bk)
        if old_c != new_c:
            diffs.append((dir_name, bk, old_c, new_c))
    return diffs


def refresh_index(env_dir: Path, threshold: int = QUALIFIED_THRESHOLD_DEFAULT,
                  force: bool = False, bust_ttl: bool = False) -> dict:
    """增量刷新索引，返回最新的 index data。

    - TTL 内直接返回内存缓存（<1ms）
    - 过期后只 stat 活跃目录，frozen 目录跳过
    - force=True 跳过 TTL + frozen，全量检查
    - bust_ttl=True 跳过 10s 内存 TTL 早返回（强制重扫签名），但仍走 force=False
      的签名短路（未变叶子命中缓存、不重算）。供「刷新」按钮用。

    index["_changed_buckets"] 记录本次刷新中变化的 bucket 列表，
    供 build_stats_from_index 做定向增量更新。
    """
    from utils.log_scan import iter_index_dirs, dir_key_for

    env_dir_str = os.path.normpath(str(env_dir))
    now = time.time()

    cached = _mem_index_map.get(env_dir_str)
    if (not force and not bust_ttl and cached is not None
            and (now - _mem_index_ts_map.get(env_dir_str, 0)) < _MEM_TTL):
        cached["_changed_buckets"] = []
        return cached

    with _lock:
        cached = _mem_index_map.get(env_dir_str)
        if (not force and not bust_ttl and cached is not None
                and (time.time() - _mem_index_ts_map.get(env_dir_str, 0)) < _MEM_TTL):
            cached["_changed_buckets"] = []
            return cached

        index = cached if cached else _load_index(env_dir)
        dirs_cache = index.get("dirs", {})
        changed = False
        active_tag = _get_active_tag()
        changed_buckets: list = []

        # new-api：页面刷新**不再自动触发回填**（回填是重操作、会起进程池，只应在数据管理
        # 界面手动执行）。未构建的叶子在下方 use_index 分支走裸计数近似（前端标「聚合中·近似」）；
        # 手动回填跑完后，下次刷新读到 sessions 即自动收敛为归并值。

        # 叶子枚举来源：已同步的 root（leaf_status 有记录）走**免扫盘**枚举 +
        # 签名判活（下方 sig 短路）；未同步/本地 root 回退 iter_index_dirs 全盘 walk。
        rid = _resolve_rid(env_dir)
        use_leaf_db = False
        if rid:
            try:
                import utils.logdir_store as _lds
                use_leaf_db = _lds.has_any(rid)
            except Exception:
                use_leaf_db = False
        if use_leaf_db:
            leaf_iter = _leaf_status_subs(env_dir, rid)
        else:
            leaf_iter = [(sub, None) for sub in iter_index_dirs(env_dir)]
        # dir_name → (sub, leaf_row)：供循环后按签名回写 leaf_status 统计缓存。
        sub_by_dir: Dict[str, tuple] = {}

        current_dirs = set()
        # 本轮**真正重算**过的目录。回写 leaf_status 时只有它们才能带上新的
        # stats_sync_rowid —— 否则会把「沿用旧缓存」的 sessions 盖上最新水位，
        # 让过期快照永远看起来有效（实测某叶 index.db 有 26 个 key、stats_json
        # 只有 24 个，且再也不会自愈）。
        _recomputed: set = set()
        for sub, leaf_row in leaf_iter:
                dir_name = dir_key_for(env_dir, sub)
                current_dirs.add(dir_name)
                sub_by_dir[dir_name] = (sub, leaf_row)

                prev = dirs_cache.get(dir_name)

                # ── 已归档叶子短路（备份上传 OBS 后删了本地目录）────────────
                # 必须放在所有触盘分支之前。归档叶的 index.jsonl / cache_file 都
                # 已不存在、db_count=0，落到下面的重算分支会得到 new_sessions={}，
                # 把该叶缓存的统计清零 —— /sessions 的总量会静默下降。内容还在
                # OBS 上，统计快照也还在 leaf_status.stats_json 里，直接沿用即可。
                # 注意仍然 current_dirs.add（上面已加），否则会进入 removed 集合，
                # 效果与清零相同。
                if leaf_row is not None and leaf_row.get("archived"):
                    raw = leaf_row.get("stats_json") or ""
                    if raw:
                        try:
                            cached_sessions = json.loads(raw)
                        except (ValueError, TypeError):
                            cached_sessions = None
                        if isinstance(cached_sessions, dict):
                            entry = dict(prev) if prev else {}
                            entry["sessions"] = cached_sessions
                            entry["frozen"] = True
                            entry["archived"] = True
                            if entry != prev:
                                dirs_cache[dir_name] = entry
                                changed = True
                            continue
                    # 没有快照（不该发生）：沿用内存里的旧值，凭空造数据更糟。
                    if prev is not None:
                        continue

                # frozen 快路径：历史目录不重扫。但必须先确认 index.db 的同步水位
                # 没有推进过 —— watcher 折叠新 session 时不改 index.jsonl，
                # 光看 frozen 会让快照永远停在旧内容（实测某叶 index.db 有 26 个
                # key、stats_json 只有 24 个，且不会自愈）。
                _sig_stale = (leaf_row is not None
                              and leaf_row.get("stats_sync_rowid", -1) != leaf_row.get("sync_rowid", -1))
                if (not force and prev
                        and prev.get("frozen")
                        and dir_name != active_tag
                        and not _sig_stale):
                    continue

                # ── leaf_status 统计缓存签名短路 ──────────────────────────────
                # 一次 stat index.jsonl（不开 SQLite）：size+mtime 未变、threshold
                # 相符、且**同步水位未推进**→ 命中，直接用缓存 sessions，跳过下方
                # 所有磁盘重扫/聚合。任一不满足 → 落下方原分支重算，循环末尾再
                # 回写 leaf_status（见 _writeback）。
                #
                # sync_rowid 这一条必不可少：watcher 把新 session 折叠进 index.db
                # 时并不改 index.jsonl，只靠 size+mtime 会让快照「名义有效」却落后
                # 于 index.db（实测某叶 sessions 有 26 个 key、stats_json 只有 24）。
                # 两个 rowid 都是本地 log_dir.db 里的整数，比对不触网。
                if (not force and use_leaf_db and leaf_row is not None
                        and leaf_row.get("stats_json")
                        and leaf_row.get("stats_threshold") == threshold
                        and leaf_row.get("stats_sync_rowid", -1) == leaf_row.get("sync_rowid", -1)):
                    try:
                        ist = (sub / "index.jsonl").stat()
                    except OSError:
                        ist = None
                    if (ist is not None
                            and leaf_row.get("idx_size") == ist.st_size
                            and leaf_row.get("idx_mtime") == ist.st_mtime):
                        try:
                            cached_sessions = json.loads(leaf_row["stats_json"])
                        except (ValueError, TypeError):
                            cached_sessions = None
                        if cached_sessions is not None:
                            old_sessions = prev.get("sessions", {}) if prev else {}
                            changed_buckets.extend(
                                _diff_sessions(old_sessions, cached_sessions, dir_name))
                            entry = dict(prev) if prev else {}
                            entry["sessions"] = cached_sessions
                            entry["idx_size"] = ist.st_size
                            entry["idx_mtime"] = ist.st_mtime
                            entry["frozen"] = dir_name != active_tag
                            dirs_cache[dir_name] = entry
                            changed = True
                            continue
                # ── 签名未命中：落原分支重算 ───────────────────────────────────

                cache_file = sub / ".session_cache.jsonl"
                if not cache_file.is_file():
                    # .session_cache.jsonl 已不在运行时生成（数据迁到 session_cache.db）。
                    # 文件不存在不代表无 session：先查 DB，有数据就从 DB 聚合，
                    # 否则退回 req 文件计数占位。
                    # new-api 叶子的数据在叶子内 index.db（非 per-port session_cache.db），
                    # 跳过 session_store 检查，直接走下方 use_index 分支（避免旧库残留数据反噬）。
                    db_count = 0
                    if not _sub_is_newapi(sub):
                        try:
                            import utils.session_store as _ss
                            db_count = _ss.get_session_count_by_root(str(sub))
                        except Exception:
                            db_count = 0

                    if db_count > 0:
                        # DB 有数据：count 未变则跳过重扫（省一次聚合查询）
                        if (not force and prev
                                and prev.get("db_count") == db_count):
                            if dir_name != active_tag and not prev.get("frozen"):
                                prev["frozen"] = True
                                changed = True
                            continue

                        sessions = _scan_session_cache(cache_file, threshold)
                        old_sessions = prev.get("sessions", {}) if prev else {}
                        changed_buckets.extend(_diff_sessions(old_sessions, sessions, dir_name))

                        try:
                            dir_mt = os.path.getmtime(str(sub))
                        except OSError:
                            dir_mt = 0
                        if prev and prev.get("req_count_mtime") == dir_mt and prev.get("req_count", 0) > 0:
                            req_count = prev["req_count"]
                            req_mt = prev["req_count_mtime"]
                        else:
                            req_count = _count_req_files(sub)
                            req_mt = dir_mt

                        dirs_cache[dir_name] = {
                            "cache_mtime": 0,
                            "cache_size": 0,
                            "db_count": db_count,
                            "req_count": req_count,
                            "req_count_mtime": req_mt,
                            "frozen": dir_name != active_tag,
                            "sessions": sessions,
                        }
                        _recomputed.add(dir_name)
                        changed = True
                        continue

                    # DB 无数据。区分两种情况：
                    #   1) 有 -req.json（本项目目录，会话尚未消费）→ 仅 req 计数占位
                    #   2) 无 -req.json 但有 index.jsonl（new-api）→ 从 index 聚合 sessions
                    req_count = _count_req_files(sub)
                    index_file = sub / "index.jsonl"
                    use_index = (req_count == 0 and index_file.is_file())

                    try:
                        dir_mt = os.path.getmtime(str(sub))
                        idx_mt = index_file.stat().st_mtime if use_index else dir_mt
                    except OSError:
                        dir_mt = 0
                        idx_mt = 0

                    marker_mt = idx_mt if use_index else dir_mt
                    if prev and not force and prev.get("req_count_mtime") == marker_mt:
                        if dir_name != active_tag and not prev.get("frozen"):
                            prev["frozen"] = True
                            changed = True
                        continue

                    old_sessions = prev.get("sessions", {}) if prev else {}
                    # new-api（use_index）：懒回填——consume_leaf 进 DB 后按 q1 归并口径读，
                    # 而非裸计数（每请求 1 session）。首次访问会稍慢，之后走 db_count>0 快路径。
                    _approx = False
                    if use_index:
                        new_sessions, _approx = _sessions_lazy_backfill(sub, threshold)
                    else:
                        new_sessions = {}
                    changed_buckets.extend(_diff_sessions(old_sessions, new_sessions, dir_name))
                    if use_index:
                        req_count = sum(v["total"] for v in new_sessions.values())
                    dirs_cache[dir_name] = {
                        "cache_mtime": 0,
                        "cache_size": 0,
                        "req_count": req_count,
                        "req_count_mtime": marker_mt,
                        "frozen": dir_name != active_tag,
                        "sessions": new_sessions,
                        # 裸计数近似值标脏：回写 leaf_status 时跳过，否则会被签名
                        # 「永久有效」固化下来（实测 1668 叶中招，偏大 5~9 倍）。
                        "stats_approx": _approx,
                    }
                    _recomputed.add(dir_name)
                    changed = True
                    continue

                try:
                    st = cache_file.stat()
                    c_mtime = st.st_mtime
                    c_size = st.st_size
                except OSError:
                    continue

                if (not force and prev
                        and prev.get("cache_mtime") == c_mtime
                        and prev.get("cache_size") == c_size):
                    if dir_name != active_tag and not prev.get("frozen"):
                        prev["frozen"] = True
                        changed = True
                    continue

                sessions = _scan_session_cache(cache_file, threshold)

                old_sessions = prev.get("sessions", {}) if prev else {}
                changed_buckets.extend(_diff_sessions(old_sessions, sessions, dir_name))

                try:
                    dir_mt = os.path.getmtime(str(sub))
                except OSError:
                    dir_mt = 0
                if prev and prev.get("req_count_mtime") == dir_mt and prev.get("req_count", 0) > 0:
                    req_count = prev["req_count"]
                    req_mt = prev["req_count_mtime"]
                else:
                    req_count = _count_req_files(sub)
                    req_mt = dir_mt

                dirs_cache[dir_name] = {
                    "cache_mtime": c_mtime,
                    "cache_size": c_size,
                    "req_count": req_count,
                    "req_count_mtime": req_mt,
                    "frozen": dir_name != active_tag,
                    "sessions": sessions,
                }
                _recomputed.add(dir_name)
                changed = True

        # ── 回写 leaf_status 统计缓存 ──────────────────────────────────────────
        # 只在走 leaf_db 枚举时回写。对本次枚举到的每个叶子，若其内存 sessions 已定，
        # 且 (签名/threshold) 与 leaf_row 里记录的不同，就 stat 一次 index.jsonl 落库。
        # 命中签名短路的叶子其 leaf_row 已相符 → 跳过（不产生写）；只有真正重算/首建
        # 的叶子才写，避免每次刷新对 2018 叶全表写。
        if use_leaf_db:
            for dir_name, (sub, leaf_row) in sub_by_dir.items():
                # 只回写已在 leaf_status 里的叶子；活跃小时叶子（leaf_row is None）
                # 还没进表，且它每请求都重算（永不冻结），不必落库污染叶子管理计数。
                if leaf_row is None:
                    continue
                entry = dirs_cache.get(dir_name)
                if not entry:
                    continue
                sessions = entry.get("sessions")
                if sessions is None:
                    continue
                idx_size = entry.get("idx_size")
                idx_mtime = entry.get("idx_mtime")
                if idx_size is None or idx_mtime is None:
                    try:
                        ist = (sub / "index.jsonl").stat()
                        idx_size, idx_mtime = ist.st_size, ist.st_mtime
                    except OSError:
                        continue
                # 与库中记录一致（签名短路命中那批）→ 无需重复写
                if (leaf_row.get("idx_size") == idx_size
                        and leaf_row.get("idx_mtime") == idx_mtime
                        and leaf_row.get("stats_threshold") == threshold
                        and leaf_row.get("stats_sync_rowid", -1) == leaf_row.get("sync_rowid", -1)
                        and not entry.get("stats_approx")):
                    continue
                # 裸计数近似值绝不落库：index.db 建好后 index.jsonl 的 size/mtime
                # 不会变，一旦写进去就被签名「永久有效」固化，偏大 5~9 倍且不自愈。
                # 已存在的这类脏数据由 session_worker 按 sync_rowid 水位纠偏。
                if entry.get("stats_approx"):
                    continue
                # 只有本轮真正重算过的目录才能盖上最新水位。沿用了旧缓存的目录
                # （frozen 快路径 / marker_mt 短路）其 sessions 仍是旧快照，
                # 若也盖新水位，过期数据就永远看起来有效、再也不会自愈。
                # 对这类目录写 stats_sync_rowid=-1，下次刷新会继续判过期并重算。
                _new_stats_rowid = (leaf_row.get("sync_rowid", -1)
                                    if dir_name in _recomputed else -1)
                try:
                    _lds.set_leaf_stats(
                        rid, dir_name, leaf_path=str(sub),
                        idx_size=idx_size, idx_mtime=idx_mtime,
                        stats_json=json.dumps(sessions, ensure_ascii=False),
                        stats_threshold=threshold,
                        stats_sync_rowid=_new_stats_rowid,
                    )
                except Exception:
                    pass

        removed = set(dirs_cache.keys()) - current_dirs
        if removed:
            for r in removed:
                old_sessions = dirs_cache[r].get("sessions", {})
                for bk, counts in old_sessions.items():
                    changed_buckets.append((r, bk, counts, None))
                del dirs_cache[r]
            changed = True

        if changed:
            index["dirs"] = dirs_cache
            index["updated_at"] = time.time()
            _save_index(env_dir, index)

        index["_changed_buckets"] = changed_buckets

        _mem_index_map[env_dir_str] = index
        _mem_index_ts_map[env_dir_str] = time.time()

        return index


# ---------------------------------------------------------------------------
# key 聚合缓存 — 持久化 + 增量更新
# ---------------------------------------------------------------------------

def _key_cache_path(env_dir: Path) -> str:
    return _writable_cache_path(env_dir, _KEY_CACHE_FILE)


def _load_key_cache(env_dir: Path) -> Optional[dict]:
    path = _key_cache_path(env_dir)
    if not os.path.isfile(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        v = data.get("_version", 0)
        if v < 1:
            return None
        if v < _KEY_CACHE_VERSION:
            data["_version"] = _KEY_CACHE_VERSION
            data.setdefault("key_meta", {})
            data.setdefault("key_records", {})
        return data
    except (OSError, json.JSONDecodeError):
        return None


def _save_key_cache(env_dir: Path, cache: dict) -> None:
    path = _key_cache_path(env_dir)
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False)
        safe_replace(tmp, path)
    except OSError:
        pass


def _ensure_cell(table: dict, api_key: str, sub_key: str) -> dict:
    if api_key not in table:
        table[api_key] = {}
    if sub_key not in table[api_key]:
        table[api_key][sub_key] = {"total": 0, "qualified": 0}
    return table[api_key][sub_key]


def _apply_delta(cache: dict, dir_name: str, bucket_key: str,
                 old_c: Optional[dict], new_c: Optional[dict]) -> None:
    """对 cache 的 table/mtime_table/totals 做一次 bucket 级增量。"""
    parts = bucket_key.split("|", 1)
    if len(parts) != 2:
        return
    api_key, date_str = parts

    table = cache["table"]
    mtime_table = cache["mtime_table"]
    totals = cache["totals"]

    if old_c:
        cell = _ensure_cell(table, api_key, date_str)
        cell["total"] -= old_c.get("total", 0)
        cell["qualified"] -= old_c.get("qualified", 0)

        mt_cell = _ensure_cell(mtime_table, api_key, dir_name)
        mt_cell["total"] -= old_c.get("total", 0)
        mt_cell["qualified"] -= old_c.get("qualified", 0)

        totals["total"] -= old_c.get("total", 0)
        totals["qualified"] -= old_c.get("qualified", 0)

    if new_c:
        cell = _ensure_cell(table, api_key, date_str)
        cell["total"] += new_c.get("total", 0)
        cell["qualified"] += new_c.get("qualified", 0)

        mt_cell = _ensure_cell(mtime_table, api_key, dir_name)
        mt_cell["total"] += new_c.get("total", 0)
        mt_cell["qualified"] += new_c.get("qualified", 0)

        totals["total"] += new_c.get("total", 0)
        totals["qualified"] += new_c.get("qualified", 0)


def _cleanup_zeros(cache: dict) -> None:
    """清理 total=0 的空条目。"""
    for tbl in (cache["table"], cache["mtime_table"]):
        empty_keys = []
        for api_key, sub in tbl.items():
            empty_subs = [k for k, v in sub.items() if v.get("total", 0) <= 0]
            for es in empty_subs:
                del sub[es]
            if not sub:
                empty_keys.append(api_key)
        for ek in empty_keys:
            del tbl[ek]


def _full_build_key_cache(index: dict) -> dict:
    """从 index 全量构建 key cache。"""
    table: Dict[str, Dict[str, Dict[str, int]]] = {}
    mtime_table: Dict[str, Dict[str, Dict[str, int]]] = {}
    total = 0
    qualified = 0

    for dir_name, dir_info in index.get("dirs", {}).items():
        sessions = dir_info.get("sessions", {})
        if not sessions:
            continue
        for bucket_key, counts in sessions.items():
            parts = bucket_key.split("|", 1)
            if len(parts) != 2:
                continue
            api_key, date_str = parts
            t = counts["total"]
            q = counts["qualified"]
            total += t
            qualified += q

            cell = _ensure_cell(table, api_key, date_str)
            cell["total"] += t
            cell["qualified"] += q

            mt_cell = _ensure_cell(mtime_table, api_key, dir_name)
            mt_cell["total"] += t
            mt_cell["qualified"] += q

    return {
        "_version": _KEY_CACHE_VERSION,
        "updated_at": time.time(),
        "table": table,
        "mtime_table": mtime_table,
        "totals": {"total": total, "qualified": qualified},
        "key_meta": {},
        "key_records": {},
    }


def _build_rows(cache: dict, threshold: int, include_rows: bool = True) -> dict:
    """从 key cache 构建 build_stats_from_index 的输出格式。

    include_rows=False 时跳过 rows 明细（key×date 的 cells + key×mtime 的
    mtime_cells）——本机实测这部分占整个响应 99.9%（2.8MB / 2287 行 /
    39036 个 mtime 单元），而首页、/sessions、/overview 只读 total_sessions /
    qualified_sessions / threshold / dates。跳过它省掉每请求 ~0.45s 的纯 Python
    重建与序列化，只保留几个标量。
    """
    table = cache.get("table", {})
    totals = cache.get("totals", {})

    all_dates: set = set()
    for sub in table.values():
        all_dates.update(sub.keys())
    dates = sorted(all_dates)

    result = {
        "total_sessions": totals.get("total", 0),
        "qualified_sessions": totals.get("qualified", 0),
        "threshold": threshold,
        "dates": dates,
    }
    if not include_rows:
        return result

    mtime_table = cache.get("mtime_table", {})
    all_mtimes: set = set()
    for sub in mtime_table.values():
        all_mtimes.update(sub.keys())
    mtimes = sorted(all_mtimes)
    keys = sorted(table.keys())

    rows = []
    for key in keys:
        row_total = 0
        row_qualified = 0
        cells = {}
        for d in dates:
            c = table[key].get(d, {"total": 0, "qualified": 0})
            if c["total"] > 0:
                cells[d] = {"total": c["total"], "qualified": c["qualified"]}
                row_total += c["total"]
                row_qualified += c["qualified"]
        mtime_cells = {}
        for mt in mtimes:
            mc = mtime_table.get(key, {}).get(mt, {"total": 0, "qualified": 0})
            if mc["total"] > 0:
                mtime_cells[mt] = {"total": mc["total"], "qualified": mc["qualified"]}
        if row_total > 0:
            rows.append({
                "api_key": key,
                "cells": cells,
                "mtime_cells": mtime_cells,
                "row_total": row_total,
                "row_qualified": row_qualified,
            })

    result["rows"] = rows
    return result


def build_stats_from_leaf_db(threshold: int = QUALIFIED_THRESHOLD_DEFAULT,
                             roots: Optional[List[str]] = None,
                             active_env_dir: Optional[str] = None,
                             include_rows: bool = True) -> Optional[dict]:
    """直接用 leaf_status.stats_json 重建 build_stats_multi 的返回结构。

    返回 None 表示「不适合走这条快路径」（调用方回退到 build_stats_multi）：
      - log_dir.db 未初始化 / 没有任何可用的 stats_json
      - 存在 threshold 不符的叶子（口径不一致，混用会得到错的 qualified）
      - 存在快照内容与 sessions 矛盾的叶子（裸计数近似等脏数据）

    为什么快：build_stats_multi 的 4.89s 里有 4.29s 是 refresh_index 扫盘
    （逐叶 stat index.jsonl + 解析 index.jsonl），而同样的聚合结果在同步阶段
    已经算好落库了。这里只读 4709 行本地行 + 解析 JSON，实测 0.24s。

    结构与 build_stats_multi 完全一致（total_sessions / qualified_sessions /
    threshold / dates / rows），其中 rows[].mtime_cells 的 key 同样带
    ``<root_id>/`` 前缀（多 root 时）。
    """
    import utils.logdir_store as _lds
    from utils.logs_config import get_root_id

    if not _lds._ready():
        return None
    root_list = roots or []
    if not root_list:
        return None

    multi = len(root_list) > 1
    table: Dict[str, Dict[str, Dict[str, int]]] = {}
    mtime_table: Dict[str, Dict[str, Dict[str, int]]] = {}
    total_sessions = 0
    qualified_sessions = 0
    dates: set = set()
    leaves = 0

    for root in root_list:
        rid = get_root_id(root, active_env_dir)
        for row in _lds.bulk_get_stats(rid):
            raw = row.get("stats_json") or ""
            if not raw:
                return None        # 有叶子没快照：结果会偏小，回退
            if row.get("stats_threshold") != threshold:
                return None        # 口径不符：qualified 不可比，回退
            try:
                buckets = _json_loads(raw)
            except (ValueError, TypeError):
                return None
            if not isinstance(buckets, dict):
                return None
            dk = row.get("dir_key") or ""
            mt = f"{rid}/{dk}" if multi else dk
            leaf_total = 0
            for bucket_key, counts in buckets.items():
                api_key, _, date_str = bucket_key.rpartition("|")
                if not api_key:
                    continue
                t = counts.get("total", 0)
                q = counts.get("qualified", 0)
                leaf_total += t
                total_sessions += t
                qualified_sessions += q
                dates.add(date_str)
                cell = table.setdefault(api_key, {}).setdefault(date_str, {"total": 0, "qualified": 0})
                cell["total"] += t
                cell["qualified"] += q
                mtcell = mtime_table.setdefault(api_key, {}).setdefault(mt, {"total": 0, "qualified": 0})
                mtcell["total"] += t
                mtcell["qualified"] += q
            # 内容校验：快照总量必须等于库里记录的会话数。不等说明这份快照是
            # 「index.db 未构建时的裸计数近似」，偏大 5~9 倍（实测 1672 个叶子）。
            if leaf_total != (row.get("sessions") or 0):
                return None
            leaves += 1

    if not leaves:
        return None

    result = {
        "total_sessions": total_sessions,
        "qualified_sessions": qualified_sessions,
        "threshold": threshold,
        "dates": sorted(dates),
    }
    if include_rows:
        rows = []
        for api_key, cells in table.items():
            row_total = sum(c["total"] for c in cells.values())
            rows.append({
                "api_key": api_key,
                "cells": {d: dict(c) for d, c in cells.items() if c["total"] > 0},
                "mtime_cells": {m: dict(c) for m, c in mtime_table.get(api_key, {}).items()
                                if c["total"] > 0},
                "row_total": row_total,
                "row_qualified": sum(c["qualified"] for c in cells.values()),
            })
        rows.sort(key=lambda r: r["row_total"], reverse=True)
        result["rows"] = rows
    return result


def _json_loads(raw: str):
    import json as _j
    return _j.loads(raw)


def build_stats_multi(roots: List[str], threshold: int = QUALIFIED_THRESHOLD_DEFAULT,
                      force: bool = False, active_env_dir: Optional[str] = None,
                      bust_ttl: bool = False, include_rows: bool = True) -> dict:
    """跨多个 root 刷新 + 构建 session 统计，合并 rows / dates / totals。

    每个 root 独立走 refresh_index + build_stats_from_index（各自缓存），
    再按 api_key 合并 cells / mtime_cells / 总数。

    bust_ttl=True（「刷新」按钮）跳过 10s 内存 TTL 早返回、强制重扫签名，但仍
    force=False 享受签名短路（未变叶子命中缓存）。

    include_rows=False 时不下传 rows 构建（见 _build_rows），只返回标量 +
    dates；供只需要总数/达标数的页面使用，省掉每请求 ~0.45s 的重建。
    """
    # 快路径：直接读 leaf_status 里同步阶段算好的聚合结果（实测 4.89s → 0.24s）。
    # 仅当全部叶子都通过「threshold 一致 + 快照总量与 sessions 相符」校验时采用；
    # 任何一个叶子不可信就整体回退到扫盘路径 —— 宁可慢，也不给导出页错的数字。
    if not force and not bust_ttl:
        fast = build_stats_from_leaf_db(threshold, roots, active_env_dir, include_rows)
        if fast is not None:
            return fast

    merged_rows: Dict[str, dict] = {}
    all_dates: set = set()
    total_sessions = 0
    qualified_sessions = 0

    for root in roots:
        env_path = Path(root)
        index = refresh_index(env_path, threshold, force=force, bust_ttl=bust_ttl)
        stats = build_stats_from_index(index, threshold, env_dir=env_path,
                                       include_rows=include_rows)

        total_sessions += stats.get("total_sessions", 0)
        qualified_sessions += stats.get("qualified_sessions", 0)
        all_dates.update(stats.get("dates", []))

        for row in stats.get("rows", []):
            api_key = row["api_key"]
            if api_key not in merged_rows:
                merged_rows[api_key] = {
                    "api_key": api_key,
                    "cells": {},
                    "mtime_cells": {},
                    "row_total": 0,
                    "row_qualified": 0,
                }
            m = merged_rows[api_key]
            m["row_total"] += row.get("row_total", 0)
            m["row_qualified"] += row.get("row_qualified", 0)
            for d, c in row.get("cells", {}).items():
                cell = m["cells"].setdefault(d, {"total": 0, "qualified": 0})
                cell["total"] += c.get("total", 0)
                cell["qualified"] += c.get("qualified", 0)
            # mtime_cells 的 key 已是相对 root 的 dir_key，可能跨 root 重名，
            # 加 root_id 前缀防冲突（root_id 稳定且消除同 basename 碰撞）
            from utils.logs_config import get_root_id
            prefix = get_root_id(root, active_env_dir)
            for mt, c in row.get("mtime_cells", {}).items():
                key = f"{prefix}/{mt}" if len(roots) > 1 else mt
                cell = m["mtime_cells"].setdefault(key, {"total": 0, "qualified": 0})
                cell["total"] += c.get("total", 0)
                cell["qualified"] += c.get("qualified", 0)

    result = {
        "total_sessions": total_sessions,
        "qualified_sessions": qualified_sessions,
        "threshold": threshold,
        "dates": sorted(all_dates),
    }
    if include_rows:
        result["rows"] = sorted(merged_rows.values(), key=lambda r: r["row_total"], reverse=True)
    return result


def build_stats_from_index(index: dict, threshold: int = QUALIFIED_THRESHOLD_DEFAULT,
                           env_dir: Optional[Path] = None,
                           include_rows: bool = True) -> dict:
    """从索引数据构建统计结果，支持 bucket 级增量更新 + 磁盘持久化。

    三条路径:
      A) _changed_buckets 为空 → 直接从内存 key cache 构建 rows
      B) _changed_buckets 非空 → 定向增量更新 key cache，持久化后构建 rows
      C) 首次 / env 切换 → 全量构建，持久化后构建 rows

    include_rows=False 跳过 rows 明细构建（见 _build_rows 说明），只产出标量。
    """
    global _current_key_cache_env

    changed_buckets = index.get("_changed_buckets")
    _env_dir = env_dir
    if _env_dir is None:
        _env_dir = Path(_current_key_cache_env) if _current_key_cache_env else None
    env_dir_str = os.path.normpath(str(_env_dir)) if _env_dir else ""

    mem_cache = _mem_key_cache_map.get(env_dir_str)
    is_same_env = mem_cache is not None

    if is_same_env and changed_buckets is not None and len(changed_buckets) == 0:
        _current_key_cache_env = env_dir_str
        return _build_rows(mem_cache, threshold, include_rows=include_rows)

    if is_same_env and changed_buckets:
        for dir_name, bk, old_c, new_c in changed_buckets:
            _apply_delta(mem_cache, dir_name, bk, old_c, new_c)
        _cleanup_zeros(mem_cache)
        mem_cache["updated_at"] = time.time()
        if _env_dir:
            _save_key_cache(_env_dir, mem_cache)
        _current_key_cache_env = env_dir_str
        return _build_rows(mem_cache, threshold, include_rows=include_rows)

    if _env_dir and not is_same_env:
        disk_cache = _load_key_cache(_env_dir)
        if disk_cache:
            idx_dirs = {d for d, info in index.get("dirs", {}).items() if info.get("sessions")}
            cache_dirs = set()
            for sub in disk_cache.get("mtime_table", {}).values():
                cache_dirs.update(sub.keys())
            if idx_dirs - cache_dirs:
                disk_cache = None

        if disk_cache:
            _mem_key_cache_map[env_dir_str] = disk_cache
            _current_key_cache_env = env_dir_str
            if changed_buckets:
                for dir_name, bk, old_c, new_c in changed_buckets:
                    _apply_delta(disk_cache, dir_name, bk, old_c, new_c)
                _cleanup_zeros(disk_cache)
                disk_cache["updated_at"] = time.time()
                _save_key_cache(_env_dir, disk_cache)
            return _build_rows(disk_cache, threshold, include_rows=include_rows)

    cache = _full_build_key_cache(index)
    _mem_key_cache_map[env_dir_str] = cache
    _current_key_cache_env = env_dir_str
    if _env_dir:
        _save_key_cache(_env_dir, cache)
    return _build_rows(cache, threshold, include_rows=include_rows)


# ---------------------------------------------------------------------------
# key 元数据 + records 缓存（供 /api/export/keys 使用）
# ---------------------------------------------------------------------------

def get_current_key_cache() -> Optional[dict]:
    """返回当前内存中的 key cache（引用，非拷贝），供外部读取 key_meta / key_records。"""
    if _current_key_cache_env is None:
        return None
    return _mem_key_cache_map.get(_current_key_cache_env)


def _key_slot(api_key: str) -> str:
    if not api_key or api_key == "(empty)":
        return "all"
    return "key-" + api_key[-4:]


def _compute_keys_hash(db_keys_list: list) -> str:
    raw = "|".join(
        f"{k.get('key', '')}:{k.get('name', '')}:{k.get('created_at', '')}"
        for k in db_keys_list
    )
    return hashlib.md5(raw.encode()).hexdigest()


def update_key_meta(cache: dict, db_keys_list: list) -> bool:
    """用 list_keys() 的结果更新 cache 中的 key_meta。返回是否有变更。

    db_keys_list: list_keys() 返回的 [{key, name, created_at, ...}, ...]
    """
    if cache is None:
        return False

    meta = cache.get("key_meta") or {}
    new_hash = _compute_keys_hash(db_keys_list)

    if meta.get("db_keys_hash") == new_hash and meta.get("mapping"):
        return False

    mapping: dict = {}
    for row_key in cache.get("table", {}):
        matched_name = ""
        created_at = ""
        for k in db_keys_list:
            masked = k.get("key", "")
            if row_key != "(empty)" and len(row_key) >= 4 and masked.endswith(row_key[-4:]):
                matched_name = k.get("name", "")
                created_at = k.get("created_at", "")
                break
        mapping[row_key] = {
            "key_name": matched_name,
            "key_slot": _key_slot(row_key),
            "created_at": created_at,
        }

    cache["key_meta"] = {"db_keys_hash": new_hash, "mapping": mapping}
    return True


def update_key_records(cache: dict, env_dir: Optional[Path] = None) -> bool:
    """检查 export_records 是否有变化，有变化则一次性刷新 records 缓存。返回是否有变更。"""
    if cache is None:
        return False

    try:
        from utils.export_store import get_records_summary, list_records_all_slim
    except ImportError:
        return False

    max_id, count, running = get_records_summary()
    rec_cache = cache.get("key_records") or {}

    if (rec_cache.get("db_max_id") == max_id
            and rec_cache.get("db_count") == count
            and rec_cache.get("db_running") == running
            and rec_cache.get("records")):
        return False

    grouped = list_records_all_slim(limit_per_key=10)
    cache["key_records"] = {
        "db_max_id": max_id,
        "db_count": count,
        "db_running": running,
        "records": grouped,
    }

    if env_dir:
        _save_key_cache(env_dir, cache)

    return True


# ---------------------------------------------------------------------------
# 查询接口
# ---------------------------------------------------------------------------

def get_dir_counts(index: dict, token_index: Optional[dict] = None) -> Dict[str, int]:
    """从索引中提取每个 mtime_dir 的请求数，供 /logs/dirs 使用。

    优先用 token_index 的 entry_count（已增量维护，无需 iterdir），
    token_index 中没有该目录时回退到 stats_index 的 req_count。
    """
    tok_dirs = token_index.get("dirs", {}) if token_index else {}
    result = {}
    for dir_name, dir_info in index.get("dirs", {}).items():
        tok_info = tok_dirs.get(dir_name)
        if tok_info is not None and tok_info.get("entry_count", 0) > 0:
            result[dir_name] = tok_info["entry_count"]
        else:
            result[dir_name] = dir_info.get("req_count", 0)
    return result


def get_date_to_mtime_map(index: dict) -> Dict[str, List[str]]:
    """从索引中构建 date -> [mtime_dir, ...] 映射。"""
    mapping: Dict[str, List[str]] = {}
    for dir_name, dir_info in index.get("dirs", {}).items():
        dates_in_dir: set = set()
        for bucket_key in dir_info.get("sessions", {}):
            parts = bucket_key.split("|", 1)
            if len(parts) == 2:
                dates_in_dir.add(parts[1])
        for d in dates_in_dir:
            if d not in mapping:
                mapping[d] = []
            if dir_name not in mapping[d]:
                mapping[d].append(dir_name)
    return mapping


# ---------------------------------------------------------------------------
# 后台预热线程 — stats_index
# ---------------------------------------------------------------------------

_stats_warmer_thread = None
_stats_warmer_stop = threading.Event()
_stats_logger = logging.getLogger("stats-warmer")

_last_refresh_ts: float = 0.0


def get_last_refresh_ts() -> float:
    return _last_refresh_ts


def _warmer_roots(env_dir: str) -> List[str]:
    """预热覆盖的 root 列表：全部已登记源，env_dir 排第一。

    原先只预热 env_dir（活跃本地 root，5 叶），而 /sessions/stats 走
    build_stats_multi(get_registered_roots())，覆盖 11 个 root / 4589 叶。
    97% 的冷成本（~9200 次 NFS 元数据查询）从未被预热覆盖，首个用户请求必然
    全价支付 —— 这是冷启动 6.5s 的主因。这里要覆盖全集。
    """
    roots: List[str] = [env_dir]
    try:
        from utils.logs_config import get_registered_roots
        for r in get_registered_roots(env_dir):
            if os.path.normpath(r) not in {os.path.normpath(x) for x in roots}:
                roots.append(r)
    except Exception:  # noqa: BLE001 — 取不到就只预热 env_dir
        pass
    return [r for r in roots if os.path.isdir(r)]


def _stats_warmer_loop(env_dir: str, threshold: int, interval: float = 22.0) -> None:
    global _last_refresh_ts
    env_path = Path(env_dir)
    roots = _warmer_roots(env_dir)
    _stats_logger.info("stats-warmer: 开始预热 env_dir=%s，覆盖 %d 个 root", env_dir, len(roots))

    # 首轮：env_dir 用 force=True（活跃本地 root，叶子少，成本可忽略，顺带把每叶
    # 签名落进 leaf_status）；其余 root 一律 force=False —— 绝不能 force=True，
    # 那会重扫 82GB index.jsonl。它们的 leaf_status 签名缓存已齐备（实测 4589 叶
    # 中仅 3 叶失配），force=False 只需逐叶一次 stat，签名命中即返回。
    for r in roots:
        p = Path(r)
        is_env = os.path.normpath(r) == os.path.normpath(env_dir)
        try:
            index = refresh_index(p, threshold, force=is_env)
            build_stats_from_index(index, threshold, env_dir=p, include_rows=False)
        except Exception:  # noqa: BLE001 — 单个 root 失败不影响其它 root
            _stats_logger.warning("stats-warmer: 预热失败 root=%s", r, exc_info=True)
    _last_refresh_ts = time.time()
    _stats_logger.info("stats-warmer: 首次预热完成")

    while not _stats_warmer_stop.is_set():
        _stats_warmer_stop.wait(interval)
        if _stats_warmer_stop.is_set():
            break
        for r in roots:
            p = Path(r)
            try:
                index = refresh_index(p, threshold, force=False)
                build_stats_from_index(index, threshold, env_dir=p, include_rows=False)
            except Exception:  # noqa: BLE001
                _stats_logger.warning("stats-warmer: 周期预热失败 root=%s", r, exc_info=True)
        _last_refresh_ts = time.time()


def start_stats_warmer(env_dir: str, threshold: int = QUALIFIED_THRESHOLD_DEFAULT) -> None:
    global _stats_warmer_thread
    if _stats_warmer_thread is not None and _stats_warmer_thread.is_alive():
        return
    _stats_warmer_stop.clear()
    _stats_warmer_thread = threading.Thread(
        target=_stats_warmer_loop,
        args=(env_dir, threshold),
        daemon=True,
        name="stats-cache-warmer",
    )
    _stats_warmer_thread.start()
