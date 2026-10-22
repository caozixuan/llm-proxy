"""
utils/export_sync.py — 导出 session_index.jsonl 并同步到 OBS

export_session_index(logs_dir):
  读取 logs_dir/.session_cache.jsonl → 生成 logs_dir/session_index.jsonl
  输出统计信息（session 总数、平均 msg 轮数）

sync_session_index(logs_dir, obs_dst, ...):
  读取 logs_dir/session_index.jsonl → 上传 latest_file 三元组到 OBS
  目录结构: obs_dst/raw/{文件}, obs_dst/session/{文件}
"""

import hashlib
import json
import logging
import os
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from utils.obs_sync import _run_upload_cmd

logger = logging.getLogger(__name__)
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(_h)
    logger.setLevel(logging.INFO)

SESSION_CACHE_NAME = ".session_cache.jsonl"
SESSION_INDEX_NAME = "session_index.jsonl"
SYNC_STATE_NAME = ".sync_export_state.json"

# 读 session_index.jsonl 尾部找 `_meta` 行的窗口大小。`_meta` 行约 200 字节，
# 8 KB 足够覆盖；取不到时 _read_session_index_meta 会回退全文扫描。
_META_TAIL_BYTES = 8192


def _is_newapi(logs_dir: str) -> bool:
    try:
        from utils.log_scan import detect_format
        return detect_format(logs_dir) == "newapi"
    except Exception:
        return False


def _read_session_cache(logs_dir: str, api_key: str = "") -> List[dict]:
    """优先从 DB 读取，降级到读 .session_cache.jsonl 文件。

    new-api 叶子的 session 数据在叶子内 index.db（非 per-port session_cache.db），故先取它。

    api_key 非空时下推到 SQL（只取该 key 的 session），避免把整叶读进内存再过滤 ——
    单叶动辄 3-6 万 session，而一次导出通常只关心其中一个 key（实测省 98%）。

    读取失败**不吞异常**：曾因 `except: return []` 把「读取报错」伪装成「没有数据」，
    上层据此判成「无 session 数据」，真实原因（如 too many SQL variables）被掩盖。
    现在记完整堆栈后上抛，由 _process_mtime 收进 errors，导出记录显示真实失败原因。
    """
    if _is_newapi(logs_dir):
        import utils.newapi_index_db as nidb
        try:
            return nidb.export_sessions(logs_dir, api_key=api_key)
        except Exception:
            logger.exception("读取 new-api 叶子 index.db 失败: %s", logs_dir)
            raise

    db_err: Optional[Exception] = None
    try:
        import utils.session_store as _ss
        count = _ss.get_session_count_by_root(logs_dir)
        if count > 0:
            if api_key:
                return _ss.export_sessions(logs_dir, api_key=api_key)
            return _ss.export_sessions(logs_dir)
    except Exception as e:  # noqa: BLE001
        db_err = e
        logger.exception("读取 session_cache.db 失败，尝试降级读文件: %s", logs_dir)

    # 降级：直接读文件
    cache_path = Path(logs_dir) / SESSION_CACHE_NAME
    if not cache_path.is_file():
        if db_err is not None:
            raise db_err
        return []
    sessions = []
    with open(cache_path, "r", encoding="utf-8") as f:
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
            sessions.append(obj)
    return sessions


def _read_session_index_meta(logs_dir: str) -> Optional[dict]:
    """读取 session_index.jsonl 的 `_meta` 行。

    `_meta` 恒定写在该文件的**最后一行**（见 export_session_index 的写入顺序），
    因此只需读尾部一段即可，不必从头解析整个文件——53 MB 的索引从头扫一遍要 0.65s，
    读尾部 8 KB 只要 2ms。尾部没找到（老格式/文件被截断）时再退回全文扫描。
    """
    index_path = Path(logs_dir) / SESSION_INDEX_NAME
    if not index_path.is_file():
        return None

    def _scan(lines):
        found = None
        for line in lines:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if obj.get("_meta"):
                found = obj
        return found

    tail_at = max(0, index_path.stat().st_size - _META_TAIL_BYTES)
    with open(index_path, "rb") as f:
        f.seek(tail_at)
        tail = f.read().decode("utf-8", errors="replace")
    lines = tail.splitlines()
    if tail_at > 0:
        lines = lines[1:]  # 首行可能被截断（偏移落在行中间），丢弃
    meta = _scan(lines)
    if meta is not None:
        return meta
    # 兜底：尾部没有 _meta（旧格式 / 异常文件），回退全文扫描
    with open(index_path, "r", encoding="utf-8") as f:
        return _scan(f)


def _newapi_change_token(logs_dir: str) -> Optional[float]:
    """new-api 叶子的「内容是否变化」判据，供 session_index.jsonl 增量跳过使用。

    以前用 index.db 的 session 条数（一次全表 COUNT，4–5s）。条数还有个语义缺陷：
    条数不变但内容更新（补 meta、session 合并）时会被误判为「未变更」而跳过。
    改用新 API 的 probe()：meta 三键（ingest_offset / sessions_built_at /
    sessions_dirty）任一变化都算变更，且只需两次点查（合计 <0.1s）。

    返回值编码成负数，与 `.session_cache.jsonl` 的 mtime（正 float）区分开。
    没有 session 数据时返回 None（调用方据此跳过）。
    """
    import utils.newapi_index_db as nidb
    info = nidb.probe(logs_dir)
    if not info.get("has_sessions"):
        return None
    # 把 fingerprint 压成稳定的负 float：取前 12 位十六进制做整数哈希。
    # 加负号与文件 mtime 区分；不含 0（避免与「无变更」哨兵混淆）。
    fp = info.get("fingerprint") or ""
    h = int(hashlib.sha1(fp.encode("utf-8")).hexdigest()[:12], 16)
    return -float(h or 1)


def _refresh_session_cache(logs_dir: str) -> bool:
    """导出前把 session 缓存追平 index.jsonl 当前状态，确保导出完整。

    new-api：叶子 index.db 是唯一来源，但导出**不再触发回填/构建**（回填是重操作、会起
    进程池，只应在数据管理界面手动执行）。此处只判断现有 index.db 是否已有 session，导出即
    "读多少导多少"；是否需要先回填由上层 _run_task_inner 用 needs_build 前置判定并提示。
    native：复用 log_routes 的增量聚合（廉价、不起池）。
    """
    if _is_newapi(logs_dir):
        index_path = Path(logs_dir) / "index.jsonl"
        if not index_path.is_file():
            return False
        try:
            import utils.newapi_index_db as nidb
            # 用 probe()（EXISTS + meta 点查，~0.08s）而非 status()（三次全表 COUNT，
            # 4–5s）。这里只需要「有没有 session」，不需要精确条数。
            return bool(nidb.probe(logs_dir).get("has_sessions"))
        except Exception:
            logger.warning("new-api index.db 状态读取失败: %s", logs_dir)
            return False

    from utils.log_routes import _refresh_state

    index_path = Path(logs_dir) / "index.jsonl"
    if not index_path.is_file():
        return False

    _refresh_state("export", logs_dir)
    return Path(logs_dir, SESSION_CACHE_NAME).is_file()


def export_session_index(logs_dir: str, force: bool = False, with_sessions: bool = False,
                         api_key: str = "") -> dict:
    """
    生成 logs_dir/session_index.jsonl。

    session 数据优先来自 session_cache.db（DB），降级读 .session_cache.jsonl。
    new-api：只读现有 index.db，不触发回填（回填仅数据管理界面手动执行）。
    native：每次先从 index.jsonl 增量刷新（廉价、不起池），确保不遗漏新数据。

    with_sessions: 重建时把本次已解析/写出的 session 列表一并返回（键 `sessions`），
        供上层直接复用，避免对刚写出的文件再做一次完整解析。跳过（skipped）时不返回
        该键——调用方需自行从文件加载。

    api_key: 非空时**只导出该 key 的 session**，并把 sql 下推（见 _read_session_cache）。
        注意此时**不写** logs_dir/session_index.jsonl —— 那个文件是全 key 共享的缓存
        （meta.source_mtime 也只描述整叶），写入子集会让其他 key 的导出读到残缺索引。
        结果通过返回值里的 `sessions` 键直接交给上层，不落盘。
    Returns dict with total_sessions, avg_msg_count, skipped (bool).
    """
    # 先刷新 cache（native 增量；new-api 仅探测现有 index.db，不构建）
    _refresh_session_cache(logs_dir)

    if api_key:
        # 按 key 导出：跳过整叶的「是否最新」判定（它对子集无意义，且判定成本不低），
        # 直接下推查询。Shared session_index.jsonl 保持不变。
        sessions = _read_session_cache(logs_dir, api_key=api_key)
        total = len(sessions)
        if total == 0:
            logger.info("按 key=%s 过滤后无 session 数据", api_key)
            return {"total_sessions": 0, "avg_msg_count": 0, "skipped": False}
        from utils.session_store import to_unified_record
        sessions = [to_unified_record(s) for s in sessions]
        total_msg = sum(s.get("total_messages", s.get("msg_count", 0)) for s in sessions)
        avg_msg = round(total_msg / total)
        logger.info("已按 key 导出 %d 条 session（未写共享索引）: %s", total, logs_dir)
        out = {"total_sessions": total, "avg_msg_count": avg_msg, "skipped": False}
        if with_sessions:
            out["sessions"] = sessions
        return out

    cache_path = Path(logs_dir) / SESSION_CACHE_NAME

    # source_mtime 作为“是否跳过”的变更判据：
    #  - 有 .session_cache.jsonl 文件（旧路径）：用文件 mtime
    #  - new-api：用 index.db 的 probe() 指纹（meta 三键），点查即可，不做全表 COUNT
    #  - 其余（数据已迁到 session_cache.db）：用 DB 的 session 条数
    if cache_path.is_file():
        source_mtime = cache_path.stat().st_mtime
    elif _is_newapi(logs_dir):
        try:
            source_mtime = _newapi_change_token(logs_dir)
        except Exception:
            logger.warning("new-api index.db 状态读取失败，按有变更处理: %s", logs_dir)
            source_mtime = None
        if source_mtime is None:
            logger.warning("new-api 叶子 index.db 尚未构建，跳过 export: %s", logs_dir)
            return {"total_sessions": 0, "avg_msg_count": 0, "skipped": True}
    else:
        db_count = 0
        try:
            import utils.session_store as _ss
            db_count = _ss.get_session_count_by_root(logs_dir)
        except Exception:
            db_count = 0
        if db_count <= 0:
            logger.warning("未找到 %s 且 DB 无数据", cache_path)
            return {"total_sessions": 0, "avg_msg_count": 0, "skipped": True}
        # 用负数 count 作 source_mtime，与文件 mtime（正 float）区分，
        # 且 DB 条数变化时该值随之变化，触发重新导出。
        source_mtime = float(-db_count)

    if not force:
        meta = _read_session_index_meta(logs_dir)
        if meta and meta.get("source_mtime") == source_mtime:
            logger.info("session 数据未变更，跳过 export (source_mtime=%.1f)", source_mtime)
            return {
                "total_sessions": meta.get("total_sessions", 0),
                "avg_msg_count": meta.get("avg_msg_count", 0),
                "skipped": True,
            }

    sessions = _read_session_cache(logs_dir)
    total = len(sessions)
    if total == 0:
        logger.info("无 session 数据")
        return {"total_sessions": 0, "avg_msg_count": 0, "skipped": False}

    # 统一记录格式（version 2，兼容超集）：规范键 + 旧别名，纯导出无评估字段
    from utils.session_store import to_unified_record
    sessions = [to_unified_record(s) for s in sessions]

    total_msg = sum(s.get("total_messages", s.get("msg_count", 0)) for s in sessions)
    avg_msg = round(total_msg / total)

    index_path = Path(logs_dir) / SESSION_INDEX_NAME
    tmp_path = str(index_path) + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        for s in sessions:
            f.write(json.dumps(s, ensure_ascii=False) + "\n")
        meta_line = {
            "_meta": True,
            "version": 2,
            "total_sessions": total,
            "avg_msg_count": avg_msg,
            "source_mtime": source_mtime,
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        }
        f.write(json.dumps(meta_line, ensure_ascii=False) + "\n")
    from utils.atomic_write import safe_replace
    safe_replace(tmp_path, str(index_path))

    logger.info("已导出 session_index.jsonl: %d 条 session, 平均 %d 轮 msg", total, avg_msg)
    out = {"total_sessions": total, "avg_msg_count": avg_msg, "skipped": False}
    if with_sessions:
        # 刚重建完，这批 session 已在内存里。上层（reformat/eval 类 pipeline）紧接着就要
        # 用它，直接回传可省掉一次对同一文件的完整解析（53 MB 索引约 0.6s/目录）。
        out["sessions"] = sessions
    return out


def _load_session_index(logs_dir: str) -> List[dict]:
    index_path = Path(logs_dir) / SESSION_INDEX_NAME
    if not index_path.is_file():
        return []
    sessions = []
    with open(index_path, "r", encoding="utf-8") as f:
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
            sessions.append(obj)
    return sessions


def _collect_triplet_files(logs_dir: str, latest_file: str) -> List[Path]:
    base = Path(logs_dir)

    # new-api：latest_file 是合并单文件（无 -req/-res/-headers 三元组），直接上传该文件
    if latest_file.endswith(".json") and not latest_file.endswith("-req.json"):
        p = base / latest_file
        if p.is_file():
            return [p]

    stem = latest_file
    if stem.endswith("-req.json"):
        stem = stem[: -len("-req.json")]
    elif stem.endswith(".json"):
        stem = stem[: -len(".json")]

    files = []
    for suffix in ("-req.json", "-headers.json", "-res.json"):
        p = base / f"{stem}{suffix}"
        if p.is_file():
            files.append(p)
    return files


def _upload_file(
    local_path: Path,
    obs_dst: str,
    upload_script: Optional[str],
) -> Tuple[str, bool, str]:
    dst = obs_dst.rstrip("/") + "/" + local_path.name
    ok, msg = _run_upload_cmd(str(local_path), dst, upload_script)
    return local_path.name, ok, msg


def _load_sync_state(logs_dir: str) -> dict:
    sp = Path(logs_dir) / SYNC_STATE_NAME
    if sp.exists():
        try:
            with open(sp, "r", encoding="utf-8") as f:
                data = json.load(f)
            if "uploaded_keys" in data and "slots" not in data:
                data["slots"] = {"nokey": {"uploaded_keys": data.pop("uploaded_keys")}}
            return data
        except Exception:
            pass
    return {"slots": {}}


def _save_sync_state(logs_dir: str, state: dict):
    sp = Path(logs_dir) / SYNC_STATE_NAME
    state["last_sync_at"] = datetime.now().isoformat(timespec="seconds")
    with open(sp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)


def sync_session_index(
    logs_dir: str,
    obs_dst: str,
    workers: int = 8,
    upload_script: Optional[str] = None,
    key: Optional[str] = None,
    local_copy_dir: Optional[str] = None,
    force: bool = False,
    upload: bool = True,
    sessions: Optional[List[dict]] = None,
) -> dict:
    """
    读取 session_index.jsonl，将 session 三元组文件复制到 local_copy_dir，
    然后一次性上传整个文件夹到 OBS。

    key: 按 api_key 精确过滤 session
    local_copy_dir: 本地复制目标路径，上传后保留
    force: 忽略已上传缓存，全部重新上传
    upload: False 时只复制到 local_copy_dir、不上传，也不更新 .sync_state
        （多目录并行导出时用：各目录只复制到共享 local_base，收尾由上层统一上传一次，
        避免每目录都整目录上传导致重复上传 + 写入中上传的竞态）。
    sessions: 已解析好的 session 列表。上层刚调完 export_session_index 时手上就有这批
        数据（with_sessions=True），传进来可省掉对同一个 session_index.jsonl 的又一次
        完整解析（单叶索引可达 90MB）。为 None 时保持原有行为：自己从文件读。
    """
    if sessions is None:
        sessions = _load_session_index(logs_dir)
    if not sessions:
        logger.info("无 session 数据，跳过 sync")
        return {"uploaded": 0, "skipped": 0, "failed": 0, "total_files": 0, "matched_sessions": 0}

    if key:
        sessions = [s for s in sessions if s.get("api_key") == key]
        if not sessions:
            logger.info("按 key=%s 过滤后无 session 数据", key)
            return {"uploaded": 0, "skipped": 0, "failed": 0, "total_files": 0, "matched_sessions": 0}
        logger.info("按 key=%s 过滤，共 %d 条 session", key, len(sessions))

    state = _load_sync_state(logs_dir)
    slot_key = "key-" + key[-4:] if key else "nokey"
    slot_state = state.get("slots", {}).get(slot_key, {})
    uploaded_keys = set() if force else set(slot_state.get("uploaded_keys", []))

    collect_files: List[Path] = []
    new_keys: List[str] = []

    for s in sessions:
        skey = s.get("_key", "")
        if skey in uploaded_keys:
            continue
        latest_file = s.get("latest_file", "")
        if not latest_file:
            continue
        new_keys.append(skey)
        triplet_files = _collect_triplet_files(logs_dir, latest_file)
        collect_files.extend(triplet_files)

    matched_sessions = len(sessions)

    if not collect_files and not new_keys:
        logger.info("无新 session 需要上传")
        return {"uploaded": 0, "skipped": len(uploaded_keys), "failed": 0, "total_files": 0, "matched_sessions": matched_sessions}

    if not local_copy_dir:
        logger.error("未指定 local_copy_dir，无法复制文件")
        return {"uploaded": 0, "skipped": 0, "failed": 1, "total_files": 0, "matched_sessions": matched_sessions}

    copy_dir = Path(local_copy_dir)
    copy_dir.mkdir(parents=True, exist_ok=True)

    def _copy_one(fp: Path) -> bool:
        try:
            shutil.copy2(fp, copy_dir / fp.name)
            return True
        except Exception as e:
            logger.error("复制失败 %s: %s", fp.name, e)
            return False

    copied = 0
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for ok in executor.map(_copy_one, collect_files):
            if ok:
                copied += 1

    if key:
        filtered_index = copy_dir / SESSION_INDEX_NAME
        with open(filtered_index, "w", encoding="utf-8") as f:
            for s in sessions:
                f.write(json.dumps(s, ensure_ascii=False) + "\n")
    else:
        index_file = Path(logs_dir) / SESSION_INDEX_NAME
        if index_file.is_file():
            shutil.copy2(index_file, copy_dir / SESSION_INDEX_NAME)

    logger.info("已复制 %d 个文件到 %s", copied, copy_dir)

    # 仅本地复制的两种情形：
    #  1) 未指定 OBS 目标（前端「留空仅本地复制」）；
    #  2) upload=False（多目录并行导出：各目录只复制到共享 local_base，
    #     收尾由上层统一上传一次）。
    # 两者都：标记 new_keys 已处理（写 .sync_state，保证跨任务增量不重复复制），
    # 但不在此上传。
    if not obs_dst or not upload:
        uploaded_keys.update(new_keys)
        slots = state.setdefault("slots", {})
        slots[slot_key] = {"uploaded_keys": sorted(uploaded_keys)}
        _save_sync_state(logs_dir, state)
        if not obs_dst:
            logger.info("仅本地复制完成: %d 个文件, %d 个新 session", copied, len(new_keys))
        else:
            logger.info("已复制（上传延后到收尾）: %d 个文件, %d 个新 session", copied, len(new_keys))
        return {"uploaded": copied, "skipped": len(uploaded_keys) - len(new_keys), "failed": 0, "total_files": copied, "matched_sessions": matched_sessions, "new_sessions": len(new_keys)}

    obs_parent = obs_dst.rstrip("/").rsplit("/", 1)[0] + "/"
    ok, msg = _run_upload_cmd(str(copy_dir), obs_parent, upload_script)
    if ok:
        logger.info("文件夹上传成功: %s -> %s", copy_dir, obs_dst)
    else:
        logger.error("文件夹上传失败: %s", msg)
        return {"uploaded": 0, "skipped": 0, "failed": 1, "total_files": copied, "matched_sessions": matched_sessions}

    uploaded_keys.update(new_keys)
    slots = state.setdefault("slots", {})
    slots[slot_key] = {"uploaded_keys": sorted(uploaded_keys)}
    _save_sync_state(logs_dir, state)

    logger.info("上传完成: %d 个文件, %d 个新 session", copied, len(new_keys))
    return {"uploaded": copied, "skipped": len(uploaded_keys) - len(new_keys), "failed": 0, "total_files": copied, "matched_sessions": matched_sessions, "new_sessions": len(new_keys)}
