"""
utils/newapi_backfill.py — new-api 富 index（index.db）的批量构建

给每个 new-api 小时叶子构建/补齐 `{leaf}/index.db`（见 utils/newapi_index_db）：
  - ingest：从 index.jsonl 按 offset 增量摄取 request 行（cheap）
  - enrich：解析合并文件补 q1_hash/msg_count/... 元信息（贵，叶子内多进程分片）
  - rebuild_sessions：从富 index 内存聚合出 sessions/traces（cheap）

叶子间由全局 FIFO 调度线程「串行」执行（一个 root 跑完再跑下一个，root 内逐叶子），
叶子内部由 index_db.enrich 开进程池并行——摆脱旧版「1 叶子=1 worker」。
进度状态存进程内存，经 get_backfill_status(root) 暴露给 API。
"""

import os
import logging
import logging.handlers
import threading
import time
from concurrent.futures.process import BrokenProcessPool
from pathlib import Path
from typing import Dict, Optional

from utils.log_scan import (
    iter_index_dirs, fast_scan_leaf_dirs_by_templates, default_templates,
    detect_format, dir_key_for,
)
import utils.newapi_index_db as nidb
from utils.leaf_lock import LeafLockBusy, leaf_lock

# 回填取叶子锁的等待时间（秒）。回填是整 root 的串行队列，为一片被 watcher
# 占着的叶子长时间等待会拖垮整条队列 —— 拿不到就跳到下一片。
_LEAF_LOCK_WAIT = 5.0

logger = logging.getLogger(__name__)


def _is_broken_pool_err(e: BaseException) -> bool:
    """是否进程池 worker 被 OS 终止（BrokenProcessPool 或其链上包裹）。"""
    while e is not None:
        if isinstance(e, BrokenProcessPool):
            return True
        e = getattr(e, "__cause__", None)
    return False


# 专用构建日志：new-api index 回填的生命周期事件（入队/开始/单叶成功失败/卡死/root 崩溃/完成）
# 单独落一份 {SERVICE_LOG_DIR}/backfill.log，便于事后单独排查构建历史——
# 此前整条链路几乎无日志，root 级崩溃只写进程内存且被前端 DB 权威状态盖掉，异常静默丢失。
# 该 logger 不向 root 传播（propagate=False），避免把逐叶细节灌进 app.log；
# 真正需要 app.log 也看到的（root 崩溃）仍另经模块级 logger.error 打一份。
bf_logger = logging.getLogger("newapi_backfill_events")
bf_logger.setLevel(logging.INFO)
bf_logger.propagate = False


def init_backfill_logger(log_dir: str) -> None:
    """挂 RotatingFileHandler 到 backfill.log。幂等：重复调用不叠加 handler。"""
    for h in bf_logger.handlers:
        if isinstance(h, logging.handlers.RotatingFileHandler):
            return
    try:
        os.makedirs(log_dir, exist_ok=True)
        path = os.path.join(log_dir, "backfill.log")
        handler = logging.handlers.RotatingFileHandler(
            path, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8")
        handler.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S"))
        bf_logger.addHandler(handler)
        bf_logger.info("=== backfill logger initialized -> %s ===", path)
    except Exception:  # noqa: BLE001 — 日志初始化失败不应拖垮服务启动
        logger.exception("init_backfill_logger failed")


def _backfill_log_path() -> Optional[str]:
    """从已挂的 handler 拿 backfill.log 绝对路径（未初始化则 None）。"""
    for h in bf_logger.handlers:
        if isinstance(h, logging.handlers.RotatingFileHandler):
            return h.baseFilename
    return None


def read_backfill_log(root_filter: str = "", limit: int = 400) -> dict:
    """读 backfill.log 尾部；给了 root_filter 则只返回含该源 root 的行。

    每条构建日志都带 `root=<normpath>`，据此按源过滤。返回最后 limit 条匹配行。
    读文件尾部即可（回填日志按时间追加），不做全量读，避免大文件卡住请求。
    """
    path = _backfill_log_path()
    if not path or not os.path.isfile(path):
        return {"ok": True, "path": path or "", "lines": [], "note": "日志文件尚未生成"}
    norm = os.path.normpath(root_filter) if root_filter else ""
    try:
        # 只读尾部一段（约 2MB 足够覆盖近期若干次构建），逐行匹配。
        size = os.path.getsize(path)
        tail_bytes = min(size, 2 * 1024 * 1024)
        with open(path, "rb") as f:
            if size > tail_bytes:
                f.seek(size - tail_bytes)
                f.readline()  # 丢弃可能被截断的半行
            raw = f.read().decode("utf-8", errors="replace")
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "path": path, "lines": [], "note": f"读取失败: {e}"}
    lines = raw.splitlines()
    if norm:
        # 行内以 `root=<path> ` 形式出现；用带空格/行尾的边界避免前缀误匹配
        needle = f"root={norm}"
        lines = [ln for ln in lines
                 if f"{needle} " in ln or ln.rstrip().endswith(needle)]
    return {"ok": True, "path": path, "lines": lines[-limit:]}

# 单叶子 enrich「距上次分片完成」超过此秒数无进展即判卡住（多为 NFS 读死等），
# 跳过该叶子 + 预警。可经环境变量覆盖，便于按网络盘抖动实际调。
try:
    _STALL_TIMEOUT = float(os.getenv("NEWAPI_BACKFILL_STALL_TIMEOUT", "300"))
except ValueError:
    _STALL_TIMEOUT = 300.0

# root(normpath) -> 状态 dict
_status: Dict[str, dict] = {}
_status_lock = threading.Lock()

# 磁盘回退探测的短 TTL 缓存：内存无运行态时，据叶子 index.db 数判断是否「已就绪」。
# 状态非持久化（存内存，重启即丢），但 index.db 在磁盘上——重启后据磁盘反推构建情况。
# sfs 等网络盘 stat 有延迟，TTL 内复用结果，避免每次轮询全量 stat。
_disk_probe_cache: Dict[str, tuple] = {}   # root -> (ts, status_dict)
_disk_probe_lock = threading.Lock()
_DISK_PROBE_TTL = 30.0

# ── 并发回填 ───────────────────────────────────────────────────────
# 点击「增量构建/全量重建」即为该 root 起一个 daemon 线程跑 _run，多个 root 可同时回填
# （各自开进程池）。不再走全局 FIFO 串行队列——去掉排队，谁点谁立即起。
# 仅保留「同一 root 去重」：该 root 已在跑就不重复起（避免同源两份进程池打架）。
_running: Dict[str, threading.Thread] = {}   # root(normpath) -> 正在跑的线程
_running_lock = threading.Lock()

_DEFAULT_WORKERS = min(8, (os.cpu_count() or 4))


def _set(root: str, **kw):
    with _status_lock:
        st = _status.setdefault(root, {})
        st.update(kw)


def _disk_probe_status(root: str) -> dict:
    """内存无运行态时，据磁盘上叶子的 index.db 反推构建情况（带 TTL 缓存）。

    只用轻量的 _db_exists（os.path.isfile），不开 db 连接，避免网络盘每叶子多次 IO。
    - 全部叶子已有 index.db → done（"已就绪"）
    - 部分 → done 且带 done/total（UI 显示已就绪 N/M）
    - 无叶子或全未建 → idle（保持"未构建"）
    """
    with _disk_probe_lock:
        cached = _disk_probe_cache.get(root)
        if cached and (time.time() - cached[0]) < _DISK_PROBE_TTL:
            return dict(cached[1])
    try:
        leaves = [str(d) for d in iter_index_dirs(Path(root))]
        total = len(leaves)
        built = sum(1 for lf in leaves if nidb._db_exists(lf))
    except Exception:
        total = built = 0
    if total and built:
        st = {"status": "done", "total_leaves": total, "done_leaves": built,
              "skipped_leaves": built, "from_disk": True}
    else:
        st = {"status": "idle", "total_leaves": total, "done_leaves": 0}
    with _disk_probe_lock:
        _disk_probe_cache[root] = (time.time(), dict(st))
    return st


def get_backfill_status(path: str) -> dict:
    root = os.path.normpath(path)
    with _status_lock:
        st = dict(_status.get(root, {}))
    if not st:
        # 本进程未跑过：回退到磁盘探测（index.db 是否已就绪），反映重启前的构建结果
        return _disk_probe_status(root)
    return st


def count_built_leaves(path: str) -> tuple:
    """(已回填叶子数, 总叶子数)。供列表页展示"已回填/总数"。

    优先取本进程运行态（正在跑时的实时计数）；无运行态则据磁盘 index.db 探测。
    两者都复用 _disk_probe_status 的 TTL 缓存路径，避免网络盘频繁全量 stat。
    """
    root = os.path.normpath(path)
    with _status_lock:
        st = dict(_status.get(root, {}))
    if st and st.get("status") in ("running", "done", "error"):
        total = int(st.get("total_leaves") or 0)
        built = int(st.get("done_leaves") or 0)
        if total:
            return built, total
    probe = _disk_probe_status(root)
    return int(probe.get("done_leaves") or 0), int(probe.get("total_leaves") or 0)


def _root_id(root: str) -> str:
    """源标识；用于 logdir_store 的 (root_id, dir_key) 主键。"""
    try:
        from utils.logs_config import get_root_id
        return get_root_id(root)
    except Exception:
        return ""


def _native_leaf_built(leaf: Path, progress: Dict[str, int]) -> bool:
    """native 叶子是否「完全构建」：index.jsonl 已被 session 聚合消费到末尾。

    native 无独立 index.db，其「已 index」不再等价于「index.jsonl 存在」，而是
    session_cache.db 的 index_progress.byte_offset 已追平 index.jsonl 字节大小
    （惰性增量聚合，见 log_routes._refresh_state）。progress 由调用方一次性传入
    （session_store.get_all_progress()，{叶子绝对路径: byte_offset}）。

    off is None（该库从未聚合过此叶子）→ 未 built。用 >= 而非 == 容错：网络盘上
    index.jsonl 大小读取抖动、或历史 offset 略超时，语义仍是「追平即 built」。
    stat 失败（文件消失/权限）→ 保守判未 built。
    """
    off = progress.get(str(leaf))
    if off is None:
        return False
    try:
        return off >= (leaf / "index.jsonl").stat().st_size
    except OSError:
        return False


def sync_leaves(path: str, templates=None) -> dict:
    """扫描某源的叶子目录，把节点清单同步进 logdir_store（不触发回填）。

    只做「清单 + 是否已建」的轻量同步：
    - built 判定按格式分口径：
        · newapi：用 _db_exists（一次 stat）判该叶是否已建 index.db。
        · native：以 session 聚合进度为准 —— index_progress.byte_offset 追平
          index.jsonl 字节大小才算 built（见 _native_leaf_built）。「index.jsonl
          存在」不再等价于「已 index」：惰性增量聚合可能只消费了一部分。判定用
          循环外一次性取的 get_all_progress()（内存 dict 命中），每叶仍只一次 stat。
      两者都只做一次 stat，不打开 SQLite、不跑统计查询——网络盘上每叶
      open+多查询会让同步卡很久。
    - ingested/pending 精确数不在此写：留给「构建」阶段填（对已存在叶子不传
      → upsert 保留其上次构建写入的值，不清零）。
    - sessions：
        · native：以 session_cache.db 为准，一次 GROUP BY 批量取真实会话数写回
          （无论是否追平；get 不到=0，覆盖历史脏值）。native 无 index.db，会话数
          只在这本聚合库里，故 sync 时一并回填，叶子详情才显示得对。
        · newapi：仅「已建」且 DB 里 sessions 仍为 0 的叶子读一次 nidb.status 写回，
          补齐历史/异地同步注册那批「有 index.db 但 DB sessions=0」的叶子。已有非零
          sessions 的叶子跳过读 status（open sqlite 贵），sessions 留 None → upsert 不覆盖。

    templates：用户注册的占位符层级模板列表（多行/多序列取并集）。空则按 fmt
    回退默认模板（default_templates）。同步末尾 upsert 一行 sources（表 1）。
    返回 {total, built, added, updated}。
    """
    import utils.logdir_store as lds
    root = os.path.normpath(path)
    rid = _root_id(root)
    root_path = Path(root)
    fmt = detect_format(root)
    is_native = (fmt == "native")
    # 「同步」用用户注册的占位符层级模板枚举叶子（逐段 iterdir，不递归 walk）。
    # templates 空 → 回退该 fmt 的默认模板（等价旧硬规则），保证老源不填也能同步。
    tpls = templates if templates else default_templates(fmt)
    total = built = added = updated = 0
    # 现有叶子的 sessions（一次查询）：已有非零值的叶子跳过 nidb.status()——那是每叶
    # open sqlite 的最贵操作。仅历史 sessions=0（或 DB 无此叶）的已建叶子才补读。
    existing_sessions = {r["dir_key"]: (r.get("sessions") or 0) for r in lds.bulk_get(rid)}
    # native 的 built 判定要读 session 聚合进度：一次性取该库全部 index_progress
    # （{叶子绝对路径: byte_offset}），循环内 dict 命中，避免逐叶查 SQLite。
    # session_store 未初始化时返回 {} → native 叶子一律判未 built（软失败，不崩同步）。
    # newapi 不用进度，跳过取以省一次全表读。
    native_progress: Dict[str, int] = {}
    native_sessions: Dict[str, int] = {}
    if is_native:
        import utils.session_store as session_store
        native_progress = session_store.get_all_progress()
        # native 的 session 数以 session_cache.db 为准（惰性聚合的真实结果），一次
        # GROUP BY 批量取，回填 leaf_status.sessions —— native 无 index.db 可读 status，
        # 旧逻辑此字段恒 0，叶子详情显示的 session 数一直不对。
        native_sessions = session_store.get_all_session_counts()
    for leaf in fast_scan_leaf_dirs_by_templates(root_path, tpls):
        dir_key = dir_key_for(root_path, leaf)
        total += 1
        # native：index.jsonl 消费追平才算 built；newapi：看 index.db 是否已建。
        is_built = (_native_leaf_built(leaf, native_progress)
                    if is_native else nidb._db_exists(str(leaf)))
        sessions = None
        if is_native:
            # native：无论 built 与否都回填真实 session 数（未追平的叶子也已聚合了
            # 部分会话，如实显示）。get 不到（从未聚合）→ 0，覆盖历史遗留脏值。
            sessions = native_sessions.get(str(leaf), 0)
            if is_built:
                built += 1
        elif is_built:
            built += 1
            # newapi 已建叶子：仅当 DB 里该叶 sessions 还是 0（历史遗留/新叶）时才读一次
            # status 落库（open sqlite 贵）；已有值的跳过，sessions 留 None → upsert 不覆盖。
            if existing_sessions.get(dir_key, 0) == 0:
                try:
                    st = nidb.status(str(leaf))
                    sessions = st.get("sessions", 0)
                except Exception:
                    sessions = None
        # state：已建=done。未建时 —— native 有 index.jsonl 却未追平 = 聚合在途，标
        # building 更贴切；newapi 无 index.db = 尚未回填，仍标 pending。
        if is_built:
            state = "done"
        else:
            state = "building" if is_native else "pending"
        res = lds.upsert_leaf(
            rid, dir_key, root_path=root,
            leaf_path=str(leaf),  # 真实折叠叶子绝对路径，供 mtime→叶子解析直接查表
            built=is_built,
            sessions=sessions,
            state=state,
        )
        if res == "added":
            added += 1
        elif res == "updated":
            updated += 1
    # 表 1 数据源行：同步末尾刷新格式/模板/叶子数/已建数（name 由 add/rename 维护，不传）
    lds.upsert_source(rid, root_path=root, format=fmt, templates=tpls,
                      leaf_count=total, built_count=built)
    # 探测缓存失效，让列表页下次读到最新（虽然现在改读 DB，仍保守清一下）
    with _disk_probe_lock:
        _disk_probe_cache.pop(root, None)
    return {"total": total, "built": built, "added": added, "updated": updated}


def _run(root: str, workers: int, force: bool = False):
    import utils.logdir_store as lds
    root_path = Path(root)
    rid = _root_id(root)
    all_leaves = [str(d) for d in iter_index_dirs(root_path)]

    # 非 force：只处理需要构建的叶子（无 db / index.jsonl 有新增 / 有待补 meta / sessions 脏）。
    if force:
        leaves = all_leaves
        skipped = 0
    else:
        leaves = [lf for lf in all_leaves if nidb.needs_build(lf)]
        skipped = len(all_leaves) - len(leaves)

    # 新的先建：叶子目录名是 YYMMDDHH，「当前小时」的数据决定页面可不可用。
    # 顺序不影响正确性（每叶独立、幂等），只影响用户多久能看见最新数据。
    leaves.sort(key=lambda lf: os.path.basename(lf.rstrip("/")), reverse=True)

    total = len(all_leaves)
    _set(root, status="running", total_leaves=total, done_leaves=skipped,
         skipped_leaves=skipped, force=force, started_at=time.time(), error="",
         current_leaf="", leaf_total=0, leaf_done=0,
         stuck_leaves=0, last_stuck="")

    bf_logger.info("ROOT START root=%s force=%s total=%d to_build=%d skipped=%d workers=%d",
                   root, force, total, len(leaves), skipped, workers)

    if not leaves:
        _set(root, status="done", finished_at=time.time())
        bf_logger.info("ROOT DONE root=%s nothing to build (all %d up-to-date)", root, total)
        return

    done = skipped
    stuck = 0
    failed = 0
    ok = 0
    locked_skipped = 0
    try:
        for leaf in leaves:
            _set(root, current_leaf=os.path.basename(leaf.rstrip("/")),
                 leaf_total=0, leaf_done=0)
            dir_key = dir_key_for(root_path, Path(leaf))
            lds.set_leaf_state(rid, dir_key, "building")

            def _progress(d, t, _root=root):
                _set(_root, leaf_done=d, leaf_total=t)

            t0 = time.time()
            try:
                # 跨进程互斥：watcher（src/watch/worker.py）可能正在同步同一片叶子。
                # 拿不到锁就跳过它，下一轮（或被 watcher 追平后）再说——不标 error，
                # 因为这不是失败，只是「这块地现在有别人在种」。短超时：回填不该为了
                # 一片叶子堵住整条串行队列，它后面还有几十片。
                try:
                    lock = leaf_lock(leaf, timeout=_LEAF_LOCK_WAIT).__enter__()
                except LeafLockBusy:
                    locked_skipped += 1
                    bf_logger.info("LEAF LOCKED root=%s leaf=%s (watcher 正在同步，本轮跳过)",
                                   root, dir_key)
                    lds.set_leaf_state(rid, dir_key, "pending")
                    done += 1
                    _set(root, done_leaves=done, current_leaf="", leaf_total=0, leaf_done=0)
                    continue
                try:
                    if force:
                        nidb.remove_db(leaf)
                    nidb.build_leaf(leaf, workers=workers, progress_cb=_progress,
                                    stall_timeout=_STALL_TIMEOUT)
                    # 构建后读事实源，逐叶落盘（重启后据此显示，无需再全盘 stat）
                    try:
                        st = nidb.status(leaf)
                    except Exception:
                        st = {"built": False, "ingested": 0, "pending": 0, "sessions": 0}
                finally:
                    lock.__exit__(None, None, None)
                is_built = bool(st.get("built"))
                lds.set_leaf_state(
                    rid, dir_key, "done" if is_built else "pending",
                    built=is_built, ingested=st.get("ingested", 0),
                    pending=st.get("pending", 0), sessions=st.get("sessions", 0),
                )
                ok += 1
                bf_logger.info(
                    "LEAF OK root=%s leaf=%s built=%s ingested=%d pending=%d sessions=%d took=%.1fs",
                    root, dir_key, is_built, st.get("ingested", 0),
                    st.get("pending", 0), st.get("sessions", 0), time.time() - t0)
            except nidb.LeafStalledError as e:
                # 疑似 NFS 读死等：跳过该叶子 + 预警。标 error → needs_build 下次重扫，
                # 待人在日志管理页手动「增量构建」补跑（NFS 恢复后）。
                leaf_name = os.path.basename(leaf.rstrip("/"))
                stuck += 1
                _set(root, last_error=f"{leaf}: {e}",
                     stuck_leaves=stuck,
                     last_stuck=f"{leaf_name}（{e.elapsed:.0f}s 无进展）")
                lds.set_leaf_state(rid, dir_key, "error",
                                   last_error=f"stalled: NFS read timeout ({e.elapsed:.0f}s)")
                bf_logger.warning(
                    "LEAF STALLED root=%s leaf=%s done=%d/%d elapsed=%.0fs (疑似 NFS 阻塞，已跳过待补跑)",
                    root, dir_key, e.done, e.total, e.elapsed)
                logger.warning(
                    "new-api 回填叶子卡住（疑似 NFS 阻塞），已跳过并待补跑: %s "
                    "done=%d/%d elapsed=%.0fs", leaf, e.done, e.total, e.elapsed)
            except Exception as e:  # noqa: BLE001 — 单叶子失败不拖垮整个 root
                failed += 1
                _set(root, last_error=f"{leaf}: {e}")
                lds.set_leaf_state(rid, dir_key, "error", last_error=str(e))
                if _is_broken_pool_err(e):
                    # worker 进程被 OS 终止（enrich 内部已自动重建池续跑，这是重试仍失败）：
                    # 单独提示，方便与普通解析失败区分。
                    bf_logger.exception(
                        "LEAF FAIL root=%s leaf=%s took=%.1fs err=%s "
                        "(worker 进程被终止，enrich 已自动重建进程池续跑，仍失败)",
                        root, dir_key, time.time() - t0, e)
                    logger.exception(
                        "new-api 回填叶子 worker 崩溃: %s err=%s", leaf, e)
                else:
                    bf_logger.exception(
                        "LEAF FAIL root=%s leaf=%s took=%.1fs err=%s",
                        root, dir_key, time.time() - t0, e)
            done += 1
            _set(root, done_leaves=done, current_leaf="", leaf_total=0, leaf_done=0)
        _set(root, status="done", finished_at=time.time())
        bf_logger.info(
            "ROOT DONE root=%s ok=%d failed=%d stalled=%d locked_skipped=%d skipped=%d total=%d",
            root, ok, failed, stuck, locked_skipped, skipped, total)
    except Exception as e:  # noqa: BLE001
        _set(root, status="error", error=str(e), finished_at=time.time())
        # root 级崩溃（如 needs_build 遍历撞上损坏 db）：既落专用构建日志，也打一份到 app.log，
        # 避免像这次那样静默——error 只进内存又被前端 DB 权威状态盖掉，全程无痕。
        bf_logger.exception("ROOT CRASH root=%s err=%s", root, e)
        logger.exception("new-api 回填 root 崩溃: %s", root)


def _run_guarded(root: str, workers: int, force: bool):
    """[DEPRECATED] 进程内线程入口：跑完 _run 后从 _running 摘除自己。

    回填已剥离到独立 backfill_worker 进程（走 backfill_requests 表 + _run），此线程
    版仅在 EXPORT/BACKFILL 尚未切换的历史路径保留，正常不再调用。
    """
    try:
        _run(root, workers, force)
    except Exception as e:  # noqa: BLE001 — 单个 root 失败不拖垮别的回填线程
        _set(root, status="error", error=str(e), finished_at=time.time())
        bf_logger.exception("ROOT CRASH root=%s err=%s", root, e)
        logger.exception("new-api 回填 root 崩溃: %s", root)
    finally:
        with _running_lock:
            _running.pop(root, None)


def _running_roots() -> list:
    """当前正在回填的 root 列表（清理已结束的线程后返回）。"""
    with _running_lock:
        for r in [k for k, t in _running.items() if not t.is_alive()]:
            _running.pop(r, None)
        return list(_running.keys())


def has_active_backfill(root: str) -> bool:
    """该 root 是否有在途回填请求（pending/running）。跨进程真相源 = backfill_requests 表。"""
    try:
        import utils.logdir_store as lds
        return lds.has_active_backfill(os.path.normpath(root))
    except Exception:
        return False


def get_queue_snapshot() -> dict:
    """供 UI/导出页展示当前排队 / 正在回填的 root。

    真相源改为 backfill_requests 表（跨进程）：worker 领取一条置 running，串行执行，
    故 running 至多一条，其余 pending 即真实排队。为兼容旧调用方保留字段：
      - current：正在跑的 root（running 行，至多一个）
      - running_roots：所有 running 行的 root（完整列表，通常 0/1 个）
      - queued：pending 行的 root 列表（真实排队，向用户透明）
    进度（total/done 叶子）由调用方另读 get_backfill_status/count_summary，不在此拼。
    """
    try:
        import utils.logdir_store as lds
        active = lds.list_active_backfill()
    except Exception:
        active = []
    running = [r["root"] for r in active if r.get("status") == "running"]
    queued = [r["root"] for r in active if r.get("status") == "pending"]
    current = running[0] if running else None
    with _status_lock:
        cur_st = dict(_status.get(current, {})) if current else None
    return {
        "current": current,
        "current_status": cur_st,
        "running_roots": running,
        "queued": queued,
        "queue_len": len(queued),
        "running": bool(running),
    }


def start_backfill(path: str, workers: Optional[int] = None, force: bool = False) -> dict:
    """[DEPRECATED] 进程内起线程立即回填。已被「入队 backfill_requests + backfill_worker
    独立进程」取代（见 logs_routes.logs_admin_backfill）。保留仅供历史/排障回退，
    正常路径不再调用——它把执行放回 app 进程内，正是本次剥离要消除的。

    幂等、立即返回：同一 root 已在跑 → 不重复起。force 语义同 _run。
    """
    root = os.path.normpath(path)
    if detect_format(root) != "newapi":
        bf_logger.info("BACKFILL SKIP root=%s not a new-api root", root)
        return {"status": "skipped", "reason": "not a new-api root"}

    w = workers or _DEFAULT_WORKERS
    with _running_lock:
        t = _running.get(root)
        if t is not None and t.is_alive():
            bf_logger.info("BACKFILL DEDUP root=%s already running", root)
            return {"status": "running", "root": root}
        t = threading.Thread(target=_run_guarded, args=(root, w, force),
                             daemon=True, name=f"newapi-backfill-{os.path.basename(root)}")
        _running[root] = t
        # 占位状态：线程刚起、_run 尚未 _set 前的极短窗口里，UI 也能读到 running。
        _set(root, status="running", started_at=time.time())
        t.start()
    bf_logger.info("BACKFILL START root=%s workers=%d force=%s", root, w, force)
    return {"status": "running", "root": root, "workers": w, "force": force}
