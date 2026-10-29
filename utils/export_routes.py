"""
utils/export_routes.py — Session 导出 Web 路由

与 keys 管理界面共享认证（key_state.yaml 密码），
通过 /keys/export 进入导出页面。
"""

import json
import logging
import os
import queue
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Request, Query
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from utils.export_modes import get_config as get_export_modes
from utils.export_store import (
    append_log,
    update_progress,
    cancel_interrupted,
    create_record,
    delete_record,
    restore_record,
    get_record,
    get_leaves_cache,
    list_records,
    list_records_all_slim,
    list_records_by_key,
    save_leaves_cache,
    set_manage_name,
    update_status,
)
from utils.export_jobs import persist_params, persist_upload_retry
from utils.export_sync import export_session_index, sync_session_index, _load_session_index
from utils.key_config import load_key_state
from utils.key_store import list_keys, mask_key
from utils.log_paths import get_service_log_dir, get_log_dir
from utils.obs_utils import load_obs_base, load_sync_config, obsutil_ls
from utils.stats_index import (
    refresh_index, build_stats_from_index, build_stats_multi, get_date_to_mtime_map,
    get_current_key_cache, update_key_meta, update_key_records,
    get_last_refresh_ts, start_stats_warmer,
)
from utils.logs_config import get_stats_roots
from utils.timing import StageTimer

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 全局任务队列（串行执行，支持取消排队中任务）
# ---------------------------------------------------------------------------
# 队列项: (record_id, fn, args)
# fn 是无参可调用，内部捕获所有上下文
#
# 多 worker 说明：任务 fn 是内存闭包，只存在于「接收入队请求」的那个 worker 的
# 队列里，无法跨进程。这里保证两条不变量：
#   1) 取消跨 worker 生效：cancel 请求（可能落到别的 worker）把 DB 状态置为
#      cancelled；执行 worker 出队时以 DB 状态为准重新判定，取消则跳过。
#   2) 全局串行：用文件锁（flock 阻塞）保证任意时刻只有一个 worker 在真正执行
#      导出任务，避免多 worker 各自的 runner 并发跑导出撞 OBS/本地拷贝。
_task_queue: queue.Queue = queue.Queue()
_queue_lock = threading.Lock()

# record_id -> True 表示该任务已被取消，调度线程出队后直接跳过（本进程内快速路径）
_cancelled_ids: set = set()

# 全局并发上限：同一时刻最多几个导出任务在真正执行（跨 worker）。
# 用 EXPORT_CONCURRENCY 配置，默认 1（同一时刻只跑一个导出/质检，其余自动排队）。
_EXPORT_CONCURRENCY = max(1, int(os.getenv("EXPORT_CONCURRENCY", "1")))

# 单任务内的默认并发：workers = 每个 mtime 目录的并行度，dir_workers = 同时处理几个目录。
#
# 两者相乘就是「同时在跑的评估单元数」。评估器改成进程池后（见 utils/eval/pool.py），
# 这个乘积直接等于同时存在的子进程数 —— 8×20=160 个进程 × 约 40–60MB RSS，且全部在
# 同一 NFS 上争用，得不偿失。实测 32 个并行单元已能打满单机吞吐，故默认收到 4×8。
# 两者都可由任务记录里的 workers / dir_workers 覆盖。
_DEFAULT_WORKERS = max(1, int(os.getenv("EXPORT_WORKERS", "8")))
_DEFAULT_DIR_WORKERS = max(1, int(os.getenv("EXPORT_DIR_WORKERS", "4")))

# N 个槽位锁文件，任务执行前非阻塞抢占任一空槽，抢到才执行 → 全局并发严格 ≤ N。
#
# 关键：锁目录必须是**绝对路径**，且在运行时（而非模块 import 时）解析。
# app 进程与独立 export_worker 进程可能有不同 cwd，若在 import 时用相对路径求值，
# 两者会解析到不同目录 → 跨进程文件锁失效、导出并发失控。因此这里留空，由
# init_export_lock_dir() 在启动时（app 的 register_export_routes / worker 的 _init_env）
# 显式初始化为同一绝对路径。
_EXPORT_LOCK_DIR = ""
_EXPORT_LOCK_PATHS: list = []


def init_export_lock_dir(d: "str | None" = None) -> None:
    """初始化导出槽位锁目录为绝对路径，并生成 N 个槽位锁文件路径。

    app 与 worker 双端在启动时各调一次；只要 get_service_log_dir() 在相同环境变量
    下解析一致（PROXY_PORT/LOG_TASK_TAG/UPSTREAM_API_KEY 相同），两端就落到同一
    绝对目录，跨进程互斥成立。
    """
    global _EXPORT_LOCK_DIR
    _EXPORT_LOCK_DIR = os.path.abspath(d or get_service_log_dir())
    _EXPORT_LOCK_PATHS[:] = [
        os.path.join(_EXPORT_LOCK_DIR, f"export_queue.slot{i}.lock")
        for i in range(_EXPORT_CONCURRENCY)
    ]


def _acquire_slot():
    """非阻塞轮询抢占任一空槽，返回持锁的文件对象；抢不到返回 None。"""
    import fcntl
    if not _EXPORT_LOCK_DIR:
        # 兜底：调用方未显式 init（不应发生），按当前环境解析一次。
        init_export_lock_dir()
    os.makedirs(_EXPORT_LOCK_DIR, exist_ok=True)
    for path in _EXPORT_LOCK_PATHS:
        try:
            fp = open(path, "w")
            fcntl.flock(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return fp
        except OSError:
            try:
                fp.close()
            except OSError:
                pass
            continue
    return None


def _release_slot(fp) -> None:
    import fcntl
    if fp is None:
        return
    try:
        fcntl.flock(fp, fcntl.LOCK_UN)
        fp.close()
    except OSError:
        pass


def _is_cancelled(record_id: int) -> bool:
    """出队时判定任务是否已取消：先查本进程集合，再以 DB 状态为准（跨 worker）。"""
    with _queue_lock:
        if record_id in _cancelled_ids:
            _cancelled_ids.discard(record_id)
            return True
    try:
        rec = get_record(record_id)
        if rec and rec.get("status") == "cancelled":
            return True
    except Exception:
        pass
    return False


def verify_access_key(access_key: str) -> bool:
    """公开导出浏览/提交的鉴权 key（URL 参数 access-key）校验。

    值取自 .env 的 ACCESS_KEY，**无默认值**：未配置时恒拒。

    用 hmac.compare_digest 做常数时间比较，防时序侧信道。
    """
    expected = (os.getenv("ACCESS_KEY") or "").strip()
    if not expected:
        return False
    import hmac as _hmac
    return _hmac.compare_digest((access_key or "").strip(), expected)


# 共享 key 命名空间缓存的 TTL（秒）。resolve_export_key 在 /export/view 及其子 API 里
# 每个请求都会调用一次 _collect_log_keys（跨 root 扫盘）；日志 key 集近期稳定，
# 短 TTL 缓存可避免同一页面的 page/view/api/... 串行请求各自重扫。负缓存也缓存
# （空集是合法结果——该环境确实可能没有任何已落库 key）。
_EXPORT_KEY_CACHE: dict = {"ts": 0.0, "data": None}
# build_stats_multi 要 refresh_index 扫盘，比读 DB 慢一档，TTL 放宽些（默认 30s）。
_EXPORT_KEY_TTL = float(os.getenv("EXPORT_KEY_TTL", "30"))


def _collect_log_keys(roots: list, env_dir: str, *, limit: int = 200000) -> set:
    """汇总「日志命名空间」里出现过的全部 api_key —— 以 build_stats_multi 扫盘结果为真相。

    这是 key 校验应参照的真相源：列表（/api/export/keys）与导出数据过滤
    （backend.list_sessions 的 `api_key = ?` 精确匹配）都消费日志 api_key，
    而 keys.db（api_keys 表）只记录 Web 端**签发**的少量 key，二者是不同命名空间。
    用 build_stats_multi 而非读 DB 的原因：newapi 叶子大多**未构建 index.db**
    （get_known_keys 返回空），其 key 只出现在原始日志里，DB 聚合读不到；只有
    build_stats_multi 的 refresh_index 扫盘（从 index.jsonl / 原始 req 文件聚合）
    能覆盖到全量 key。这正是过去「完整 key 明明在日志里却报 Key not found」的根因。

    roots 传 None/空时直接返回空集（调用方自行兜底）。

    返回 set[str]。带 limit 防御极端数量下误爆内存。
    """
    if not roots:
        return set()
    import time as _t
    _now = _t.monotonic()
    if _EXPORT_KEY_CACHE["data"] is not None and (_now - _EXPORT_KEY_CACHE["ts"] < _EXPORT_KEY_TTL):
        return _EXPORT_KEY_CACHE["data"]
    all_keys: set = set()
    # 与 build_stats_multi 同源：它内部先 refresh_index（扫盘/增量）+ 按 api_key 归并，
    # rows[].api_key 即跨 root 去重后的完整 key 集。force=False 走 TTL/增量，避免每请求全量扫。
    try:
        import utils.stats_index as _si
        stats = _si.build_stats_multi(list(roots), active_env_dir=str(env_dir))
        for row in stats.get("rows", []):
            k = row.get("api_key")
            if not k:
                continue
            all_keys.add(k)
            if len(all_keys) >= limit:
                _EXPORT_KEY_CACHE["ts"] = _now
                _EXPORT_KEY_CACHE["data"] = all_keys
                return all_keys
    except Exception:
        pass
    # 并上 leaf_status.token_stats_json 里的 key。
    # stats_json 是 /sessions 统计的缓存，它的失效签名只校验 index.jsonl 的
    # size+mtime；而 watcher 补进 index.db 的 session 不会改 index.jsonl，于是
    # 那份快照会「名义有效」却落后于 index.db（实测某叶 sessions 有 26 个 key、
    # stats_json 只有 24 个）。token_stats_json 由 watcher/回填在写库时刷新，
    # 覆盖更全 —— 实测两者并集比单用 stats_json 多 51 个 key，且读本地库约 0.02s。
    try:
        import json as _json
        import utils.logdir_store as _lds
        if _lds._ready():
            with _lds._lock:
                rows = _lds._conn.execute(
                    "SELECT token_stats_json FROM leaf_status WHERE token_stats_json != ''"
                ).fetchall()
            for (raw,) in rows:
                try:
                    tj = _json.loads(raw)
                except (ValueError, TypeError):
                    continue
                for bk in (tj.get("keys") or {}):
                    k = bk.split("|", 1)[0]
                    if k:
                        all_keys.add(k)
    except Exception:  # noqa: BLE001
        pass
    _EXPORT_KEY_CACHE["ts"] = _now
    _EXPORT_KEY_CACHE["data"] = all_keys
    return all_keys


def resolve_export_key(key: str, roots: list, env_dir: str) -> "Optional[str] | int":
    """把用户经 /export/* 传入的 key 解析成**日志命名空间**里的完整 api_key。

    传入 key 可以是完整串（`sk-...mQOk`）、后四位后缀（`mQOk`）、
    或带 `key-` 前缀的 slot 形式（`key-mQOk`，即「导出概览」里显示的 key_slot）。规则：
      - 归一化：去掉 `key-` 前缀；空串 → None。
      - 传入值非空且是某日志 api_key 的完整值 → 返回该值本身。
      - 否则若作为后四位后缀唯一命中一个日志 api_key → 返回该完整 key。
      - 命中多个 → 返回 -1（调用方返回「key 不唯一」错误）。
      - 一个都命中不了 → 返回 None（调用方返回 404 Key not found）。
    从 keys.db 的 api_keys 表（Web 签发命名空间）里找不到不再视为失败——因为
    列表中绝大多数 key 从未被签发（见 _collect_log_keys 注释）。完整 key 若不在日志里
    （如只在 keys.db 里、从未产生会话）也会返回 None，行为与「日志命名空间」一致。

    注意：纯数字串（如 "1234"）会被误当作后四位后缀参与匹配——这没问题，按后缀语义
    匹配即可；旧 find_key 的 `isdigit()` 走 id 分支现在是明确的非目标。

    返回：完整 api_key / None / -1。
    """
    key = (key or "").strip()
    if key.startswith("key-"):
        key = key[len("key-"):].strip()
    if not key:
        return None
    log_keys = _collect_log_keys(roots, env_dir)
    if not log_keys:
        return None
    # 1) 完整命中优先（区分大小写精确）。
    if key in log_keys:
        return key
    # 2) 后四位后缀匹配（含 slot 形式剥除 key- 前缀后的值）。
    if len(key) <= 4:
        hits = [k for k in log_keys if k.endswith(key)]
        if not hits:
            return None
        if len(hits) > 1:
            return -1  # 不唯一
        return hits[0]
    return None


def _queue_runner():
    """调度线程，逐个执行队列中的任务。跨 worker 用 N 槽位文件锁保证全局并发 ≤ N。"""
    import time
    while True:
        record_id, fn = _task_queue.get()
        if _is_cancelled(record_id):
            _task_queue.task_done()
            continue
        lock_fp = None
        try:
            # 抢占任一空槽；所有槽被占（已达全局并发上限）时轮询等待
            while True:
                if _is_cancelled(record_id):
                    break
                lock_fp = _acquire_slot()
                if lock_fp is not None:
                    break
                time.sleep(1)
            if lock_fp is None:  # 等待期间被取消
                continue
            # 拿到槽后再确认一次是否在等待期间被取消
            if _is_cancelled(record_id):
                continue
            fn()
        except Exception:
            logger.exception("queue_runner: task %s raised", record_id)
        finally:
            _release_slot(lock_fp)
            _task_queue.task_done()


# 是否在 app 进程内跑导出队列（旧行为）。默认关闭：导出执行已剥离到独立的
# export_worker 进程。仅在临时排障 / 回退时置 EXPORT_QUEUE_IN_PROCESS=1 恢复旧路径。
_EXPORT_QUEUE_IN_PROCESS = os.getenv("EXPORT_QUEUE_IN_PROCESS", "") in ("1", "true", "True")


def _enqueue_task(record_id: int, fn=None) -> None:
    """入队 = 把记录状态置 queued，交由独立 export_worker 进程从 DB 领取执行。

    任务参数在入队前已由调用方 persist_params() 写进 export_records.task_json，
    worker 靠 record_id + task_json 重建执行，不依赖内存闭包。

    fn 参数仅在 EXPORT_QUEUE_IN_PROCESS=1 回退路径下使用（塞进本进程内存队列）。
    """
    from utils.export_store import update_status
    update_status(record_id, "queued")
    if _EXPORT_QUEUE_IN_PROCESS and fn is not None:
        _task_queue.put((record_id, fn))


# 回退路径：仅当 EXPORT_QUEUE_IN_PROCESS=1 时，才在 app 进程内起 N 个 runner。
# 默认（剥离模式）不启动任何线程，导出执行完全交给 export_worker 进程。
if _EXPORT_QUEUE_IN_PROCESS:
    for _i in range(_EXPORT_CONCURRENCY):
        _t = threading.Thread(target=_queue_runner, daemon=True, name=f"export-queue-runner-{_i}")
        _t.start()


def _load_sync_config() -> dict:
    return load_sync_config()


def _require_key_api(request: Request):
    if request.headers.get("x-requested-with") != "XMLHttpRequest":
        return JSONResponse({"detail": "Not found"}, status_code=404)
    return None


def _key_slot(api_key: str) -> str:
    if not api_key:
        return "all"
    return "key-" + api_key[-4:]


def _key_name_snapshot(api_key: str) -> str:
    """建任务时快照 key 名称（按后 4 位匹配 DB keys 列表），供历史记录展示。

    key 后续改名不会影响已建记录；无匹配时返回 ''，前端回退显示 api_key 尾部。
    """
    if not api_key or api_key == "(empty)":
        return ""
    try:
        from utils.key_store import list_keys
        for k in list_keys():
            if k.get("key", "").endswith(api_key[-4:]):
                return k.get("name", "") or ""
    except Exception:
        return ""
    return ""


# ---------------------------------------------------------------------------
# 目录解析 & 任务执行（模块级）
#
# 这些函数原为 register_export_routes 内的闭包，现提到模块级并显式接收 env_dir，
# 使独立的 export_worker 进程无需构造 FastAPI app 即可复用同一套执行逻辑。
# app 内的 register_export_routes 保留同名薄封装委托到这里，端点调用点不变。
# ---------------------------------------------------------------------------

def _all_roots_for(env_dir) -> list:
    """活跃 env_dir + 配置的历史路径（去重）。"""
    return get_stats_roots(str(env_dir))


def _existing_roots_for(env_dir) -> list:
    """只保留实际存在的 root。决定 mtime key 是否带 <root_basename>/ 前缀。"""
    return [r for r in _all_roots_for(env_dir) if Path(r).is_dir()]


def _resolve_mt_for(env_dir, mt: str) -> Optional[str]:
    """把 mtime key（<root_id>/<rel> 或裸 <rel>）解析为叶子目录绝对路径。

    以 leaf_status.leaf_path 为准（同步落库的真实叶子路径，含同名折叠层补回）：
    逐 root 拆前缀得到 rel_key，DB 查表命中即返回；均未命中再退回「拼 root/rel_key
    并 is_dir 校验」定位归属 root（未同步/裸 native 单层）。mtime 列表也读自
    leaf_status，正常情况下首个 DB 查表即命中，无需扫盘。
    """
    from utils.logs_config import get_root_id
    from utils.logdir_store import resolve_leaf_path
    roots = _existing_roots_for(env_dir)
    multi = len(roots) > 1
    # 拆前缀候选：(root, rel_key)。多 root 时按 <rid>/ 或旧 <basename>/ 前缀拆，
    # 单 root（或裸 key）时 rel_key = mt。
    cands = []
    for root in roots:
        rid = get_root_id(root, str(env_dir))
        base = os.path.basename(os.path.normpath(root))
        matched = False
        for pfx in (rid, base):
            if multi and mt.startswith(pfx + "/"):
                cands.append((root, rid, mt[len(pfx) + 1:]))
                matched = True
        if not matched:
            cands.append((root, rid, mt))
    # 1) DB 查表优先：首个命中即返回（O(1)，不扫盘）
    for root, rid, rel_key in cands:
        try:
            hit = resolve_leaf_path(rid, rel_key)
        except Exception:
            hit = None
        if hit:
            return hit
    # 2) DB 未命中兜底：拼 root/rel_key 并 is_dir 校验，定位归属 root
    for root, rid, rel_key in cands:
        cand = Path(root) / rel_key
        if cand.is_dir():
            return str(cand)
    cand = Path(env_dir) / mt
    return str(cand) if cand.is_dir() else None


class _ProgressReporter:
    """把「已完成 / 总数」节流写进 export_records，供前端画进度条。

    为什么要节流：处理器每完成一个 session 都会回调一次，一个任务动辄几万次。
    每次都 UPDATE 会在 SQLite 上打出成吨无意义的写；这里限制到最多每 interval 秒
    一次，且进度百分比没有变化时直接跳过（大任务初期的抖动也被吞掉）。

    多目录并行时各线程共用同一个 reporter：进度按「全局累计」合并上报，
    而不是各目录各报各的（否则进度条会来回跳）。
    """

    def __init__(self, record_id: int, total: int = 0, phase: str = "",
                 interval: float = 1.0):
        self.record_id = record_id
        self.total = int(total or 0)
        self.phase = phase
        self.interval = float(interval)
        self._done = 0
        self._last_ts = 0.0
        self._last_pct = -1
        self._lock = threading.Lock()

    def set_phase(self, phase: str, total: Optional[int] = None) -> None:
        """切换阶段（如 索引 → 重构 → 上传），并按需重置分母。"""
        with self._lock:
            self.phase = phase
            if total is not None:
                self.total = int(total or 0)
            self._done = 0
            self._last_pct = -1
        self._flush(force=True)

    def set_progress(self, done: int, total: int, phase: str = "") -> None:
        """直接设定绝对值（多目录并行时由调用方持有全局计数）。

        与 advance() 的区别：advance 是本 reporter 内部累加，适合单目录；
        多目录并行时每个目录各推进一格，全局计数由调用方维护，这里只负责落库。
        """
        with self._lock:
            self._done = int(done)
            self.total = int(total)
            if phase:
                self.phase = phase
            now = time.monotonic()
            if now - self._last_ts < self.interval:
                return
            pct = self._pct_locked()
            if pct == self._last_pct:
                return
            self._last_ts = now
            self._last_pct = pct
            done_v, total_v, phase_v = self._done, self.total, self.phase
        try:
            update_progress(self.record_id, phase=phase_v, done=done_v, total=total_v)
        except Exception:  # noqa: BLE001
            logger.debug("update_progress 失败（忽略）", exc_info=True)

    def advance(self, n: int = 1) -> None:
        """完成 n 个单元。超过 interval 才真正落库。"""
        with self._lock:
            self._done += n
            now = time.monotonic()
            if now - self._last_ts < self.interval:
                return
            pct = self._pct_locked()
            if pct == self._last_pct:
                return
            self._last_ts = now
            self._last_pct = pct
            done, total, phase = self._done, self.total, self.phase
        try:
            update_progress(self.record_id, phase=phase, done=done, total=total)
        except Exception:  # noqa: BLE001  进度条失败绝不能影响导出本身
            logger.debug("update_progress 失败（忽略）", exc_info=True)

    def _pct_locked(self) -> int:
        if self.total <= 0:
            return -1
        return min(99, int(100 * self._done / self.total))

    def _flush(self, force: bool = False) -> None:
        with self._lock:
            done, total, phase = self._done, self.total, self.phase
            self._last_ts = time.monotonic()
            self._last_pct = self._pct_locked()
        try:
            update_progress(self.record_id, phase=phase, done=done, total=total)
        except Exception:  # noqa: BLE001
            logger.debug("update_progress 失败（忽略）", exc_info=True)

    def finish(self) -> None:
        """任务收尾：把进度补到满，避免留下 97% 这种尾巴。"""
        with self._lock:
            if self.total > 0:
                self._done = self.total
        self._flush(force=True)


def _log_dir_key_for(env_dir, mt_src: str) -> str:
    """把叶子目录绝对路径解析为相对 root 的 dir_key（如 "260728/26072813"）。"""
    from utils.log_scan import dir_key_for
    p = Path(mt_src)
    for root in _existing_roots_for(env_dir):
        rp = Path(root)
        try:
            if p == rp or rp in p.parents:
                return dir_key_for(rp, p)
        except (OSError, ValueError):
            continue
    return p.name


def _run_upload_only(record_id, local_copy_dir, obs_dst):
    """仅执行上传步骤，用于对已完成导出/质检但上传失败的记录进行重试。"""
    from utils.obs_sync import _run_upload_cmd
    _log = lambda msg: append_log(record_id, msg)
    try:
        sync_cfg = _load_sync_config()
        upload_script = sync_cfg.get("upload_script") or None
        obs_parent = obs_dst.rstrip("/").rsplit("/", 1)[0] + "/"
        _log(f"重试上传: {local_copy_dir} -> {obs_parent}")
        ok, msg = _run_upload_cmd(local_copy_dir, obs_parent, upload_script, log_cb=_log)
        if ok:
            _log("上传成功")
            rec = get_record(record_id) or {}
            update_status(record_id, "success",
                          files_uploaded=rec.get("total_sessions", 0),
                          error_message="")
        else:
            _log(f"上传失败: {msg}")
            update_status(record_id, "failed", error_message=f"OBS upload: {msg}")
    except Exception as e:
        logger.exception("_run_upload_only crashed (record=%s)", record_id)
        update_status(record_id, "failed", error_message=str(e))


def _new_now_tag() -> str:
    """生成产物目录/OBS 前缀用的时间戳，精确到毫秒。

    原先是 %y%m%d%H%M%S（秒级）。同一秒内向同一 key 发起两个任务时，两者会算出
    同一个 ex-<tag>，落到同一个 local_base 与同一个 OBS 前缀上互相覆盖 —— 单 worker
    串行时几乎撞不到，但多 worker 并发（EXPORT_CONCURRENCY>1）或批量提交时会撞。
    保留原有可读格式，追加 3 位毫秒，长度从 12 位变 15 位。
    """
    return datetime.now().strftime("%y%m%d%H%M%S%f")[:-3]


def _default_mode() -> str:
    """注册表里 pipeline=copy 的首个模式（即「导出原始文件」），作为 mode 兜底默认值。"""
    cfg = get_export_modes()
    for m in cfg.all():
        if m.pipeline == "copy":
            return m.id
    return cfg.ids()[0] if cfg.ids() else "export"


def _run_merged_hours(mtime_dirs, _env_dir, local_base, api_key, force, workers,
                      processor, _mcfg, _log, _tm, errors, warnings, _should_cancel,
                      _progress_lock, _progress_done, _progress_total, _progress_phase,
                      _progress_reporter, dir_workers: int = 4):
    """跨小时节点聚合导出：先全收集、再合并、最后统一产出。

    为什么要分三阶段：newapi 的数据按小时分叶子库，同一段对话跨整点会被切成多个
    session。逐叶导出（原有路径）会把它们导出成 N 条独立产物。要合并就必须先把手
    所有叶子里的会话都读出来，才能知道哪几段属于同一段对话。

    与逐叶路径的差异只在「怎么组织 work」，产出格式与 processor 完全一致：
      - gather：每个小时目录只做 export_session_index 拿 sessions，并给每个 trace
        标注它实际所在的叶子目录（_src_dir）
      - merge：按 chain_lookup 把同一段对话的多个小时段合并成一条
      - process：把合并后的 session 交给 processor，用 file_dirs 让它跨目录取源文件

    返回 (all_results, all_entries, total_sessions)，与逐叶路径的累加量同义。
    """
    from utils.session_merge import merge_segments
    from utils.export_sync import export_session_index

    all_results = []
    all_entries = []
    gathered = []          # [(session_entry, leaf_dir), ...]
    leaf_of_session = {}   # id(session_entry) -> leaf_dir

    # ── 阶段 1：收集 ──────────────────────────────────────────────────
    with _tm.stage("收集会话（跨小时）"):
        # 并发收集：原逐叶路径有 dir_workers 层目录并发，这里如果写成串行循环，
        # 14 个目录实测 16.1s，而并发 8 只要 3.0s（大目录单叶 2.7~3.3s）。
        # 收集是纯读 + 各叶子独立，并发没有写竞态。
        from concurrent.futures import ThreadPoolExecutor as _GTPE

        def _gather_one(mt):
            """返回 (mt, [(session, leaf)], warnings, errors, 是否被取消)。"""
            if _should_cancel():
                return mt, [], [], [], True
            mt_src = _resolve_mt_for(_env_dir, mt) or str(_env_dir / mt)
            try:
                from utils.log_scan import detect_format as _df
                _fmt = _df(mt_src)
                if not _mcfg.supports_format(_fmt):
                    return mt, [], [f"{mt}: 非 {'/'.join(_mcfg.formats)} 格式（{_fmt}），已跳过"], [], False
                if _fmt == "newapi":
                    import utils.newapi_index_db as _nidb
                    if _nidb.needs_build(mt_src):
                        return mt, [], [f"{mt}: 索引未构建/不完整，已跳过（请先手动构建索引）"], [], False
            except Exception as _pe:  # noqa: BLE001
                return mt, [], [f"{mt}: 索引未构建/不完整，已跳过（检查异常: {_pe}）"], [], False

            try:
                exp = export_session_index(mt_src, force=force, with_sessions=True,
                                           api_key=api_key)
            except Exception as e:  # noqa: BLE001
                logger.exception("跨小时收集失败 %s", mt_src)
                return mt, [], [], [f"{mt}: {e}"], False

            sessions = exp.get("sessions")
            if sessions is None:
                # 不带 api_key 且索引未变更时会走 skipped 分支，不返回 sessions，
                # 必须回落到读 session_index.jsonl，否则这一小时的数据会被静默丢掉。
                from utils.export_sync import _load_session_index
                sessions = _load_session_index(mt_src)
            out = [(s, mt_src) for s in sessions
                   if not api_key or s.get("api_key") == api_key]
            _log(f"[{mt}] 收集 {len(out)} sessions")
            return mt, out, [], [], False

        _cancelled = False
        with _GTPE(max_workers=min(max(1, dir_workers), len(mtime_dirs))) as _ex:
            # map 保序，warnings/errors 的先后与原串行循环一致
            for mt, pairs, warns, errs, cancel in _ex.map(_gather_one, mtime_dirs):
                warnings.extend(warns)
                errors.extend(errs)
                if cancel:
                    _cancelled = True
                gathered.extend(pairs)
        if _cancelled:
            _log("任务已终止（用户取消）")
            return all_results, all_entries, 0

    if not gathered:
        _log("跨小时聚合：没有收集到任何 session")
        return all_results, all_entries, 0

    # ── 阶段 2：合并 ──────────────────────────────────────────────────
    with _tm.stage("跨小时合并"):
        # 给每个 trace 标上它真实所在的叶子目录，合并后 processor 靠它取源文件。
        for sess, leaf in gathered:
            for tr in (sess.get("trace_list") or []):
                if isinstance(tr, dict):
                    tr.setdefault("_src_dir", leaf)
        entries = [s for s, _ in gathered]
        before = len(entries)
        merged = merge_segments(
            entries,
            first_field="first_ts", last_field="last_ts",
            count_field="msg_count", file_count_field=None,
            models_field="models", identity_field="chain_lookup",
            trace_field="trace_list", trace_ts_field="ts", trace_name_field="filename",
            latest_file_field="latest_file",
        )
        _log(f"跨小时合并：{before} 段 → {len(merged)} 条会话"
             + (f"（合并掉 {before - len(merged)} 段）" if before > len(merged) else ""))

    total = len(merged)
    if not total:
        return all_results, all_entries, 0

    with _progress_lock:
        _progress_total[0] += total
        _progress_phase[0] = _mcfg.step or "重构合并"
        _progress_reporter.set_progress(_progress_done[0], _progress_total[0],
                                       _progress_phase[0])

    def _on_done():
        with _progress_lock:
            _progress_done[0] += 1
            done, tot = _progress_done[0], _progress_total[0]
        _progress_reporter.set_progress(done, tot)

    # ── 阶段 3：产出 ──────────────────────────────────────────────────
    # file_dirs: filename -> 叶子目录。processor 用它跨叶子取源文件。
    file_dirs = {}
    for sess in merged:
        for tr in (sess.get("trace_list") or []):
            fn = tr.get("filename") if isinstance(tr, dict) else None
            src = tr.get("_src_dir") if isinstance(tr, dict) else None
            if fn and src:
                file_dirs.setdefault(fn, src)
        lf = sess.get("latest_file")
        if lf and file_dirs.get(lf) is None and sess.get("trace_list"):
            first = next((t for t in sess["trace_list"]
                          if isinstance(t, dict) and t.get("_src_dir")), None)
            if first:
                file_dirs[lf] = first["_src_dir"]

    with _tm.stage(_mcfg.step or "重构合并"):
        if _mcfg.pipeline == "copy":            # copy 模式只按 trace 逐个复制源文件；合并的意义在于清单只写一份
            # （逐叶路径下每叶都覆盖同一份 session_index.jsonl，最终只剩最后一小时）。
            copied, failed = _copy_merged_files(merged, file_dirs, local_base, workers)
            _log(f"跨小时复制完成：{copied} 个文件" + (f"，失败 {failed}" if failed else ""))
            if failed:
                warnings.append(f"跨小时复制有 {failed} 个文件失败")
            # 逐叶路径里每个叶子都会写一份 session_index.jsonl 且用 "w" 覆盖同一个
            # local_base，多目录时最终只剩最后一小时的内容。合并路径在这里**统一写一次**，
            # 清单与已复制的文件才对得上。
            try:
                with open(Path(local_base) / "session_index.jsonl", "w", encoding="utf-8") as f:
                    for e in merged:
                        f.write(json.dumps(e, ensure_ascii=False) + "\n")
            except OSError as e:
                warnings.append(f"跨小时写 session_index.jsonl 失败: {e}")
            all_entries = merged
            all_results = merged
            return all_results, all_entries, total

        _log(f"进入 {_mcfg.step}: {total} sessions（跨小时合并后）, workers={workers}...")
        # src_dir 用第一个叶子的绝对路径：合并后的会话绝大多数靠 file_dirs 定位，
        # 这里只作为「找不到时的兜底」，用 env_dir 会让兜底路径完全错误。
        _fallback_src = (_resolve_mt_for(_env_dir, mtime_dirs[0])
                         or str(_env_dir / mtime_dirs[0]))
        # 并行度：逐叶路径是「每叶子一次 processor」，配合 dir_workers 层目录并发，
        # 实际并行单元 ≈ min(dir_workers, 目录数) × workers（默认 4×8 = 32）。
        # 合并路径只剩一次 processor 调用，若仍用 workers 就白白少掉一层并发。
        # #263 实测：沿用 workers=8 比 #262 慢 56s（170s -> 226s）。这里把目录并发
        # 补回来；上限按 CPU×4 收口，避免大盘目录数把它放大到失控。
        try:
            _proc_workers = int(workers) * max(1, int(dir_workers))
        except (TypeError, ValueError):
            _proc_workers = int(workers or 8)
        _proc_workers = max(1, min(_proc_workers, (os.cpu_count() or 4) * 4))

        kw = dict(
            src_dir=_fallback_src, out_dir=str(local_base),
            session_entries=merged, api_key=api_key or None, workers=_proc_workers,
            progress_cb=lambda msg: _log(f"[merged] {msg}"),
            log_dir=_env_dir.name,
            should_cancel=_should_cancel,
            on_session_done=_on_done,
            file_dirs=file_dirs,
        )
        if _mcfg.analyze is not None:
            kw["analyze"] = _mcfg.analyze
        ra_result = processor(**kw)

    all_results = ra_result.get("results", []) or []
    all_entries = merged
    for e in ra_result.get("errors", []) or []:
        errors.append(f"merged: {e}")
    _log(f"跨小时聚合完成：成功 {ra_result.get('total_files', 0)} 个 session"
         + (f"，失败 {len(ra_result.get('errors', []) or [])}" if ra_result.get("errors") else ""))
    return all_results, all_entries, total


def _copy_merged_files(merged, file_dirs, local_base, workers):
    """copy 模式跨小时聚合：按 trace 的源目录复制文件到共享 local_base。

    逐叶路径的复制由 sync_session_index 做，它绑定在「一个叶子目录」上；
    合并后一条会话的文件可能来自多个叶子，只能按 filename 逐个定位。

    定位复用 export_sync._collect_triplet_files：newapi 是「单个合并 .json」，
    native 才是 req/headers/res 三元组，两者后缀规则不同，不能在这里硬编码后缀，
    否则 newapi 会把 3 个不存在的三元组文件当成复制失败。
    """
    import shutil
    from concurrent.futures import ThreadPoolExecutor
    from utils.export_sync import _collect_triplet_files

    targets = []
    for sess in merged:
        files = [tr.get("filename") for tr in (sess.get("trace_list") or [])
                 if isinstance(tr, dict) and tr.get("filename")]
        if not files and sess.get("latest_file"):
            files = [sess["latest_file"]]
        for fn in files:
            src_dir = file_dirs.get(fn)
            if not src_dir:
                continue
            # 该文件名可能对应 1 个文件（newapi）或 3 个文件（native 三元组）
            targets.extend(_collect_triplet_files(src_dir, fn))

    # 并发落地到同一个目录，按文件名去重（同一文件可能被多条会话引用）
    seen = set()
    uniq = []
    for p in targets:
        if p.name in seen:
            continue
        seen.add(p.name)
        uniq.append(p)

    copied = 0
    failed = 0

    def _one(p):
        try:
            shutil.copy2(p, Path(local_base) / p.name)
            return True
        except Exception:  # noqa: BLE001
            logger.warning("跨小时复制失败: %s", p)
            return False

    with ThreadPoolExecutor(max_workers=max(1, int(workers or 4))) as ex:
        for ok in ex.map(_one, uniq):
            if ok:
                copied += 1
            else:
                failed += 1
    return copied, failed


def _eval_mode() -> str:
    """注册表里 pipeline=eval 的模式 id（「从导出记录发起质检」用它）。"""
    for m in get_export_modes().all():
        if m.pipeline == "eval":
            return m.id
    return "eval"


def _run_task(record_id, _env_dir, _env_key_name, obs_prefix, now_tag, mode, force=False,
              merge_hours: bool = False):
    from utils.eval.eval import evaluate_sessions
    from utils.obs_sync import _run_upload_cmd

    _log = lambda msg: append_log(record_id, msg)
    try:
        # 处理器由 settings/export_modes.yaml 的 processor 字段解析（importlib）。
        # 各模式处理粒度不同：reformat/eval 只处理 latest_file；reconstruct 逐 session
        # 聚合多个 trace 文件（去重 + 保留分支 + 回填 reasoning）；full_reformat /
        # jiuwen_reformat 逐 session 合并 trace_list 全部文件。差异体现在 processor
        # 与它的 analyze 形参有无上，这里只负责注入，不再硬编码 mode 分支。
        mcfg = get_export_modes().get(mode)
        if mcfg is None:
            raise RuntimeError(f"未知导出模式 '{mode}'（未在 settings/export_modes.yaml 中注册）")
        # pipeline="copy"（原始三元组复制）没有 processor——它走 export_session_index /
        # sync_session_index 那套，不加载任何 reformat 处理器。只有合并/质检类模式
        # 才要求 processor 必须解析成功。
        if mcfg.pipeline == "copy":
            processor = None
        else:
            processor = get_export_modes().resolve_processor(mode)
            if processor is None:
                raise RuntimeError(f"导出模式 '{mode}' 未配置可用处理器（检查 settings/export_modes.yaml）")
        _run_task_inner(record_id, _env_dir, _env_key_name, obs_prefix, now_tag, mode, force,
                        processor, evaluate_sessions, _run_upload_cmd, _log,
                        merge_hours=merge_hours)
    except Exception as exc:
        logger.exception("_run_task crashed (record=%s)", record_id)
        try:
            _log(f"任务异常终止: {exc}")
        except Exception:
            logger.warning("append_log also failed for record=%s", record_id)
        update_status(record_id, "failed", error_message=str(exc))


def _run_task_from_record(record_id: int, params: dict) -> None:
    """worker 侧入口：从 DB 记录 + task_json 参数重建执行上下文并跑任务。

    env_dir / env_key_name 取自 persist_params 落库的 task_json（绝对路径）。
    这两者不能由 SERVICE_LOG_DIR（get_service_log_dir(): logs/port<P>/<seg>）反推——
    app 侧 register_export_routes 收到的是 get_log_dir("logs_all")（logs_all/<env>），
    base 与日期段都不同，反推会解析到错误目录（历史 bug）。故必须随任务持久化。

    旧记录（升级前入队）task_json 缺 env_dir：兜底用 get_log_dir("logs_all") 的
    父目录，与 app 注册路由时同源，避免旧任务直接失败。
    """
    env_dir_str = params.get("env_dir", "") or ""
    env_key_name = params.get("env_key_name", "") or ""
    if env_dir_str:
        env_dir = Path(env_dir_str)
    else:
        # 兜底：与 app register_export_routes(app, get_log_dir("logs_all")) 同源
        env_dir = Path(get_log_dir("logs_all")).parent
        logger.warning("record=%s task_json 缺 env_dir，兜底用 %s", record_id, env_dir)
    if not env_key_name:
        env_key_name = env_dir.name
    _run_task(record_id, env_dir, env_key_name,
              params.get("obs_prefix", ""), params.get("now_tag", ""),
              params.get("mode") or _default_mode(), force=bool(params.get("force", False)),
              merge_hours=bool(params.get("merge_hours", False)))


def _run_task_inner(record_id, _env_dir, _env_key_name, obs_prefix, now_tag, mode, force,
                    processor, evaluate_sessions, _run_upload_cmd, _log,
                    merge_hours: bool = False):
    """逐 mtime 目录并行导出。processor 与 analyze 形参由 settings/export_modes.yaml
    的 processor / analyze 字段决定：reformat/eval 用 reformat_and_analyze（analyze
    显式传 true/false）；reconstruct 用 reconstruct_and_export（无 analyze 形参，
    逐 session 聚合多个 trace）。"""
    rec = get_record(record_id)
    update_status(record_id, "running")

    # 透传给 processor 的协作式取消回调：processor 在逐 session 的 drain 循环里频繁调用，
    # 而 _is_cancelled 每次都查一次 DB（外部日志目录 sqlite）。这里做时间节流——最多每
    # ~2s 真查一次；一旦查到取消就永久缓存 True（取消不可逆），后续调用零成本立即返回。
    # 这样既让「网页取消」能在几秒内穿透到重构/合并的内层循环，又不至于每 session 都打 DB。
    import time as _time
    _cancel_state = {"cancelled": False, "last_ts": 0.0}

    def _should_cancel() -> bool:
        if _cancel_state["cancelled"]:
            return True
        now = _time.monotonic()
        if now - _cancel_state["last_ts"] < 2.0:
            return False
        _cancel_state["last_ts"] = now
        if _is_cancelled(record_id):
            _cancel_state["cancelled"] = True
            return True
        return False

    sync_cfg = _load_sync_config()
    # workers：优先用任务记录里显式配置的并发数（新建任务时可选）；
    # 记录未设（0/缺失，含旧任务）时回退全局 sync 配置默认。
    #
    # 配置键读 export_workers（settings/obs_base.yaml 里语义分离后的新键），
    # 老键 workers 仅作兼容回退 —— 它以前同时被当成评估并行度/复制线程数/上传默认。
    _rec_workers = rec.get("workers") or 0
    workers = (_rec_workers if _rec_workers > 0
               else sync_cfg.get("export_workers") or sync_cfg.get("workers") or _DEFAULT_WORKERS)
    # dir_workers：多个 mtime 目录并行处理的并发数（每目录内部仍各用 workers 并行单元）。
    # 0/缺失（含旧任务）= 默认值。
    #
    # 总并行单元 ≈ dir_workers × workers。评估器改成进程池后，这个乘积直接等于同时存在的
    # 子进程数：旧的 8×20 = 160 个进程、每个约 40–60MB RSS，且全在同一 NFS 上争用。
    # 实测 32 个并行单元已能打满单机吞吐，再高只是放大内存与 IO 争用，故默认 4×8 = 32。
    dir_workers = (int(rec.get("dir_workers") or 0)
                   or int(sync_cfg.get("export_dir_workers") or 0)
                   or _DEFAULT_DIR_WORKERS)
    upload_script = sync_cfg.get("upload_script") or None

    mtime_dirs = json.loads(rec.get("mtime_dirs", "[]"))
    api_key = rec.get("api_key", "")
    slot = rec.get("key_slot", "all")

    # 本地落盘根 + OBS 子前缀 + 落盘子目录名，全部来自 settings/export_modes.yaml。
    # temp_path 留空时回退到 env_dir.parent.parent（项目根），与旧行为一致。
    _mcfg = get_export_modes().get(mode)
    if _mcfg is None:
        raise RuntimeError(f"未知导出模式 '{mode}'（未在 settings/export_modes.yaml 中注册）")
    _temp_root = get_export_modes().temp_path
    if not str(_temp_root).strip() or str(_temp_root) == ".":
        _temp_root = _env_dir.parent.parent
    local_base = (_temp_root / _mcfg.local_dir / _env_key_name / slot / f"ex-{now_tag}").resolve()
    obs_sub = _mcfg.obs_sub

    obs_dst = f"{obs_prefix}/{obs_sub}/{_env_key_name}/{slot}/ex-{now_tag}/" if obs_prefix else ""
    local_base.mkdir(parents=True, exist_ok=True)

    update_status(record_id, "running", obs_dst=obs_dst, local_copy_dir=str(local_base))

    _mode_label = _mcfg.mode_label
    _log(f"开始{_mode_label}: key_slot={slot}, mtime_dirs={mtime_dirs}")
    _log(f"本地目录: {local_base}")
    if obs_dst:
        _log(f"OBS 目标: {obs_dst}")

    errors = []
    warnings = []  # 非致命跳过（如 new-api 索引未构建）：记为提示，不判任务失败
    total_sessions = 0
    total_uploaded = 0
    total_skipped = 0
    all_results = []
    all_entries = []

    # 分段耗时（墙钟）：多目录并行时各段按线程累加，因此分项之和可能大于总耗时。
    # 结果只写 progress_log，不落新 DB 列（避免 worker/app 版本错配）。
    _tm = StageTimer()

    # 进度条：多目录并行时各目录各推进会话计数，故全局计数放在共享容器里，
    # 由 _progress_lock 保护；分母随每个目录进入处理器时累加（事先不知道总数）。
    _progress_reporter = _ProgressReporter(record_id)
    _progress_lock = threading.Lock()
    _progress_done = [0]
    _progress_total = [0]
    _progress_phase = [""]

    def _process_mtime(mt):
        """处理单个 mtime 目录，返回一份独立结果 dict（供外层串行合并，避免并发写共享量）。
        每目录各自读/写自己的 session_index.jsonl 与 .sync_state，互不干扰。"""
        res = {"mt": mt, "errors": [], "warnings": [], "skip": False,
               "new_sessions": 0, "uploaded": 0, "skipped": 0,
               "results": [], "entries": []}
        # 协作式取消：用户「终止」后，尚未开始的目录直接跳过（并行/串行同理）。
        if _should_cancel():
            res["skip"] = True
            return res
        mt_src = _resolve_mt_for(_env_dir, mt) or str(_env_dir / mt)
        try:
            # 格式检测：new-api 导出不触发回填；reconstruct 仅支持 new-api 合并文件，
            # native 三元组叶子没有合并文件实物（hermes 聚合器读合并文件本身，见
            # doc/REAMDE_traj.md §3.2），在生成 session_index 前就跳过，省一次构建。
            try:
                from utils.log_scan import detect_format as _df
                with _tm.stage("格式检测"):
                    _fmt = _df(mt_src)
                if not _mcfg.supports_format(_fmt):
                    _log(f"[{mt}] 目录格式 {_fmt}，{_mcfg.mode_label}仅支持 {'/'.join(_mcfg.formats)} 合并文件，跳过")
                    res["warnings"].append(f"{mt}: 非 {'/'.join(_mcfg.formats)} 格式（{_fmt}），已跳过")
                    res["skip"] = True
                    return res
                if _fmt == "newapi":
                    import utils.newapi_index_db as _nidb
                    with _tm.stage("格式检测"):
                        _needs = _nidb.needs_build(mt_src)
                    if _needs:
                        _log(f"[{mt}] 索引未构建/不完整，跳过；请先在数据管理界面手动构建索引后再导出")
                        res["warnings"].append(f"{mt}: 索引未构建/不完整，已跳过（请先在数据管理界面手动构建索引）")
                        res["skip"] = True
                        return res
            except Exception as _pe:
                # 索引状态检查本身异常（如 index.db 存在但 meta 表未建好、
                # 半成品库 no such table: meta 等）与 needs_build=True 是同一语义：
                # 叶子索引尚未就绪。跳过并提示，不判整个任务失败（warning 非致命）。
                _log(f"[{mt}] 索引未就绪（检查异常: {_pe}），跳过；请先在数据管理界面手动构建索引")
                res["warnings"].append(f"{mt}: 索引未构建/不完整，已跳过（检查异常: {_pe}）")
                res["skip"] = True
                return res
            _log(f"[{mt}] 生成 session_index...")
            with _tm.stage("生成 session_index"):
                # 带 api_key：只下推查询该 key 的 session，且不写叶子共享的
                # session_index.jsonl（详见 export_session_index 的说明）。
                exp_result = export_session_index(mt_src, force=force, with_sessions=True,
                                                  api_key=api_key)
            _log(f"[{mt}] session_index: {exp_result.get('total_sessions', 0)} sessions"
                 + (" (已是最新，跳过)" if exp_result.get("skipped") else ""))

            if _mcfg.pipeline == "copy":
                _log(f"[{mt}] 开始复制文件" + (f" (按 key 过滤: ...{api_key[-8:]})" if api_key else " (全量)"))
                # 只复制到共享 local_base，不在此上传；收尾统一上传一次（见下方 finalize）。
                # 避免多目录并行时每目录都整目录上传 → 重复上传 + 写入中上传的竞态。
                with _tm.stage("复制文件"):
                    sync_result = sync_session_index(
                        mt_src, obs_dst=obs_dst, key=api_key or None,
                        local_copy_dir=str(local_base), force=force, workers=workers,
                        upload=False,
                        # 刚生成的索引已在内存里，直接传下去，省掉对同一文件的再次解析。
                        sessions=exp_result.get("sessions"),
                    )
                matched = sync_result.get("matched_sessions", 0)
                res["new_sessions"] = sync_result.get("new_sessions", 0)
                res["uploaded"] = sync_result.get("uploaded", 0)
                res["skipped"] = sync_result.get("skipped", 0)
                _log(f"[{mt}] 匹配 {matched} sessions, 新导出 {res['new_sessions']}, 跳过 {res['skipped']}, 复制文件 {res['uploaded']}")
            else:
                # 索引刚重建时 exp_result 已带着这批 session，直接复用；
                # 只有「跳过重建」时才需要从 session_index.jsonl 里再解析一次。
                with _tm.stage("读取 session_index"):
                    session_entries = exp_result.get("sessions")
                    if session_entries is None:
                        session_entries = _load_session_index(mt_src)
                    total_before = len(session_entries)
                    if api_key:
                        session_entries = [s for s in session_entries if s.get("api_key") == api_key]
                if not session_entries:
                    _log(f"[{mt}] 共 {total_before} sessions, 按 key 过滤后 0, 跳过")
                    res["skip"] = True
                    return res
                if api_key:
                    _log(f"[{mt}] 共 {total_before} sessions, 按 key 过滤后 {len(session_entries)}")
                # analyze 三态：None = 处理器签名里没有这个形参（reconstruct /
                # full_reformat / jiuwen_reformat），调用时不传。有值则传入。
                #
                # 进度上报：本目录的 session 数只占全局的一部分，报表按「全局已完成 /
                # 全局总数」算，所以先把本目录的分母加进 _progress_total，再让回调
                # 每完成一个 session 推进一格。
                _n_dir = len(session_entries)
                with _progress_lock:
                    _progress_total[0] += _n_dir
                    _progress_phase[0] = _mcfg.step or "重构合并"
                    _progress_reporter.set_progress(_progress_done[0], _progress_total[0],
                                                    _progress_phase[0])
                def _on_done(_mt=mt):
                    # 只维护全局计数，统一由 set_progress 落库：
                    # 不再同时调 advance()，否则 reporter 内部还有一份自增计数，
                    # 与全局计数各算各的，多目录交错时会互相覆盖、进度条来回跳。
                    with _progress_lock:
                        _progress_done[0] += 1
                        done, total = _progress_done[0], _progress_total[0]
                    _progress_reporter.set_progress(done, total)

                with _tm.stage(_mcfg.step or "重构合并"):
                    if _mcfg.analyze is None:
                        _log(f"[{mt}] 进入 {_mcfg.step}: {len(session_entries)} sessions, workers={workers}...")
                        ra_result = processor(
                            src_dir=mt_src, out_dir=str(local_base),
                            session_entries=session_entries, api_key=api_key, workers=workers,
                            progress_cb=lambda msg, _mt=mt: _log(f"[{_mt}] {msg}"),
                            log_dir=_log_dir_key_for(_env_dir, mt_src),
                            should_cancel=_should_cancel,
                            on_session_done=_on_done,
                        )
                    else:
                        _log(f"[{mt}] 进入 {_mcfg.step}: {len(session_entries)} sessions, workers={workers}...")
                        ra_result = processor(
                            src_dir=mt_src, out_dir=str(local_base),
                            session_entries=session_entries, api_key=api_key, workers=workers,
                            progress_cb=lambda msg, _mt=mt: _log(f"[{_mt}] {msg}"),
                            log_dir=_log_dir_key_for(_env_dir, mt_src),
                            analyze=_mcfg.analyze,
                            should_cancel=_should_cancel,
                            on_session_done=_on_done,
                        )
                res["results"] = ra_result["results"]
                res["entries"] = session_entries
                _n_err = len(ra_result.get('errors', []))
                _log(f"[{mt}] {_mcfg.mode_label}完成：成功 {ra_result['total_files']} 个 session"
                     + (f"，失败 {_n_err}" if _n_err else ""))
        except Exception as e:
            _log(f"[{mt}] 错误: {e}")
            res["errors"].append(f"{mt}: {e}")
            logger.exception("task failed for %s (mode=%s)", mt, mode)
        return res

    # 多目录调度：dir_workers=1 顺序执行；>1 用线程池并行（每目录内部仍各用 workers 线程）。
    # 结果按提交顺序收集后【串行合并】到共享累加量，避免并发写竞态。
    if _should_cancel():
        _log("任务已终止（用户取消）")
        update_status(record_id, "cancelled", error_message="用户终止")
        return

    # ── 跨小时节点聚合路径 ──────────────────────────────────────────────
    # newapi 的 index.db 按「日/时」分叶子，chain_index 只在叶子内部有效，新建会话的
    # session_key 就是它的 ts。于是同一段对话跨过整点会被切成 N 段，逐叶导出就变成
    # N 条独立产物。merge_hours 打开时改成「先全收集 → 合并 → 再统一产出」。
    #
    # 只在多目录 + processor 支持 file_dirs 的模式下启用：合并后一条会话的文件可能
    # 分散在多个叶子目录里，processor 必须能按文件名跨目录取源文件。
    #
    # 处理器是否支持用签名探测，而不是硬编码模式名：full_reformat 同样是 reformat
    # pipeline，但它的处理器不收 file_dirs（它靠 max_mtimes:1 挡在多目录之外）。
    # 硬编码模式白名单会在以后新增/mode 改名时静默失效，签名探测不会。
    _fd_supported = False
    if processor is not None:
        try:
            import inspect as _inspect
            _fd_supported = "file_dirs" in _inspect.signature(processor).parameters
        except (TypeError, ValueError):
            _fd_supported = False

    if merge_hours and len(mtime_dirs) > 1 and processor is not None and not _fd_supported:
        warnings.append(f"{_mcfg.mode_label}的处理器不支持跨目录取文件，已回退逐目录导出")
        _log(f"[warn] {_mcfg.mode_label}处理器不支持 file_dirs，merge_hours 忽略，按逐目录导出")

    if (merge_hours and len(mtime_dirs) > 1 and _fd_supported
            and _mcfg.pipeline in ("reformat", "copy")):
        all_results, all_entries, total_sessions = _run_merged_hours(
            mtime_dirs=mtime_dirs, _env_dir=_env_dir, local_base=local_base,
            api_key=api_key, force=force, workers=workers, processor=processor,
            _mcfg=_mcfg, _log=_log, _tm=_tm, errors=errors, warnings=warnings,
            _should_cancel=_should_cancel, _progress_lock=_progress_lock,
            _progress_done=_progress_done, _progress_total=_progress_total,
            _progress_phase=_progress_phase, _progress_reporter=_progress_reporter,
            dir_workers=dir_workers,
        )
        mt_results = []
    elif dir_workers <= 1 or len(mtime_dirs) <= 1:
        with _tm.stage("逐目录处理"):
            mt_results = [_process_mtime(mt) for mt in mtime_dirs]
    else:
        n_par = min(dir_workers, len(mtime_dirs))
        _log(f"多目录并行处理：{len(mtime_dirs)} 个目录，目录并发 {n_par}，每目录 workers={workers}")
        from concurrent.futures import ThreadPoolExecutor as _TPE
        mt_results = [None] * len(mtime_dirs)
        with _tm.stage("逐目录处理"):
            with _TPE(max_workers=n_par) as _ex:
                _fut = {_ex.submit(_process_mtime, mt): i for i, mt in enumerate(mtime_dirs)}
                for f in _fut:
                    idx = _fut[f]
                    try:
                        mt_results[idx] = f.result()
                    except Exception as e:  # noqa: BLE001
                        logger.exception("mtime worker crashed: %s", mtime_dirs[idx])
                        mt_results[idx] = {"mt": mtime_dirs[idx], "errors": [f"{mtime_dirs[idx]}: {e}"],
                                           "warnings": [], "skip": False, "new_sessions": 0,
                                           "uploaded": 0, "skipped": 0, "results": [], "entries": []}

    # 串行合并各目录结果（保持 mtime_dirs 原顺序，结果稳定）
    for res in mt_results:
        if not res:
            continue
        errors.extend(res["errors"])
        warnings.extend(res["warnings"])
        total_uploaded += res["uploaded"]
        total_skipped += res["skipped"]
        if _mcfg.pipeline == "copy":
            total_sessions += res["new_sessions"]
        else:
            all_results.extend(res["results"])
            all_entries.extend(res["entries"])
            total_sessions += len(res["results"])

    # 目录循环结束后、进入收尾（evaluate/上传/判定）前再查一次取消：
    # 若用户在处理最后一个目录期间点了「终止」，这里退出，避免跑完整个 evaluate
    # 与 OBS 上传。用 _should_cancel（带缓存）：processor 内已检测到取消时零成本命中。
    if _should_cancel():
        _log("任务已终止（用户取消）")
        update_status(record_id, "cancelled", error_message="用户终止")
        return

    eval_report_path = ""
    analysis_json_stored = ""
    if _mcfg.pipeline == "copy":
        # 收尾统一上传：所有 mtime 目录已把三元组文件复制到共享 local_base，
        # 这里对整个 local_base 上传一次。obsutil 用 -u 增量，已存在的对象自动跳过，
        # 所以「重试上传」再跑一遍时只补上次失败的文件。
        if total_sessions > 0:
            if obs_dst:
                _log(f"复制完成，开始统一上传到 OBS: {obs_dst}")
                obs_parent = obs_dst.rstrip("/").rsplit("/", 1)[0] + "/"
                with _tm.stage("上传 OBS"):
                    ok, msg = _run_upload_cmd(str(local_base), obs_parent, upload_script, log_cb=_log)
                if ok:
                    _log("上传成功")
                else:
                    _log(f"上传失败: {msg}")
                    errors.append(f"OBS upload: {msg}")
            else:
                _log("未配置 OBS 目标，仅本地复制完成")
        else:
            _log("无新 session 需要导出")
    elif _mcfg.pipeline == "reformat":
        # 合并/聚合后的 session JSON 已由 worker 落在 local_base/<session>/，这里补一份
        # session_index.jsonl 清单后整目录上传，不跑 evaluate/质检。
        # reformat / reconstruct / full_reformat / jiuwen_reformat 都走这条分支。
        if all_results:
            idx_path = local_base / "session_index.jsonl"
            with _tm.stage("写 session_index.jsonl"):
                with open(idx_path, "w", encoding="utf-8") as f:
                    for entry in all_entries:
                        f.write(json.dumps(entry, ensure_ascii=False) + "\n")
            if obs_dst:
                _log(f"同步到 OBS: {obs_dst}")
                obs_parent = obs_dst.rstrip("/").rsplit("/", 1)[0] + "/"
                with _tm.stage("上传 OBS"):
                    ok, msg = _run_upload_cmd(str(local_base), obs_parent, upload_script, log_cb=_log)
                if ok:
                    _log("上传成功")
                    # 整目录上传成功：记入本次合并的 session 数（否则 files_uploaded 恒为 0）
                    total_uploaded = len(all_results)
                else:
                    _log(f"上传失败: {msg}")
                    errors.append(f"OBS upload: {msg}")
            else:
                _log("仅本地复制，未上传 OBS（提交时 OBS 前缀为空）；如需上传可对本记录「重试上传」")
        else:
            _log("无 session 数据")
    elif _mcfg.pipeline == "eval":
        if all_results:
            idx_path = local_base / "session_index.jsonl"
            with _tm.stage("写 session_index.jsonl"):
                with open(idx_path, "w", encoding="utf-8") as f:
                    for entry in all_entries:
                        f.write(json.dumps(entry, ensure_ascii=False) + "\n")
            _log(f"开始 evaluate_sessions: {len(all_results)} sessions...")
            with _tm.stage("质检 evaluate"):
                eval_result = evaluate_sessions(
                    sessions=all_results, report_dir=str(local_base), progress_cb=_log,
                    key_name=f"{_env_key_name}/{slot}",
                    obs_path=obs_dst,
                )
            _log(f"evaluate_sessions 完成: total_sessions={eval_result.get('total_sessions', 0)}")
            eval_report_path = eval_result.get("report_path", "")
            analysis_json_path = eval_result.get("analysis_json_path", "")
            # 只有文件 ≤100MB 才存入 DB（超大文件直接从 local_copy_dir 读取即可）
            _MAX_ANALYSIS_JSON = 100 * 1024 * 1024
            if analysis_json_path and Path(analysis_json_path).is_file():
                if Path(analysis_json_path).stat().st_size <= _MAX_ANALYSIS_JSON:
                    from utils.export_store import _externalize_field
                    analysis_json_content = Path(analysis_json_path).read_text(encoding="utf-8")
                    analysis_json_stored = _externalize_field(record_id, "analysis_json", analysis_json_content)
                else:
                    _log(f"analysis_json 过大（{Path(analysis_json_path).stat().st_size // 1024 // 1024}MB），跳过存储，报告直接使用 session_report.html")

            from utils.eval.reformat import write_eval_to_cache
            for mt in mtime_dirs:
                try:
                    write_eval_to_cache(_resolve_mt_for(_env_dir, mt) or str(_env_dir / mt), all_results)
                except Exception:
                    logger.debug("write_eval_to_cache failed for %s", mt, exc_info=True)
        else:
            _log("无 session 数据")

        if obs_dst:
            _log(f"同步到 OBS: {obs_dst}")
            obs_parent = obs_dst.rstrip("/").rsplit("/", 1)[0] + "/"
            with _tm.stage("上传 OBS"):
                ok, msg = _run_upload_cmd(str(local_base), obs_parent, upload_script, log_cb=_log)
            if ok:
                _log("上传成功")
                # 整目录上传成功：记入本次质检的 session 数（否则 files_uploaded 恒为 0）
                total_uploaded = len(all_results)
            else:
                _log(f"上传失败: {msg}")
                errors.append(f"OBS upload: {msg}")
        elif all_results:
            _log("仅本地复制，未上传 OBS（提交时 OBS 前缀为空）；如需上传可对本记录「重试上传」")

    _done_label = _mcfg.mode_label

    # 分段耗时汇总：回答「总耗时里有多少是真正的复制/上传」。并行下载目录时各段是
    # 各线程墙钟累加，分项之和可能大于总耗时，故注明并发度避免误读。
    _tm_note = (f"dir_workers={dir_workers} 并行，分项为各线程累加"
                if dir_workers > 1 and len(mtime_dirs) > 1 else "")
    _log(_tm.summary_line(f"耗时 {mode}", note=_tm_note))

    # 判定失败只看「真错误」(errors)：上传失败、异常等。
    # warnings（如 new-api 索引未构建被跳过）不影响已成功部分的产出，不判失败。
    # pipeline=copy 的产出在 res["new_sessions"]，不产 results，故不参与 no_output 判定。
    no_output = (_mcfg.pipeline != "copy" and not all_results)
    warn_suffix = f"（{len(warnings)} 个目录跳过：{'; '.join(warnings)}）" if warnings else ""

    if errors or no_output:
        # no_output 且无真错误：说明目录全被跳过（多为索引未构建），如实说明而非笼统“失败”
        if no_output and not errors:
            reason = f"无 session 数据{warn_suffix}" if warnings else "无 session 数据"
        else:
            reason = "; ".join(errors) + warn_suffix
        _log(f"{_done_label}失败: {reason}")
        update_status(record_id, "failed",
                      error_message=reason,
                      total_sessions=total_sessions,
                      files_uploaded=total_uploaded,
                      files_skipped=total_skipped,
                      eval_report_path=eval_report_path,
                      analysis_json=analysis_json_stored)
    else:
        # 成功；若有跳过目录，把提示写进 error_message 供前端展示，但状态是 success
        msg = f"{_done_label}完成: {total_sessions} sessions"
        if warnings:
            msg += f"，{len(warnings)} 个目录跳过（索引未构建）"
        _log(msg)
        update_status(record_id, "success",
                      error_message=(f"部分目录跳过：{'; '.join(warnings)}" if warnings else ""),
                      total_sessions=total_sessions,
                      files_uploaded=total_uploaded,
                      files_skipped=total_skipped,
                      eval_report_path=eval_report_path,
                      analysis_json=analysis_json_stored)

    # 收尾：把进度补满（成功/失败都补，避免列表里留着 97% 这种尾巴）。
    try:
        _progress_reporter.set_phase(_mcfg.mode_label, total=max(_progress_total[0], 0))
        _progress_reporter.finish()
    except Exception:  # noqa: BLE001  进度条失败不影响任务结果
        logger.debug("进度收尾失败（忽略）", exc_info=True)



def register_export_routes(app: FastAPI, logs_dir: str) -> None:
    env_dir = Path(logs_dir).parent
    env_key_name = env_dir.name
    templates = Jinja2Templates(directory="templates")

    # 初始化导出槽位锁目录为绝对路径（与独立 export_worker 进程对齐，见 init_export_lock_dir）。
    init_export_lock_dir()

    # keys 统计（跨 root 扫盘）的内存缓存，10 分钟 TTL。前端「刷新统计」传 force=1 强刷。
    _keys_cache = {"ts": 0.0, "data": None}
    _KEYS_TTL = 600

    def _all_roots() -> list:
        """活跃 env_dir + 配置的历史路径（去重）。"""
        return get_stats_roots(str(env_dir))

    def _existing_roots() -> list:
        """只保留实际存在的 root。与 build_stats_multi 收到的列表一致，
        决定 mtime key 是否带 <root_basename>/ 前缀（多 root 才带）。"""
        return [r for r in _all_roots() if Path(r).is_dir()]

    def _list_all_mtimes() -> list:
        """列出所有 root 的叶子目录，多 root 时带 <root_id>/ 前缀，供前端 mtime 选择。

        读自 leaf_status（同步产物）而非实时扫盘：与 leaf_path 同源，保证「列表里有」
        ≡「DB 有 leaf_path」，_resolve_mt 查表必命中（无需扫盘兜底）。未同步的源在此
        不产出——先在「数据管理」页同步。key 格式与 build_stats_multi 的 mtime_cells 一致。
        """
        import utils.logdir_store as lds
        from utils.logs_config import get_root_id
        roots = _existing_roots()
        multi = len(roots) > 1
        out = []
        for root in roots:
            rid = get_root_id(root, str(env_dir))
            for row in lds.bulk_get(rid):
                dk = row.get("dir_key", "")
                if dk:
                    out.append(f"{rid}/{dk}" if multi else dk)
        return sorted(set(out), reverse=True)

    def _resolve_mt(mt: str) -> Optional[str]:
        """把 mtime key（<root_id>/<rel> 或裸 <rel>）解析为叶子目录绝对路径。

        优先按 root_id 前缀匹配；对 export_records 里遗留的旧 <basename>/ 前缀
        保留回退匹配，保证历史记录仍可解析。委托模块级 _resolve_mt_for，复用其
        「同名折叠层」兜底（双层折叠源解析到内层真叶子），与离线脚本口径一致。
        """
        return _resolve_mt_for(env_dir, mt)

    def _log_dir_key(mt_src: str) -> str:
        """把叶子目录绝对路径解析为相对 root 的 dir_key（如 "260728/26072813"）。

        与 stats_index / log_scan 的 dir_key 一致，供报告生成 /history 链接。
        找不到归属 root 时退回叶子目录名（单级 native 布局下即正确）。"""
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

    @app.get("/keys/export")
    def export_page(request: Request):
        # no-store：导出页模板已重构为任务列表，避免浏览器用旧的缓存页面
        # （页面路由默认无 Cache-Control，浏览器会启发式缓存 HTML）。
        return templates.TemplateResponse(
            request, "export.html",
            context={"active_page": "export", "user_role": request.session.get("monitor_role", "user"), "user_name": request.session.get("monitor_user", ""), "user_permissions": [p.strip() for p in (request.session.get("monitor_permissions") or "").split(",") if p.strip()]},
            headers={"Cache-Control": "no-store"},
        )

    @app.get("/keys/export/browse")
    def export_browse_page(request: Request):
        # 导出浏览已下线：浏览功能并入「对话浏览」（/history 与公开 /export/view 均渲染 chat-viewer.html）。
        # 保留路由仅为避免直接访问旧链接时 404 未定义（跳回导出页）。
        return RedirectResponse(url="/keys/export", status_code=302)

    @app.get("/keys/export/report/{record_id}")
    def export_report_page(request: Request, record_id: int):
        return templates.TemplateResponse(request, "export_report.html", context={"active_page": "export", "user_role": request.session.get("monitor_role", "user"), "user_name": request.session.get("monitor_user", ""), "user_permissions": [p.strip() for p in (request.session.get("monitor_permissions") or "").split(",") if p.strip()]})

    @app.get("/api/export/config")
    def export_config(request: Request):
        denied = _require_key_api(request)
        if denied:
            return denied
        port = os.getenv("PROXY_PORT", "")
        mtimes = _list_all_mtimes()
        sync_cfg = _load_sync_config()
        return JSONResponse({
            "port": port,
            "env_key": env_key_name,
            "env_dir": str(env_dir),
            "current_logs_dir": logs_dir,
            "roots": _all_roots(),
            "obs_base": load_obs_base(),
            "workers": sync_cfg.get("workers", 8),
            "interval": sync_cfg.get("interval", 600),
            "upload_script": sync_cfg.get("upload_script", ""),
            "mtimes": mtimes,
            # 导出模式清单（settings/export_modes.yaml）。前端下拉/筛选/徽章全部由它渲染，
            # 新增模式不必改模板。顺序即 YAML 中的顺序。
            "modes": get_export_modes().to_public_list(),
        })

    @app.get("/api/export/backfill-status")
    def export_backfill_status(request: Request):
        """new-api 会话回填的后台队列状态：正在跑哪个 root、排队哪些、各自进度。

        供导出页透明展示——回填串行执行，一个 root 跑完再跑下一个。
        """
        denied = _require_key_api(request)
        if denied:
            return denied
        try:
            from utils.newapi_backfill import get_queue_snapshot, get_backfill_status
        except Exception:
            return JSONResponse({"enabled": False, "current": None, "queued": [], "roots": []})

        snap = get_queue_snapshot()
        # 回填在独立 worker 进程，app 内存无运行态；运行/排队真相取自 backfill_requests
        # 表（get_queue_snapshot 已据此拼 current/queued/running）。这里额外用它标注每个
        # root 的 running/queued，避免逐叶进度虽由磁盘探测得出、状态却恒显 idle。
        active_by_root = {}
        for _rr in snap.get("running_roots", []):
            active_by_root[_rr] = "running"
        for _qr in snap.get("queued", []):
            active_by_root.setdefault(_qr, "queued")
        # 汇总每个 new-api root 的进度（total/done 叶子）
        roots_prog = []
        for r in _existing_roots():
            try:
                from utils.log_scan import detect_format as _df
                if _df(r) != "newapi":
                    continue
            except Exception:
                continue
            st = get_backfill_status(r)
            active = active_by_root.get(os.path.normpath(r))
            roots_prog.append({
                "root": r,
                "name": Path(r).name,
                "status": active or st.get("status", "idle"),
                "total_leaves": st.get("total_leaves", 0),
                "done_leaves": st.get("done_leaves", 0),
                "last_error": st.get("last_error", ""),
            })
        return JSONResponse({
            "enabled": True,
            "current": snap.get("current"),
            "current_name": Path(snap["current"]).name if snap.get("current") else None,
            "current_status": snap.get("current_status"),
            "queued": [Path(q).name for q in snap.get("queued", [])],
            "queue_len": snap.get("queue_len", 0),
            "running": snap.get("running", False),
            "roots": roots_prog,
        })

    @app.get("/api/export/keys")
    def export_keys(request: Request, threshold: int = 5, force: int = 0):
        denied = _require_key_api(request)
        if denied:
            return denied
        import time as _time
        now = _time.time()
        # 10 分钟内存缓存：keys 统计是跨 root 扫盘的重接口，避免每次进页面/开新建任务都重扫。
        # force=1（点「刷新统计」）时强制重算并刷新缓存。
        if not force and _keys_cache["data"] is not None and (now - _keys_cache["ts"] < _KEYS_TTL):
            payload = dict(_keys_cache["data"])
            payload["cached"] = True
            payload["cache_age"] = int(now - _keys_cache["ts"])
            return JSONResponse(payload)

        payload = _compute_keys_payload(threshold)
        _keys_cache["ts"] = now
        _keys_cache["data"] = payload
        out = dict(payload)
        out["cached"] = False
        out["cache_age"] = 0
        return JSONResponse(out)

    def _compute_keys_payload(threshold: int = 5) -> dict:
        """组装 /api/export/keys 的完整响应体（跨 root 扫盘统计 + 模型分布 + 每 key 记录）。"""
        roots = [r for r in _all_roots() if Path(r).is_dir()]
        if not roots:
            return {"keys": [], "mtimes": [], "models": [], "last_refresh_ts": get_last_refresh_ts()}

        # 跨 root 合并 session 统计（rows 的 mtime_cells key 已带 <root_id>/ 前缀）
        stats = build_stats_multi(roots, threshold, active_env_dir=str(env_dir))

        # 每个 key 的模型分布（{api_key: {model: session_count}}）。DB 已按 service 绑定，
        # 一次查询覆盖全部叶子，避免逐叶重扫 index.jsonl（那是「导出一直加载中」的根因）。
        try:
            import utils.session_store as _ss
            model_stats = _ss.get_model_stats_by_key()
        except Exception:
            model_stats = {}
        all_models = sorted({m for dist in model_stats.values() for m in dist})

        db_keys_list = list_keys()

        # 名称/slot/创建时间：直接用 DB key 列表按后 4 位匹配（多 root 不复用单 env 的 key_meta 缓存）
        def _match_meta(api_key: str):
            slot = _key_slot(api_key if api_key != "(empty)" else "")
            matched_name = ""
            created_at = ""
            for k in db_keys_list:
                masked = k.get("key", "")
                if api_key != "(empty)" and masked.endswith(api_key[-4:]):
                    matched_name = k.get("name", "")
                    created_at = k.get("created_at", "")
                    break
            return matched_name, slot, created_at

        keys_result = []
        for row in stats.get("rows", []):
            api_key = row["api_key"]
            matched_name, slot, created_at = _match_meta(api_key)
            records = list_records_by_key(slot, limit=10)

            keys_result.append({
                "api_key": api_key,
                "key_name": matched_name,
                "key_slot": slot,
                "created_at": created_at,
                "total_sessions": row["row_total"],
                "qualified_sessions": row["row_qualified"],
                "mtime_distribution": row.get("mtime_cells", {}),
                "model_distribution": model_stats.get(api_key, {}),
                "records": records,
            })

        keys_result.sort(key=lambda x: x.get("created_at", ""), reverse=True)

        return {"keys": keys_result, "mtimes": _list_all_mtimes(),
                "models": all_models, "last_refresh_ts": get_last_refresh_ts()}

    # -----------------------------------------------------------------
    # 统一任务执行（导出 / 质检）
    # -----------------------------------------------------------------


    # -----------------------------------------------------------------
    # API 端点
    # -----------------------------------------------------------------

    @app.post("/api/export/run")
    async def export_run(request: Request):
        denied = _require_key_api(request)
        if denied:
            return denied
        body = await request.json()
        api_key = body.get("api_key", "")
        if api_key == "(empty)":
            api_key = ""
        mtime_dirs = body.get("mtime_dirs", [])
        obs_prefix = body.get("obs_prefix", "").strip().rstrip("/")
        force = body.get("force", False)
        # 并发 worker 数：>0 时按任务记录执行；0/缺失回退全局配置默认。
        try:
            workers = int(body.get("workers", 0) or 0)
        except (TypeError, ValueError):
            workers = 0
        if workers < 0:
            workers = 0
        # 目录并发数：多个 mtime 目录并行处理的数量。0/缺失=串行；总线程≈dir_workers×workers。
        try:
            dir_workers = int(body.get("dir_workers", 0) or 0)
        except (TypeError, ValueError):
            dir_workers = 0
        if dir_workers < 0:
            dir_workers = 0
        # 跨小时节点聚合：把同一条对话在多个小时目录里的段拼成一条产物。
        # 默认关——产物结构会变（一条记录可能引用多个源目录），下游需先确认兼容。
        merge_hours = bool(body.get("merge_hours", False))
        # 默认立即入队执行；start=False 时只建草稿（draft）记录，由前端手动「启动」。
        start = body.get("start", True)
        # mode 取值来自 settings/export_modes.yaml 注册表；非法/缺失回退默认。
        # 兼容旧的 auto_eval 布尔（true → 默认质检模式）。
        _mc = get_export_modes()
        mode = body.get("mode")
        if mode not in _mc.ids():
            _fallback_mode = "eval" if body.get("auto_eval", False) else _default_mode()
            mode = _fallback_mode
        # 用户自定义的名字（可选），建任务时存进 manage_name，列表里显示。
        export_name = (body.get("export_name") or body.get("name") or "").strip()

        if not mtime_dirs:
            return JSONResponse({"detail": "mtime_dirs is required"}, status_code=400)
        # max_mtimes=1 的模式（全量导出）单目录即产出海量文件，限定一次只导一个 mtime，
        # 避免多目录放大资源占用/误操作。上限来自配置，不再硬编码模式名。
        _max_mt = get_export_modes().get(mode).max_mtimes
        if _max_mt and len(mtime_dirs) != _max_mt:
            return JSONResponse({"detail": f"{get_export_modes().get(mode).mode_label}仅支持单个 mtime 目录"}, status_code=400)
        for mt in mtime_dirs:
            if _resolve_mt(mt) is None:
                return JSONResponse({"detail": f"目录不存在: {mt}"}, status_code=400)

        slot = _key_slot(api_key)
        now_tag = _new_now_tag()

        record_id = create_record(
            api_key=api_key, key_slot=slot,
            mtime_dirs=json.dumps(mtime_dirs),
            # 草稿阶段 obs_dst 存裸前缀，供「启动」时复原 obs_prefix；
            # 真正执行时 _run_task_inner 会用 now_tag 重拼完整 obs_dst 覆盖。
            obs_dst=obs_prefix if not start else "",
            local_copy_dir="",
            mode=mode,
            key_name=_key_name_snapshot(api_key),
            workers=workers,
            dir_workers=dir_workers,
            merge_hours=merge_hours,
        )

        # 有名字就写入 manage_name；无名字保持默认空串。
        if export_name:
            set_manage_name(record_id, export_name)

        if not start:
            # 草稿：只登记不入队，等待用户手动「启动」。
            update_status(record_id, "draft")
            return JSONResponse({"record_id": record_id, "status": "draft", "mode": mode})

        persist_params(record_id, mode=mode, obs_prefix=obs_prefix, force=force,
                       now_tag=now_tag, env_dir=str(env_dir), env_key_name=env_key_name,
                       workers=workers, dir_workers=dir_workers, merge_hours=merge_hours)
        _enqueue_task(record_id, lambda rid=record_id, ed=env_dir, ek=env_key_name, op=obs_prefix, nt=now_tag, m=mode, f=force, mh=merge_hours: _run_task(rid, ed, ek, op, nt, m, force=f, merge_hours=mh))

        return JSONResponse({"record_id": record_id, "status": "queued", "mode": mode})

    @app.post("/api/export/start")
    async def export_start(request: Request):
        """启动一条草稿（draft）记录：复原参数后入队执行。

        草稿在创建时不执行，obs_dst 存的是裸 obs_prefix（见 export_run）；
        这里从记录复原 prefix / mode / api_key / mtime_dirs 后入队，_run_task 内部
        会用新 now_tag 重拼完整 obs_dst。
        """
        denied = _require_key_api(request)
        if denied:
            return denied
        body = await request.json()
        record_id = body.get("record_id")
        if not record_id:
            return JSONResponse({"detail": "record_id is required"}, status_code=400)

        rec = get_record(record_id)
        if not rec:
            return JSONResponse({"detail": "Record not found"}, status_code=404)
        if rec["status"] != "draft":
            return JSONResponse({"detail": "只能启动草稿任务"}, status_code=400)

        mode = rec.get("mode") or _default_mode()
        obs_prefix = (rec.get("obs_dst", "") or "").strip().rstrip("/")
        force = False
        now_tag = _new_now_tag()

        persist_params(record_id, mode=mode, obs_prefix=obs_prefix, force=force,
                       now_tag=now_tag, env_dir=str(env_dir), env_key_name=env_key_name,
                       workers=rec.get("workers") or 0,
                       dir_workers=rec.get("dir_workers") or 0,
                       merge_hours=bool(rec.get("merge_hours") or 0))
        _enqueue_task(record_id, lambda rid=record_id, ed=env_dir, ek=env_key_name, op=obs_prefix, nt=now_tag, m=mode, f=force, mh=bool(rec.get("merge_hours") or 0): _run_task(rid, ed, ek, op, nt, m, force=f, merge_hours=mh))

        return JSONResponse({"record_id": record_id, "status": "queued", "mode": mode})

    @app.post("/api/export/eval")
    async def export_eval(request: Request):
        denied = _require_key_api(request)
        if denied:
            return denied
        body = await request.json()
        src_record_id = body.get("record_id")
        if not src_record_id:
            return JSONResponse({"detail": "record_id is required"}, status_code=400)

        rec = get_record(src_record_id)
        if not rec:
            return JSONResponse({"detail": "Record not found"}, status_code=404)
        if rec["status"] != "success":
            return JSONResponse({"detail": "导出未完成，无法质检"}, status_code=400)

        # obs_prefix 复原：obs_dst 形如 <prefix>/<obs_sub>/<env_key>/<slot>/ex-<tag>/，
        # 按已注册的 obs_sub 长度降序匹配，避免 session_analysis 抢在
        # session_analysis_full 前面把前缀切错。
        _eval_sub = _public_obs_sub(_eval_mode())
        obs_prefix = ""
        if rec.get("obs_dst"):
            _d = rec["obs_dst"]
            _segs = sorted({m.obs_sub for m in get_export_modes().all() if m.obs_sub} |
                           ({_eval_sub} if _eval_sub else set()), key=len, reverse=True)
            for _s in _segs:
                if f"/{_s}/" in _d:
                    obs_prefix = _d.split(f"/{_s}/")[0]
                    break
            else:
                obs_prefix = _d.rstrip("/").rsplit("/", 1)[0]
        now_tag = _new_now_tag()

        new_record_id = create_record(
            api_key=rec["api_key"], key_slot=rec["key_slot"],
            mtime_dirs=rec["mtime_dirs"],
            mode=_eval_mode(),
            source_export_id=src_record_id,
            key_name=rec.get("key_name", ""),
        )

        persist_params(new_record_id, mode=_eval_mode(), obs_prefix=obs_prefix, force=False,
                       now_tag=now_tag, env_dir=str(env_dir), env_key_name=env_key_name)
        _enqueue_task(new_record_id, lambda rid=new_record_id, ed=env_dir, ek=env_key_name, op=obs_prefix, nt=now_tag: _run_task(rid, ed, ek, op, nt, _eval_mode()))

        return JSONResponse({"record_id": new_record_id, "status": "queued", "mode": _eval_mode()})

    @app.post("/api/export/retry")
    async def export_retry(request: Request):
        """重试失败/已取消（含用户终止）的任务：用原记录参数新建一条记录重跑。

        原失败记录保留作历史（同 export_eval 的"从原记录重建"模式）。沿用原
        mode（export/reformat/eval/reconstruct/full_reformat/jiuwen_reformat）、api_key、
        mtime_dirs；obs_prefix 从原
        obs_dst 复原，保证与原任务一致。
        """
        denied = _require_key_api(request)
        if denied:
            return denied
        body = await request.json()
        src_record_id = body.get("record_id")
        if not src_record_id:
            return JSONResponse({"detail": "record_id is required"}, status_code=400)

        rec = get_record(src_record_id)
        if not rec:
            return JSONResponse({"detail": "Record not found"}, status_code=404)
        if rec["status"] not in ("failed", "cancelled"):
            return JSONResponse({"detail": "只能重试失败/已取消的任务"}, status_code=400)

        mode = rec.get("mode") or _default_mode()
        # obs_prefix 复原：obs_dst 形如 <prefix>/<obs_sub>/<env_key>/<slot>/ex-<tag>/，
        # obs_sub 取自配置。按 obs_sub 长度降序匹配，避免 session_analysis 抢在
        # session_analysis_full 前面把前缀切错。
        obs_dst = rec.get("obs_dst", "") or ""
        obs_prefix = ""
        if obs_dst:
            _subs = sorted({m.obs_sub for m in get_export_modes().all() if m.obs_sub},
                           key=len, reverse=True)
            for _sub in _subs:
                _seg = f"/{_sub}/"
                if _seg in obs_dst:
                    obs_prefix = obs_dst.split(_seg)[0]
                    break
            else:
                obs_prefix = obs_dst.rstrip("/").rsplit("/", 1)[0]
        now_tag = _new_now_tag()

        new_record_id = create_record(
            api_key=rec["api_key"], key_slot=rec["key_slot"],
            mtime_dirs=rec["mtime_dirs"],
            obs_dst="", local_copy_dir="",
            mode=mode,
            key_name=rec.get("key_name", ""),
            merge_hours=bool(rec.get("merge_hours") or 0),
        )

        persist_params(new_record_id, mode=mode, obs_prefix=obs_prefix, force=False,
                       now_tag=now_tag, env_dir=str(env_dir), env_key_name=env_key_name,
                       workers=rec.get("workers") or 0,
                       dir_workers=rec.get("dir_workers") or 0,
                       merge_hours=bool(rec.get("merge_hours") or 0))
        _enqueue_task(new_record_id, lambda rid=new_record_id, ed=env_dir, ek=env_key_name, op=obs_prefix, nt=now_tag, m=mode, mh=bool(rec.get("merge_hours") or 0): _run_task(rid, ed, ek, op, nt, m, merge_hours=mh))

        return JSONResponse({"record_id": new_record_id, "status": "queued", "mode": mode})

    @app.post("/api/export/duplicate")
    async def export_duplicate(request: Request):
        """复制任务：按原记录的参数新建一条，立即排队执行。

        与 retry 的区别：retry 只接受失败/已取消，且强制 force=False；复制接受
        **任意状态**（含 success/running），用于「同样的条件再跑一遍」。新任务用新的
        now_tag，因此落盘目录与 OBS 前缀都与原任务错开，不会覆盖原产物。
        """
        denied = _require_key_api(request)
        if denied:
            return denied
        body = await request.json()
        src_record_id = body.get("record_id")
        if not src_record_id:
            return JSONResponse({"detail": "record_id is required"}, status_code=400)

        rec = get_record(src_record_id)
        if not rec:
            return JSONResponse({"detail": "Record not found"}, status_code=404)

        mode = rec.get("mode") or _default_mode()
        if get_export_modes().get(mode) is None:
            return JSONResponse({"detail": f"未知导出模式 '{mode}'"}, status_code=400)

        # obs_prefix 复原：与原任务保持一致（obs_dst 形如
        # <prefix>/<obs_sub>/<env_key>/<slot>/ex-<tag>/）。按 obs_sub 长度降序匹配，
        # 避免 session_analysis 抢在 session_analysis_full 前面把前缀切错。
        obs_dst = rec.get("obs_dst", "") or ""
        obs_prefix = ""
        if obs_dst:
            _subs = sorted({m.obs_sub for m in get_export_modes().all() if m.obs_sub},
                           key=len, reverse=True)
            for _sub in _subs:
                _seg = f"/{_sub}/"
                if _seg in obs_dst:
                    obs_prefix = obs_dst.split(_seg)[0]
                    break
            else:
                obs_prefix = obs_dst.rstrip("/").rsplit("/", 1)[0]

        now_tag = _new_now_tag()
        new_record_id = create_record(
            api_key=rec.get("api_key", ""), key_slot=rec.get("key_slot", "all"),
            mtime_dirs=rec.get("mtime_dirs", "[]"),
            obs_dst="", local_copy_dir="",
            mode=mode,
            key_name=rec.get("key_name", ""),
            workers=rec.get("workers") or 0,
            dir_workers=rec.get("dir_workers") or 0,
            merge_hours=bool(rec.get("merge_hours") or 0),
        )
        if rec.get("manage_name"):
            set_manage_name(new_record_id, f"{rec['manage_name']}（副本）")

        persist_params(new_record_id, mode=mode, obs_prefix=obs_prefix, force=False,
                       now_tag=now_tag, env_dir=str(env_dir), env_key_name=env_key_name,
                       workers=rec.get("workers") or 0,
                       dir_workers=rec.get("dir_workers") or 0,
                       merge_hours=bool(rec.get("merge_hours") or 0))
        _enqueue_task(new_record_id, lambda rid=new_record_id, ed=env_dir, ek=env_key_name, op=obs_prefix, nt=now_tag, m=mode, mh=bool(rec.get("merge_hours") or 0): _run_task(rid, ed, ek, op, nt, m, merge_hours=mh))
        logger.info("复制任务 record=%s -> %s (mode=%s)", src_record_id, new_record_id, mode)

        return JSONResponse({"record_id": new_record_id, "status": "queued", "mode": mode,
                             "source_record_id": src_record_id})

    @app.post("/api/export/upload_retry")
    async def export_upload_retry(request: Request):
        denied = _require_key_api(request)
        if denied:
            return denied
        body = await request.json()
        record_id = body.get("record_id")
        if not record_id:
            return JSONResponse({"detail": "record_id is required"}, status_code=400)
        rec = get_record(record_id)
        if not rec:
            return JSONResponse({"detail": "Record not found"}, status_code=404)
        # 允许 failed（上传失败重试）与 success（当初空前缀仅本地、事后补传）。
        if rec["status"] not in ("failed", "success"):
            return JSONResponse({"detail": "只能对失败或已完成的记录重试上传"}, status_code=400)
        local_copy_dir = rec.get("local_copy_dir", "")
        if not local_copy_dir or not Path(local_copy_dir).is_dir():
            return JSONResponse({"detail": "本地文件不存在，无法重试（可能已被清理）"}, status_code=400)
        obs_dst = rec.get("obs_dst", "") or ""
        if not obs_dst:
            # 空 obs_dst（当初提交时 OBS 前缀为空，仅本地复制）：用 obs_base 现拼目标。
            # 拼法与 _run_task_inner 一致：{obs_base}/{obs_sub}/{env_key}/{slot}/ex-<tag>/。
            obs_base = (_load_sync_config().get("obs_base") or "").strip().rstrip("/")
            if not obs_base:
                return JSONResponse(
                    {"detail": "无 OBS 目标路径，且未配置 obs_base，无法上传"}, status_code=400)
            mode = rec.get("mode") or _default_mode()
            _mcfg_ur = get_export_modes().get(mode)
            obs_sub = _mcfg_ur.obs_sub if _mcfg_ur else "session"
            # env_key / slot / ex-tag 从 local_copy_dir 末三段解析：
            #   .../<obs_sub 对应本地目录>/<env_key>/<slot>/ex-<tag>
            lparts = Path(local_copy_dir).parts
            if len(lparts) < 3:
                return JSONResponse(
                    {"detail": f"本地目录结构异常，无法推断 OBS 路径: {local_copy_dir}"},
                    status_code=400)
            env_key, slot, ex_tag = lparts[-3], lparts[-2], lparts[-1]
            obs_dst = f"{obs_base}/{obs_sub}/{env_key}/{slot}/{ex_tag}/"
            # 回写记录，后续 _run_upload_only 与前端展示都用这个完整路径。
            update_status(record_id, rec["status"], obs_dst=obs_dst)

        def _upload_retry_task(rid=record_id, lcd=local_copy_dir, od=obs_dst):
            update_status(rid, "running", error_message="")
            _run_upload_only(rid, lcd, od)

        persist_upload_retry(record_id)
        _enqueue_task(record_id, _upload_retry_task)
        return JSONResponse({"record_id": record_id, "status": "queued"})

    @app.post("/api/export/cancel")
    async def export_cancel(request: Request):
        denied = _require_key_api(request)
        if denied:
            return denied
        body = await request.json()
        record_id = body.get("record_id")
        if not record_id:
            return JSONResponse({"detail": "record_id is required"}, status_code=400)
        rec = get_record(record_id)
        if not rec:
            return JSONResponse({"detail": "Record not found"}, status_code=404)
        if rec["status"] not in ("queued", "pending", "running"):
            return JSONResponse({"detail": "只能终止排队中或运行中的任务"}, status_code=400)
        # 标记取消：加入本进程集合（供出队/抢槽/目录循环检查点识别），并把 DB 置
        # cancelled（跨 worker 真相源）。
        #   - queued/pending：出队时被 _is_cancelled 跳过。
        #   - running：执行线程在目录循环顶部的检查点自行退出；即使线程已死
        #     （如被 sqlite 撞崩的僵尸），DB 置 cancelled 后前端"运行中"立即消失。
        with _queue_lock:
            _cancelled_ids.add(record_id)
        update_status(record_id, "cancelled", error_message="用户终止")
        return JSONResponse({"record_id": record_id, "status": "cancelled"})

    @app.post("/api/export/delete")
    async def export_delete(request: Request):
        """删除一条导出记录。

        默认软删除：置 is_delete=1，从列表移除，行与产物元数据全部保留，可 restore 恢复。
        只允许删终态/草稿（draft/success/failed/cancelled）；running/queued/pending 需先取消——
        避免删掉 worker 正在写、或即将出队执行的行。

        purge=true 为永久删除（硬删 DB 行 + 清外置日志）；再叠加 purge_local=true 连本地产物
        目录一并删（不可逆，OBS 云端不动）。
        """
        denied = _require_key_api(request)
        if denied:
            return denied
        body = await request.json()
        record_id = body.get("record_id")
        if not record_id:
            return JSONResponse({"detail": "record_id is required"}, status_code=400)
        purge = bool(body.get("purge", False))
        purge_local = bool(body.get("purge_local", False))
        rec = get_record(record_id)
        if not rec:
            return JSONResponse({"detail": "Record not found"}, status_code=404)
        if rec["status"] not in ("draft", "success", "failed", "cancelled"):
            return JSONResponse(
                {"detail": "只能删除草稿/已完成/失败/已取消的任务，运行中或排队中请先取消"},
                status_code=400)
        delete_record(record_id, purge=purge, purge_local=purge_local)
        return JSONResponse({
            "record_id": record_id, "deleted": True,
            "soft": not purge, "purge": purge, "purge_local": purge_local,
        })

    @app.post("/api/export/restore")
    async def export_restore(request: Request):
        """撤销软删除：is_delete 置 0，记录重新出现在列表。"""
        denied = _require_key_api(request)
        if denied:
            return denied
        body = await request.json()
        record_id = body.get("record_id")
        if not record_id:
            return JSONResponse({"detail": "record_id is required"}, status_code=400)
        rec = restore_record(record_id)
        if not rec:
            return JSONResponse({"detail": "Record not found"}, status_code=404)
        return JSONResponse({"record_id": record_id, "restored": True})

    @app.get("/api/export/status/{record_id}")
    def export_status(request: Request, record_id: int):
        denied = _require_key_api(request)
        if denied:
            return denied
        from utils.export_store import get_record_resolved
        rec = get_record_resolved(record_id)
        if not rec:
            return JSONResponse({"detail": "Not found"}, status_code=404)
        rec.pop("analysis_json", None)  # 可达 2GB+，不返回给前端
        return JSONResponse(rec)

    @app.get("/api/export/records")
    def export_records_list(request: Request, key_slot: str = ""):
        denied = _require_key_api(request)
        if denied:
            return denied
        if key_slot:
            recs = list_records_by_key(key_slot)
        else:
            recs = list_records()
        return JSONResponse({"records": recs})

    @app.get("/api/export/records-slim")
    def export_records_slim(request: Request, limit_per_key: int = 10):
        """轻量记录列表：只查 export_records（不重扫统计/不读大字段），按 key_slot 分组。

        供前端高频轮询（几秒一次）只更新每张卡片的记录状态/进度，重的
        /api/export/keys（含跨 root 统计扫盘）保持手动或低频刷新。
        """
        denied = _require_key_api(request)
        if denied:
            return denied
        grouped = list_records_all_slim(limit_per_key=limit_per_key)
        # 任务化后记录就是页面主列表，不再按 slot 截断（limit_per_key 仅作兜底）。
        # 记录按 key_slot 分组返回，前端把各组拍平即为完整任务表。
        return JSONResponse({"records_by_slot": grouped})

    def _compute_leaves(rec: dict) -> tuple:
        """把记录的 mtime_dirs 逐叶解析为 [{dir_key, sessions, state, error}], warnings。
        session 数读各叶 session_index.jsonl 的 meta；构建状态来自 log_dir.db。"""
        from utils.export_sync import _read_session_index_meta
        from utils.log_scan import dir_key_for

        mtime_dirs = json.loads(rec.get("mtime_dirs", "[]") or "[]")
        leaves = []
        warnings = []
        for mt in mtime_dirs:
            mt_src = _resolve_mt(mt) or (str(env_dir / mt) if Path(env_dir / mt).is_dir() else "")
            if not mt_src:
                warnings.append(f"{mt}: 目录不存在")
                leaves.append({"dir_key": mt, "sessions": 0, "state": "missing",
                               "built": False, "error": "目录不存在"})
                continue
            leaf = {"dir_key": mt, "sessions": 0, "state": "unknown",
                    "built": False, "error": ""}
            try:
                meta = _read_session_index_meta(mt_src)
                if meta:
                    leaf["sessions"] = int(meta.get("total_sessions", 0) or 0)
                try:
                    import utils.logdir_store as lds
                    from utils.logs_config import get_root_id
                    # dir_key 是相对叶子所属 root 的标识（与 log_dir.db / 叶子详情同口径）。
                    # 遍历已配置 roots，找到包含该叶子的 root 再算 dir_key。
                    dk = ""
                    rid = ""
                    for _r in _existing_roots():
                        _rp = Path(_r)
                        try:
                            if Path(mt_src) == _rp or _rp in Path(mt_src).parents:
                                rid = get_root_id(_r, str(env_dir))
                                dk = dir_key_for(_rp, Path(mt_src))
                                break
                        except (OSError, ValueError):
                            continue
                    info = None
                    try:
                        # bulk_get 返回 list，转成 dir_key -> row 的查找表
                        _by_key = {r.get("dir_key", ""): r for r in lds.bulk_get(rid)} if (rid and lds.has_any(rid)) else {}
                        info = _by_key.get(dk) if dk else None
                    except Exception:
                        info = None
                    if info:
                        leaf["state"] = info.get("state", "unknown")
                        leaf["built"] = bool(info.get("built"))
                        leaf["error"] = info.get("last_error", "") or ""
                except Exception:
                    pass  # log_dir.db 未同步/缺失：状态留 unknown
            except Exception as e:  # noqa: BLE001
                leaf["state"] = "error"
                leaf["error"] = str(e)
            leaves.append(leaf)
        return leaves, warnings

    @app.get("/api/export/leaves")
    def export_leaves(request: Request, record_id: int = 0, refresh: int = 0):
        """某导出记录的节点分布：把 mtime_dirs 逐叶解析为 {dir_key, sessions, state, error}。

        供任务行内「节点分布」展开（叶子详情样式）。终态任务（success/failed/cancelled）
        首次计算后把结果缓存进 DB（leaves_cache 列），之后展开直接读缓存、不再扫盘；
        非终态（running 等）或 refresh=1 时实时重算。
        """
        denied = _require_key_api(request)
        if denied:
            return denied
        if not record_id:
            return JSONResponse({"detail": "record_id is required"}, status_code=400)
        rec = get_record(record_id)
        if not rec:
            return JSONResponse({"detail": "Record not found"}, status_code=404)

        status = rec.get("status", "")
        is_terminal = status in ("success", "failed", "cancelled")

        # 终态任务优先读 DB 缓存（除非显式 refresh）：节点分布对已结束的任务是固定的。
        if is_terminal and not refresh:
            cached = get_leaves_cache(record_id)
            if cached is not None:
                out = dict(cached)
                out["record_id"] = record_id
                out["cached"] = True
                return JSONResponse(out)

        leaves, warnings = _compute_leaves(rec)

        # 终态任务算完落库，供后续直接读；非终态不缓存（数据还会变）。
        if is_terminal:
            try:
                save_leaves_cache(record_id, leaves, warnings)
            except Exception:
                logger.debug("save_leaves_cache failed for record=%s", record_id, exc_info=True)

        return JSONResponse({"record_id": record_id, "leaves": leaves,
                             "warnings": warnings, "cached": False})

    @app.get("/api/export/obs/ls")
    def export_obs_ls(request: Request, path: str = ""):
        denied = _require_key_api(request)
        if denied:
            return denied
        if not path or not path.startswith("obs://"):
            return JSONResponse({"detail": "Invalid OBS path"}, status_code=400)
        if not path.endswith("/"):
            path += "/"
        items = obsutil_ls(path)
        return JSONResponse({"path": path, "items": items})

    @app.get("/api/export/eval/report/{record_id}")
    def export_eval_report(request: Request, record_id: int):
        rec = get_record(record_id)
        if not rec:
            return JSONResponse({"detail": "Not found"}, status_code=404)

        # 优先直接 serve 本地 HTML 文件（FileResponse 流式传输，不占内存）
        report_path = rec.get("eval_report_path", "")
        html_path = Path(report_path).parent / "session_report.html" if report_path else None
        if html_path and html_path.is_file():
            return FileResponse(str(html_path), media_type="text/html")

        # 降级：analysis_json 重建报告（限制文件大小 ≤ 200MB，防止 OOM）
        analysis_json_ref = rec.get("analysis_json", "")
        if analysis_json_ref:
            from utils.export_store import _read_field_content
            # 先检查外部文件大小
            if analysis_json_ref.startswith("file://"):
                fpath = Path(analysis_json_ref[7:])
                if fpath.is_file() and fpath.stat().st_size > 200 * 1024 * 1024:
                    return JSONResponse(
                        {"detail": f"analysis_json 过大（{fpath.stat().st_size // 1024 // 1024}MB），无法在线渲染，请直接访问 OBS"},
                        status_code=413,
                    )
            try:
                from utils.eval.eval import load_analysis_json, compute_stats, render_html_report_string
                analysis_json = _read_field_content(analysis_json_ref)
                sessions = load_analysis_json(analysis_json)
                stats = compute_stats(sessions)
                key_slot = rec.get("key_slot", "all")
                content = render_html_report_string(
                    sessions, stats,
                    key_name=f"{env_key_name}/{key_slot}",
                    obs_path=rec.get("obs_dst", ""),
                )
                return HTMLResponse(content=content)
            except Exception as e:
                logger.exception("Failed to rebuild report from analysis JSON")
                return JSONResponse({"detail": f"报告重建失败: {e}"}, status_code=500)

        if report_path and Path(report_path).is_file():
            return FileResponse(str(report_path), media_type="text/plain")

        return JSONResponse({"detail": "报告未生成"}, status_code=404)

    # --- 公开导出提交/状态路由（access-key 验证） ---

    def _check_public_export(key: str, access_key: str):
        """access-key 验证身份（.env 的 ACCESS_KEY）；key 解析到日志 api_key。

        返回 (err, resolved_key)：err 为 JSONResponse 错误或 None；resolved_key 为
        解析后的完整日志 api_key，err 非 None 时置 None。
        """
        if not verify_access_key(access_key):
            return JSONResponse({"detail": "Invalid access-key"}, status_code=403), None
        if not key:
            return JSONResponse({"detail": "Key not found"}, status_code=404), None
        roots = [r for r in _all_roots() if Path(r).is_dir()]
        resolved = resolve_export_key(key, roots, str(env_dir))
        if resolved is None:
            return JSONResponse({"detail": "Key not found"}, status_code=404), None
        if resolved == -1:
            return JSONResponse({"detail": "Key not unique (ambiguous suffix)"}, status_code=404), None
        return None, resolved

    # 公开 submit 可用的 mode 及各自落到 OBS 的子目录，取自配置里的 public: true。
    # 与 _run_task_inner 的 obs_sub 同源（都读 export_modes.yaml），不会漂移。
    def _public_modes() -> tuple:
        return get_export_modes().public_ids()

    def _public_obs_sub(mode: str) -> str:
        mcfg = get_export_modes().get(mode)
        return mcfg.obs_sub if mcfg else ""

    @app.get("/export/submit")
    def public_export_submit(key: str = "", model: str = "", export_name: str = "",
                             mode: str = "", merge_hours: bool = False,
                             access_key: str = Query(default="", alias="access-key")):
        """按给定 key 正式导出：创建真实导出任务（export_records），显示名用 export_name。

        与「对话浏览」保持一致：找 build_stats_multi 里该 key 的全部 mtime 目录导出。
        model 参数仅为兼容 URL 语义保留——导出任务按现有 mtime_dirs 全量执行，
        不做按 model 的会话过滤（与既有「导出」页行为一致）。

        mode 指定导出类型，可选值 = settings/export_modes.yaml 中 public: true 的模式
        （当前 export/reformat/eval/reconstruct/full_reformat/jiuwen_reformat），默认
        export（即原始三元组复制）。max_mtimes=1 的模式（全量导出）与内部
        /api/export/run 一致：只允许单个 mtime 目录，多目录时拒绝（不静默取第一个）。
        """
        err, api_key = _check_public_export(key, access_key)
        if err:
            return err

        if not mode:
            mode = _default_mode()
        if mode not in _public_modes():
            return JSONResponse(
                {"detail": f"Invalid mode '{mode}' (allowed: {', '.join(_public_modes())})"},
                status_code=400,
            )

        roots = [r for r in _all_roots() if Path(r).is_dir()]
        stats = build_stats_multi(roots, active_env_dir=str(env_dir)) if roots else {"rows": []}
        mtime_dirs = []
        for row in stats.get("rows", []):
            if row["api_key"] == api_key:
                mtime_dirs = sorted(row.get("mtime_cells", {}).keys(), reverse=True)
                break
        if not mtime_dirs:
            return JSONResponse({"detail": "No session data found for this key"}, status_code=404)

        _pub_max_mt = get_export_modes().get(mode).max_mtimes
        if _pub_max_mt and len(mtime_dirs) != _pub_max_mt:
            return JSONResponse(
                {"detail": f"{get_export_modes().get(mode).mode_label}({mode})仅支持单个 mtime 目录，该 key 落到 {len(mtime_dirs)} 个目录"},
                status_code=400,
            )

        slot = _key_slot(api_key)
        obs_prefix = load_obs_base()
        now_tag = _new_now_tag()
        obs_sub = _public_obs_sub(mode)

        record_id = create_record(
            api_key=api_key, key_slot=slot,
            mtime_dirs=json.dumps(mtime_dirs),
            mode=mode,
            key_name=_key_name_snapshot(api_key),
            merge_hours=merge_hours,
        )
        if export_name:
            set_manage_name(record_id, export_name)

        obs_dst = f"{obs_prefix}/{obs_sub}/{env_key_name}/{slot}/ex-{now_tag}/" if obs_prefix else ""
        persist_params(record_id, mode=mode, obs_prefix=obs_prefix, force=False, now_tag=now_tag,
                       env_dir=str(env_dir), env_key_name=env_key_name, merge_hours=merge_hours)
        _enqueue_task(record_id, lambda rid=record_id, ed=env_dir, ek=env_key_name, op=obs_prefix, nt=now_tag, m=mode, mh=merge_hours: _run_task(rid, ed, ek, op, nt, m, merge_hours=mh))

        return JSONResponse({
            "export_id": record_id,
            "session_path": obs_dst,
            "status": "queued",
            "success": True,
            "mode": mode,
            "export_name": export_name,
        })

    @app.get("/export/status")
    def public_export_status(export_id: int = 0, key: str = "",
                             access_key: str = Query(default="", alias="access-key")):
        """按 export_id 查询导出状态；仅需 access-key（URL 无 key 也可）。"""
        if not verify_access_key(access_key):
            return JSONResponse({"detail": "Invalid access-key"}, status_code=403)

        from utils.export_store import get_record_resolved
        rec = get_record_resolved(export_id)  # 自动解析外部文件
        if not rec:
            return JSONResponse({"detail": "Not found"}, status_code=404)

        # URL 若带 key，解析后校验与记录归属一致。
        # 统一走 resolve_export_key（支持后四位后缀），与提交/浏览的 key 语义一致。
        if key:
            roots = [r for r in _all_roots() if Path(r).is_dir()]
            resolved_key = resolve_export_key(key, roots, str(env_dir))
            if resolved_key is None or resolved_key == -1:
                return JSONResponse({"detail": "Key not found"}, status_code=404)
            if rec.get("api_key") and rec["api_key"] != resolved_key:
                return JSONResponse({"detail": "Access denied"}, status_code=403)

        return JSONResponse({
            "export_id": rec["id"],
            "status": rec["status"],
            "session_path": rec.get("obs_dst", ""),
            "total_sessions": rec.get("total_sessions", 0),
            "error_message": rec.get("error_message", ""),
            "mode": rec.get("mode", ""),
        })

    # 注意：导出执行已剥离到独立 export_worker 进程，app 启动**不再**清理
    # running/queued/pending —— 否则 app 重启会误杀 worker 正在跑/待跑的任务。
    # 队列的「重启即清空」语义改由 export_worker 在它自己的进程启动时执行
    # （见 utils/export_worker.py 的 cancel_interrupted 调用）。

    # start_stats_warmer(str(env_dir))
