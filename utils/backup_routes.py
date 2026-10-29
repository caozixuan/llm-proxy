"""
utils/backup_routes.py — 备份管理 Web 路由

管理所有 mtime 目录的 OBS 同步状态，支持手动触发同步和删除。
"""

import logging
import os
import signal
import shutil
import subprocess as _subprocess
import sys
import threading
import time
from pathlib import Path
from typing import List

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.templating import Jinja2Templates

from utils.backup_store import (
    append_log,
    claim_next_job,
    clear_logs,
    clear_failed_files,
    count_unresolved_failed,
    enqueue_job,
    finish_job,
    get_dir,
    get_live_syncing_dirs,
    get_logs,
    has_any_dirs,
    list_dirs,
    list_env_names,
    list_clear_queue,
    list_failed_files,
    list_queue,
    mark_backed_up,
    mark_file_resolved,
    record_failed_files,
    reset_running_on_startup,
    set_deleted,
    set_upload_stats,
    set_uploaded,
    update_sync_pid,
    update_sync_status,
    upsert_dir,
)
from utils.obs_utils import (
    load_obs_base,
    load_sync_config,
    get_sync_config_path,
    obsutil_ls,
    obsutil_object_size,
    find_failed_report,
    parse_failed_report,
    reupload_file,
)

logger = logging.getLogger(__name__)

_sync_lock = threading.Lock()
_syncing_dirs: set = set()
_live_sync_procs: dict = {}

# 手动上传串行队列：单 dispatcher 线程逐个消费 DB 里的 backup_queue
_queue_cond = threading.Condition()
_dispatcher_thread = None
_dispatcher_ctx: dict = {}   # {logs_all, resolver} —— 由 register_backup_routes 填充

# 自动备份守护线程的共享状态（供 /api/backup/auto-status 透明展示）
_auto_lock = threading.Lock()
_auto_state: dict = {
    "enabled": False,
    "running": False,        # 是否正在跑一轮上传
    "disk_percent": None,
    "threshold": None,
    "queue": [],             # 待上传的 dir_path
    "current": "",           # 正在上传的 dir_path
    "uploaded": [],          # 本轮已成功上传（转 pending_delete）的 dir_path
    "pending_delete_count": 0,
    "last_check_ts": 0,
    "message": "",
}
_auto_thread = None


def _auto_set(**kw):
    with _auto_lock:
        _auto_state.update(kw)


def _auto_snapshot() -> dict:
    with _auto_lock:
        return dict(_auto_state)


def _is_pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _capture_sync_logs(proc: _subprocess.Popen, dir_path: str):
    try:
        for raw_line in iter(proc.stdout.readline, b""):
            line = raw_line.decode("utf-8", errors="replace").rstrip()
            if not line:
                continue
            level = "error" if "[ERROR]" in line or "[WARNING]" in line else "info"
            try:
                append_log(dir_path, line, level=level)
            except Exception:
                pass
    except Exception:
        pass
    finally:
        try:
            proc.stdout.close()
        except Exception:
            pass
        exit_code = proc.wait()
        try:
            dir_info = get_dir(dir_path)
            if dir_info and dir_info.get("status") == "live_syncing":
                if exit_code == 0:
                    update_sync_status(dir_path, "done")
                    append_log(dir_path, "在线同步进程正常退出")
                else:
                    update_sync_status(dir_path, "error", error_msg=f"exit_code={exit_code}")
                    append_log(dir_path, f"在线同步进程异常退出: exit_code={exit_code}", level="error")
                update_sync_pid(dir_path, None)
        except Exception:
            pass
        _live_sync_procs.pop(dir_path, None)


def _stop_live_sync(dir_path: str, timeout: int = 120) -> dict:
    if dir_path in _live_sync_procs:
        proc = _live_sync_procs[dir_path]["proc"]
        if proc.poll() is None:
            try:
                os.kill(proc.pid, signal.SIGTERM)
            except OSError:
                pass
            try:
                proc.wait(timeout=timeout)
            except _subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)
            _live_sync_procs.pop(dir_path, None)
            update_sync_status(dir_path, "done")
            update_sync_pid(dir_path, None)
            append_log(dir_path, "在线同步已停止")
            return {"ok": True}
        else:
            _live_sync_procs.pop(dir_path, None)

    dir_info = get_dir(dir_path)
    if not dir_info:
        return {"ok": False, "detail": "目录不存在"}

    pid = dir_info.get("sync_pid")
    if pid and _is_pid_alive(pid):
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError:
            pass
        for _ in range(timeout * 2):
            time.sleep(0.5)
            if not _is_pid_alive(pid):
                break
        if _is_pid_alive(pid):
            try:
                os.kill(pid, signal.SIGKILL)
            except OSError:
                pass
            time.sleep(0.2)
        update_sync_status(dir_path, "done")
        update_sync_pid(dir_path, None)
        append_log(dir_path, "在线同步已停止 (via PID)")
        return {"ok": True}

    if dir_info.get("status") == "live_syncing":
        update_sync_status(dir_path, "done")
        update_sync_pid(dir_path, None)
        append_log(dir_path, "在线同步进程已不存在，状态已更新")
    return {"ok": True}


def _cleanup_stale_live_syncs():
    try:
        stale_dirs = get_live_syncing_dirs()
        for d in stale_dirs:
            pid = d.get("sync_pid")
            dir_path = d["dir_path"]
            if pid and _is_pid_alive(pid):
                logger.info("Stopping stale live sync: dir=%s pid=%d", dir_path, pid)
                try:
                    os.kill(pid, signal.SIGTERM)
                except OSError:
                    pass
                for _ in range(20):
                    time.sleep(0.5)
                    if not _is_pid_alive(pid):
                        break
                if _is_pid_alive(pid):
                    try:
                        os.kill(pid, signal.SIGKILL)
                    except OSError:
                        pass
            update_sync_status(dir_path, "done")
            update_sync_pid(dir_path, None)
            append_log(dir_path, "应用重启，在线同步已自动停止")
    except Exception as e:
        logger.warning("Failed to cleanup stale live syncs: %s", e)


def _require_ajax(request: Request):
    if request.headers.get("x-requested-with") != "XMLHttpRequest":
        return JSONResponse({"detail": "Not found"}, status_code=404)
    return None


def _human_size(nbytes: int) -> str:
    val = float(nbytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if val < 1024 or unit == "TB":
            return f"{int(val)}{unit}" if unit == "B" else f"{val:.1f}{unit}"
        val /= 1024
    return f"{val:.1f}TB"


def _disk_usage(path: Path) -> dict:
    """返回 path 所在挂载点的磁盘使用情况（df）。"""
    try:
        st = shutil.disk_usage(str(path))
        return {
            "total": st.total, "used": st.used, "free": st.free,
            "total_h": _human_size(st.total), "used_h": _human_size(st.used),
            "free_h": _human_size(st.free),
            "percent": round(st.used / st.total * 100, 1) if st.total else 0,
        }
    except OSError:
        return {}


def _mount_point(path) -> str:
    """返回 path 所在挂载点（st_dev 向上爬取，找到 dev 变化处的顶层目录）。

    同盘多个数据根共享同一 mount_point（df 值相同），供前端「按挂载盘归并统计」。
    路径不存在时沿父级向上找存在的最近祖先；失败时原样返回 path。
    """
    p = Path(os.path.realpath(path))
    while not p.exists():
        parent = p.parent
        if str(parent) == str(p):
            break
        p = parent
    try:
        dev = p.stat().st_dev
    except OSError:
        return str(path)
    top = p
    cur = p
    while True:
        parent = cur.parent
        if str(parent) == str(cur):
            break
        try:
            if parent.stat().st_dev != dev:
                break
        except OSError:
            break
        cur = parent
        top = cur
    return str(top)


def _du_size(path: Path) -> int:
    """递归统计目录字节数（du）。大目录可能较慢。"""
    total = 0
    try:
        for dirpath, _dn, filenames in os.walk(str(path)):
            for fn in filenames:
                try:
                    total += os.path.getsize(os.path.join(dirpath, fn))
                except OSError:
                    continue
    except OSError:
        pass
    return total


def _scan_data_dirs(logs_all: Path, env_name: str = None, port_env_map: dict = None) -> List[dict]:
    """扫描 logs_all 下有数据的 mtime 目录。可选只扫描指定 env。"""
    result = []
    if not logs_all.is_dir():
        return result
    if env_name:
        target = logs_all / env_name
        env_dirs = [target] if target.is_dir() else []
    else:
        env_dirs = sorted(d for d in logs_all.iterdir()
                          if d.is_dir() and not d.name.startswith("logs_") and not d.name.startswith("."))
    port_map = port_env_map or {}
    for env_dir in env_dirs:
        if not env_dir.is_dir() or env_dir.name.startswith("logs_") or env_dir.name.startswith("."):
            continue
        for mtime_dir in sorted(env_dir.iterdir()):
            if not mtime_dir.is_dir():
                continue
            index_file = mtime_dir / "index.jsonl"
            has_data = index_file.is_file()
            if not has_data:
                try:
                    has_data = any(f.name.endswith("-req.json") for f in mtime_dir.iterdir())
                except OSError:
                    continue
            if not has_data:
                continue
            file_count = 0
            try:
                file_count = sum(1 for f in mtime_dir.iterdir() if f.is_file())
            except OSError:
                pass
            result.append({
                "dir_path": f"{env_dir.name}/{mtime_dir.name}",
                "env_name": env_dir.name,
                "mtime_tag": mtime_dir.name,
                "file_count": file_count,
                "port": port_map.get(env_dir.name, ""),
            })
    return result


def _seed_dirs_from_leaf_status(env_name: str) -> int:
    """用 leaf_status **补齐**某个来源在 backup_dirs 里缺失的叶子行。

    幂等：只插缺失的 (root_id, dir_key)，已有的行（含已上传/已删除的）不动。
    返回本次新增条数。

    为什么这样做而不是「首次扫盘」：
      - _scan_root_leaves / _scan_data_dirs 走 iter_index_dirs，是 NFS 上的
        递归 walk，选择从未备份过的来源时实测 45s+ 不返回。
      - 旧逻辑用 has_any_dirs() 判断「是否需要初始扫描」，只要有 1 行就跳过；
        而 backup_dirs 里某些来源只有零星几行（历史遗留 / 之前的扫描被超时
        打断），于是剩下的叶子永远补不进来。

    dir_path 用 f"{root_id}/{dir_key}"，与 backup_dirs 既有口径一致。
    """
    try:
        import utils.logdir_store as _lds
        if not _lds._ready():
            return 0
        rows = _lds.bulk_get_stats(env_name)
    except Exception:  # noqa: BLE001
        logger.exception("seed dirs from leaf_status failed: %s", env_name)
        return 0

    # 已有行一次性取出来，避免逐条 SELECT（upsert_dir 内部会查一次）
    try:
        existing = {r["dir_key"] for r in list_dirs(env_name=env_name)}
    except Exception:  # noqa: BLE001
        existing = set()

    n = 0
    for r in rows:
        dk = (r.get("dir_key") or "").strip()
        if not dk or dk in existing:
            continue
        # 已归档（本地已删）的叶子不该再进备份队列
        if r.get("archived"):
            continue
        # file_count 故意留 0：逐个 iterdir 数文件是 NFS 上的元数据风暴
        # （72 个叶子实测几十秒）。前端无值时显示「—」，不影响归档本身。
        try:
            upsert_dir(f"{env_name}/{dk}", env_name, dk, 0, has_local=True)
            existing.add(dk)
            n += 1
        except Exception:  # noqa: BLE001
            logger.exception("seed dir failed: %s/%s", env_name, dk)
    if n:
        logger.info("从 leaf_status 补齐 %d 个目录到备份队列（env=%s）", n, env_name)
    return n


def _scan_root_leaves(root: str) -> List[dict]:
    """扫描一个非 logs_all 根（如 new-api 的 details）下含 index.jsonl 的叶子。

    env_name = 根的稳定 root_id（消除同 basename 冲突）；
    mtime_tag = 叶子相对根的路径（天/小时）；dir_path = env_name/mtime_tag。
    """
    result = []
    try:
        from utils.log_scan import iter_index_dirs, dir_key_for
        from utils.logs_config import get_root_id
    except Exception:
        return result
    rp = Path(root)
    if not rp.is_dir():
        return result
    env_name = get_root_id(root)
    for leaf in iter_index_dirs(rp):
        rel = dir_key_for(rp, leaf)
        try:
            file_count = sum(1 for f in leaf.iterdir() if f.is_file())
        except OSError:
            file_count = 0
        result.append({
            "dir_path": f"{env_name}/{rel}",
            "env_name": env_name,
            "mtime_tag": rel,
            "file_count": file_count,
            "port": "",
        })
    return result


def _scan_obs_dirs(obs_base: str, env_name: str = None) -> List[dict]:
    """扫描 OBS 上 {obs_base}/raw/ 下的 env/mtime 目录结构。"""
    result = []
    if not obs_base:
        return result
    raw_path = f"{obs_base.rstrip('/')}/raw/"
    env_items = obsutil_ls(raw_path, show_dirs=True)
    for env_item in env_items:
        if not env_item.get("is_dir"):
            continue
        ename = env_item["name"]
        if env_name and ename != env_name:
            continue
        env_obs_path = env_item["path"]
        mtime_items = obsutil_ls(env_obs_path, show_dirs=True)
        for mt_item in mtime_items:
            if not mt_item.get("is_dir"):
                continue
            mtag = mt_item["name"]
            result.append({
                "dir_path": f"{ename}/{mtag}",
                "env_name": ename,
                "mtime_tag": mtag,
                "obs_path": mt_item["path"],
            })
    return result


def _confirm_delete_sync(dirs: list) -> dict:
    """人工确认删除：**不直接写 clear 队列**，而是交回 backup_worker 处理。

    所有删除申请都必须由 backup_worker 发出（它掌握上传状态与校核逻辑），
    clear_worker 只负责执行。这样删除来源单一、可追溯。

    流程：web 把目录重置为 pending 并重新入上传队列 → backup_worker 领到后
    走 `synced=1` 分支（已上传过）→ 做 OBS 体积校核 → 通过才提交 clear。
    """
    from utils.backup_store import enqueue_job
    submitted, skipped = [], []
    for dir_path in dirs:
        info = get_dir(dir_path)
        # 安全前提：必须确认内容在 OBS 上（有 obs_path）
        if not info or not info.get("has_obs") or not info.get("obs_path"):
            skipped.append(dir_path)
            append_log(dir_path, "清理申请被拒绝：未确认已上传 OBS", level="error")
            continue
        # 用 purge 任务类型：worker 领到后直接走「校核 + 提交 clear」，
        # 不受 assert auto_delete / retain_days 约束（人工确认是明确意图）。
        update_sync_status(dir_path, "pending", obs_path=info.get("obs_path", ""))
        if enqueue_job(dir_path, "purge"):
            append_log(dir_path, "已提交清理申请，等待 backup_worker 校核后转入 clear",
                       event="delete_requested")
            submitted.append(dir_path)
        else:
            skipped.append(dir_path)
    return {"ok": True, "queued": submitted, "skipped": skipped}


def run_purge_job(dir_path: str, logs_dir: str) -> bool:
    """处理「人工确认删除」申请（job_type='purge'）。

    人工确认是明确意图，**不受 auto_delete / retain_days 约束**，但校核照做：
    仍然要先核对 OBS 体积，通过才提交 clear —— 删除不可逆，人工点了也不跳过验证。

    与自动路径一样，删除申请由 backup_worker 发出，web 不直接写 clear 队列。
    """
    real_dir = resolve_real_backup_dir(dir_path, logs_dir)
    info = get_dir(dir_path) or {}
    obs_path = info.get("obs_path") or ""
    if not obs_path:
        append_log(dir_path, "清理申请被拒绝：无 OBS 路径", level="error", event="verify_fail")
        update_sync_status(dir_path, "pending", obs_path="", error_msg="无 OBS 路径")
        return False
    append_log(dir_path, "收到人工清理申请，开始 OBS 体积校核")
    return _submit_clear_verified(dir_path, real_dir, obs_path)


def _maybe_auto_delete(dir_path: str, real_dir: Path, obs_dst: str,
                       auto_delete: bool, retain_days: int) -> bool:
    """**刚上传完**的目录：保留期已到就提交清理申请。

    与「已备份目录」的处理刻意分开（见 _submit_clear_verified）：
    这里的内容刚刚走了完整上传 + 哨兵校验（obsutil -vlength 逐文件 +
    index.jsonl 大小比对），已经证明「确实传上去了」，**不需要再做
    ls -du 全量体积核对** —— 那要递归列举几十万对象，对刚传完的目录是重复劳动。

    规则：保留期内 → 保持 done（页面显示「已备份」），等到期后由
    run_pipeline_step 的 clear 分支接手；已到期 → 直接提交 clear。
    """
    _env, _, mtime_tag = dir_path.partition("/")
    if not auto_delete:
        append_log(dir_path, "自动清理已关闭，本地数据保留")
        return False
    if _leaf_too_new(mtime_tag, retain_days):
        append_log(dir_path,
                   f"仍在保留期内（{retain_days} 天），保持已备份状态")
        return False
    # 简单校核已完成（哨兵 + -vlength），直接提交
    return _enqueue_clear_now(dir_path, real_dir, obs_dst,
                              reason="上传校验通过")


def _enqueue_clear_now(dir_path: str, real_dir: Path, obs_dst: str,
                       reason: str = "") -> bool:
    """把目录提交到 clear 队列（状态转 pending_delete）。

    **只有 backup_worker 会调用这里** —— 所有删除申请都必须从 worker 发出，
    web 侧不直接写 clear 队列。这样删除来源单一、可追溯。
    """
    from utils.backup_store import enqueue_clear

    if not real_dir.is_dir():
        mark_backed_up(dir_path)
        set_uploaded(dir_path)
        set_deleted(dir_path)
        return False

    abs_path = str(real_dir.resolve())
    try:
        queued = enqueue_clear(dir_path, abs_path)
    except Exception as e:  # noqa: BLE001
        logger.exception("enqueue_clear failed for %s", dir_path)
        update_sync_status(dir_path, "done", obs_path=obs_dst,
                           error_msg=f"清理入队失败: {e}")
        return False

    update_sync_status(dir_path, "pending_delete", obs_path=obs_dst)
    if queued:
        append_log(dir_path,
                   f"{reason}，已提交清理申请: {abs_path}", event="delete_queued")
    else:
        append_log(dir_path, "本地清理任务已在队列中")
    return queued


def _submit_clear_verified(dir_path: str, real_dir: Path, obs_dst: str) -> bool:
    """**已经备份过**的目录：先做 OBS 体积校核，通过才提交清理申请。

    与 _maybe_auto_delete 的区别：这些目录的上传发生在过去（可能几天前），
    当时的上传结果不可复验，所以这里用 `obsutil ls -du` 取 OBS 前缀的
    **真实字节数**，与 backup_dirs.size_bytes 比对（容差 1%）。
    不通过 → 保持 done + 记 verify_fail，绝不删除。
    """
    from utils.backup_store import set_upload_stats
    from utils.obs_utils import obsutil_prefix_size

    if not real_dir.is_dir():
        mark_backed_up(dir_path)
        set_uploaded(dir_path)
        set_deleted(dir_path)
        return False

    info = get_dir(dir_path) or {}
    expect = int(info.get("size_bytes") or -1)
    actual = obsutil_prefix_size(obs_dst) if obs_dst else None

    if actual is None:
        msg = "OBS 体积核对失败（无法读取前缀大小），暂不清理"
        append_log(dir_path, msg, level="error", event="verify_fail")
        update_sync_status(dir_path, "done", obs_path=obs_dst, error_msg=msg)
        return False

    # 首次核对成功时把真实体积写回（比上传时的 Succeed bytes 更准）
    if expect <= 0:
        set_upload_stats(dir_path, size_bytes=actual)
        expect = actual
    if actual < expect * 0.99:
        msg = (f"OBS 体积小于本地记录（OBS {actual} < 记录 {expect}），"
               f"疑似未传完，暂不清理")
        append_log(dir_path, msg, level="error", event="verify_fail")
        update_sync_status(dir_path, "done", obs_path=obs_dst, error_msg=msg)
        return False

    return _enqueue_clear_now(
        dir_path, real_dir, obs_dst,
        reason=f"已备份目录 OBS 体积核对通过（{actual} 字节）")


def _verify_upload_sentinels(real_dir: Path, obs_dst: str) -> tuple:
    """核对哨兵对象是否真的落到了 OBS 上，返回 (ok, problems, checked)。

    哨兵 = 叶子内 index.jsonl 与 .session_cache.jsonl（存在才查）。它们是叶子里
    最关键的两个文件：前者是全部请求的索引，后者是会话缓存。任何一个在 OBS 上
    缺失或大小不符，都说明这次上传没有把内容真正落盘，绝不能据此删除本地。

    只查单个对象（每个一次 obsutil ls，秒级）。不要递归统计整个叶子 ——
    实测单叶 80 万个对象，`ls -du` 要跑几分钟。

    checked=0 表示该叶没有哨兵文件（如纯 -req.json 目录），没有可核对的锚点，
    只能退化为依赖退出码 + obsutil 的 -vlength。

    落点有两层可能是历史遗留：obsutil `cp <dir> <dst> -r` 会把**目录自身**也带上，
    因此文件实际落在 `{obs_dst}/{dirname}/{file}`（实测 index.jsonl 在
    `.../26080115/26080115/index.jsonl`），而不是 `{obs_dst}/{file}`。两个位置
    都要查，否则所有 newapi 叶子都会被误判为核对失败。
    """
    problems: list = []
    checked = 0
    base = obs_dst.rstrip("/")
    dir_name = Path(real_dir).name
    for name in ("index.jsonl", ".session_cache.jsonl"):
        local = real_dir / name
        try:
            if not local.is_file():
                continue
            local_size = local.stat().st_size
        except OSError:
            continue

        candidates = [f"{base}/{name}"]
        if dir_name:
            candidates.append(f"{base}/{dir_name}/{name}")

        remote_size = None
        hit_url = candidates[0]
        for obs_url in candidates:
            sz = obsutil_object_size(obs_url)
            if sz is not None:
                remote_size, hit_url = sz, obs_url
                break

        if remote_size is None:
            problems.append({
                "local": str(local), "obs": candidates[0],
                "detail": f"{name} 在 OBS 上不存在（查过 {' / '.join(candidates)}）",
            })
            continue
        if remote_size != local_size:
            problems.append({
                "local": str(local), "obs": hit_url,
                "detail": f"{name} 大小不符（本地 {local_size} / OBS {remote_size}）",
            })
            continue
        checked += 1

    return (not problems), problems, checked


def _run_sync_for_dir(dir_path: str, logs_all: Path, obs_base: str, workers: int, upload_script: str,
                      resolver=None, auto_delete: bool = False, retain_days: int = 7):
    """在后台线程中同步单个目录到 OBS，直接上传整个文件夹，流式输出日志。

    resolver: 可选 dir_path -> 真实磁盘 Path 的解析器（供 new-api 等非 logs_all 根）。
    auto_delete/retain_days: 校验通过后是否自动删除本地（见 _maybe_auto_delete）。
    """
    import subprocess as sp
    from utils.obs_utils import DEFAULT_UPLOAD_SCRIPT

    real_dir = resolver(dir_path) if resolver else (logs_all / dir_path)
    logs_dir = str(real_dir)
    env_name, mtime_tag = dir_path.split("/", 1)
    obs_dst = f"{obs_base.rstrip('/')}/raw/{env_name}/{mtime_tag}/"
    script = upload_script or DEFAULT_UPLOAD_SCRIPT
    if not Path(script).is_absolute():
        script = str((Path(__file__).resolve().parent.parent / script).resolve())

    try:
        # 本地目录不存在：已被清理，标记为 backed_up 跳过上传
        if not Path(real_dir).is_dir():
            dir_info = get_dir(dir_path)
            if dir_info and dir_info.get("obs_path"):
                update_sync_status(dir_path, "backed_up", obs_path=dir_info["obs_path"])
                append_log(dir_path, "本地目录不存在，已标记为已备份（跳过上传）")
            else:
                append_log(dir_path, "本地目录不存在且无 OBS 路径，跳过", level="error")
            return

        # synced=1：之前已成功同步过（**已备份**），跳过重复上传。
        #
        # 这条路径对应「已经备份过的目录」：当初的上传结果无法复验，所以这里
        # 走 _submit_clear_verified —— 先 `obsutil ls -du` 取 OBS 真实体积，
        # 与记录比对通过才提交 clear 申请。与刚上传完的 _maybe_auto_delete
        # （走简单校核）刻意区分开。
        #
        # 早先这里直接 return，导致「已传过但没删」的目录永远停在 done，
        # 实测 26070714 就是这样卡住的。
        dir_info = get_dir(dir_path)
        if dir_info and dir_info.get("synced") and dir_info.get("obs_path"):
            if dir_info.get("status") != "done":
                update_sync_status(dir_path, "done", obs_path=dir_info["obs_path"])
                append_log(dir_path, "已有成功同步记录（synced=1），修正状态为 done，跳过重复上传")
            else:
                append_log(dir_path, "已有成功同步记录（synced=1），跳过重复上传")
            if auto_delete and Path(real_dir).is_dir() \
                    and not _leaf_too_new(mtime_tag, retain_days):
                _submit_clear_verified(dir_path, Path(real_dir), dir_info["obs_path"])
            return

        update_sync_status(dir_path, "syncing", obs_path=obs_dst)
        append_log(dir_path, f"开始同步: {dir_path} -> {obs_dst}")

        cmd = [script, logs_dir, obs_dst]
        append_log(dir_path, f"执行: {' '.join(cmd)}")
        proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.STDOUT, text=True)
        task_id = ""
        output_dir = ""
        uploaded_bytes = None
        succeeded_count = None
        for line in proc.stdout:
            line = line.rstrip()
            if line:
                append_log(dir_path, line)
                # obsutil 开始时打印 Task id / OutputDir，捕获用于定位失败报告
                if not task_id and line.startswith("Task id:"):
                    task_id = line.split(":", 1)[1].strip()
                elif not output_dir and line.startswith("OutputDir:"):
                    output_dir = line.split(":", 1)[1].strip()
                # 上传结束时的统计行：「Succeed bytes:      962.01MB」
                # 「Succeed count:      2827      Failed count:       0」
                # 这是唯一能拿到真实数据量的地方（本地不预先统计）。
                elif line.startswith("Succeed bytes:"):
                    from utils.obs_utils import _parse_size_bytes
                    tok = line.split(":", 1)[1].strip().split()[0] if line.split(":", 1)[1].strip() else ""
                    parsed = _parse_size_bytes(tok)
                    if parsed is not None:
                        uploaded_bytes = parsed
                elif line.startswith("Succeed count:"):
                    try:
                        succeeded_count = int(line.split(":", 1)[1].split()[0])
                    except (IndexError, ValueError):
                        pass
        proc.wait(timeout=600)

        if proc.returncode == 0:
            # 先落数据量与文件数：即使后面校验失败，这两个值也是真实的观测结果，
            # 有助于判断「到底传了多少」。
            try:
                set_upload_stats(dir_path, size_bytes=uploaded_bytes,
                                 file_count=succeeded_count)
            except Exception:  # noqa: BLE001
                logger.exception("set_upload_stats failed for %s", dir_path)
            # 退出码之外再核一次哨兵对象：obsutil 的 -u 增量语义下，退出码 0 只
            # 代表「本次没有失败的任务」，不保证目标前缀里真的落了数据（前缀写错、
            # 对象被跳过都会是 0）。删除本地是不可逆操作，删之前必须确认内容在
            # OBS 上，否则一次静默的路径错误就是永久丢数据。
            ok, problems, checked = _verify_upload_sentinels(Path(real_dir), obs_dst)
            if ok:
                update_sync_status(dir_path, "done", obs_path=obs_dst)
                clear_failed_files(dir_path)
                append_log(dir_path,
                           f"同步完成（哨兵已核对 {checked} 个）" if checked
                           else "同步完成（无哨兵文件，仅依赖退出码与 -vlength）",
                           event="upload_done")
                append_log(dir_path, f"校验通过（哨兵 {checked} 个）", event="verify_ok")
                # 校验通过后才允许删本地 —— 这是唯一会 rmtree 的路径。
                _maybe_auto_delete(dir_path, Path(real_dir), obs_dst,
                                   auto_delete=auto_delete, retain_days=retain_days)
            else:
                msg = "；".join(p["detail"] for p in problems)
                append_log(dir_path, f"哨兵核对失败: {msg}", level="error", event="verify_fail")
                record_failed_files(dir_path, [
                    (p["local"], p["obs"], p["detail"]) for p in problems if p.get("local")
                ])
                update_sync_status(dir_path, "partial",
                                   error_msg=f"哨兵核对失败: {msg}", obs_path=obs_dst)
        else:
            # 负退出码 = 被信号杀死（-15=SIGTERM，-9=SIGKILL）。这通常是我们自己
            # 停机/重启时把正在跑的 obsutil 一起杀了，不是数据或 OBS 的问题。
            # 这类情况退回 queued，让下次启动自动重跑，不要污染失败统计。
            if proc.returncode is not None and proc.returncode < 0:
                sig = -proc.returncode
                append_log(dir_path,
                           f"上传被中断（signal {sig}），已退回队列等待重跑",
                           level="warning", event="upload_interrupted")
                update_sync_status(dir_path, "pending", obs_path="", error_msg="")
                # 不能在这里直接 enqueue_job：当前 job 在 backup_queue 里仍是
                # 'running'，唯一部分索引 (dir_path WHERE state IN ('queued',
                # 'running')) 会拒绝插入。交给调用方在 finish_job 之后再入队。
                return "interrupted"
            # 尝试从 obsutil 失败报告解析出失败文件清单 → 标记 partial（部分失败）
            failed_items = []
            report = find_failed_report(task_id, output_dir) if task_id else None
            if report:
                failed_items = parse_failed_report(report)
            if failed_items:
                record_failed_files(dir_path, failed_items)
                msg = f"部分失败: {len(failed_items)} 个文件上传失败 (exit_code={proc.returncode})"
                append_log(dir_path, f"{msg}，可点击「补同步」重传", level="error",
                           event="upload_fail")
                update_sync_status(dir_path, "partial", error_msg=msg, obs_path=obs_dst)
            else:
                append_log(dir_path, f"上传失败: exit_code={proc.returncode}", level="error",
                           event="upload_fail")
                update_sync_status(dir_path, "error", error_msg=f"exit_code={proc.returncode}", obs_path=obs_dst)
    except Exception as e:
        update_sync_status(dir_path, "error", error_msg=str(e))
        append_log(dir_path, f"同步失败: {e}", level="error", event="upload_fail")
        logger.exception("backup sync failed for %s", dir_path)
    finally:
        _syncing_dirs.discard(dir_path)


def _run_resync_failed(dir_path: str):
    """补同步：逐个重传该目录 obsutil 上报的失败文件。"""
    try:
        items = list_failed_files(dir_path, only_unresolved=True)
        if not items:
            append_log(dir_path, "没有待补传的失败文件")
            # 无遗留失败 → 若之前是 partial，视为已完成
            dir_info = get_dir(dir_path)
            if dir_info and dir_info.get("status") == "partial":
                update_sync_status(dir_path, "done", obs_path=dir_info.get("obs_path", ""))
                append_log(dir_path, "无待补文件，状态修正为 done")
            return

        update_sync_status(dir_path, "syncing")
        append_log(dir_path, f"开始补同步: {len(items)} 个失败文件")
        ok_count = 0
        fail_count = 0
        for it in items:
            local = it["local_path"]
            obs = it["obs_path"]
            success, msg = reupload_file(local, obs)
            if success:
                mark_file_resolved(dir_path, obs)
                ok_count += 1
                append_log(dir_path, f"补传成功: {os.path.basename(local)}")
            else:
                fail_count += 1
                append_log(dir_path, f"补传失败: {os.path.basename(local)} — {msg}", level="error")

        remaining = count_unresolved_failed(dir_path)
        dir_info = get_dir(dir_path)
        obs_path = dir_info.get("obs_path", "") if dir_info else ""
        if remaining == 0:
            update_sync_status(dir_path, "done", obs_path=obs_path)
            clear_failed_files(dir_path)
            append_log(dir_path, f"补同步完成: {ok_count} 个成功，全部已备份")
        else:
            update_sync_status(dir_path, "partial",
                               error_msg=f"仍有 {remaining} 个文件未成功", obs_path=obs_path)
            append_log(dir_path, f"补同步部分完成: {ok_count} 成功 / {fail_count} 失败，剩余 {remaining}",
                       level="error")
    except Exception as e:
        update_sync_status(dir_path, "partial", error_msg=f"补同步异常: {e}")
        append_log(dir_path, f"补同步异常: {e}", level="error")
        logger.exception("resync failed for %s", dir_path)
    finally:
        _syncing_dirs.discard(dir_path)


def _queue_dispatch_loop():
    """单 dispatcher：从 backup_queue 逐个领取手动上传任务，串行执行。"""
    while True:
        job = claim_next_job()
        if not job:
            with _queue_cond:
                _queue_cond.wait(timeout=30)
            continue

        dir_path = job["dir_path"]
        job_type = job.get("job_type", "sync")
        _requeue_after_finish = ""
        _syncing_dirs.add(dir_path)
        try:
            if job_type == "resync":
                _run_resync_failed(dir_path)
            elif job_type == "purge":
                # 人工清理申请：run_purge_job 需要 SERVICE_LOG_DIR
                # （resolve_real_backup_dir 内部用它的 parent.parent 反推 logs_all）
                from utils.log_paths import get_service_log_dir
                run_purge_job(dir_path, get_service_log_dir())
            else:
                acfg = _auto_cfg()
                obs_base = acfg["obs_base"]
                workers = acfg["workers"]
                upload_script = acfg["upload_script"]
                if not obs_base:
                    update_sync_status(dir_path, "error", error_msg="obs_base 未配置")
                    append_log(dir_path, "同步失败: obs_base 未配置", level="error")
                    _syncing_dirs.discard(dir_path)
                else:
                    logs_all = _dispatcher_ctx.get("logs_all")
                    resolver = _dispatcher_ctx.get("resolver")
                    res = _run_sync_for_dir(dir_path, logs_all, obs_base, workers,
                                            upload_script, resolver=resolver,
                                            auto_delete=acfg["auto_delete"],
                                            retain_days=acfg["retain_days"])
                    if res == "interrupted":
                        _requeue_after_finish = dir_path
        except Exception:
            logger.exception("queue dispatch failed for %s", dir_path)
            _syncing_dirs.discard(dir_path)
        finally:
            finish_job(job["id"])
            # 被信号打断的任务：此时队列行已释放，才能重新入队（唯一部分索引
            # 会拒绝与仍在 running 的同名任务并存）。
            if _requeue_after_finish:
                try:
                    if enqueue_job(_requeue_after_finish, "sync"):
                        logger.info("已重新入队被中断的任务: %s", _requeue_after_finish)
                    _requeue_after_finish = ""
                except Exception:  # noqa: BLE001
                    logger.exception("re-enqueue failed: %s", _requeue_after_finish)


def _backup_worker_alive() -> bool:
    """独立 backup_worker 进程是否在跑（读 pid 文件 + os.kill 探活）。

    worker 在跑时 web 侧不再起 dispatcher 线程 —— 两者抢同一个 backup_queue。
    虽然 claim_next_job 已改成跨进程原子领取，不会双领，但 web 线程会和 worker
    并发上传，把带宽/IO 摊薄，也让「谁在执行」变得难以追溯。让 worker 独占。
    """
    try:
        from utils.log_paths import get_service_log_dir
        pid_file = Path(get_service_log_dir()) / "backup_worker.pid"
        if not pid_file.is_file():
            return False
        pid = int(pid_file.read_text(encoding="utf-8").strip() or 0)
        return _is_pid_alive(pid)
    except Exception:  # noqa: BLE001
        return False


def _ensure_dispatcher():
    """懒启动单 dispatcher 线程（幂等）。worker 在跑时让位给 worker。"""
    global _dispatcher_thread
    with _queue_cond:
        if _dispatcher_thread is not None and _dispatcher_thread.is_alive():
            return
        if _backup_worker_alive():
            logger.info("backup_worker 正在运行，web 侧 dispatcher 不启动")
            return
        _dispatcher_thread = threading.Thread(
            target=_queue_dispatch_loop, daemon=True, name="backup-queue-dispatcher"
        )
        _dispatcher_thread.start()


def _notify_dispatcher():
    with _queue_cond:
        _queue_cond.notify()


# ---------------------------------------------------------------------------
# 路径解析（模块级：web 路由与独立 backup_worker 进程共用）
# ---------------------------------------------------------------------------

def _registered_extra_roots(logs_dir) -> list:
    """配置的历史根（排除活跃 logs_all 自身）。

    以 log_dir.db 的 sources 为准（「数据管理」登记清单）；DB 为空时返回 []。
    纯 module 级版本，供 backup_worker 在没有 register 闭包时使用。
    """
    try:
        from utils.logs_config import get_registered_roots
        active_env = os.path.normpath(str(Path(logs_dir).parent))
        roots = get_registered_roots(active_env)
        return [r for r in roots if r and Path(r).is_dir()
                and os.path.normpath(r) != active_env]
    except Exception:  # noqa: BLE001
        return []


def resolve_real_backup_dir(dir_path: str, logs_dir) -> Path:
    """把 dir_path（env_name/mtime_tag[/...]）解析为真实磁盘路径。

    优先按 logs_all/{dir_path}；不存在时在配置的历史根里按 root_id/<rel> 匹配，
    并对存量记录里遗留的旧 basename 前缀保留回退匹配。

    与 register_backup_routes 内的闭包 _resolve_real_dir 同一逻辑 —— 提到模块级
    是为了让 backup_worker 进程也能解析路径（闭包跨进程拿不到）。
    """
    from utils.logs_config import get_root_id
    from utils.log_scan import resolve_leaf

    logs_all = Path(logs_dir).parent.parent
    cand = logs_all / dir_path
    if cand.is_dir():
        return cand
    if "/" not in dir_path:
        return cand
    env_name, rel = dir_path.split("/", 1)
    for root in _registered_extra_roots(logs_dir):
        rid = get_root_id(root, logs_dir)
        if rid == env_name or os.path.basename(os.path.normpath(root)) == env_name:
            real = resolve_leaf(root, rel)
            if real.is_dir():
                return real
    return cand  # 兜底：返回 logs_all 下的路径（可能不存在）


def select_archive_candidates(active_dir_path: str = "", envs=None,
                              older_than: str = "", limit: int = 20,
                              busy: set = None):
    """挑出可归档的目录，按时间**最远→最近**排序，返回 (chosen, matched, skipped)。

    web 的 /api/backup/archive 与 backup_worker 的自动提交共用这一份规则，
    避免两处筛选口径不一致（例如一边跳过了活跃目录、另一边没跳过）。

    envs: 限定来源 root_id 集合；空表示全部。
    older_than: 只取时间键严格早于该值的目录（YYMMDDHH）。
    """
    from utils.log_scan import scan_time_key

    queue_paths = {j.get("dir_path") for j in list_queue()}
    busy = set(busy or ())
    envs = {e for e in (envs or []) if e}

    candidates, skipped = [], []
    for d in list_dirs():
        dp = d.get("dir_path") or ""
        if not dp:
            continue
        if envs and d.get("env_name") not in envs:
            continue
        if active_dir_path and dp == active_dir_path:
            skipped.append({"dir_path": dp, "reason": "活跃目录"})
            continue
        if not d.get("has_local"):
            skipped.append({"dir_path": dp, "reason": "本地不存在"})
            continue
        st = d.get("status") or "pending"
        if st in ("done", "pending_delete", "backed_up", "live_syncing"):
            skipped.append({"dir_path": dp, "reason": f"状态 {st}"})
            continue
        if dp in queue_paths:
            skipped.append({"dir_path": dp, "reason": "已在队列"})
            continue
        if dp in busy:
            skipped.append({"dir_path": dp, "reason": "正在上传"})
            continue
        t = scan_time_key(d.get("mtime_tag") or dp)
        if older_than and t:
            if t >= older_than:
                continue
        elif older_than and not t:
            # 时间键解析不出来就不参与「早于某时间」的筛选，避免误归档
            skipped.append({"dir_path": dp, "reason": "无时间键"})
            continue
        candidates.append({"dir_path": dp, "time": t,
                           "file_count": int(d.get("file_count") or 0),
                           "status": st})

    # 最远 → 最近：入队顺序即执行顺序（claim_next_job 是 ORDER BY id ASC）
    candidates.sort(key=lambda c: (c["time"] or "99999999", c["dir_path"]))
    return candidates[:max(1, int(limit))], len(candidates), skipped


def _leaf_too_new(mtime_tag: str, retain_days: int) -> bool:
    """叶子时间是否「太新」到不允许自动删除。

    时间基准取目录名里的 YYMMDDHH（scan_time_key），不用文件 mtime ——
    归档/dedup 会改 mtime，而目录名是数据产生时间，稳定可靠。
    解析不出时间键时返回 True（保守：不删）。
    """
    from datetime import datetime, timedelta
    from utils.log_scan import scan_time_key

    key = scan_time_key(mtime_tag)
    if not key:
        return True
    try:
        ts = datetime.strptime(key, "%y%m%d%H")
    except ValueError:
        return True
    return ts > (datetime.now() - timedelta(days=max(0, int(retain_days))))


# ---------------------------------------------------------------------------
# 自动备份：磁盘达阈值时自动上传未备份目录，成功后转 pending_delete（等人工确认删除）
# ---------------------------------------------------------------------------

def _auto_cfg() -> dict:
    """从 sync_config(settings/obs_rl.yaml) 读自动备份配置。"""
    cfg = load_sync_config()
    try:
        retain_days = int(cfg.get("backup_retain_days", 7))
    except (TypeError, ValueError):
        retain_days = 7
    return {
        "enabled": bool(cfg.get("auto_backup_enabled", False)),
        "threshold": float(cfg.get("auto_backup_disk_percent", 70) or 70),
        "interval": max(30, int(cfg.get("auto_backup_check_interval", 120) or 120)),
        "stop_percent": float(cfg.get("auto_backup_stop_percent", 0) or 0),
        "workers": int(cfg.get("workers", 4) or 4),
        "upload_script": cfg.get("upload_script", "") or "",
        "obs_base": cfg.get("obs_base", "") or load_obs_base(),
        # 校验通过后是否自动删本地（默认开）。关掉时退回 pending_delete 人工闸门。
        "auto_delete": bool(cfg.get("backup_auto_delete", True)),
        # 最小文件保留天数：叶子时间晚于 N 天前的，即使策略开启也不自动删。
        "retain_days": max(0, retain_days),
        # 自动提交归档任务：开启后由 backup_worker 自动按「最远→最近」入队，
        # 不再需要人工在页面上点「备份选中节点」。
        "auto_submit": bool(cfg.get("backup_auto_submit", False)),
        # 每轮最多自动入队多少个叶子（避免一次把几千个任务塞进队列）
        "submit_batch": max(1, int(cfg.get("backup_submit_batch", 20) or 20)),
        # 只自动提交「早于 N 天」的叶子：避免自动任务动到近期数据。
        # 与 retain_days（删本地的保留期）是两件事，但默认取同一个值更直观。
        "submit_min_age_days": max(0, int(cfg.get("backup_submit_min_age_days",
                                                  retain_days) or 0)),
        # 每轮最多提交多少个「到期清理」（每个都要跑一次 obsutil ls -du 校核）
        "clear_batch": max(1, int(cfg.get("backup_clear_batch", 3) or 3)),
    }


def _submit_age_cutoff(min_age_days: int) -> str:
    """把「早于 N 天」换算成 YYMMDDHH 时间键上界（严格小于它才可自动提交）。

    时间键与叶子目录名同构（YYMMDDHH），可以直接做字符串比较；
    返回空串表示不限制。
    """
    if min_age_days <= 0:
        return ""
    from datetime import datetime, timedelta
    return (datetime.now() - timedelta(days=min_age_days)).strftime("%y%m%d%H")


def _run_auto_backup_round(logs_all: Path, active_env: str, active_path: str,
                           resolver, forced: bool = False) -> None:
    """跑一轮自动上传：把 active_env 下未备份目录最旧优先逐个上传，成功转 pending_delete。

    forced=True 时无视阈值直接跑（手动触发/立即备份）。
    """
    from utils.backup_store import list_backup_candidates, count_pending_delete

    cfg = _auto_cfg()
    if not cfg["obs_base"]:
        _auto_set(running=False, message="未配置 OBS(obs_base)，无法自动备份")
        return

    candidates = list_backup_candidates(active_env)
    queue = [d["dir_path"] for d in candidates]
    _auto_set(running=True, queue=list(queue), current="", uploaded=[],
              message=("手动触发，" if forced else "") + f"待上传 {len(queue)} 个目录")

    if not queue:
        _auto_set(running=False, current="", message="没有待备份的目录")
        return

    uploaded: List[str] = []
    for dir_path in queue:
        # 与手动同步互斥
        with _sync_lock:
            if dir_path in _syncing_dirs:
                continue
            _syncing_dirs.add(dir_path)
        _auto_set(current=dir_path,
                  queue=[d for d in queue if d not in uploaded and d != dir_path])
        try:
            _run_sync_for_dir(dir_path, logs_all, cfg["obs_base"], cfg["workers"],
                              cfg["upload_script"], resolver=resolver,
                              auto_delete=cfg["auto_delete"],
                              retain_days=cfg["retain_days"])
        except Exception as e:  # noqa: BLE001
            logger.exception("auto backup upload failed: %s", dir_path)
            append_log(dir_path, f"自动备份上传异常: {e}", level="error")
            continue

        # 上传结果以 DB 状态为准。**不要在这里强行转 pending_delete**：
        # _run_sync_for_dir 末尾的 _maybe_auto_delete 已经按保留期做过判断
        # （到期→提交 clear 转 pending_delete；未到期→保持 done 显示「已备份」）。
        # 早先在这里无条件转 pending_delete，会把还在保留期的目录也推进删除流程，
        # 与「时间到了再删」的语义冲突。
        info = get_dir(dir_path)
        if info and info.get("status") in ("done", "pending_delete") and info.get("obs_path"):
            uploaded.append(dir_path)
            _auto_set(uploaded=list(uploaded),
                      pending_delete_count=count_pending_delete(active_env))
        else:
            append_log(dir_path, "自动备份未成功（保留状态，需人工处理）", level="error")

        # 每传完一个重查磁盘：若配置了 stop_percent 且已降到其下则提前结束本轮
        disk = _disk_usage(Path(active_path))
        pct = disk.get("percent")
        _auto_set(disk_percent=pct)
        if not forced and cfg["stop_percent"] and pct is not None and pct <= cfg["stop_percent"]:
            _auto_set(message=f"磁盘已降至 {pct}% ≤ 停止阈值 {cfg['stop_percent']}%，本轮结束")
            break

    _auto_set(running=False, current="", queue=[],
              pending_delete_count=count_pending_delete(active_env),
              message=f"本轮完成：成功上传 {len(uploaded)} 个，待确认删除")


# ---------------------------------------------------------------------------
# 串行流水线：整个叶子列表按时间从远到近，一次只推进一个
# ---------------------------------------------------------------------------

# 校核/上传失败的叶子在这么久内不再重试，避免一个永远失败的目录卡死整条流水线。
VERIFY_RETRY_BACKOFF_SEC = 3600


def _recent_event(dir_path: str, events, within_sec: int) -> bool:
    """该目录最近是否发生过指定事件之一（用于流水线失败退避）。"""
    from utils.backup_store import _conn, _lock
    from datetime import datetime as _dt, timedelta as _td
    events = tuple(events)
    if not events:
        return False
    since = (_dt.now() - _td(seconds=within_sec)).strftime("%Y-%m-%d %H:%M:%S")
    marks = ",".join("?" * len(events))
    try:
        with _lock:
            row = _conn.execute(
                f"SELECT 1 FROM backup_logs WHERE dir_path = ? "
                f"AND event IN ({marks}) AND created_at >= ? LIMIT 1",
                (dir_path, *events, since)).fetchone()
        return row is not None
    except Exception:  # noqa: BLE001
        return False


def _recent_verify_fail(dir_path: str, within_sec: int = VERIFY_RETRY_BACKOFF_SEC) -> bool:
    return _recent_event(dir_path, ("verify_fail",), within_sec)


def select_pipeline_next(auto_delete: bool, retain_days: int,
                         submit_min_age_days: int = 0):
    """串行流水线的**唯一**决策点：返回下一个要处理的叶子，或 None。

    规则（严格按叶子时间从远到近，最旧的先处理）：
      - `pending` / `error` / `partial` → 动作 `backup`（上传 → 简单校核）；
      - `done` 且已过保留期           → 动作 `clear`（OBS 体积校核 → 提交删除）；
      - `done` 但仍在保留期           → 跳过（等时间到，不删）；
      - `pending_delete` / `backed_up` → 跳过（已提交/已删，等后续）；
      - `syncing` / `live_syncing`     → 跳过（正在被别的流程占用）。

    返回 {"action": "backup"|"clear", "dir_path", "time", "obs_path", "reason"}。
    """
    from utils.log_paths import get_service_log_dir
    from utils.log_scan import scan_time_key
    from datetime import datetime as _dt, timedelta as _td

    svc_dir = get_service_log_dir()
    clear_cutoff = _dt.now() - _td(days=max(0, int(retain_days)))
    submit_cutoff = _submit_age_cutoff(submit_min_age_days)

    queued = {j.get("dir_path") for j in list_queue()}
    clearing = {j.get("dir_path") for j in list_clear_queue()}

    rows = []
    for d in list_dirs():
        key = scan_time_key(d.get("mtime_tag") or d.get("dir_path") or "")
        if not key:
            continue
        rows.append((key, d))
    rows.sort(key=lambda x: (x[0], x[1].get("dir_path") or ""))

    for key, d in rows:
        dp = d.get("dir_path") or ""
        st = (d.get("status") or "pending")
        if not d.get("has_local"):
            continue
        if st in ("pending_delete", "backed_up", "syncing", "live_syncing"):
            continue
        if dp in queued or dp in clearing:
            continue

        if st == "done":
            if not auto_delete or not d.get("obs_path"):
                continue
            try:
                ts = _dt.strptime(key, "%y%m%d%H")
            except ValueError:
                continue
            if ts >= clear_cutoff:
                continue                       # 还在保留期，等时间到
            if _recent_verify_fail(dp):
                continue                       # 刚校核失败过，先放一边
            return {"action": "clear", "dir_path": dp, "time": key,
                    "obs_path": d["obs_path"], "reason": "保留期已到"}

        # pending / error / partial / 其它未知状态 → 视为待备份
        if submit_cutoff and key >= submit_cutoff:
            continue                           # 太新，不自动动
        if _recent_event(dp, ("upload_fail",), VERIFY_RETRY_BACKOFF_SEC):
            continue                           # 刚上传失败过，先放一边别反复撞墙
        return {"action": "backup", "dir_path": dp, "time": key,
                "obs_path": d.get("obs_path") or "", "reason": f"状态 {st}"}

    return None


def run_pipeline_step(auto_delete: bool, retain_days: int,
                      submit_min_age_days: int = 0, auto_submit: bool = True):
    """推进串行流水线的一步。

    返回 (action, dir_path) 或 (None, "")。真正的上传/删除由调用方执行：
      - action == "backup" → 入队 sync 任务，由 run_once 执行上传；
      - action == "clear"  → 这里就地完成校核并提交 clear。

    auto_submit=False 时只推进 clear 分支，不再自动入队新的上传任务
    （对应页面上「自动提交归档任务」开关）；已经排进队列的活不受影响。
    """
    from utils.log_paths import get_service_log_dir

    step = select_pipeline_next(auto_delete, retain_days, submit_min_age_days)
    if not step:
        return None, ""
    dp = step["dir_path"]
    if step["action"] == "clear":
        real = resolve_real_backup_dir(dp, get_service_log_dir())
        ok = _submit_clear_verified(dp, real, step["obs_path"])
        return ("clear" if ok else None), dp
    if not auto_submit:
        return None, ""
    # backup：只入队一个，保证队列里恒为 0/1，严格串行
    enqueued = enqueue_job(dp, "sync")
    return ("backup" if enqueued else None), dp


def _auto_backup_loop(logs_all: Path, active_env: str, active_path: str, resolver) -> None:
    """后台守护：定时查 df，达阈值则跑一轮自动上传。仅 leader worker 启动。"""
    from utils.backup_store import count_pending_delete

    logger.info("auto-backup daemon started (env=%s, path=%s)", active_env, active_path)
    while True:
        try:
            cfg = _auto_cfg()
            disk = _disk_usage(Path(active_path))
            pct = disk.get("percent")
            _auto_set(enabled=cfg["enabled"], threshold=cfg["threshold"],
                      disk_percent=pct, last_check_ts=time.time(),
                      pending_delete_count=count_pending_delete(active_env))

            if not cfg["enabled"]:
                _auto_set(message="自动备份未开启")
            elif pct is None:
                _auto_set(message="无法读取磁盘使用率")
            elif pct < cfg["threshold"]:
                _auto_set(message=f"磁盘 {pct}% < 阈值 {cfg['threshold']}%，无需自动备份")
            else:
                _auto_set(message=f"磁盘 {pct}% ≥ 阈值 {cfg['threshold']}%，开始自动上传")
                _run_auto_backup_round(logs_all, active_env, active_path, resolver)

            # 注意：到期清理（流水线的 clear 分支）**不在 web 进程里跑**。
            # 所有删除申请必须由 backup_worker 发出，web 只负责展示与上传入队。

            time.sleep(cfg["interval"])
        except Exception:  # noqa: BLE001 — 守护线程不能崩
            logger.exception("auto-backup loop error")
            time.sleep(60)


def register_backup_routes(app: FastAPI, logs_dir: str, port: str = "") -> None:
    logs_all = Path(logs_dir).parent.parent
    templates = Jinja2Templates(directory="templates")
    parts = Path(logs_dir).parts
    _active_dir_path = f"{parts[-2]}/{parts[-1]}" if len(parts) >= 2 else ""
    _active_env_name = parts[-2] if len(parts) >= 2 else ""
    _project_root = str(Path(__file__).resolve().parent.parent)

    _port_env_map: dict = {}
    logs_root = Path("logs")
    if logs_root.is_dir():
        for port_dir in logs_root.iterdir():
            if port_dir.is_dir() and port_dir.name.startswith("port"):
                p = port_dir.name[4:]
                for env_d in port_dir.iterdir():
                    if env_d.is_dir():
                        _port_env_map[env_d.name] = p
    if _active_env_name and port:
        _port_env_map[_active_env_name] = port

    # --- 多根支持：把配置的历史路径（含 new-api）也纳入备份 ---
    # env_name = 根目录的稳定 root_id（消除同 basename 冲突，如两个 new-api details 根）。
    # 对历史根，其 root_id 作为一个「env」，其 天/小时 叶子作为 mtime_tag。
    def _extra_roots() -> list:
        """配置的历史路径（排除活跃 logs_all 自身，那条已由 logs_all 扫描覆盖）。

        以 log_dir.db 的 sources 为准（「数据管理」登记清单）；DB 为空时返回 []。
        """
        try:
            from utils.logs_config import get_registered_roots
            active_env = os.path.normpath(str(Path(logs_dir).parent))
            roots = get_registered_roots(active_env)
            return [r for r in roots if r and Path(r).is_dir()
                    and os.path.normpath(r) != active_env]
        except Exception:
            return []

    def _root_env_name(root: str) -> str:
        from utils.logs_config import get_root_id
        return get_root_id(root, logs_dir)

    def _root_basename(root: str) -> str:
        """旧标识（basename），仅用于兼容存量 dir_path 的回退匹配。"""
        return os.path.basename(os.path.normpath(root))

    def _extra_root_for_env(env_name: str):
        for root in _extra_roots():
            if _root_env_name(root) == env_name or _root_basename(root) == env_name:
                return root
        return None

    def _resolve_real_dir(dir_path: str) -> Path:
        """把 dir_path（env_name/mtime_tag[/...]）解析为真实磁盘路径。

        委托模块级 resolve_real_backup_dir —— backup_worker 进程用的是同一个
        实现，两边解析结果必须一致，否则 worker 删的目录和 web 展示的不是一个。
        """
        return resolve_real_backup_dir(dir_path, logs_dir)

    def _mark_leaves_archived(dir_path: str, archived: bool = True) -> int:
        """把某个备份目录对应的 leaf_status 行打上/取消归档标记，返回命中行数。

        这是备份 ↔ session worker 之间的通知渠道：worker 每轮读同一个 WAL 库的
        stale_stats_leaves()（已按 archived=0 过滤），所以这里写完标记后，worker
        下一轮（≤15s）自然就不再重试这个问题叶子，不需要任何进程间消息。

        dir_path（env_name/mtime_tag）与 leaf_status 的 (root_id, dir_key) 不是
        一一对应：logs_all 的 env_name 是目录名，而 leaf_status 的 root_id 是路径
        md5 或 'default'。所以主键用**绝对路径**归一化匹配；leaf_path 为空的存量行
        再按 (root_id, dir_key) 兜底。
        """
        try:
            import utils.logdir_store as _lds
            if not _lds._ready():
                return 0
        except Exception:  # noqa: BLE001
            return 0

        n = 0
        try:
            real = _resolve_real_dir(dir_path)
            n += _lds.mark_archived([str(real)], archived=archived)
        except Exception:  # noqa: BLE001
            logger.exception("mark_archived by path failed: %s", dir_path)

        if n == 0 and "/" in dir_path:
            try:
                env_name, rel = dir_path.split("/", 1)
                root = _extra_root_for_env(env_name)
                rid = _root_env_name(root) if root else env_name
                n += _lds.mark_archived_by_root(rid, [rel], archived=archived)
            except Exception:  # noqa: BLE001
                logger.exception("mark_archived by root failed: %s", dir_path)
        return n

    # 一次性幂等迁移：存量记录旧 env 前缀（basename）-> root_id
    try:
        from utils.backup_store import migrate_env_prefix
        _mig = {}
        for root in _extra_roots():
            old = _root_basename(root)
            new = _root_env_name(root)
            if old != new:
                _mig[old] = new
        migrate_env_prefix(_mig)
    except Exception:
        logger.exception("backup env-prefix migration failed")

    # 启动手动上传串行队列 dispatcher：登记上下文 + 恢复中断项 + 起线程
    _dispatcher_ctx["logs_all"] = logs_all
    _dispatcher_ctx["resolver"] = _resolve_real_dir
    # 只在没有 backup_worker 运行时才重置队列：worker 存活时任务由它持有，
    # web 重启（很常见）不该把它正在跑的大叶子打回 queued —— 那会让队列显示
    # 「排队中」而 worker 仍在传，看起来像任务卡死。
    if not _backup_worker_alive():
        try:
            reset_running_on_startup()
        except Exception:
            logger.exception("reset backup_queue running-on-startup failed")
    _ensure_dispatcher()

    def _ctx(request: Request) -> dict:
        return {
            "active_page": "backup",
            "user_role": request.session.get("monitor_role", "user"),
            "user_name": request.session.get("monitor_user", ""),
            "user_permissions": [
                p.strip()
                for p in (request.session.get("monitor_permissions") or "").split(",")
                if p.strip()
            ],
        }

    @app.get("/backup")
    def backup_page(request: Request):
        return templates.TemplateResponse(request, "backup.html", context=_ctx(request))

    @app.get("/api/backup/dirs")
    def backup_list_dirs(request: Request):
        denied = _require_ajax(request)
        if denied:
            return denied

        env_name = request.query_params.get("env_name", "")
        # 分页（可选）：不传则保持原行为（返回全量），避免影响既有调用方
        # （/api/backup/archive、自动备份轮等都直接消费全量）。
        page_raw = request.query_params.get("page", "")
        size_raw = request.query_params.get("page_size", "")
        status_f = request.query_params.get("status", "")
        order_raw = (request.query_params.get("order", "") or "").lower()
        paged = bool(page_raw or size_raw)
        _newapi_root = _extra_root_for_env(env_name) if env_name else None

        # 用 leaf_status 幂等补齐该来源缺失的叶子行。
        #
        # 不能在这里扫盘：旧实现对历史根调 _scan_root_leaves → iter_index_dirs，
        # 那是 NFS 上的递归 walk（proxy45 这类源实测 >45s 不返回），前端把
        # 「选中来源」变成一次超时，表现为「选了没反应」。
        #
        # 原来用 has_any_dirs() 做闸门（有 1 行就跳过），导致备份队列只有零星
        # 几行的来源（历史遗留 / 扫描被超时打断）永远补不全 —— 例如某源
        # leaf_status 有 1427 个叶子、backup_dirs 只有 32 行。改成每次都补齐：
        # _seed 内部先取已有 dir_key 集合，只插缺失的，纯本地 SQLite，毫秒级。
        if env_name:
            _seed_dirs_from_leaf_status(env_name)

        db_dirs = list_dirs(env_name=env_name if env_name else None)

        # root_id -> 展示名（配置的历史根用其 name；logs_all 下 env 目录名即展示名）
        from utils.logs_config import get_path_name
        _label_map = {}
        for root in _extra_roots():
            _label_map[_root_env_name(root)] = get_path_name(root)

        # 从 token_index 按 env 批量加载统计数据（内存缓存，无额外 IO）
        # new-api env 的 token_index 以真实根为 key（mtime_tag 为 天/小时 相对路径）。
        _tok_cache: dict = {}
        def _get_tok_stats(env_n: str, mtime_t: str) -> dict:
            if env_n not in _tok_cache:
                try:
                    from utils.token_index import refresh_token_index
                    _root = _extra_root_for_env(env_n) or str(logs_all / env_n)
                    idx = refresh_token_index(str(_root))
                    _tok_cache[env_n] = idx.get("dirs", {})
                except Exception:
                    _tok_cache[env_n] = {}
            di = _tok_cache[env_n].get(mtime_t, {})
            return {
                "req_total": di.get("entry_count", 0),
                "req_success": sum(v.get("s_count", 0) for v in di.get("models", {}).values()),
                "req_error": sum(v.get("e_count", 0) for v in di.get("models", {}).values()),
                "tok_in": sum(v.get("s_tok_in", 0) + v.get("e_tok_in", 0) for v in di.get("models", {}).values()),
                "tok_out": sum(v.get("s_tok_out", 0) for v in di.get("models", {}).values()),
            }

        merged = []
        for d in db_dirs:
            stats = _get_tok_stats(d["env_name"], d["mtime_tag"])
            entry = {
                "dir_path": d["dir_path"],
                "env_name": d["env_name"],
                "env_label": _label_map.get(d["env_name"], d["env_name"]),
                "mtime_tag": d["mtime_tag"],
                "file_count": d["file_count"],
                # 上传时 obsutil 报告的真实字节数；-1 表示尚未上传
                "size_bytes": int(d.get("size_bytes", -1) or -1),
                "synced": d.get("synced", 0),
                "status": d.get("status", "pending"),
                "sync_time": d.get("sync_time", ""),
                "obs_path": d.get("obs_path", ""),
                "error_msg": d.get("error_msg", ""),
                "sync_pid": d.get("sync_pid"),
                "is_active": d["dir_path"] == _active_dir_path,
                "has_local": bool(d.get("has_local", 1)),
                "has_obs": bool(d.get("has_obs", 0)),
                "port": d.get("port", ""),
                "failed_count": count_unresolved_failed(d["dir_path"]),
                **stats,
            }
            merged.append(entry)

        if not paged:
            return JSONResponse({"dirs": merged})

        # ── 分页模式（备份管理 Tab 的队列表格用）──────────────────────────
        # 排序键用 mtime_tag（YYMMDDHH 定长，字典序即时间序）；同一时间可能
        # 跨多个来源，再按 dir_path 兜底保证跨页顺序稳定、不重不漏。
        if status_f:
            merged = [e for e in merged if e.get("status") == status_f]
        desc = order_raw == "desc"
        merged.sort(key=lambda e: (e.get("mtime_tag") or "", e.get("dir_path") or ""),
                    reverse=desc)
        total = len(merged)
        try:
            size = max(1, min(int(size_raw or 15), 200))
        except (TypeError, ValueError):
            size = 15
        try:
            page = max(1, int(page_raw or 1))
        except (TypeError, ValueError):
            page = 1
        pages = max(1, (total + size - 1) // size)
        page = min(page, pages)
        start = (page - 1) * size
        return JSONResponse({
            "dirs": merged[start:start + size],
            "total": total, "page": page, "page_size": size, "pages": pages,
        })

    @app.post("/api/backup/sync")
    async def backup_sync(request: Request):
        denied = _require_ajax(request)
        if denied:
            return denied

        body = await request.json()
        dirs = body.get("dirs", [])
        if not dirs:
            return JSONResponse({"detail": "没有选择目录"}, status_code=400)

        enqueued = []
        already = []
        for d in dirs:
            if enqueue_job(d, "sync"):
                enqueued.append(d)
            else:
                already.append(d)

        if not enqueued:
            return JSONResponse({"detail": "所选目录已在队列中", "already": already},
                                status_code=409)

        _notify_dispatcher()
        return JSONResponse({"enqueued": enqueued, "already": already})

    @app.post("/api/backup/resync-failed")
    async def backup_resync_failed(request: Request):
        denied = _require_ajax(request)
        if denied:
            return denied

        body = await request.json()
        dir_path = body.get("dir_path", "")
        if not dir_path:
            return JSONResponse({"detail": "缺少 dir_path"}, status_code=400)

        dir_info = get_dir(dir_path)
        if not dir_info:
            return JSONResponse({"detail": "目录不存在"}, status_code=404)

        failed = list_failed_files(dir_path, only_unresolved=True)
        if not failed:
            return JSONResponse({"detail": "没有待补传的失败文件，请重新同步"}, status_code=400)

        if not enqueue_job(dir_path, "resync"):
            return JSONResponse({"detail": "该目录已在队列中", "already": dir_path},
                                status_code=409)

        _notify_dispatcher()
        return JSONResponse({"enqueued": dir_path, "total": len(failed)})

    @app.get("/api/backup/queue-status")
    def backup_queue_status(request: Request):
        denied = _require_ajax(request)
        if denied:
            return denied
        jobs = list_queue()
        running = ""
        queued = []
        for j in jobs:
            if j.get("state") == "running":
                running = j["dir_path"]
            else:
                queued.append(j["dir_path"])

        # 自动提交模式下队列常常是「刚被领走 → 空 → 立刻补下一个」，
        # 前端只看 running/queue_len 会觉得「开了自动备份却什么都没发生」。
        # 这里带上开关状态与剩余候选数，让界面能解释「现在处于什么阶段」。
        cfg = _auto_cfg()
        candidates = None
        if cfg["auto_submit"]:
            try:
                _chosen, matched, _skipped = select_archive_candidates(
                    _active_dir_path, None,
                    _submit_age_cutoff(cfg["submit_min_age_days"]),
                    1, set())
                candidates = matched
            except Exception:  # noqa: BLE001
                candidates = None

        # 本地清理队列（clear_worker 消费）：与上传队列分开统计，
        # 界面上要能分别看到「正在上传」和「正在清理」。
        try:
            from utils.backup_store import clear_queue_stats
            clear_stats = clear_queue_stats()
        except Exception:  # noqa: BLE001
            clear_stats = {"queued": 0, "running": "", "failed": 0}

        return JSONResponse({
            "running": running,
            "queued": queued,
            "queue_len": len(queued),
            "auto_submit": cfg["auto_submit"],
            "candidates": candidates,
            "clear": clear_stats,
            "jobs": [
                {
                    "dir_path": j["dir_path"],
                    "job_type": j.get("job_type", "sync"),
                    "state": j.get("state", "queued"),
                    "enqueued_at": j.get("enqueued_at", ""),
                }
                for j in jobs
            ],
        })

    @app.get("/api/backup/config")
    def backup_config_get(request: Request):
        denied = _require_ajax(request)
        if denied:
            return denied
        cfg = load_sync_config()
        return JSONResponse({
            "obs_base": cfg.get("obs_base", "") or load_obs_base(),
            "workers": cfg.get("workers", 4),
            "interval": cfg.get("interval", 600),
            "upload_script": cfg.get("upload_script", ""),
            "upload_timeout": cfg.get("upload_timeout", 3600),
            "upload_jobs": cfg.get("upload_jobs", 8),
        })

    @app.put("/api/backup/config")
    async def backup_config_put(request: Request):
        denied = _require_ajax(request)
        if denied:
            return denied

        body = await request.json()
        cfg_path = get_sync_config_path()
        if not cfg_path:
            return JSONResponse({"detail": "sync_config 未配置"}, status_code=400)

        import yaml
        try:
            if cfg_path.is_file():
                with open(cfg_path, "r", encoding="utf-8") as f:
                    existing = yaml.safe_load(f) or {}
            else:
                existing = {}

            for key in ("obs_base", "workers", "interval", "upload_script", "upload_timeout", "upload_jobs"):
                if key in body:
                    existing[key] = body[key]

            with open(cfg_path, "w", encoding="utf-8") as f:
                yaml.dump(existing, f, default_flow_style=False, allow_unicode=True)

            return JSONResponse({"ok": True})
        except Exception as e:
            return JSONResponse({"detail": str(e)}, status_code=500)

    @app.put("/api/backup/strategy")
    async def backup_strategy_put(request: Request):
        """保存自动备份策略（sync_config 的 auto_backup_* 键）。

        与 /api/backup/config 分开：那边管 obs_base / workers 这类上传参数，
        这边管「开不开自动备份、磁盘阈值、检查周期」。两边都是先 safe_load
        再改键后整体写回，所以互不覆盖对方的键。
        """
        denied = _require_ajax(request)
        if denied:
            return denied

        body = await request.json()
        cfg_path = get_sync_config_path()
        if not cfg_path:
            return JSONResponse({"detail": "sync_config 未配置"}, status_code=400)

        import yaml
        try:
            if cfg_path.is_file():
                with open(cfg_path, "r", encoding="utf-8") as f:
                    existing = yaml.safe_load(f) or {}
            else:
                existing = {}

            if "enabled" in body:
                existing["auto_backup_enabled"] = bool(body["enabled"])
            if "threshold" in body:
                try:
                    existing["auto_backup_disk_percent"] = float(body["threshold"])
                except (TypeError, ValueError):
                    return JSONResponse({"detail": "阈值必须是数字"}, status_code=400)
            if "interval" in body:
                try:
                    existing["auto_backup_check_interval"] = max(30, int(body["interval"]))
                except (TypeError, ValueError):
                    return JSONResponse({"detail": "检查周期必须是整数秒"}, status_code=400)
            # 校验通过后是否自动删本地
            if "auto_delete" in body:
                existing["backup_auto_delete"] = bool(body["auto_delete"])
            # 最小文件保留天数：叶子时间早于 N 天前的才允许自动删
            if "retain_days" in body:
                try:
                    existing["backup_retain_days"] = max(0, int(body["retain_days"]))
                except (TypeError, ValueError):
                    return JSONResponse({"detail": "保留天数必须是整数"}, status_code=400)
            # 自动提交归档任务（最远→最近）+ 每轮批量
            if "auto_submit" in body:
                existing["backup_auto_submit"] = bool(body["auto_submit"])
            if "submit_batch" in body:
                try:
                    existing["backup_submit_batch"] = max(1, int(body["submit_batch"]))
                except (TypeError, ValueError):
                    return JSONResponse({"detail": "每轮批量必须是整数"}, status_code=400)
            # 只自动提交早于 N 天的叶子（避免自动任务动到近期数据）
            if "submit_min_age_days" in body:
                try:
                    existing["backup_submit_min_age_days"] = max(
                        0, int(body["submit_min_age_days"]))
                except (TypeError, ValueError):
                    return JSONResponse({"detail": "自动提交最小天数必须是整数"}, status_code=400)

            with open(cfg_path, "w", encoding="utf-8") as f:
                yaml.dump(existing, f, default_flow_style=False, allow_unicode=True)

            # 守护线程每轮重读配置，这里不需要重启；返回生效值便于前端确认
            cfg = _auto_cfg()
            return JSONResponse({
                "ok": True,
                "enabled": cfg["enabled"],
                "threshold": cfg["threshold"],
                "interval": cfg["interval"],
                "auto_delete": cfg["auto_delete"],
                "retain_days": cfg["retain_days"],
                "auto_submit": cfg["auto_submit"],
                "submit_batch": cfg["submit_batch"],
            })
        except Exception as e:
            return JSONResponse({"detail": str(e)}, status_code=500)

    @app.get("/api/backup/logs/{dir_path:path}")
    def backup_logs(request: Request, dir_path: str):
        denied = _require_ajax(request)
        if denied:
            return denied
        logs = get_logs(dir_path)
        dir_info = get_dir(dir_path)
        return JSONResponse({
            "logs": logs,
            "status": dir_info.get("status", "pending") if dir_info else "pending",
        })

    @app.delete("/api/backup/dirs/{dir_path:path}")
    def backup_delete_dir(request: Request, dir_path: str):
        denied = _require_ajax(request)
        if denied:
            return denied

        dir_info = get_dir(dir_path)
        if not dir_info:
            return JSONResponse({"detail": "目录不存在"}, status_code=404)

        if dir_info.get("status") != "done":
            return JSONResponse({"detail": "只能删除已同步完成的目录"}, status_code=400)

        target = _resolve_real_dir(dir_path)
        if target.is_dir():
            try:
                shutil.rmtree(str(target))
                append_log(dir_path, f"本地目录已删除: {target}")
            except Exception as e:
                append_log(dir_path, f"删除失败: {e}", level="error")
                return JSONResponse({"detail": f"删除失败: {e}"}, status_code=500)

        mark_backed_up(dir_path)
        _mark_leaves_archived(dir_path, True)
        return JSONResponse({"ok": True})

    @app.get("/api/backup/obs-browse/{obs_path:path}")
    def backup_obs_browse(request: Request, obs_path: str):
        denied = _require_ajax(request)
        if denied:
            return denied
        if not obs_path.startswith("obs://"):
            obs_path = "obs://" + obs_path
        try:
            items = obsutil_ls(obs_path)
            return JSONResponse({"path": obs_path, "items": items})
        except Exception as e:
            return JSONResponse({"detail": str(e)}, status_code=500)

    @app.get("/api/backup/active-dir")
    def backup_active_dir(request: Request):
        denied = _require_ajax(request)
        if denied:
            return denied
        return JSONResponse({
            "active_dir": _active_dir_path,
            "current_env": _active_env_name,
            "port": port,
        })

    @app.get("/api/backup/env-list")
    def backup_env_list(request: Request):
        denied = _require_ajax(request)
        if denied:
            return denied
        from utils.logs_config import get_path_name
        env_set = set(list_env_names())
        if logs_all.is_dir():
            for d in logs_all.iterdir():
                if d.is_dir() and not d.name.startswith("logs_") and not d.name.startswith("."):
                    env_set.add(d.name)
        # new-api 等配置的历史根：root_id 作为 env 标识（筛选键），配置名作展示 label
        newapi_envs = set()
        label_map = {}
        for root in _extra_roots():
            en = _root_env_name(root)
            env_set.add(en)
            newapi_envs.add(en)
            label_map[en] = get_path_name(root)
        envs = []
        for name in sorted(env_set):
            envs.append({
                "name": name,
                "label": label_map.get(name, name),
                "port": _port_env_map.get(name, ""),
                "newapi": name in newapi_envs,
            })
        return JSONResponse({"envs": envs, "current_env": _active_env_name})

    @app.post("/api/backup/scan-obs")
    async def backup_scan_obs(request: Request):
        denied = _require_ajax(request)
        if denied:
            return denied
        body = await request.json()
        env_name = body.get("env_name", "")
        cfg = load_sync_config()
        obs_base = cfg.get("obs_base", "") or load_obs_base()
        if not obs_base:
            return JSONResponse({"detail": "obs_base 未配置"}, status_code=400)
        obs_dirs = _scan_obs_dirs(obs_base, env_name=env_name if env_name else None)
        for d in obs_dirs:
            upsert_dir(d["dir_path"], d["env_name"], d["mtime_tag"],
                       port=_port_env_map.get(d["env_name"], ""), has_obs=True)
            existing = get_dir(d["dir_path"])
            if existing and existing.get("status") == "pending":
                update_sync_status(d["dir_path"], "backed_up", obs_path=d["obs_path"])
            elif existing and not existing.get("obs_path"):
                update_sync_status(d["dir_path"], existing["status"], obs_path=d["obs_path"])
        return JSONResponse({"found": len(obs_dirs)})

    @app.post("/api/backup/live-sync/start")
    async def backup_live_sync_start(request: Request):
        denied = _require_ajax(request)
        if denied:
            return denied

        body = await request.json()
        dir_path = body.get("dir_path", "")

        if dir_path != _active_dir_path:
            return JSONResponse({"detail": "只能对活跃目录开启在线同步"}, status_code=400)

        if dir_path in _live_sync_procs:
            proc = _live_sync_procs[dir_path]["proc"]
            if proc.poll() is None:
                return JSONResponse({"detail": "在线同步已在运行"}, status_code=409)

        dir_info = get_dir(dir_path)
        if dir_info and dir_info.get("status") == "live_syncing":
            old_pid = dir_info.get("sync_pid")
            if old_pid and _is_pid_alive(old_pid):
                return JSONResponse({"detail": "在线同步已在运行"}, status_code=409)

        cfg = load_sync_config()
        obs_base = cfg.get("obs_base", "") or load_obs_base()
        if not obs_base:
            return JSONResponse({"detail": "obs_base 未配置"}, status_code=400)

        env_name, mtime_tag = dir_path.split("/", 1)
        obs_dst = f"{obs_base.rstrip('/')}/raw/{env_name}/{mtime_tag}/"
        logs_dir_full = str(_resolve_real_dir(dir_path))
        interval = cfg.get("interval", 600)
        workers = cfg.get("workers", 4)
        upload_script = cfg.get("upload_script")

        cmd = [
            sys.executable, "-m", "utils.obs_sync",
            "--logs-dir", logs_dir_full,
            "--obs-dst", obs_dst,
            "--interval", str(interval),
            "--workers", str(workers),
        ]
        if upload_script:
            cmd.extend(["--upload-script", str(upload_script)])

        proc = _subprocess.Popen(
            cmd,
            cwd=_project_root,
            stdout=_subprocess.PIPE,
            stderr=_subprocess.STDOUT,
            start_new_session=True,
        )

        log_thread = threading.Thread(
            target=_capture_sync_logs,
            args=(proc, dir_path),
            daemon=True,
        )
        log_thread.start()

        _live_sync_procs[dir_path] = {"proc": proc, "log_thread": log_thread}

        upsert_dir(dir_path, env_name, mtime_tag)
        update_sync_status(dir_path, "live_syncing", obs_path=obs_dst)
        update_sync_pid(dir_path, proc.pid)
        append_log(dir_path, f"在线同步已启动: pid={proc.pid}, interval={interval}s")

        return JSONResponse({"ok": True, "pid": proc.pid})

    @app.post("/api/backup/live-sync/stop")
    async def backup_live_sync_stop(request: Request):
        denied = _require_ajax(request)
        if denied:
            return denied

        body = await request.json()
        dir_path = body.get("dir_path", "")

        result = _stop_live_sync(dir_path)
        if result["ok"]:
            return JSONResponse({"ok": True})
        return JSONResponse({"detail": result.get("detail", "停止失败")}, status_code=400)

    # du 结果缓存：{path: (size_bytes, computed_at)}；du 在后台线程算，避免大目录阻塞请求
    _du_cache: dict = {}
    _du_inflight: set = set()
    _du_lock = threading.Lock()
    _DU_TTL = 600  # 秒

    def _du_worker(path: str):
        try:
            sz = _du_size(Path(path))
            with _du_lock:
                _du_cache[path] = (sz, time.time())
        finally:
            with _du_lock:
                _du_inflight.discard(path)

    @app.get("/api/backup/storage")
    def backup_storage(request: Request):
        """返回各日志根所在磁盘用量（df，实时）+ 目录占用（du，后台计算带缓存）。

        with_size=true 时：命中缓存直接返回；否则后台起线程计算，本次返回 size_pending=true，
        前端轮询后续请求获取结果（不阻塞、不超时）。
        """
        denied = _require_ajax(request)
        if denied:
            return denied
        with_size = request.query_params.get("with_size", "").lower() in ("1", "true", "yes")

        # 参与备份的所有根：活跃 env 的父（logs_all）+ 各历史根
        roots = []
        active_root = str(logs_all)
        _active_path = str((logs_all / _active_env_name)) if _active_env_name else active_root
        # 活跃 env 目录尚未创建时，退回 logs_all（再退回项目根），保证 df 仍有值
        if not Path(_active_path).exists():
            _active_path = active_root if Path(active_root).exists() else _project_root
        roots.append({"name": _active_env_name or os.path.basename(active_root),
                      "path": _active_path,
                      "active": True})
        from utils.logs_config import get_path_name
        for r in _extra_roots():
            roots.append({"name": get_path_name(r), "path": r, "active": False})

        out = []
        for r in roots:
            p = Path(r["path"])
            info = {"name": r["name"], "path": r["path"], "active": r["active"],
                    "mount": _mount_point(p),
                    "disk": _disk_usage(p)}
            if with_size and p.is_dir():
                with _du_lock:
                    cached = _du_cache.get(r["path"])
                    fresh = cached and (time.time() - cached[1] < _DU_TTL)
                    if fresh:
                        info["size_bytes"] = cached[0]
                        info["size_h"] = _human_size(cached[0])
                    else:
                        info["size_pending"] = True
                        if r["path"] not in _du_inflight:
                            _du_inflight.add(r["path"])
                            threading.Thread(target=_du_worker, args=(r["path"],),
                                             daemon=True, name="backup-du").start()
            out.append(info)

        return JSONResponse({"roots": out})

    # -----------------------------------------------------------------
    # 自动备份：透明状态 / 立即触发 / 待确认删除
    # -----------------------------------------------------------------
    def _auto_active_path() -> str:
        active_root = str(logs_all)
        p = str((logs_all / _active_env_name)) if _active_env_name else active_root
        if not Path(p).exists():
            p = active_root if Path(active_root).exists() else _project_root
        return p

    @app.get("/api/backup/auto-status")
    def backup_auto_status(request: Request):
        denied = _require_ajax(request)
        if denied:
            return denied
        snap = _auto_snapshot()
        # 补一份实时配置（阈值/开关可能刚改了 yaml）
        cfg = _auto_cfg()
        snap["enabled"] = cfg["enabled"]
        snap["threshold"] = cfg["threshold"]
        snap["obs_configured"] = bool(cfg["obs_base"])
        return JSONResponse(snap)

    @app.post("/api/backup/auto-trigger")
    def backup_auto_trigger(request: Request):
        """手动立即跑一轮自动上传（无视阈值，仍需 obs_base 已配置）。"""
        denied = _require_ajax(request)
        if denied:
            return denied
        with _auto_lock:
            if _auto_state.get("running"):
                return JSONResponse({"ok": False, "msg": "自动备份正在运行中"}, status_code=409)
        env = _active_env_name
        apath = _auto_active_path()
        t = threading.Thread(
            target=_run_auto_backup_round,
            args=(logs_all, env, apath, _resolve_real_dir),
            kwargs={"forced": True}, daemon=True, name="backup-auto-manual")
        t.start()
        return JSONResponse({"ok": True, "msg": "已开始自动上传"})

    @app.get("/api/backup/timeline")
    def backup_timeline(request: Request):
        """跨环境的时间轴：每个时间点有多少叶子 / 文件 / 各状态分布。

        数据来自两处，在 Python 里合并（各有一次性读，都是本地 SQLite）：
          - backup_sync.db 的 backup_dirs：备份视角（状态、有无本地/OBS、文件数）
          - log_dir.db 的 leaf_status：全部叶子（含备份表尚未登记的），带归档标记

        合并键：优先归一化绝对路径；拿不到时退 f"{root_id}/{dir_key}"。
        时间键用 log_scan.scan_time_key（与导出浏览同一解析器）；解析不出来时
        回退 idx_mtime 换算成本机小时。

        不做字节统计：没有现成表，du 走 NFS/递归列举都太慢（实测单叶 80 万个
        对象、`obsutil ls -du` 要几分钟）。规模用「文件数 + 叶子数」表达。
        """
        denied = _require_ajax(request)
        if denied:
            return denied

        from utils.log_scan import scan_time_key, day_key
        from utils.logs_config import get_root_id

        bucket = (request.query_params.get("bucket") or "day").lower()
        if bucket not in ("hour", "day"):
            bucket = "day"
        order = (request.query_params.get("order") or "asc").lower()
        env_filter = request.query_params.get("env_name", "") or ""
        frm = (request.query_params.get("from") or "").strip()
        to = (request.query_params.get("to") or "").strip()

        def _bucket_of(hour_key: str, fallback_ts: float) -> str:
            k = hour_key
            if not k and fallback_ts:
                try:
                    from datetime import datetime as _dt
                    k = _dt.fromtimestamp(fallback_ts).strftime("%y%m%d%H")
                except (ValueError, OSError):
                    k = ""
            if not k:
                return "unknown"
            return day_key(k) if bucket == "day" else k

        points: dict = {}

        def _point(t: str) -> dict:
            p = points.get(t)
            if p is None:
                p = points[t] = {
                    "time": t, "label": t, "leaves": 0, "files": 0,
                    "envs": set(), "status": {}, "has_local": 0, "has_obs": 0,
                    "archived": 0,
                }
            return p

        # ── 两路数据的职责分工 ─────────────────────────────────────────────
        # 叶子数 / 归档数 / 本地存在数：只来自 leaf_status（唯一权威来源）。
        # 状态分布 / 文件数 / 有无 OBS：来自 backup_dirs，按**归一化路径**逐个
        # 叶子对齐。backup_dirs 里对不上任何叶子的陈旧行（实测 131 条，其中 78
        # 条本地目录早已不存在）只贡献文件数，绝不增加叶子数 —— 否则时间轴的
        # 叶子总和会比真实叶子数多，页面上的数字就没法对账。
        dir_rows = list_dirs()
        # logs_all 在 register 里可能是相对路径（LOGS_DIR 来自 get_log_dir），而
        # leaf_status.leaf_path 一律是绝对路径。不转成绝对路径的话，下面的路径
        # 归一化永远对不上，时间轴会显示 files=0 且没有任何状态分布。
        logs_all_abs = Path(os.path.abspath(str(logs_all)))
        dir_by_norm: dict = {}
        extra_root_cache: dict = {}
        matched_dir_paths: set = set()
        for d in dir_rows:
            dp = d.get("dir_path") or ""
            if not dp:
                continue
            c = os.path.normpath(str(logs_all_abs / dp))
            dir_by_norm[c] = (dp, d)
            if "/" not in dp:
                continue
            env_name, rel = dp.split("/", 1)
            if env_name not in extra_root_cache:
                extra_root_cache[env_name] = _extra_root_for_env(env_name)
            root = extra_root_cache[env_name]
            if root:
                dir_by_norm[os.path.normpath(str(Path(root) / rel))] = (dp, d)
                dir_by_norm[os.path.normpath(str(Path(root) / rel / Path(rel).name))] = (dp, d)

        try:
            import utils.logdir_store as _lds
            leaf_rows = _lds.all_leaf_rows() if _lds._ready() else []
        except Exception:  # noqa: BLE001
            leaf_rows = []

        # env_filter 的 rid 集合：显式选中的 env 既可能是历史根（root_id = 路径
        # md5），也可能是活跃 env 目录（root_id = 'default' 或该目录自己的 md5，
        # 不在 _extra_roots 里）。三种都收进来，否则选中活跃 env 时叶子会被滤空。
        want_rids = None
        if env_filter:
            want_rids = {env_filter}
            root = _extra_root_for_env(env_filter)
            if root:
                want_rids.add(_root_env_name(root))
            try:
                from utils.logs_config import get_registered_roots
                active_env = os.path.normpath(str(Path(logs_dir).parent))
                for r0 in get_registered_roots(active_env):
                    if r0 and (os.path.basename(os.path.normpath(r0)) == env_filter
                               or os.path.normpath(r0) == os.path.normpath(str(logs_all / env_filter))):
                        want_rids.add(_root_env_name(r0))
            except Exception:  # noqa: BLE001
                pass

        for r in leaf_rows:
            lp = r.get("leaf_path") or ""
            rid = r.get("root_id") or ""
            dk = r.get("dir_key") or ""
            if want_rids is not None and rid not in want_rids:
                continue
            key_path = os.path.normpath(lp) if lp else ""
            # logs_all 下的活跃 env：root_id 可能是 'default' 或路径 md5，
            # 时间键只能从 dir_key 取（dir_key 就是小时目录名）。
            t = _bucket_of(scan_time_key(lp) or scan_time_key(dk),
                           float(r.get("idx_mtime") or 0))
            if frm and t != "unknown" and t < frm:
                continue
            if to and t != "unknown" and t > to:
                continue
            p = _point(t)
            p["leaves"] += 1
            if rid:
                p["envs"].add(rid)

            b = dir_by_norm.get(key_path) if key_path else None
            if b is not None:
                dp, bd = b
                matched_dir_paths.add(dp)
                st = bd.get("status") or "pending"
                p["status"][st] = p["status"].get(st, 0) + 1
                p["files"] += int(bd.get("file_count") or 0)
                if bd.get("has_obs"):
                    p["has_obs"] += 1

            if r.get("archived"):
                p["archived"] += 1
            elif lp:
                # 不逐叶 os.path.isdir()：4700 次 NFS 元数据查询会把时间轴接口
                # 拖到秒级甚至更久。archived=0 且登记过 leaf_path 就视为本地存在
                # （归档流程一定会打标记，所以这个推断是可靠的）。
                p["has_local"] += 1

        # 未被任何叶子认领的 backup_dirs 行：只补文件数，不增加叶子数。
        orphans = 0
        for dp, bd in dir_by_norm.values():
            if dp in matched_dir_paths:
                continue
            matched_dir_paths.add(dp)
            orphans += 1
            t = _bucket_of(scan_time_key(bd.get("mtime_tag") or dp), 0)
            if env_filter and bd.get("env_name") != env_filter:
                continue
            if frm and t != "unknown" and t < frm:
                continue
            if to and t != "unknown" and t > to:
                continue
            _point(t)["files"] += int(bd.get("file_count") or 0)

        out = []
        for t in sorted(points, key=lambda x: (x == "unknown", x), reverse=(order == "desc")):
            p = points[t]
            out.append({
                "time": t, "label": p["label"],
                "leaves": p["leaves"], "files": p["files"],
                "envs": sorted(p["envs"]),
                "status": p["status"],
                "has_local": p["has_local"], "has_obs": p["has_obs"],
                "archived": p["archived"],
            })
        return JSONResponse({"bucket": bucket, "order": order, "points": out,
                             "files_partial": True})

    @app.get("/api/backup/overview")
    def backup_overview(request: Request):
        """「数据管理 → 备份管理」tab 的聚合总览。

        一次返回四张统计卡、按月备份分布、来源列表、最近操作，避免前端开页
        连打 6 个接口。叶子数仍以 leaf_status 为权威（与 /timeline 同口径），
        backup_dirs 只提供备份状态与文件数。

        没有现成数据源的字段直接给 null，前端显示为「—」，不臆造。
        """
        denied = _require_ajax(request)
        if denied:
            return denied

        from utils.log_scan import scan_time_key, day_key

        logs_all_abs = Path(os.path.abspath(str(logs_all)))
        dir_rows = list_dirs()

        # 归一化路径 → 备份行（与 /timeline 同一套对齐规则）
        dir_by_norm: dict = {}
        extra_root_cache: dict = {}
        for d in dir_rows:
            dp = d.get("dir_path") or ""
            if not dp:
                continue
            dir_by_norm[os.path.normpath(str(logs_all_abs / dp))] = d
            if "/" not in dp:
                continue
            env_name, rel = dp.split("/", 1)
            if env_name not in extra_root_cache:
                extra_root_cache[env_name] = _extra_root_for_env(env_name)
            root = extra_root_cache[env_name]
            if root:
                dir_by_norm[os.path.normpath(str(Path(root) / rel))] = d
                dir_by_norm[os.path.normpath(str(Path(root) / rel / Path(rel).name))] = d

        import utils.logdir_store as _lds
        try:
            leaf_rows = _lds.all_leaf_rows() if _lds._ready() else []
        except Exception:  # noqa: BLE001
            leaf_rows = []

        total_leaves = 0
        pending_leaves = 0
        done_leaves = 0
        archived_leaves = 0
        pending_files = 0
        source_stat: dict = {}
        # 按「数据日期」聚合：{YYMMDD: {"leaves": 总数, "done": 已备份}}
        days: dict = {}

        for r in leaf_rows:
            lp = r.get("leaf_path") or ""
            rid = r.get("root_id") or ""
            dk = r.get("dir_key") or ""
            total_leaves += 1
            st = source_stat.get(rid)
            if st is None:
                st = source_stat[rid] = {"leaves": 0, "pending": 0, "done": 0}
            st["leaves"] += 1

            b = dir_by_norm.get(os.path.normpath(lp)) if lp else None
            bstatus = (b.get("status") if b else None) or "pending"
            # 每个源保留一个样本 OBS 路径，供来源列表的「OBS」列展示与下钻。
            # 只取已经真实上传过的（backup_dirs.obs_path），没传过就留空，
            # 前端显示「—」—— 按模板拼出来的路径并不代表内容真的在 OBS 上。
            if b and b.get("obs_path") and not st.get("obs_path"):
                st["obs_path"] = b["obs_path"]
            if b and b.get("has_obs"):
                done_leaves += 1
                st["done"] += 1
            elif r.get("archived"):
                archived_leaves += 1
                done_leaves += 1
                st["done"] += 1
            else:
                pending_leaves += 1
                st["pending"] += 1
                if b:
                    pending_files += int(b.get("file_count") or 0)

            t = scan_time_key(lp) or scan_time_key(dk)
            if t:
                dk_day = day_key(t)          # YYMMDD
                cell = days.get(dk_day)
                if cell is None:
                    cell = days[dk_day] = {"leaves": 0, "done": 0}
                cell["leaves"] += 1
                # 已备份口径与 totals 一致：有 OBS 内容，或已归档（本地已删）
                if (b and b.get("has_obs")) or r.get("archived"):
                    cell["done"] += 1

        # 按「数据日期」分布，支持 天 / 周 / 月 三种粒度。
        # 用数据日期（目录名 YYMMDDHH）而不是备份时间 —— 这张图回答的是
        # 「哪天的数据备份完了」，与按备份时间的趋势图互补。
        #
        # 日粒度的键是 YYMMDD；周/月 由日粒度聚合而来（先算日，再 roll up），
        # 保证三种粒度下的总数一致。
        def _week_key(k: str) -> str:
            """YYMMDD → 该周周一（YYMMDD）。用 ISO 周，跨年也不会串。"""
            from datetime import datetime, timedelta
            try:
                d = datetime.strptime("20" + k, "%Y%m%d")
            except ValueError:
                return k
            return (d - timedelta(days=d.weekday())).strftime("%y%m%d")

        day_points, week_points, month_points = [], [], []
        if days:
            keys = sorted(days)
            # 日：最近 60 个有数据的日子
            for k in keys[-60:]:
                cell = days[k]
                pct = round(cell["done"] * 100 / cell["leaves"], 1) if cell["leaves"] else 0
                day_points.append({
                    "key": k,
                    # 键是 YYMMDD：k[2:4]=月，k[4:6]=日
                    "label": k[2:4].lstrip("0") + "/" + k[4:6].lstrip("0"),
                    "leaves": cell["leaves"],
                    "done": cell["done"],
                    "pct": pct,
                })

            # 周 / 月：把日粒度 roll up
            # 键格式说明：YYMMDD（如 260901）。周/月的标签都要取「月/日」两段：
            #   k[2:4] = MM，k[4:6] = DD
            # 之前误写成 k[4:6]/k[6:8]（那是 DD/HH，越界成空串），标签才会
            # 变成「8/周」「6/1」这种错位结果。
            for gran, target, n_keep, lab in (
                ("week", week_points, 26,
                 lambda k: k[2:4].lstrip("0") + "/" + k[4:6].lstrip("0") + "周"),
                ("month", month_points, 12,
                 lambda k: k[2:4].lstrip("0") + "月"),
            ):
                roll: dict = {}
                for k in keys:
                    gk = _week_key(k) if gran == "week" else k[:4] + "01"
                    c = roll.get(gk)
                    if c is None:
                        c = roll[gk] = {"leaves": 0, "done": 0}
                    c["leaves"] += days[k]["leaves"]
                    c["done"] += days[k]["done"]
                for gk in sorted(roll)[-n_keep:]:
                    c = roll[gk]
                    target.append({
                        "key": gk,
                        "label": lab(gk),
                        "leaves": c["leaves"],
                        "done": c["done"],
                        "pct": round(c["done"] * 100 / c["leaves"], 1) if c["leaves"] else 0,
                    })


        # 按**天**的「备份完成量」趋势，支持 7/30/90 天窗口。
        # 与下面按小时的口径同源（backup_dirs.sync_time），只是粒度不同：
        # 备份是间歇性的，按小时会出现大量 0 值柱子，按天更能看出节奏。
        from datetime import datetime as _dt, timedelta as _td
        try:
            trend_days = int(request.query_params.get("trend_days") or 30)
        except (TypeError, ValueError):
            trend_days = 30
        trend_days = max(1, min(trend_days, 365))

        daily: dict = {}
        try:
            from utils.backup_store import daily_sync_counts
            for r in daily_sync_counts():
                if r["day"]:
                    daily[r["day"]] = r["leaves"]
        except Exception:  # noqa: BLE001
            daily = {}

        daily_points = []
        today = _dt.now().date()
        for i in range(trend_days - 1, -1, -1):
            d = (today - _td(days=i)).strftime("%Y-%m-%d")
            daily_points.append({
                "day": d,
                "label": d[5:7].lstrip("0") + "/" + d[8:10].lstrip("0"),
                "leaves": daily.get(d, 0),
            })
        daily_in_window = sum(p["leaves"] for p in daily_points)

        # 来源列表（名称走 sources 表）
        name_map: dict = {}
        try:
            for s in _lds.list_sources():
                name_map[s.get("root_id")] = s.get("name") or s.get("root_id")
        except Exception:  # noqa: BLE001
            pass
        sources = []
        for rid, st in source_stat.items():
            sources.append({
                "root_id": rid,
                "name": name_map.get(rid) or rid,
                "leaves": st["leaves"],
                "pending": st["pending"],
                "done": st["done"],
                "status": "ok" if st["pending"] == 0 else "idle",
                # 该源下已上传叶子的样本 OBS 前缀（未上传过为 None）
                "obs_path": st.get("obs_path") or "",
            })
        sources.sort(key=lambda x: x["leaves"], reverse=True)

        # 「最近备份操作」分页：前端用 recent_page 翻页，不再只取 20 条。
        try:
            recent_page = max(1, int(request.query_params.get("recent_page") or 1))
        except (TypeError, ValueError):
            recent_page = 1
        try:
            recent_size = max(1, min(int(request.query_params.get("recent_size") or 20), 200))
        except (TypeError, ValueError):
            recent_size = 20
        try:
            from utils.backup_store import get_recent_logs, count_recent_logs
            recent_total = count_recent_logs()
            recent = get_recent_logs(limit=recent_size,
                                     offset=(recent_page - 1) * recent_size)
        except Exception:  # noqa: BLE001
            recent, recent_total = [], 0

        cfg = _auto_cfg()
        # 本地磁盘实际用量（df）：前端「待释放本地空间」卡片改为显示真实值，
        # 不再用「已上传但未删除目录的 du 之和」（那需要递归统计，代价高且不准）。
        active_path = _auto_active_path()
        disk = _disk_usage(Path(active_path))
        return JSONResponse({
            "totals": {
                "leaves": total_leaves,
                "pending": pending_leaves,
                "done": done_leaves,
                "archived": archived_leaves,
                "pending_files": pending_files,
                "progress_pct": (round(done_leaves * 100 / total_leaves, 1)
                                 if total_leaves else 0),
            },
            # 按「数据日期」分布：三种粒度都返回，前端切换不需再请求
            "days": day_points,
            "weeks": week_points,
            "months": month_points,
            # 按实际备份时间（sync_time）聚合的日趋势，窗口由 trend_days 决定
            "trend_daily": daily_points,
            "trend_days_window": trend_days,
            "trend_daily_total": daily_in_window,
            "sources": sources,
            "recent": recent,
            "recent_total": recent_total,
            "recent_page": recent_page,
            "recent_size": recent_size,
            "auto": {
                "enabled": cfg["enabled"],
                "threshold": cfg["threshold"],
                "interval": cfg["interval"],
                "obs_base": cfg["obs_base"],
                # 自动提交归档任务的开关与批量
                "auto_submit": cfg["auto_submit"],
                "submit_batch": cfg["submit_batch"],
                "submit_min_age_days": cfg["submit_min_age_days"],
            },
            "disk": {
                "path": active_path,
                **disk,
            },
        })

    @app.post("/api/backup/archive")
    async def backup_archive(request: Request):
        """按时间从最远到最近批量归档到 OBS。

        dry_run=true（默认）：只返回候选与预览，不入队。
        dry_run=false：按时间升序入队 backup_queue。claim_next_job 是
        ORDER BY id ASC，所以「先入队 = 先被领取」，天然最远优先，不需要
        动 dispatcher。

        上传成功后仍然只转 pending_delete（不自动删本地），删除由人工在
        「待确认删除」面板确认 —— 保留安全闸门。
        """
        denied = _require_ajax(request)
        if denied:
            return denied
        body = await request.json()
        dry_run = body.get("dry_run", True)
        if isinstance(dry_run, str):
            dry_run = dry_run.lower() in ("1", "true", "yes")
        try:
            limit = int(body.get("limit") or 20)
        except (TypeError, ValueError):
            limit = 20
        limit = max(1, min(limit, 200))
        older_than = str(body.get("older_than") or "").strip()
        envs = body.get("envs") or []
        if isinstance(envs, str):
            envs = [envs]
        envs = {e for e in envs if e}

        with _sync_lock:
            busy = set(_syncing_dirs)
        chosen, matched, skipped = select_archive_candidates(
            _active_dir_path, envs, older_than, limit, busy)

        if dry_run:
            return JSONResponse({
                "dry_run": True,
                "matched": matched,
                "preview": chosen,
                "skipped": skipped[:50],
            })

        enqueued, already = [], []
        for c in chosen:
            if enqueue_job(c["dir_path"], "sync"):
                enqueued.append(c["dir_path"])
            else:
                already.append(c["dir_path"])
        if enqueued:
            _notify_dispatcher()
            append_log(enqueued[0], f"批量归档入队 {len(enqueued)} 个目录（按时间从远到近）")
        return JSONResponse({
            "dry_run": False,
            "enqueued": enqueued,
            "already": already,
            "remaining": max(0, len(candidates) - len(enqueued)),
        })

    @app.get("/api/backup/pending-delete")
    def backup_pending_delete_list(request: Request):
        denied = _require_ajax(request)
        if denied:
            return denied
        from utils.backup_store import list_pending_delete
        from utils.logs_config import get_path_name
        env = request.query_params.get("env_name", "") or None
        rows = list_pending_delete(env)
        _label_map = {_root_env_name(r): get_path_name(r) for r in _extra_roots()}
        for row in rows:
            row["env_label"] = _label_map.get(row.get("env_name"), row.get("env_name"))
        return JSONResponse({"dirs": rows})

    @app.post("/api/backup/confirm-delete")
    async def backup_confirm_delete(request: Request):
        """人工确认删除：仅对 pending_delete 且已上传(has_obs)的目录，删本地并标 backed_up。

        **必须把删除放到线程池**：NFS 的 unlink 可能长时间阻塞（实测卡死过
        30 分钟），而这里是 async 端点、直接调用 shutil.rmtree 会阻塞 uvicorn
        的事件循环 —— 表现就是「点了确认删除没有任何反应」，而且整个站点的
        所有接口一起失去响应。
        """
        denied = _require_ajax(request)
        if denied:
            return denied
        body = await request.json()
        dirs = body.get("dirs", [])
        if not dirs:
            return JSONResponse({"detail": "没有选择目录"}, status_code=400)

        from starlette.concurrency import run_in_threadpool
        return await run_in_threadpool(_confirm_delete_sync, dirs)

    _cleanup_stale_live_syncs()

    # 启动时将当前活跃 mtime 目录注册进 DB（append-only，不触发文件系统全量扫描）
    if _active_dir_path:
        _env_n, _mtime_t = _active_dir_path.split("/", 1)
        _active_local = logs_all / _active_dir_path
        _fcount = 0
        if _active_local.is_dir():
            try:
                _fcount = sum(1 for f in _active_local.iterdir() if f.is_file())
            except OSError:
                pass
        upsert_dir(_active_dir_path, _env_n, _mtime_t, _fcount,
                   port=_port_env_map.get(_env_n, port), has_local=True)

    # 启动自动备份守护线程（仅 leader worker，避免多 worker 重复上传）
    global _auto_thread
    _is_leader = True
    try:
        from utils.leader_lock import is_leader
        _is_leader = is_leader()
    except Exception:
        _is_leader = True
    if _is_leader and _active_env_name and (_auto_thread is None or not _auto_thread.is_alive()):
        _auto_thread = threading.Thread(
            target=_auto_backup_loop,
            args=(logs_all, _active_env_name, _auto_active_path(), _resolve_real_dir),
            daemon=True, name="backup-auto-daemon")
        _auto_thread.start()
