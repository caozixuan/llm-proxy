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
    list_failed_files,
    list_queue,
    mark_backed_up,
    mark_file_resolved,
    record_failed_files,
    reset_running_on_startup,
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
                      resolver=None):
    """在后台线程中同步单个目录到 OBS，直接上传整个文件夹，流式输出日志。

    resolver: 可选 dir_path -> 真实磁盘 Path 的解析器（供 new-api 等非 logs_all 根）。
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

        # synced=1：之前已成功同步过，status 可能被后续操作误覆盖，修正并跳过
        dir_info = get_dir(dir_path)
        if dir_info and dir_info.get("synced") and dir_info.get("obs_path"):
            if dir_info.get("status") != "done":
                update_sync_status(dir_path, "done", obs_path=dir_info["obs_path"])
                append_log(dir_path, f"已有成功同步记录（synced=1），修正状态为 done，跳过重复上传")
            else:
                append_log(dir_path, "已有成功同步记录（synced=1），跳过重复上传")
            return

        update_sync_status(dir_path, "syncing", obs_path=obs_dst)
        append_log(dir_path, f"开始同步: {dir_path} -> {obs_dst}")

        cmd = [script, logs_dir, obs_dst]
        append_log(dir_path, f"执行: {' '.join(cmd)}")
        proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.STDOUT, text=True)
        task_id = ""
        output_dir = ""
        for line in proc.stdout:
            line = line.rstrip()
            if line:
                append_log(dir_path, line)
                # obsutil 开始时打印 Task id / OutputDir，捕获用于定位失败报告
                if not task_id and line.startswith("Task id:"):
                    task_id = line.split(":", 1)[1].strip()
                elif not output_dir and line.startswith("OutputDir:"):
                    output_dir = line.split(":", 1)[1].strip()
        proc.wait(timeout=600)

        if proc.returncode == 0:
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
                           else "同步完成（无哨兵文件，仅依赖退出码与 -vlength）")
            else:
                msg = "；".join(p["detail"] for p in problems)
                append_log(dir_path, f"哨兵核对失败: {msg}", level="error")
                record_failed_files(dir_path, [
                    (p["local"], p["obs"], p["detail"]) for p in problems if p.get("local")
                ])
                update_sync_status(dir_path, "partial",
                                   error_msg=f"哨兵核对失败: {msg}", obs_path=obs_dst)
        else:
            # 尝试从 obsutil 失败报告解析出失败文件清单 → 标记 partial（部分失败）
            failed_items = []
            report = find_failed_report(task_id, output_dir) if task_id else None
            if report:
                failed_items = parse_failed_report(report)
            if failed_items:
                record_failed_files(dir_path, failed_items)
                msg = f"部分失败: {len(failed_items)} 个文件上传失败 (exit_code={proc.returncode})"
                append_log(dir_path, f"{msg}，可点击「补同步」重传", level="error")
                update_sync_status(dir_path, "partial", error_msg=msg, obs_path=obs_dst)
            else:
                append_log(dir_path, f"上传失败: exit_code={proc.returncode}", level="error")
                update_sync_status(dir_path, "error", error_msg=f"exit_code={proc.returncode}", obs_path=obs_dst)
    except Exception as e:
        update_sync_status(dir_path, "error", error_msg=str(e))
        append_log(dir_path, f"同步失败: {e}", level="error")
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
        _syncing_dirs.add(dir_path)
        try:
            if job_type == "resync":
                _run_resync_failed(dir_path)
            else:
                cfg = load_sync_config()
                obs_base = cfg.get("obs_base", "") or load_obs_base()
                workers = cfg.get("workers", 4)
                upload_script = cfg.get("upload_script", "")
                if not obs_base:
                    update_sync_status(dir_path, "error", error_msg="obs_base 未配置")
                    append_log(dir_path, "同步失败: obs_base 未配置", level="error")
                    _syncing_dirs.discard(dir_path)
                else:
                    logs_all = _dispatcher_ctx.get("logs_all")
                    resolver = _dispatcher_ctx.get("resolver")
                    _run_sync_for_dir(dir_path, logs_all, obs_base, workers,
                                      upload_script, resolver=resolver)
        except Exception:
            logger.exception("queue dispatch failed for %s", dir_path)
            _syncing_dirs.discard(dir_path)
        finally:
            finish_job(job["id"])


def _ensure_dispatcher():
    """懒启动单 dispatcher 线程（幂等）。"""
    global _dispatcher_thread
    with _queue_cond:
        if _dispatcher_thread is not None and _dispatcher_thread.is_alive():
            return
        _dispatcher_thread = threading.Thread(
            target=_queue_dispatch_loop, daemon=True, name="backup-queue-dispatcher"
        )
        _dispatcher_thread.start()


def _notify_dispatcher():
    with _queue_cond:
        _queue_cond.notify()


# ---------------------------------------------------------------------------
# 自动备份：磁盘达阈值时自动上传未备份目录，成功后转 pending_delete（等人工确认删除）
# ---------------------------------------------------------------------------

def _auto_cfg() -> dict:
    """从 sync_config(settings/obs_rl.yaml) 读自动备份配置。"""
    cfg = load_sync_config()
    return {
        "enabled": bool(cfg.get("auto_backup_enabled", False)),
        "threshold": float(cfg.get("auto_backup_disk_percent", 70) or 70),
        "interval": max(30, int(cfg.get("auto_backup_check_interval", 120) or 120)),
        "stop_percent": float(cfg.get("auto_backup_stop_percent", 0) or 0),
        "workers": int(cfg.get("workers", 4) or 4),
        "upload_script": cfg.get("upload_script", "") or "",
        "obs_base": cfg.get("obs_base", "") or load_obs_base(),
    }


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
                              cfg["upload_script"], resolver=resolver)
        except Exception as e:  # noqa: BLE001
            logger.exception("auto backup upload failed: %s", dir_path)
            append_log(dir_path, f"自动备份上传异常: {e}", level="error")
            continue

        # 上传结果以 DB 状态为准：done 才转 pending_delete（partial/error 保留原状态待人工处理）
        info = get_dir(dir_path)
        if info and info.get("status") == "done" and info.get("obs_path"):
            update_sync_status(dir_path, "pending_delete", obs_path=info["obs_path"])
            append_log(dir_path, "自动备份上传成功，转入待确认删除")
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

        优先按 logs_all/{dir_path}；不存在时在配置的历史根里按 root_id/<rel> 匹配，
        并对存量记录里遗留的旧 basename 前缀保留回退匹配。
        """
        cand = logs_all / dir_path
        if cand.is_dir():
            return cand
        env_name = dir_path.split("/", 1)[0]
        for root in _extra_roots():
            if _root_env_name(root) == env_name or _root_basename(root) == env_name:
                rel = dir_path.split("/", 1)[1] if "/" in dir_path else ""
                from utils.log_scan import resolve_leaf
                real = resolve_leaf(root, rel)
                if real.is_dir():
                    return real
        return cand  # 兜底：返回 logs_all 下的路径（可能不存在）

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
        _newapi_root = _extra_root_for_env(env_name) if env_name else None

        # 只有该 env 在 DB 中完全没有记录时，才做一次性初始扫描（首次建立 DB 或 DB 丢失）
        if env_name and not has_any_dirs(env_name):
            if _newapi_root:
                fs_dirs = _scan_root_leaves(_newapi_root)
            else:
                fs_dirs = _scan_data_dirs(logs_all, env_name=env_name, port_env_map=_port_env_map)
            for d in fs_dirs:
                upsert_dir(d["dir_path"], d["env_name"], d["mtime_tag"], d["file_count"],
                           port=d.get("port", ""), has_local=True)

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

        return JSONResponse({"dirs": merged})

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
        return JSONResponse({
            "running": running,
            "queued": queued,
            "queue_len": len(queued),
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

            with open(cfg_path, "w", encoding="utf-8") as f:
                yaml.dump(existing, f, default_flow_style=False, allow_unicode=True)

            # 守护线程每轮重读配置，这里不需要重启；返回生效值便于前端确认
            cfg = _auto_cfg()
            return JSONResponse({
                "ok": True,
                "enabled": cfg["enabled"],
                "threshold": cfg["threshold"],
                "interval": cfg["interval"],
            })
        except Exception as e:
            return JSONResponse({"detail": str(e)}, status_code=500)
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
        months: dict = {}

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
                m = day_key(t)[:4]
                months[m] = months.get(m, 0) + 1

        # 按月分布：最近 5 个月，缺月补 0（保持柱状图横轴连续）
        month_points = []
        if months:
            keys = sorted(months)
            recent = keys[-5:]
            for k in recent:
                month_points.append({"month": k, "label": k[-2:].lstrip("0") + "月",
                                     "leaves": months[k]})

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
            })
        sources.sort(key=lambda x: x["leaves"], reverse=True)

        try:
            from utils.backup_store import get_recent_logs
            recent = get_recent_logs(limit=20)
        except Exception:  # noqa: BLE001
            recent = []

        cfg = _auto_cfg()
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
            "months": month_points,
            "sources": sources,
            "recent": recent,
            "auto": {
                "enabled": cfg["enabled"],
                "threshold": cfg["threshold"],
                "interval": cfg["interval"],
                "obs_base": cfg["obs_base"],
            },
            # 磁盘可释放空间需要真实 du，代价高（见 /storage 的后台计算），
            # 这里不给臆造值。
            "reclaimable_bytes": None,
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

        from utils.log_scan import scan_time_key

        queue_paths = {j.get("dir_path") for j in list_queue()}
        with _sync_lock:
            busy = set(_syncing_dirs)

        candidates, skipped = [], []
        for d in list_dirs():
            dp = d.get("dir_path") or ""
            if not dp:
                continue
            if envs and d.get("env_name") not in envs:
                continue
            if dp == _active_dir_path:
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

        # 最远 → 最近
        candidates.sort(key=lambda c: (c["time"] or "99999999", c["dir_path"]))
        chosen = candidates[:limit]

        if dry_run:
            return JSONResponse({
                "dry_run": True,
                "matched": len(candidates),
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
        """人工确认删除：仅对 pending_delete 且已上传(has_obs)的目录，删本地并标 backed_up。"""
        denied = _require_ajax(request)
        if denied:
            return denied
        body = await request.json()
        dirs = body.get("dirs", [])
        if not dirs:
            return JSONResponse({"detail": "没有选择目录"}, status_code=400)

        deleted, skipped = [], []
        for dir_path in dirs:
            info = get_dir(dir_path)
            # 安全校验：必须是待删状态 + 已上传 OBS，防止误删未备份数据
            if not info or info.get("status") != "pending_delete" \
                    or not info.get("has_obs") or not info.get("obs_path"):
                skipped.append(dir_path)
                append_log(dir_path, "确认删除被拒绝：非待删状态或未成功上传", level="error")
                continue
            target = _resolve_real_dir(dir_path)
            if target.is_dir():
                try:
                    shutil.rmtree(str(target))
                    append_log(dir_path, f"人工确认删除，本地目录已删除: {target}")
                except Exception as e:  # noqa: BLE001
                    append_log(dir_path, f"删除失败: {e}", level="error")
                    skipped.append(dir_path)
                    continue
            mark_backed_up(dir_path)
            _mark_leaves_archived(dir_path, True)
            deleted.append(dir_path)
        return JSONResponse({"ok": True, "deleted": deleted, "skipped": skipped})

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
