#!/usr/bin/env python3
import argparse
import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional
import re

BASE_DIR = Path(__file__).resolve().parent
APP_FILE = BASE_DIR / "app.py"
LOG_DIR = BASE_DIR / "logs"
STATE_FILE = BASE_DIR / ".cli_state.yaml"
DEFAULT_ENV = ".env"

try:
    import yaml  # type: ignore
except Exception:
    yaml = None


def eprint(msg: str) -> None:
    print(msg, file=sys.stderr)


def load_state() -> dict:
    if not STATE_FILE.exists():
        return {"source_env": DEFAULT_ENV, "services": {}}
    if yaml is not None:
        try:
            with STATE_FILE.open("r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            if isinstance(data, dict):
                data.setdefault("source_env", DEFAULT_ENV)
                data.setdefault("services", {})
                return data
        except Exception:
            pass

    try:
        data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            data.setdefault("source_env", DEFAULT_ENV)
            data.setdefault("services", {})
            return data
    except Exception:
        pass

    state = {}
    for line in STATE_FILE.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#") or ":" not in raw:
            continue
        key, value = raw.split(":", 1)
        key = key.strip()
        value = value.strip()
        if value == "":
            state[key] = ""
            continue
        if value in ("null", "~"):
            state[key] = None
            continue
        if value in ("true", "false"):
            state[key] = value == "true"
            continue
        if value.startswith('"') or value.startswith("[") or value.startswith("{"):
            try:
                state[key] = json.loads(value)
                continue
            except Exception:
                pass
        if value.isdigit():
            state[key] = int(value)
            continue
        state[key] = value
    state.setdefault("source_env", DEFAULT_ENV)
    state.setdefault("services", {})
    return state


def save_state(state: dict) -> None:
    if yaml is not None:
        with STATE_FILE.open("w", encoding="utf-8") as f:
            yaml.safe_dump(state, f, sort_keys=True, allow_unicode=True)
        return

    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def parse_env_file(env_path: Path) -> dict:
    values = {}
    if not env_path.exists():
        raise FileNotFoundError(f"env file not found: {env_path}")
    for line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        line = line.split(" #", 1)[0].split("\t#", 1)[0]
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key or not (key[0].isalpha() or key[0] == "_"):
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        values[key] = value
    return values


def get_api_key_suffix(env_values: dict) -> str:
    raw = (env_values.get("UPSTREAM_API_KEY") or "").strip()
    if not raw:
        return ""
    first = ""
    for part in raw.split(","):
        part = part.strip()
        if part:
            first = part
            break
    if not first:
        return ""
    return first[-4:]


def resolve_env_path(source_env: Optional[str]) -> Path:
    env_name = source_env or DEFAULT_ENV
    env_path = Path(env_name)
    if not env_path.is_absolute():
        env_path = BASE_DIR / env_name
    return env_path.resolve()


def state_runtime(state: dict) -> tuple[Path, dict, str, int, Path, Path]:
    source_env = state.get("source_env") or DEFAULT_ENV
    env_path = resolve_env_path(source_env)
    env_values = parse_env_file(env_path)
    host = env_values.get("PROXY_HOST", "127.0.0.1")
    port = int(env_values.get("PROXY_PORT", "4000"))
    pid_file = LOG_DIR / f"app-port{port}.pid"
    log_file = LOG_DIR / f"app-port{port}.log"
    return env_path, env_values, host, port, pid_file, log_file


def get_service_key(env_path: Path) -> str:
    try:
        return os.path.relpath(env_path, BASE_DIR)
    except ValueError:
        return str(env_path)


def get_service_slug(service_key: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9_-]+", "-", service_key)
    slug = slug.replace("/", "-").replace("\\", "-").replace(".", "-")
    slug = re.sub(r"-+", "-", slug).strip("-")
    return slug or "default"


def _service_log_dir(port: int, service_slug: str, key_prefix: str) -> Path:
    segment = f"{service_slug}-{key_prefix}" if key_prefix else (service_slug or "default")
    return LOG_DIR / f"port{port}" / segment


def get_selected_env(args: argparse.Namespace, state: dict) -> str:
    return args.env_file or state.get("source_env") or DEFAULT_ENV


def is_pid_running(pid: Optional[int]) -> bool:
    if not pid:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _signal_service(pid: int, sig: int) -> None:
    """给整个服务进程组发信号。

    服务用 start_new_session=True 启动，master 是进程组组长（PGID == master PID），
    多 worker（PROXY_WORKERS>1）下的 worker 都在同一进程组里。按进程组发信号可以
    把 master + 所有 worker 一起停掉，避免 SIGKILL 掉 master 后 worker 变孤儿占端口。
    取不到进程组（异常）时退回只给 master 发信号。"""
    try:
        pgid = os.getpgid(pid)
        os.killpg(pgid, sig)
    except (ProcessLookupError, PermissionError, OSError):
        try:
            os.kill(pid, sig)
        except OSError:
            pass


def read_pid(path: Path) -> Optional[int]:
    try:
        return int(path.read_text(encoding="utf-8").strip())
    except Exception:
        return None


def tail_lines(path: Path, n: int) -> list[str]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", errors="replace") as f:
        lines = f.readlines()
    return lines[-n:]


def cmd_config(args: argparse.Namespace) -> int:
    state = load_state()
    if args.env_file:
        env_path = resolve_env_path(args.env_file)
        if not env_path.exists():
            eprint(f"[app] env file not found: {env_path}")
            return 1
        state["source_env"] = get_service_key(env_path)
        state["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        save_state(state)
        print(f"[app] source_env -> {state['source_env']}")
        print(f"[app] state saved -> {STATE_FILE}")
        return 0

    env_path = resolve_env_path(state.get("source_env"))
    print(f"[app] source_env: {state.get('source_env', DEFAULT_ENV)}")
    print(f"[app] resolved_env: {env_path}")
    services = state.get("services") or {}
    if services:
        print("[app] services:")
        for key, svc in services.items():
            port = svc.get("port", "-")
            pid = svc.get("pid", "-")
            print(f"  - {key}: port={port} pid={pid}")
    print(f"[app] state_file: {STATE_FILE}")
    return 0


def cmd_start(args: argparse.Namespace) -> int:
    state = load_state()
    state["source_env"] = get_selected_env(args, state)
    env_path, env_values, host, port, pid_file, log_file = state_runtime(state)
    api_key_suffix = get_api_key_suffix(env_values)
    service_key = get_service_key(env_path)
    service_slug = get_service_slug(service_key)
    svc_dir = _service_log_dir(port, service_slug, api_key_suffix)
    svc_dir.mkdir(parents=True, exist_ok=True)
    services = state.setdefault("services", {})
    service = services.setdefault(service_key, {})
    pid_file = svc_dir / "app.pid"
    log_file = svc_dir / "app.log"

    pid = read_pid(pid_file)
    if is_pid_running(pid):
        print(f"[app] already running: pid={pid} host={host} port={port}")
        print(f"[app] log -> {log_file}")
        return 0

    child_env = os.environ.copy()
    child_env.update(env_values)
    child_env["ENV_FILE"] = str(env_path)
    child_env["LOG_TASK_TAG"] = service_slug
    # 清理历史遗留的 data_config 残留字段（旧版 `app data config` 写入，机制已移除）
    if state.get("data_config"):
        state.pop("data_config", None)
        state["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        save_state(state)

    with log_file.open("ab") as log_fp:
        proc = subprocess.Popen(
            [sys.executable, str(APP_FILE)],
            cwd=str(BASE_DIR),
            env=child_env,
            stdout=log_fp,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )

    pid_file.write_text(f"{proc.pid}\n", encoding="utf-8")
    time.sleep(1)
    if not is_pid_running(proc.pid):
        eprint(f"[app] failed to start, check log: {log_file}")
        return 1

    service.update({
        "env_path": service_key,
        "pid": proc.pid,
        "host": host,
        "port": port,
        "api_key_suffix": api_key_suffix,
        "pid_file": os.path.relpath(pid_file, BASE_DIR),
        "log_file": os.path.relpath(log_file, BASE_DIR),
        "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    })
    state["source_env"] = service_key
    save_state(state)
    print(f"[app] started: pid={proc.pid} host={host} port={port}")
    print(f"[app] env -> {service_key}")
    print(f"[app] log -> {log_file}")

    # 拉起导出 / 回填 worker（执行已剥离到独立进程）。_worker_start 本身是幂等的：
    # 已在跑的实例只做记录、不重启，所以平台重启时这里只是「补齐缺的」，
    # 不会打断 worker 上正在跑的任务。
    if not getattr(args, "no_workers", False):
        _start_all_workers(args)

    if getattr(args, "sync", False):
        config = load_sync_config(state)
        if config.get("obs_base"):
            meta_path = svc_dir / "app-meta.json"
            print("[app] --sync: waiting for app meta file...")
            for _ in range(6):
                if meta_path.exists():
                    break
                time.sleep(10)
            if not meta_path.exists():
                # COMPAT: 旧版 app.py 将 meta 写在 logs/app-meta-port{port}.json，可移除
                legacy_meta = LOG_DIR / f"app-meta-port{port}.json"
                if not legacy_meta.exists():
                    eprint("[app] --sync: meta file not ready after 60s, skipping sync")
                    return 0
            # 更新 backup DB：标记活跃目录为 live_syncing
            try:
                from utils.backup_store import init_db as init_backup_db, upsert_dir, update_sync_status, update_sync_pid, append_log
                init_backup_db(str(svc_dir))
                meta = _read_app_meta(port, service_slug, api_key_suffix)
                logs_dir_val = meta.get("logs_dir", "")
                if logs_dir_val.startswith("logs_all/"):
                    dir_path = logs_dir_val[len("logs_all/"):]
                    parts = dir_path.split("/", 1)
                    if len(parts) == 2:
                        upsert_dir(dir_path, parts[0], parts[1])
                        update_sync_status(dir_path, "live_syncing")
                        append_log(dir_path, "sync daemon started (--sync)")
            except Exception as e:
                eprint(f"[app] --sync: backup db update failed: {e}")
            args.interval = None
            cmd_sync_stop(args)
            return cmd_sync(args)
        eprint("[app] --sync: not configured. Run: sync config <yaml_path>")

    return 0


def cmd_stop(args: argparse.Namespace) -> int:
    state = load_state()
    state["source_env"] = get_selected_env(args, state)
    env_path = resolve_env_path(state["source_env"])
    service_key = get_service_key(env_path)
    service_slug = get_service_slug(service_key)
    service = (state.get("services") or {}).get(service_key, {})
    pid = service.get("pid")
    pid_file = service.get("pid_file")
    if not pid_file and service.get("port"):
        key_suffix = service.get("api_key_suffix", "")
        svc_dir = _service_log_dir(service.get("port"), service_slug, key_suffix)
        new_pid = svc_dir / "app.pid"
        # COMPAT: 旧版 pid 文件在 logs/app-{slug}-port{port}.pid，可移除 old_pid 分支
        old_pid = LOG_DIR / f"app-{service_slug}-port{service.get('port')}.pid"
        pid_file = os.path.relpath(new_pid if new_pid.exists() else old_pid, BASE_DIR)
    if pid_file:
        pid_from_file = read_pid(BASE_DIR / pid_file)
        if pid_from_file:
            pid = pid_from_file

    # 默认**不动**导出 / 回填 worker：它们是独立进程，拥有自己的 DB 连接与
    # owner_worker + 心跳，平台重启期间 worker 继续跑（正在执行的任务由心跳续命，
    # 别的 worker 启动时不会误判成孤儿）。--with-workers 可恢复「跟着一起停」的旧行为。
    if getattr(args, "with_workers", False):
        _stop_all_workers(args)

    if not is_pid_running(pid):
        print(f"[app] not running: env={service_key}")
        if pid_file:
            (BASE_DIR / pid_file).unlink(missing_ok=True)
        service["pid"] = None
        save_state(state)
        return 0

    print(f"[app] stopping pid={pid} env={service_key}")
    _signal_service(pid, signal.SIGTERM)
    for _ in range(20):
        time.sleep(0.5)
        if not is_pid_running(pid):
            break

    if is_pid_running(pid):
        print(f"[app] force kill pid={pid}")
        _signal_service(pid, signal.SIGKILL)
        time.sleep(0.2)

    if pid_file:
        (BASE_DIR / pid_file).unlink(missing_ok=True)
    service["pid"] = None
    service["stopped_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    save_state(state)
    print("[app] stopped")
    return 0


def cmd_restart(args: argparse.Namespace) -> int:
    stop_code = cmd_stop(args)
    if stop_code != 0:
        return stop_code
    return cmd_start(args)


def cmd_logs(args: argparse.Namespace) -> int:
    state = load_state()
    state["source_env"] = get_selected_env(args, state)
    env_path = resolve_env_path(state["source_env"])
    service_key = get_service_key(env_path)
    service_slug = get_service_slug(service_key)
    service = (state.get("services") or {}).get(service_key, {})
    log_file_rel = service.get("log_file")
    if log_file_rel:
        log_file = BASE_DIR / log_file_rel
    else:
        _, env_values, _, port, _, _ = state_runtime(state)
        key_suffix = get_api_key_suffix(env_values)
        svc_dir = _service_log_dir(port, service_slug, key_suffix)
        new_log = svc_dir / "app.log"
        # COMPAT: 旧版 log 文件在 logs/app-{slug}-port{port}.log，可移除 old_log 分支
        old_log = LOG_DIR / f"app-{service_slug}-port{port}.log"
        log_file = new_log if new_log.exists() else old_log

    if not log_file.exists():
        eprint(f"[app] log file not found: {log_file}")
        return 1

    if args.follow:
        try:
            subprocess.run(["tail", "-n", str(args.lines), "-f", str(log_file)], check=False)
        except KeyboardInterrupt:
            pass
        return 0

    sys.stdout.writelines(tail_lines(log_file, args.lines))
    return 0


def _print_services(state: dict) -> int:
    services = state.get("services") or {}
    if not services:
        print("[app] no recorded services")
        return 0
    for key, service in services.items():
        pid = service.get("pid")
        pid_file = service.get("pid_file")
        if pid_file:
            pid_from_file = read_pid(BASE_DIR / pid_file)
            if pid_from_file:
                pid = pid_from_file
        running = is_pid_running(pid)
        host = service.get("host", "-")
        port = service.get("port", "-")
        api_key_suffix = service.get("api_key_suffix", "")
        log_file = service.get("log_file", "-")
        marker = "*" if key == state.get("source_env") else " "
        suffix_text = f" key=***{api_key_suffix}" if api_key_suffix else ""
        print(f"{marker} {key}: {'running' if running else 'stopped'} pid={pid or '-'} host={host} port={port}{suffix_text} log={log_file}")
    return 0


def cmd_status(_args: argparse.Namespace) -> int:
    state = load_state()
    print(f"[app] source_env: {state.get('source_env', DEFAULT_ENV)}")
    return _print_services(state)


def cmd_list(_args: argparse.Namespace) -> int:
    state = load_state()
    return _print_services(state)


# ---------------------------------------------------------------------------
# Connect — connection test
# ---------------------------------------------------------------------------

def cmd_connect(args: argparse.Namespace) -> int:
    from utils.connection_test import run_test, print_result

    state = load_state()
    state["source_env"] = get_selected_env(args, state)
    env_path, env_values, host, port, _, _ = state_runtime(state)

    model = args.model
    method = args.method

    if args.noproxy:
        upstream_url = env_values.get("UPSTREAM_URL", "")
        if not upstream_url:
            eprint("[connect] UPSTREAM_URL not found in env file")
            return 1
        upstream_url = upstream_url.rstrip("/").removesuffix("/v1")
        raw_key = env_values.get("UPSTREAM_API_KEY", "").strip().strip('"')
        api_key = raw_key.split(",")[0].strip() if raw_key else ""
        print(f"[connect] direct upstream: {upstream_url}")
        # 上游统一用 openai 协议
        method = "openai"
        base_url = upstream_url
    else:
        base_url = f"http://{host}:{port}"
        if args.key:
            api_key = args.key
        else:
            # 从 env 文件读取 API_KEY（客户端 key），取第一个；fallback 到 DB
            raw_api_key = env_values.get("API_KEY", "").strip().strip('"')
            api_key = raw_api_key.split(",")[0].strip() if raw_api_key else ""
            if not api_key:
                try:
                    sys.path.insert(0, str(BASE_DIR))
                    from utils.key_store import init_db, list_keys as _list_keys
                    service_key = get_service_key(env_path)
                    service_slug = get_service_slug(service_key)
                    key_suffix = get_api_key_suffix(env_values)
                    svc_dir = _service_log_dir(port, service_slug, key_suffix)
                    init_db(str(svc_dir))
                    db_keys = _list_keys()
                    active = [k for k in db_keys if k.get("status") == "active"]
                    if active:
                        from utils.key_store import get_key_full
                        full = get_key_full(active[0]["id"])
                        if full:
                            api_key = full["key"]
                except Exception:
                    pass
        print(f"[connect] via proxy: {base_url}")

    # 查找 key 对应的渠道绑定
    if not args.noproxy and api_key:
        try:
            sys.path.insert(0, str(BASE_DIR))
            from utils.key_store import init_db as _init_key_db, get_key_id_by_value
            from utils.channel_store import init_db as _init_ch_db, get_key_channels
            service_key = get_service_key(env_path)
            service_slug = get_service_slug(service_key)
            key_suffix = get_api_key_suffix(env_values)
            svc_dir = _service_log_dir(port, service_slug, key_suffix)
            _init_key_db(str(svc_dir))
            _init_ch_db(str(svc_dir))
            kid = get_key_id_by_value(api_key)
            if kid is not None:
                chs = get_key_channels(kid)
                if chs:
                    ch_list = ", ".join(
                        f"{c.get('name') or '?'}({c['key_suffix']})"
                        + ("" if c.get("alive") else "[离线]")
                        for c in chs
                    )
                    alive_count = sum(1 for c in chs if c.get("alive"))
                    print(f"[connect] channels({alive_count}/{len(chs)}): {ch_list}")
                else:
                    print("[connect] channels: (none, fallback to .env upstream)")
        except Exception:
            pass

    print(f"[connect] method={method} model={model}")
    if args.message:
        print(f"[connect] message={args.message}")
    result = run_test(base_url, method, model, api_key, args.timeout, args.message)
    print_result(result)
    return 0 if result["ok"] else 1


# ---------------------------------------------------------------------------
# Sync config — yaml file path stored in .cli_state.yaml["sync_config"]
# ---------------------------------------------------------------------------

def _resolve_sync_config_path(state: dict) -> Optional[Path]:
    rel = state.get("sync_config")
    if not rel:
        return None
    p = Path(rel)
    if not p.is_absolute():
        p = BASE_DIR / p
    return p.resolve()


def load_sync_config(state: dict) -> dict:
    path = _resolve_sync_config_path(state)
    if not path or not path.exists():
        return {}
    if yaml is not None:
        try:
            with path.open("r", encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
        except Exception:
            return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _read_app_meta(port: int, service_slug: str = "", key_prefix: str = "") -> dict:
    if service_slug or key_prefix:
        svc_dir = _service_log_dir(port, service_slug, key_prefix)
        meta_path = svc_dir / "app-meta.json"
        if meta_path.exists():
            try:
                return json.loads(meta_path.read_text(encoding="utf-8"))
            except Exception:
                pass
    # COMPAT: 旧版 meta 在 logs/app-meta-port{port}.json，可移除以下 legacy 分支
    legacy_path = LOG_DIR / f"app-meta-port{port}.json"
    if not legacy_path.exists():
        return {}
    try:
        return json.loads(legacy_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _get_sync_service(state: dict, service_key: str) -> dict:
    return state.setdefault("sync_services", {}).setdefault(service_key, {})


# ---------------------------------------------------------------------------
# cmd_sync_config — point to a yaml config file
# ---------------------------------------------------------------------------

def cmd_sync_config(args: argparse.Namespace) -> int:
    state = load_state()
    config_path = getattr(args, "config_file", None)

    if config_path:
        p = Path(config_path)
        if not p.is_absolute():
            p = BASE_DIR / p
        p = p.resolve()
        if not p.exists():
            eprint(f"[sync] config file not found: {p}")
            return 1
        rel = os.path.relpath(p, BASE_DIR)
        state["sync_config"] = rel
        state["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        save_state(state)
        print(f"[sync] sync_config -> {rel}")
        config = load_sync_config(state)
        for k, v in config.items():
            print(f"[sync]   {k}: {v}")
        return 0

    # show current
    cfg_path = _resolve_sync_config_path(state)
    if not cfg_path:
        print("[sync] not configured")
        print("[sync] run: sync config <yaml_path>")
        return 0
    print(f"[sync] config: {state.get('sync_config')}")
    print(f"[sync] resolved: {cfg_path}")
    config = load_sync_config(state)
    for k, v in config.items():
        print(f"[sync]   {k}: {v}")
    return 0


# ---------------------------------------------------------------------------
# cmd_sync — start sync daemon
# ---------------------------------------------------------------------------

def cmd_sync(args: argparse.Namespace) -> int:
    state = load_state()
    config = load_sync_config(state)
    obs_base = config.get("obs_base", "")
    if not obs_base:
        eprint("[sync] not configured. Run: sync config <yaml_path>")
        return 1

    state = load_state()
    state["source_env"] = get_selected_env(args, state)
    env_path, env_values, host, port, _, _ = state_runtime(state)
    service_key = get_service_key(env_path)
    service_slug = get_service_slug(service_key)
    api_key_suffix = get_api_key_suffix(env_values)

    meta = _read_app_meta(port, service_slug, api_key_suffix)
    logs_dir = meta.get("logs_dir")
    if not logs_dir:
        eprint(f"[sync] app meta not found for port{port}/{service_slug}-{api_key_suffix}")
        eprint("[sync] start the app first: ./app start")
        return 1

    # OBS 目标：obs_base/raw/{env_segment}/
    # logs_dir 形如 logs_all/env-xxx-key/26052814
    if logs_dir.startswith("logs_all/"):
        env_segment = logs_dir[len("logs_all/"):]
    else:
        env_segment = os.path.basename(logs_dir)
    obs_dst = obs_base.rstrip("/") + "/raw/" + env_segment.strip("/") + "/"

    svc_dir = _service_log_dir(port, service_slug, api_key_suffix)
    svc_dir.mkdir(parents=True, exist_ok=True)
    pid_file = svc_dir / "sync.pid"
    log_file = svc_dir / "sync.log"

    pid = read_pid(pid_file)
    if is_pid_running(pid):
        print(f"[sync] already running: pid={pid} env={service_key}")
        print(f"[sync] log -> {log_file}")
        return 0

    interval = args.interval or config.get("interval", 600)
    workers = config.get("workers", 4)
    upload_script = config.get("upload_script")

    cmd = [
        sys.executable, "-m", "utils.obs_sync",
        "--logs-dir", str(logs_dir),
        "--obs-dst", str(obs_dst),
        "--interval", str(interval),
        "--workers", str(workers),
    ]
    if upload_script:
        cmd.extend(["--upload-script", str(upload_script)])

    with log_file.open("ab") as log_fp:
        proc = subprocess.Popen(
            cmd,
            cwd=str(BASE_DIR),
            stdout=log_fp,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )

    pid_file.write_text(f"{proc.pid}\n", encoding="utf-8")
    time.sleep(1)
    if not is_pid_running(proc.pid):
        eprint(f"[sync] failed to start, check log: {log_file}")
        return 1

    sync_svc = _get_sync_service(state, service_key)
    sync_svc.update({
        "pid": proc.pid,
        "pid_file": os.path.relpath(pid_file, BASE_DIR),
        "log_file": os.path.relpath(log_file, BASE_DIR),
        "logs_dir": logs_dir,
        "obs_dst": obs_dst,
        "interval": interval,
        "config": state.get("sync_config", ""),
        "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    })
    save_state(state)
    print(f"[sync] started: pid={proc.pid} env={service_key}")
    print(f"[sync] {logs_dir} -> {obs_dst}")
    print(f"[sync] log -> {log_file}")

    # 更新 backup DB：记录 PID 和 live_syncing 状态
    try:
        from utils.backup_store import init_db as init_backup_db, upsert_dir, update_sync_status, update_sync_pid, append_log
        init_backup_db(str(svc_dir))
        if logs_dir.startswith("logs_all/"):
            dir_path = logs_dir[len("logs_all/"):]
            parts = dir_path.split("/", 1)
            if len(parts) == 2:
                upsert_dir(dir_path, parts[0], parts[1])
                update_sync_status(dir_path, "live_syncing", obs_path=obs_dst)
                update_sync_pid(dir_path, proc.pid)
                append_log(dir_path, f"在线同步已启动 (CLI): pid={proc.pid}")
    except Exception as e:
        eprint(f"[sync] backup db update failed: {e}")

    return 0


# ---------------------------------------------------------------------------
# cmd_sync_stop — stop sync daemon
# ---------------------------------------------------------------------------

def cmd_sync_stop(args: argparse.Namespace) -> int:
    state = load_state()
    state["source_env"] = get_selected_env(args, state)
    env_path = resolve_env_path(state["source_env"])
    service_key = get_service_key(env_path)
    sync_svcs = state.get("sync_services") or {}
    sync_svc = sync_svcs.get(service_key, {})

    pid_file_rel = sync_svc.get("pid_file")
    pid = sync_svc.get("pid")
    if pid_file_rel:
        pid_from_file = read_pid(BASE_DIR / pid_file_rel)
        if pid_from_file:
            pid = pid_from_file

    if not is_pid_running(pid):
        print(f"[sync] not running: env={service_key}")
        if pid_file_rel:
            (BASE_DIR / pid_file_rel).unlink(missing_ok=True)
        sync_svc["pid"] = None
        save_state(state)
        return 0

    print(f"[sync] stopping pid={pid} env={service_key} (waiting for final sync...)")
    os.kill(pid, signal.SIGTERM)
    force_killed = False
    for _ in range(1200):
        time.sleep(0.5)
        if not is_pid_running(pid):
            break

    if is_pid_running(pid):
        print(f"[sync] force kill pid={pid}")
        os.kill(pid, signal.SIGKILL)
        time.sleep(0.2)
        force_killed = True

    if pid_file_rel:
        (BASE_DIR / pid_file_rel).unlink(missing_ok=True)
    sync_svc["pid"] = None
    sync_svc["stopped_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    save_state(state)
    print("[sync] stopped")

    # 更新 backup DB：标记同步完成
    logs_dir_val = sync_svc.get("logs_dir", "")
    if logs_dir_val and logs_dir_val.startswith("logs_all/"):
        dir_path = logs_dir_val[len("logs_all/"):]
        try:
            # 从 pid_file 路径反推 svc_dir: logs/port{N}/{slug}/sync.pid
            svc_dir_str = ""
            if pid_file_rel:
                svc_dir_str = str((BASE_DIR / pid_file_rel).parent)
            elif sync_svc.get("log_file"):
                svc_dir_str = str((BASE_DIR / sync_svc["log_file"]).parent)
            if svc_dir_str:
                from utils.backup_store import init_db as init_backup_db, update_sync_status, update_sync_pid, append_log
                init_backup_db(svc_dir_str)
                if force_killed:
                    update_sync_status(dir_path, "error", error_msg="sync daemon force killed")
                    update_sync_pid(dir_path, None)
                    append_log(dir_path, "sync daemon force killed", level="error")
                else:
                    update_sync_status(dir_path, "done")
                    update_sync_pid(dir_path, None)
                    append_log(dir_path, "sync daemon stopped, final sync completed")
        except Exception as e:
            eprint(f"[sync] backup db update failed: {e}")

    return 0


# ---------------------------------------------------------------------------
# cmd_sync_logs — view sync daemon logs
# ---------------------------------------------------------------------------

def cmd_sync_logs(args: argparse.Namespace) -> int:
    state = load_state()
    state["source_env"] = get_selected_env(args, state)
    env_path = resolve_env_path(state["source_env"])
    service_key = get_service_key(env_path)
    service_slug = get_service_slug(service_key)
    sync_svc = (state.get("sync_services") or {}).get(service_key, {})

    log_file_rel = sync_svc.get("log_file")
    if log_file_rel:
        log_file = BASE_DIR / log_file_rel
    else:
        _, env_values, _, port, _, _ = state_runtime(state)
        key_suffix = get_api_key_suffix(env_values)
        svc_dir = _service_log_dir(port, service_slug, key_suffix)
        new_log = svc_dir / "sync.log"
        # COMPAT: 旧版 sync log 在 logs/sync-{slug}.log，可移除 old_log 分支
        old_log = LOG_DIR / f"sync-{service_slug}.log"
        log_file = new_log if new_log.exists() else old_log

    if not log_file.exists():
        eprint(f"[sync] log file not found: {log_file}")
        return 1

    if args.follow:
        try:
            subprocess.run(["tail", "-n", str(args.lines), "-f", str(log_file)], check=False)
        except KeyboardInterrupt:
            pass
        return 0

    sys.stdout.writelines(tail_lines(log_file, args.lines))
    return 0


# ---------------------------------------------------------------------------
# cmd_sync_status / cmd_sync_list — show sync services
# ---------------------------------------------------------------------------

def _print_sync_services(state: dict) -> int:
    sync_svcs = state.get("sync_services") or {}
    if not sync_svcs:
        print("[sync] no recorded sync services")
        return 0
    for key, svc in sync_svcs.items():
        pid = svc.get("pid")
        pid_file_rel = svc.get("pid_file")
        if pid_file_rel:
            pid_from_file = read_pid(BASE_DIR / pid_file_rel)
            if pid_from_file:
                pid = pid_from_file
        running = is_pid_running(pid)
        interval = svc.get("interval", "-")
        config_file = svc.get("config", "-")
        marker = "*" if key == state.get("source_env") else " "
        print(f"{marker} {key}: {'running' if running else 'stopped'} pid={pid or '-'} interval={interval}s config={config_file}")
        print(f"    src={svc.get('logs_dir', '-')}")
        print(f"    dst={svc.get('obs_dst', '-')}")
    return 0


def cmd_sync_status(_args: argparse.Namespace) -> int:
    state = load_state()
    cfg_rel = state.get("sync_config")
    if cfg_rel:
        print(f"[sync] config: {cfg_rel}")
    else:
        print("[sync] config: not set")
    return _print_sync_services(state)


def cmd_sync_list(_args: argparse.Namespace) -> int:
    state = load_state()
    return _print_sync_services(state)


# ---------------------------------------------------------------------------
# cmd_export — export session_index.jsonl (and optionally sync to OBS)
# ---------------------------------------------------------------------------

def _resolve_sess_env(args: argparse.Namespace) -> tuple[dict, str, Path, Path, str]:
    """返回 (state, service_key, svc_dir, env_base_dir, obs_base)
    env_base_dir = logs_all/{env-key}/ 即 mtime 的父目录
    """
    state = load_state()
    state["source_env"] = get_selected_env(args, state)
    env_path, env_values, host, port, _, _ = state_runtime(state)
    service_key = get_service_key(env_path)
    service_slug = get_service_slug(service_key)
    api_key_suffix = get_api_key_suffix(env_values)

    meta = _read_app_meta(port, service_slug, api_key_suffix)
    logs_dir = meta.get("logs_dir") or ""
    if logs_dir:
        abs_logs = str(BASE_DIR / logs_dir) if not os.path.isabs(logs_dir) else logs_dir
        env_base_dir = Path(abs_logs).parent
    else:
        env_base_dir = Path(".")

    svc_dir = _service_log_dir(port, service_slug, api_key_suffix)

    config = load_sync_config(state)
    obs_base = config.get("obs_base", "")

    return state, service_key, svc_dir, env_base_dir, obs_base


SESS_STATE_FILE = "sessions.json"


def _sess_state_path(svc_dir: Path) -> Path:
    return svc_dir / SESS_STATE_FILE


def _load_sess_state(svc_dir: Path) -> dict:
    sp = _sess_state_path(svc_dir)
    if sp.is_file():
        try:
            return json.loads(sp.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"current_mtime": None, "mtimes": {}}


def _save_sess_state(svc_dir: Path, sess_state: dict) -> None:
    svc_dir.mkdir(parents=True, exist_ok=True)
    sp = _sess_state_path(svc_dir)
    sp.write_text(json.dumps(sess_state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def cmd_export_run(args: argparse.Namespace) -> int:
    from utils.export_sync import export_session_index, sync_session_index

    state, _, svc_dir, env_base_dir, obs_base = _resolve_sess_env(args)
    sess_state = _load_sess_state(svc_dir)

    mtime = sess_state.get("current_mtime")
    if not mtime:
        eprint("[sess] 未设置 current_mtime. 先运行: sess list 查看可用目录, sess config <mtime> 选择")
        return 1

    logs_dir = str(env_base_dir / mtime)
    if not os.path.isdir(logs_dir):
        eprint(f"[sess] 目录不存在: {logs_dir}")
        return 1

    svc_dir.mkdir(parents=True, exist_ok=True)
    log_file = svc_dir / "sess.log"

    def _log(msg: str) -> None:
        line = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {msg}\n"
        print(msg)
        with log_file.open("a", encoding="utf-8") as f:
            f.write(line)

    _log(f"[sess] logs_dir: {logs_dir}")
    result = export_session_index(logs_dir, force=getattr(args, "force", False))
    if result["skipped"]:
        _log(f"[sess] 跳过 (无变更). 共 {result['total_sessions']} 条 session, 平均 {result['avg_msg_count']} 轮 msg")
    else:
        _log(f"[sess] 共 {result['total_sessions']} 条 session, 平均 {result['avg_msg_count']} 轮 msg")

    # 更新 sessions.json 中该 mtime 的状态
    mtime_state = sess_state.setdefault("mtimes", {}).setdefault(mtime, {})
    mtime_state["total_sessions"] = result["total_sessions"]
    mtime_state["avg_msg_count"] = result["avg_msg_count"]
    mtime_state["exported_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if not getattr(args, "sync", False):
        _save_sess_state(svc_dir, sess_state)
        return 0

    if not obs_base:
        eprint("[sess] sync 需要 obs_base 配置. Run: sync config <yaml_path>")
        _save_sess_state(svc_dir, sess_state)
        return 1

    env_key_name = env_base_dir.name
    now_date = datetime.now().strftime("%y%m%d%H")
    filter_key = getattr(args, "key", None)
    key_dir = "key-" + filter_key[-4:] if filter_key else "nokey"
    export_dir = f"ex-{now_date}"

    obs_dst = obs_base.rstrip("/") + "/session/" + env_key_name + "/" + mtime + "/" + key_dir + "/" + export_dir + "/"

    # 本地复制目录: logs_session 与 logs_all 同级
    logs_session_base = env_base_dir.parent.parent / "logs_session"
    local_copy_dir = str(logs_session_base / env_key_name / mtime / key_dir / export_dir)

    config = load_sync_config(state)
    workers = config.get("workers", 4)
    upload_script = config.get("upload_script")

    _log(f"[sess] 复制到 {local_copy_dir}")
    _log(f"[sess] 同步到 {obs_dst}")
    if filter_key:
        _log(f"[sess] 按 key 过滤: {filter_key}")
    sync_result = sync_session_index(
        logs_dir,
        obs_dst=obs_dst,
        workers=workers,
        upload_script=upload_script,
        key=filter_key,
        local_copy_dir=local_copy_dir,
        force=getattr(args, "force", False),
    )
    _log(f"[sess] 上传 {sync_result['uploaded']} 文件, {sync_result['failed']} 失败, {sync_result['skipped']} 跳过")

    slot_key = "key-" + filter_key[-4:] if filter_key else "nokey"
    slot_state = mtime_state.setdefault("slots", {}).setdefault(slot_key, {})
    slot_state["synced_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    slot_state["sync_uploaded"] = sync_result["uploaded"]
    slot_state["obs_dst"] = obs_dst
    _save_sess_state(svc_dir, sess_state)
    return 0


def cmd_export_list(args: argparse.Namespace) -> int:
    """扫描 env-key 下所有 mtime 目录，合并 sessions.json 状态显示。"""
    _, _, svc_dir, env_base_dir, _ = _resolve_sess_env(args)
    sess_state = _load_sess_state(svc_dir)
    current_mtime = sess_state.get("current_mtime")
    recorded = sess_state.get("mtimes", {})

    if not env_base_dir.is_dir():
        eprint(f"[sess] 目录不存在: {env_base_dir}")
        return 1

    mtimes = sorted(
        d.name for d in env_base_dir.iterdir() if d.is_dir()
    )
    if not mtimes:
        print(f"[sess] {env_base_dir.name}: 无 mtime 目录")
        return 0

    # 检测是否有新 mtime 未记录
    new_count = sum(1 for m in mtimes if m not in recorded)

    print(f"[sess] {env_base_dir.name} ({len(mtimes)} dirs, {new_count} new)")
    for m in mtimes:
        marker = "*" if m == current_mtime else " "
        info = recorded.get(m)
        if info:
            sessions = info.get("total_sessions", 0)
            avg_msg = info.get("avg_msg_count", 0)
            exported_at = info.get("exported_at", "-")
            line = f"{marker} {m}: {sessions} sessions, avg {avg_msg} msg, exported={exported_at}"
            print(line)
            slots = info.get("slots", {})
            for slot_name, slot_info in sorted(slots.items()):
                synced_at = slot_info.get("synced_at", "-")
                uploaded = slot_info.get("sync_uploaded", 0)
                obs_dst = slot_info.get("obs_dst", "")
                print(f"    [{slot_name}] synced={synced_at}, files={uploaded}")
                if obs_dst:
                    print(f"      obs: {obs_dst}")
        else:
            has_index = (env_base_dir / m / "index.jsonl").is_file()
            status = "(has index.jsonl)" if has_index else "(empty)"
            print(f"{marker} {m}: - {status}")
    return 0


def cmd_export_config(args: argparse.Namespace) -> int:
    """设置或显示当前 sess 操作的 mtime 目录。"""
    _, _, svc_dir, env_base_dir, _ = _resolve_sess_env(args)
    sess_state = _load_sess_state(svc_dir)

    mtime = getattr(args, "mtime", None)
    if not mtime:
        cur = sess_state.get("current_mtime") or "(not set)"
        print(f"[sess] current_mtime: {cur}")
        print(f"[sess] env_base_dir: {env_base_dir}")
        return 0

    target = env_base_dir / mtime
    if not target.is_dir():
        eprint(f"[sess] 目录不存在: {target}")
        return 1

    sess_state["current_mtime"] = mtime
    _save_sess_state(svc_dir, sess_state)
    print(f"[sess] current_mtime -> {mtime}")
    return 0


def cmd_export_logs(args: argparse.Namespace) -> int:
    """显示 sess export 的运行日志。"""
    _, _, svc_dir, _, _ = _resolve_sess_env(args)
    log_file = svc_dir / "sess.log"

    if not log_file.exists():
        eprint(f"[sess] log file not found: {log_file}")
        return 1

    if args.follow:
        try:
            subprocess.run(["tail", "-n", str(args.lines), "-f", str(log_file)], check=False)
        except KeyboardInterrupt:
            pass
        return 0

    sys.stdout.writelines(tail_lines(log_file, args.lines))
    return 0


def cmd_export_clear(args: argparse.Namespace) -> int:
    """清除当前 mtime 目录下的导出缓存文件和 sessions.json 中的记录。"""
    _, _, svc_dir, env_base_dir, _ = _resolve_sess_env(args)
    sess_state = _load_sess_state(svc_dir)

    mtime = sess_state.get("current_mtime")
    if not mtime:
        eprint("[sess] 未设置 current_mtime. 先运行: sess config <mtime>")
        return 1

    logs_dir = env_base_dir / mtime
    if not logs_dir.is_dir():
        eprint(f"[sess] 目录不存在: {logs_dir}")
        return 1

    cache_files = [
        ("session_index.jsonl", "session 索引"),
        (".sync_export_state.json", "上传状态记录"),
    ]

    removed = 0
    for name, desc in cache_files:
        fp = logs_dir / name
        if fp.is_file():
            fp.unlink()
            print(f"[sess] 已删除 {name} ({desc})")
            removed += 1

    mtime_info = sess_state.get("mtimes", {}).get(mtime)
    if mtime_info:
        del sess_state["mtimes"][mtime]
        _save_sess_state(svc_dir, sess_state)
        print(f"[sess] 已清除 sessions.json 中 {mtime} 的记录")

    if removed == 0 and not mtime_info:
        print(f"[sess] {mtime}: 无缓存需要清理")
    else:
        print(f"[sess] {mtime}: 清理完成 (删除 {removed} 个文件)")
    return 0


def _resolve_key_db_dir(args: argparse.Namespace) -> Path:
    """根据 --env 参数定位当前 port 的 SERVICE_LOG_DIR（keys.db 所在目录）。"""
    state = load_state()
    source_env = get_selected_env(args, state)
    env_path = resolve_env_path(source_env)
    env_values = parse_env_file(env_path)
    port = int(env_values.get("PROXY_PORT", "4000"))
    service_key = get_service_key(env_path)
    service_slug = get_service_slug(service_key)
    key_suffix = get_api_key_suffix(env_values)
    svc_dir = _service_log_dir(port, service_slug, key_suffix)
    svc_dir.mkdir(parents=True, exist_ok=True)
    return svc_dir


def cmd_key(args: argparse.Namespace) -> int:
    sys.path.insert(0, str(BASE_DIR))
    from utils.key_store import (
        init_db, add_key, list_keys, find_key,
        disable_key, enable_key, delete_key, mask_key,
    )
    from utils.key_config import load_key_state, init_key_config

    db_dir = _resolve_key_db_dir(args)
    init_db(str(db_dir))
    init_key_config(str(db_dir))
    cfg = load_key_state()
    action = args.key_action

    if action == "list":
        keys = list_keys()
        if not keys:
            print("No keys found.")
        else:
            print(f"{'ID':<5} {'Name':<20} {'Key':<25} {'Status':<10} {'Invite':<12} {'Created'}")
            print("-" * 100)
            for k in keys:
                invite = k.get('invite_code', '') or '-'
                print(f"{k['id']:<5} {k['name'] or '(unnamed)':<20} {k['key']:<25} {k['status']:<10} {invite:<12} {k['created_at']}")
        print(f"\n[key] db: {db_dir / 'keys.db'}")
        return 0

    if action == "add":
        spec = args.spec
        name, key_val = "", ""
        if ":" in spec:
            name, key_val = spec.split(":", 1)
        else:
            name = spec
        result = add_key(name=name, key=key_val, key_len=cfg.get("key_len", 24))
        print(f"Key created!")
        print(f"  ID:   {result['id']}")
        print(f"  Name: {result['name'] or '(unnamed)'}")
        print(f"  Key:  {result['key']}")
        return 0

    if action == "del":
        rec = find_key(args.identifier)
        if not rec:
            print(f"Key not found: {args.identifier}")
            return 1
        delete_key(rec["id"])
        print(f"Deleted key {rec['id']} ({mask_key(rec['key'])})")
        return 0

    if action == "stop":
        rec = find_key(args.identifier)
        if not rec:
            print(f"Key not found: {args.identifier}")
            return 1
        disable_key(rec["id"])
        print(f"Disabled key {rec['id']} ({mask_key(rec['key'])})")
        return 0

    if action == "start":
        rec = find_key(args.identifier)
        if not rec:
            print(f"Key not found: {args.identifier}")
            return 1
        enable_key(rec["id"])
        print(f"Enabled key {rec['id']} ({mask_key(rec['key'])})")
        return 0

    if action == "status":
        from utils.key_store import get_key_full
        rec = find_key(args.identifier)
        if not rec:
            print(f"Key not found: {args.identifier}")
            return 1
        full = get_key_full(rec["id"])
        print(f"  ID:          {full['id']}")
        print(f"  Name:        {full['name'] or '(unnamed)'}")
        print(f"  Key:         {full['key']}")
        print(f"  Status:      {full['status']}")
        print(f"  Invite Code: {full.get('invite_code') or '-'}")
        print(f"  Created:     {full['created_at']}")
        return 0

    if action == "config":
        from utils.key_config import apply_config
        config_file = getattr(args, "config_file", None)
        state_path = db_dir / "key_state.yaml"
        do_apply = getattr(args, "apply", False)

        if not config_file:
            if state_path.exists():
                print(state_path.read_text(encoding="utf-8"))
            else:
                print(f"No state file at {state_path}")
                print("Run: key config settings/keys.yaml")
            return 0

        src = Path(config_file).resolve()
        if not src.exists():
            print(f"Config file not found: {config_file}")
            return 1

        parsed = apply_config(str(src), str(state_path))
        print(f"[key] State saved: {state_path}")
        if parsed.get("user"):
            print(f"[key] User: {parsed['user']}")
        if parsed.get("password"):
            print(f"[key] Password: set")
        codes = parsed.get("invite_codes", [])
        if codes:
            print(f"[key] Invite codes: {len(codes)} configured")
        print(f"[key] Key length: {parsed.get('key_len', 24)} bytes")

        if do_apply:
            keys_to_import = parsed.get("keys") or []
            imported = 0
            for entry in keys_to_import:
                if not isinstance(entry, dict):
                    continue
                name = entry.get("name", "")
                value = entry.get("value", "")
                if not value:
                    continue
                existing = find_key(value)
                if existing:
                    print(f"  skip (exists): {name} -> {mask_key(value)}")
                    continue
                add_key(name=name, key=value)
                print(f"  imported: {name} -> {mask_key(value)}")
                imported += 1
            if keys_to_import:
                print(f"[key] Imported {imported}/{len(keys_to_import)} keys to DB")
            elif not keys_to_import:
                print(f"[key] No keys to import")

        return 0

    return 0


# ---------------------------------------------------------------------------
# 独立 worker 进程管理（导出 export_worker / 回填 backfill_worker），仿 sync 守护模式
#
# 两个 worker 各是独立 `python -m` 入口、独立 pid/log、独立信号处理，共用下面这套
# 参数化的 start/stop/logs/status 逻辑。每类 worker 用一个 WorkerSpec 描述。
# ---------------------------------------------------------------------------

class WorkerSpec:
    def __init__(self, name: str, module: str, state_key: str, instances_env: str = ""):
        self.name = name              # 展示用 & pid/log 文件名前缀，如 "export"
        self.module = module          # python -m 目标，如 "utils.export_worker"
        self.state_key = state_key    # .cli_state.yaml 里的服务表键，如 "export_services"
        self.pid_name = f"{name}_worker.pid"
        self.log_name = f"{name}_worker.log"
        # 决定默认起几个实例的环境变量名（空 = 固定 1 个）。
        # 并发靠多进程而非单进程多线程：导出链路前半段是纯 Python，不释放 GIL，
        # 同进程内开多线程做 json.dumps 只有 0.98x 加速（实测）。
        self.instances_env = instances_env


def _worker_instance_pid_name(spec: WorkerSpec, idx: int) -> str:
    """第 idx 个实例的 pid 文件名。idx=0 沿用原文件名，兼容既有部署。"""
    if idx == 0:
        return spec.pid_name
    stem = spec.pid_name[:-len(".pid")]
    return f"{stem}.{idx}.pid"


def _worker_instance_log_name(spec: WorkerSpec, idx: int) -> str:
    """第 idx 个实例的日志文件名。所有实例共用一个日志更容易排查，故不拆。"""
    return spec.log_name


def _worker_instance_count(spec: WorkerSpec, args: argparse.Namespace,
                           env_values: dict | None = None) -> int:
    """本次要确保多少个实例在跑。

    优先级：--instances 显式指定 > env 配置（如 EXPORT_CONCURRENCY）> 1。

    注意必须优先从 env_values 里取：app 进程自己并没有把 .env 加载进 os.environ，
    只有 worker 子进程才通过 child_env 拿到这些值。只查 os.getenv 会永远读到空。
    """
    explicit = getattr(args, "instances", None)
    if explicit is not None:
        return max(1, int(explicit))
    if not spec.instances_env:
        return 1
    raw = (env_values or {}).get(spec.instances_env) or os.getenv(spec.instances_env, "1")
    try:
        return max(1, int(raw))
    except (TypeError, ValueError):
        return 1


_WORKER_SPECS = {
    "export": WorkerSpec("export", "utils.export_worker", "export_services",
                         instances_env="EXPORT_CONCURRENCY"),
    "backfill": WorkerSpec("backfill", "src.backfill.worker", "backfill_services"),
    # new-api 实时索引：每源发现线程 + 8 线程同步池，独立进程跑。
    "watch": WorkerSpec("watch", "src.watch.worker", "watch_services"),
}


def _worker_svc_dir(state: dict):
    """解析当前 env 的 svc_dir + service_key（与 app/sync 同源）。"""
    env_path, env_values, _host, port, _, _ = state_runtime(state)
    service_key = get_service_key(env_path)
    service_slug = get_service_slug(service_key)
    api_key_suffix = get_api_key_suffix(env_values)
    svc_dir = _service_log_dir(port, service_slug, api_key_suffix)
    return svc_dir, service_key, service_slug, env_path, env_values


def _worker_start(spec: WorkerSpec, args: argparse.Namespace) -> int:
    state = load_state()
    state["source_env"] = get_selected_env(args, state)
    svc_dir, service_key, service_slug, env_path, env_values = _worker_svc_dir(state)
    svc_dir.mkdir(parents=True, exist_ok=True)
    log_file = svc_dir / spec.log_name

    # 目标实例数：--instances > spec.instances_env（如 EXPORT_CONCURRENCY）> 1
    want = _worker_instance_count(spec, args, env_values)

    # child_env 与 app 完全一致，保证 worker 的 get_service_log_dir() 落到同一 svc_dir，
    # 命中同一 DB / 同一槽位锁目录（跨进程互斥的前提）。
    child_env = os.environ.copy()
    child_env.update(env_values)
    child_env["ENV_FILE"] = str(env_path)
    child_env["LOG_TASK_TAG"] = service_slug

    svcs = state.setdefault(spec.state_key, {})
    svc = svcs.setdefault(service_key, {})
    instances = svc.setdefault("instances", {})

    started, already = [], []
    for idx in range(want):
        pid_name = _worker_instance_pid_name(spec, idx)
        pid_file = svc_dir / pid_name
        pid = read_pid(pid_file)
        if is_pid_running(pid):
            already.append((idx, pid))
            instances[str(idx)] = {"pid": pid, "pid_file": os.path.relpath(pid_file, BASE_DIR)}
            continue

        with log_file.open("ab") as log_fp:
            proc = subprocess.Popen(
                [sys.executable, "-m", spec.module],
                cwd=str(BASE_DIR),
                env=child_env,
                stdout=log_fp,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        pid_file.write_text(f"{proc.pid}\n", encoding="utf-8")
        time.sleep(1)
        if not is_pid_running(proc.pid):
            eprint(f"[{spec.name}#{idx}] failed to start, check log: {log_file}")
            continue
        instances[str(idx)] = {"pid": proc.pid, "pid_file": os.path.relpath(pid_file, BASE_DIR)}
        started.append((idx, proc.pid))

    # 实例数调小（例如 EXPORT_CONCURRENCY 3→1）时，多余实例留着占槽位，需手动 stop。
    extra = [k for k in instances if int(k) >= want]
    if extra:
        print(f"[{spec.name}] 注意：仍有 {len(extra)} 个多余实例（索引 {', '.join(sorted(extra))}）"
              f"不在本次目标数 {want} 内，如需停止请手动处理对应 pid 文件")

    # 兼容旧字段：pid/pid_file 指向第 0 个实例
    if "0" in instances:
        svc["pid"] = instances["0"]["pid"]
        svc["pid_file"] = instances["0"]["pid_file"]
    svc["log_file"] = os.path.relpath(log_file, BASE_DIR)
    svc["started_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    save_state(state)

    for idx, pid in started:
        print(f"[{spec.name}#{idx}] started: pid={pid} env={service_key}")
    for idx, pid in already:
        print(f"[{spec.name}#{idx}] already running: pid={pid} env={service_key}")
    print(f"[{spec.name}] {len(instances)} 个实例 pid 记录完毕，log -> {log_file}")
    return 0


def _worker_stop(spec: WorkerSpec, args: argparse.Namespace) -> int:
    state = load_state()
    state["source_env"] = get_selected_env(args, state)
    env_path = resolve_env_path(state["source_env"])
    service_key = get_service_key(env_path)
    svc = (state.get(spec.state_key) or {}).get(service_key, {})
    instances = svc.get("instances") or {}

    # 收集所有实例的 pid：有新格式的 instances，也兼容只有 pid/pid_file 的旧记录。
    targets = {}
    for idx, inst in instances.items():
        pf = inst.get("pid_file")
        pid = read_pid(BASE_DIR / pf) if pf else None
        targets[idx] = (pid or inst.get("pid"), pf)
    if not targets and (svc.get("pid_file") or svc.get("pid")):
        pf = svc.get("pid_file")
        pid = read_pid(BASE_DIR / pf) if pf else None
        targets["0"] = (pid or svc.get("pid"), pf)

    alive = {k: v for k, v in targets.items() if is_pid_running(v[0])}
    if not alive:
        print(f"[{spec.name}] not running: env={service_key}")
        for _k, (_p, pf) in targets.items():
            if pf:
                (BASE_DIR / pf).unlink(missing_ok=True)
        if svc:
            svc["pid"] = None
            svc["instances"] = {}
            save_state(state)
        return 0

    print(f"[{spec.name}] stopping {len(alive)} 个实例 ({', '.join('#' + k for k in sorted(alive))}) "
          f"env={service_key} (waiting for current task to reach a checkpoint...)")
    # 按进程组发 SIGTERM：worker 在 _interruptible_sleep 分片醒来，正在跑的任务在
    # 目录/叶子检查点自然收尾。先全部发信号，再统一等待，避免串行等待叠加。
    for _k, (pid, _pf) in alive.items():
        _signal_service(pid, signal.SIGTERM)
    for _ in range(240):
        if not any(is_pid_running(pid) for pid, _pf in alive.values()):
            break
        time.sleep(0.5)
    for k, (pid, _pf) in alive.items():
        if is_pid_running(pid):
            print(f"[{spec.name}#{k}] force kill pid={pid}")
            _signal_service(pid, signal.SIGKILL)
    time.sleep(0.2)

    for _k, (_p, pf) in targets.items():
        if pf:
            (BASE_DIR / pf).unlink(missing_ok=True)
    if svc:
        svc["pid"] = None
        svc["pid_file"] = None
        svc["instances"] = {}
        svc["stopped_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        save_state(state)
    print(f"[{spec.name}] stopped")
    return 0


def _worker_logs(spec: WorkerSpec, args: argparse.Namespace) -> int:
    state = load_state()
    state["source_env"] = get_selected_env(args, state)
    env_path = resolve_env_path(state["source_env"])
    service_key = get_service_key(env_path)
    svc = (state.get(spec.state_key) or {}).get(service_key, {})

    log_file_rel = svc.get("log_file")
    if log_file_rel:
        log_file = BASE_DIR / log_file_rel
    else:
        svc_dir, _sk, _slug, _ep, _ev = _worker_svc_dir(state)
        log_file = svc_dir / spec.log_name

    if not log_file.exists():
        eprint(f"[{spec.name}] log file not found: {log_file}")
        return 1

    if args.follow:
        try:
            subprocess.run(["tail", "-n", str(args.lines), "-f", str(log_file)], check=False)
        except KeyboardInterrupt:
            pass
        return 0
    sys.stdout.writelines(tail_lines(log_file, args.lines))
    return 0


def _print_worker_services(spec: WorkerSpec, state: dict) -> int:
    svcs = state.get(spec.state_key) or {}
    if not svcs:
        print(f"[{spec.name}] no recorded {spec.name} workers")
        return 0
    for key, svc in svcs.items():
        marker = "*" if key == state.get("source_env") else " "
        instances = svc.get("instances") or {}
        if instances:
            running_n = 0
            for idx in sorted(instances, key=int):
                inst = instances[idx]
                pf = inst.get("pid_file")
                pid = (read_pid(BASE_DIR / pf) if pf else None) or inst.get("pid")
                ok = is_pid_running(pid)
                running_n += 1 if ok else 0
                print(f"{marker} {key}#{idx}: {'running' if ok else 'stopped'} pid={pid or '-'}")
            print(f"{marker} {key}: {running_n}/{len(instances)} 个实例在跑 "
                  f"started={svc.get('started_at', '-')}")
            if running_n == 0:
                print(f"    (start with: ./app {spec.name} start)")
            continue

        pid = svc.get("pid")
        pid_file_rel = svc.get("pid_file")
        if pid_file_rel:
            pid_from_file = read_pid(BASE_DIR / pid_file_rel)
            if pid_from_file:
                pid = pid_from_file
        running = is_pid_running(pid)
        print(f"{marker} {key}: {'running' if running else 'stopped'} "
              f"pid={pid or '-'} started={svc.get('started_at', '-')}")
        if not running:
            print(f"    (start with: ./app {spec.name} start)")
    return 0


def cmd_export_worker(args: argparse.Namespace) -> int:
    spec = _WORKER_SPECS["export"]
    action = args.export_action
    if action == "start":
        return _worker_start(spec, args)
    if action == "stop":
        return _worker_stop(spec, args)
    if action == "logs":
        return _worker_logs(spec, args)
    if action == "status":
        return _print_worker_services(spec, load_state())
    if action == "list":
        return _print_worker_services(spec, load_state())
    return 1


def cmd_backfill_worker(args: argparse.Namespace) -> int:
    spec = _WORKER_SPECS["backfill"]
    action = args.backfill_action
    if action == "start":
        return _worker_start(spec, args)
    if action == "stop":
        return _worker_stop(spec, args)
    if action == "logs":
        return _worker_logs(spec, args)
    if action == "status":
        return _print_worker_services(spec, load_state())
    if action == "list":
        return _print_worker_services(spec, load_state())
    return 1


def cmd_watch_worker(args: argparse.Namespace) -> int:
    """new-api 实时索引 worker（每源发现线程 + 同步池）。"""
    spec = _WORKER_SPECS["watch"]
    action = args.watch_action
    if action == "start":
        return _worker_start(spec, args)
    if action == "stop":
        return _worker_stop(spec, args)
    if action == "logs":
        return _worker_logs(spec, args)
    if action == "status":
        return _print_worker_services(spec, load_state())
    if action == "list":
        return _print_worker_services(spec, load_state())
    return 1


def _start_all_workers(args: argparse.Namespace) -> None:
    """随 app 一起拉起导出 / 回填 / 实时索引三个独立 worker（各自幂等：已在跑则跳过）。

    执行已从 app 进程剥离到独立进程，若不随 app 启动，导出会一直停在 queued、
    回填停在 pending、new-api 叶子不再实时同步。默认随 `app start` 启动，
    `--no-workers` 可关闭。
    """
    for name in ("export", "backfill", "watch"):
        try:
            _worker_start(_WORKER_SPECS[name], args)
        except Exception as e:
            eprint(f"[{name}] start failed: {e}")


def _stop_all_workers(args: argparse.Namespace) -> None:
    """随 app 一起停止两个 worker（各自幂等：未在跑则跳过）。"""
    for name in ("export", "backfill"):
        try:
            _worker_stop(_WORKER_SPECS[name], args)
        except Exception as e:
            eprint(f"[{name}] stop failed: {e}")


def _add_worker_subparser(subparsers, name: str, dest: str, cmd_func) -> None:
    """给 build_parser 注册一个 worker 命令组（start/stop/logs/status/list）。"""
    p = subparsers.add_parser(name, help=f"Manage {name} worker daemon (start/stop/logs/status)")
    sub = p.add_subparsers(dest=dest, required=True)

    ps = sub.add_parser("start", help=f"Start {name} worker daemon")
    ps.add_argument("--env", dest="env_file", help="Env file to use")
    ps.add_argument("--instances", type=int, default=None,
                    help="要确保在跑的实例数（默认取环境变量，如 EXPORT_CONCURRENCY；否则 1）")
    ps.set_defaults(func=cmd_func)

    pt = sub.add_parser("stop", help=f"Stop {name} worker daemon")
    pt.add_argument("--env", dest="env_file", help="Env file whose worker to stop")
    pt.set_defaults(func=cmd_func)

    pl = sub.add_parser("logs", help=f"Show {name} worker logs")
    pl.add_argument("--env", dest="env_file", help="Env file whose worker logs to show")
    pl.add_argument("-f", "--follow", action="store_true", help="Follow the log file")
    pl.add_argument("-n", "--lines", type=int, default=100, help="Number of lines to show")
    pl.set_defaults(func=cmd_func)

    pst = sub.add_parser("status", help=f"Show {name} worker status")
    pst.set_defaults(func=cmd_func)

    pls = sub.add_parser("list", help=f"List all recorded {name} workers")
    pls.set_defaults(func=cmd_func)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="LLM proxy service CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    p_start = subparsers.add_parser("start", help="Start app.py using configured source_env")
    p_start.add_argument("--env", dest="env_file", help="Env file to use for this start")
    p_start.add_argument("--sync", action="store_true", help="Also start sync daemon (requires obs_base configured)")
    p_start.add_argument("--no-workers", dest="no_workers", action="store_true",
                         help="Do not start export/backfill workers with the app")
    p_start.set_defaults(func=cmd_start)

    p_stop = subparsers.add_parser("stop", help="Stop the running app")
    p_stop.add_argument("--env", dest="env_file", help="Env file to stop")
    p_stop.add_argument("--with-workers", dest="with_workers", action="store_true",
                        help="同时停掉 export/backfill worker（默认不动：worker 是独立进程，可跨平台重启存活）")
    p_stop.set_defaults(func=cmd_stop)

    p_restart = subparsers.add_parser("restart", help="Restart the app")
    p_restart.add_argument("--env", dest="env_file", help="Env file to restart")
    p_restart.add_argument("--sync", action="store_true", help="Also start sync daemon after restart")
    p_restart.add_argument("--no-workers", dest="no_workers", action="store_true",
                           help="重启后不补齐 export/backfill worker（默认会补齐缺失的实例）")
    p_restart.add_argument("--with-workers", dest="with_workers", action="store_true",
                           help="重启时连同 worker 一起停掉再拉起（默认不停，worker 跨重启存活）")
    p_restart.set_defaults(func=cmd_restart)

    p_logs = subparsers.add_parser("logs", help="Show log output")
    p_logs.add_argument("--env", dest="env_file", help="Env file whose log should be shown")
    p_logs.add_argument("-f", "--follow", action="store_true", help="Follow the log file")
    p_logs.add_argument("-n", "--lines", type=int, default=100, help="Number of lines to show")
    p_logs.set_defaults(func=cmd_logs)

    p_config = subparsers.add_parser("config", help="Show or update source_env")
    p_config.add_argument("env_file", nargs="?", help="Env file path, such as .env or .env.prod")
    p_config.set_defaults(func=cmd_config)

    p_status = subparsers.add_parser("status", help="Show current service status")
    p_status.set_defaults(func=cmd_status)

    p_list = subparsers.add_parser("list", help="List all recorded env services")
    p_list.set_defaults(func=cmd_list)

    p_connect = subparsers.add_parser("connect", help="Connection test (proxy or upstream)")
    p_connect.add_argument("--env", dest="env_file", help="Env file to use")
    p_connect.add_argument("--model", default="claude-sonnet-4-6", help="Model to test (default: claude-sonnet-4-6)")
    p_connect.add_argument("--method", default="anthropic", choices=["anthropic", "openai"], help="API protocol (default: anthropic)")
    p_connect.add_argument("--noproxy", action="store_true", help="Test upstream directly, bypassing proxy")
    p_connect.add_argument("--timeout", type=int, default=30, help="Request timeout in seconds")
    p_connect.add_argument("--message", "-m", default="", help="Custom prompt message")
    p_connect.add_argument("--key", default="", help="Specify API key to test (overrides auto-detection)")
    p_connect.set_defaults(func=cmd_connect)

    # --- sess subcommands ---
    p_sess = subparsers.add_parser("sess", help="Session index management (export/list/logs)")
    sess_sub = p_sess.add_subparsers(dest="sess_action", required=True)

    pe_export = sess_sub.add_parser("export", help="Export session_index.jsonl for current mtime")
    pe_export.add_argument("--env", dest="env_file", help="Env file to resolve meta from")
    pe_export.add_argument("--sync", action="store_true", help="Also upload triplets to OBS")
    pe_export.add_argument("--force", action="store_true", help="Force re-export even if unchanged")
    pe_export.add_argument("--key", help="Filter sessions by api_key (exact match)")
    pe_export.set_defaults(func=cmd_export_run)

    pe_config = sess_sub.add_parser("config", help="Set or show current mtime directory")
    pe_config.add_argument("mtime", nargs="?", help="mtime directory to switch to (e.g. 26060317)")
    pe_config.add_argument("--env", dest="env_file", help="Env file to resolve meta from")
    pe_config.set_defaults(func=cmd_export_config)

    pe_list = sess_sub.add_parser("list", help="List all mtime directories and export status")
    pe_list.add_argument("--env", dest="env_file", help="Env file to resolve meta from")
    pe_list.set_defaults(func=cmd_export_list)

    pe_logs = sess_sub.add_parser("logs", help="Show sess export run logs")
    pe_logs.add_argument("--env", dest="env_file", help="Env file to resolve meta from")
    pe_logs.add_argument("-f", "--follow", action="store_true", help="Follow the log file")
    pe_logs.add_argument("-n", "--lines", type=int, default=100, help="Number of lines to show")
    pe_logs.set_defaults(func=cmd_export_logs)

    pe_clear = sess_sub.add_parser("clear", help="Clear export cache for current mtime")
    pe_clear.add_argument("--env", dest="env_file", help="Env file to resolve meta from")
    pe_clear.set_defaults(func=cmd_export_clear)

    # --- sync subcommands ---
    p_sync = subparsers.add_parser("sync", help="Manage sync daemon (start/stop/logs/status)")
    sync_sub = p_sync.add_subparsers(dest="sync_action", required=True)

    ps_start = sync_sub.add_parser("start", help="Start sync daemon")
    ps_start.add_argument("--env", dest="env_file", help="Env file to sync")
    ps_start.add_argument("--interval", type=int, default=None, help="Override sync interval (seconds)")
    ps_start.set_defaults(func=cmd_sync)

    ps_stop = sync_sub.add_parser("stop", help="Stop sync daemon")
    ps_stop.add_argument("--env", dest="env_file", help="Env file whose sync to stop")
    ps_stop.set_defaults(func=cmd_sync_stop)

    ps_logs = sync_sub.add_parser("logs", help="Show sync daemon logs")
    ps_logs.add_argument("--env", dest="env_file", help="Env file whose sync logs to show")
    ps_logs.add_argument("-f", "--follow", action="store_true", help="Follow the log file")
    ps_logs.add_argument("-n", "--lines", type=int, default=100, help="Number of lines to show")
    ps_logs.set_defaults(func=cmd_sync_logs)

    ps_status = sync_sub.add_parser("status", help="Show sync status and config")
    ps_status.set_defaults(func=cmd_sync_status)

    ps_list = sync_sub.add_parser("list", help="List all recorded sync services")
    ps_list.set_defaults(func=cmd_sync_list)

    ps_config = sync_sub.add_parser("config", help="Set or show sync config file")
    ps_config.add_argument("config_file", nargs="?", help="YAML config file path, e.g. settings/obs_base.yaml")
    ps_config.set_defaults(func=cmd_sync_config)

    # --- export / backfill / watch worker subcommands（独立进程，仿 sync 守护模式）---
    _add_worker_subparser(subparsers, "export", "export_action", cmd_export_worker)
    _add_worker_subparser(subparsers, "backfill", "backfill_action", cmd_backfill_worker)
    _add_worker_subparser(subparsers, "watch", "watch_action", cmd_watch_worker)

    # --- key subcommands ---
    p_key = subparsers.add_parser("key", help="API key management (list/add/del/stop/start/config)")
    key_sub = p_key.add_subparsers(dest="key_action", required=True)

    pk_list = key_sub.add_parser("list", help="List all keys")
    pk_list.add_argument("--env", dest="env_file", help="Env file to resolve port from")
    pk_list.set_defaults(func=cmd_key)

    pk_add = key_sub.add_parser("add", help="Add a key. Format: name:key or just name")
    pk_add.add_argument("spec", nargs="?", default="", help="name:key or name (key auto-generated)")
    pk_add.add_argument("--env", dest="env_file", help="Env file to resolve port from")
    pk_add.set_defaults(func=cmd_key)

    pk_del = key_sub.add_parser("del", help="Delete a key by ID or key value")
    pk_del.add_argument("identifier", help="Key ID or key value")
    pk_del.add_argument("--env", dest="env_file", help="Env file to resolve port from")
    pk_del.set_defaults(func=cmd_key)

    pk_stop = key_sub.add_parser("stop", help="Disable a key")
    pk_stop.add_argument("identifier", help="Key ID or key value")
    pk_stop.add_argument("--env", dest="env_file", help="Env file to resolve port from")
    pk_stop.set_defaults(func=cmd_key)

    pk_start = key_sub.add_parser("start", help="Enable a key")
    pk_start.add_argument("identifier", help="Key ID or key value")
    pk_start.add_argument("--env", dest="env_file", help="Env file to resolve port from")
    pk_start.set_defaults(func=cmd_key)

    pk_status = key_sub.add_parser("status", help="Show key details by ID or key value")
    pk_status.add_argument("identifier", help="Key ID or key value")
    pk_status.add_argument("--env", dest="env_file", help="Env file to resolve port from")
    pk_status.set_defaults(func=cmd_key)

    pk_config = key_sub.add_parser("config", help="Set key config (invite code, password, keys)")
    pk_config.add_argument("config_file", nargs="?", help="YAML config file path, e.g. settings/keys.yaml")
    pk_config.add_argument("--apply", action="store_true", help="Also import keys from yaml into DB")
    pk_config.add_argument("--env", dest="env_file", help="Env file to resolve port from")
    pk_config.set_defaults(func=cmd_key)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
