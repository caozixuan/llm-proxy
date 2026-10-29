"""
utils/obs_utils.py — OBS 路径管理与工具函数

集中管理：
- obsutil 二进制路径
- obs_base 配置读取（唯一来源：sync_config 指向的 yaml，如 settings/obs_rl.yaml）
- 上传命令（_run_upload_cmd）
- 目录/文件列表（obsutil ls）
"""

import glob
import os
import re
import subprocess
from pathlib import Path
from subprocess import PIPE, STDOUT, Popen
from typing import Dict, List, Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OBSUTIL_BIN = str(PROJECT_ROOT / "tools" / "obsutil" / "obsutil")
DEFAULT_UPLOAD_SCRIPT = str(PROJECT_ROOT / "tools" / "obs_upload.sh")
DEFAULT_DOWNLOAD_SCRIPT = str(PROJECT_ROOT / "tools" / "obs_download.sh")

# sync_config 未初始化时的默认配置文件（.cli_state.yaml 里没有 sync_config 时回退）
DEFAULT_SYNC_CONFIG = PROJECT_ROOT / "settings" / "obs_base.yaml"


# ---------------------------------------------------------------------------
# obs_base 配置读取
# ---------------------------------------------------------------------------

def get_sync_config_path() -> Optional[Path]:
    """返回 sync_config YAML 文件路径。

    优先用 .cli_state.yaml 的 sync_config 指向的文件；未配置或不存在时，
    回退到默认的 settings/obs_base.yaml（若存在）。
    """
    import yaml

    cli_state_path = PROJECT_ROOT / ".cli_state.yaml"
    if cli_state_path.is_file():
        try:
            with open(cli_state_path, "r", encoding="utf-8") as f:
                state = yaml.safe_load(f) or {}
            sync_cfg = state.get("sync_config", "")
            if sync_cfg:
                p = Path(sync_cfg)
                if not p.is_absolute():
                    p = PROJECT_ROOT / p
                if p.is_file():
                    return p
        except Exception:
            pass
    # 回退默认配置
    return DEFAULT_SYNC_CONFIG if DEFAULT_SYNC_CONFIG.is_file() else None


def load_sync_config() -> dict:
    """读取 sync_config 配置文件（obs_base, interval, workers, upload_script）。

    未初始化时回退到默认 settings/obs_base.yaml。
    """
    import yaml

    p = get_sync_config_path()
    if p is None:
        return {}
    try:
        with open(p, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


def load_obs_base() -> str:
    """读取 obs_base。来源：sync_config 指向的配置文件（如 settings/obs_rl.yaml）；
    未初始化时回退默认 settings/obs_base.yaml。"""
    return load_sync_config().get("obs_base", "").strip().rstrip("/")


# ---------------------------------------------------------------------------
# 上传
# ---------------------------------------------------------------------------

def run_upload_cmd(
    local: str,
    dst: str,
    upload_script: Optional[str] = None,
    timeout: Optional[int] = None,
    jobs: Optional[int] = None,
    log_cb=None,
) -> Tuple[bool, str]:
    """流式执行上传脚本（Popen 模式），实时打印进度。timeout/jobs 默认从 sync_config 读取。
    log_cb: 可选回调 fn(str)，每 10 秒汇报一次上传进度（避免 progress_log 过于庞大）。
    """
    import time as _time
    cfg = load_sync_config()
    if timeout is None:
        timeout = int(cfg.get("upload_timeout", 3600))
    if jobs is None:
        jobs = int(cfg.get("upload_jobs", 8))

    if upload_script is None:
        upload_script = DEFAULT_UPLOAD_SCRIPT
    else:
        p = Path(upload_script)
        if not p.is_absolute():
            upload_script = str((PROJECT_ROOT / p).resolve())

    cmd = [upload_script, local, dst, str(jobs)]
    try:
        _t_start = _time.monotonic()
        proc = Popen(cmd, stdout=PIPE, stderr=STDOUT, text=True)
        lines: List[str] = []
        line_count = 0
        last_log_ts = _time.monotonic()
        for line in proc.stdout:
            line = line.rstrip()
            if not line:
                continue
            lines.append(line)
            line_count += 1
            print(line, flush=True)
            if log_cb:
                now = _time.monotonic()
                if now - last_log_ts >= 10:
                    try:
                        log_cb(f"上传中... 已处理 {line_count} 行，最新: {line[-120:]}")
                    except Exception:
                        pass
                    last_log_ts = now
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
            if log_cb:
                try:
                    log_cb(f"上传超时（{timeout}s，已用 {_time.monotonic() - _t_start:.1f}s）")
                except Exception:
                    pass
            return False, f"Command timed out after {timeout}s"
        # 上传段耗时 + obsutil 自报的字节数/吞吐：这是「复制上传时间」的客观分母，
        # 供上层对比总耗时（obsutil 输出样例见 tools/obs_upload.sh 的 Succeed bytes/Metrics 行）。
        _up_s = _time.monotonic() - _t_start
        _bytes = _tps = None
        for ln in reversed(lines[-40:]):
            if _bytes is None:
                m = re.search(r"Succeed bytes:\s*([\d.]+\s*[A-Za-z]+)", ln)
                if m:
                    _bytes = m.group(1)
            if _tps is None:
                m = re.search(r"average tps:([\d.]+)", ln)
                if m:
                    _tps = m.group(1)
            if _bytes and _tps:
                break
        if log_cb:
            _extra = ""
            if _bytes:
                _extra += f"，{_bytes}"
            if _tps:
                _extra += f"，平均 {_tps} obj/s"
            try:
                log_cb(f"[耗时] 上传 OBS: {_up_s:.2f}s{_extra}")
            except Exception:
                pass
        if proc.returncode == 0:
            last = lines[-1] if lines else ""
            if log_cb and last:
                try:
                    log_cb(f"obsutil: {last}")
                except Exception:
                    pass
            return True, last
        tail = "\n".join(lines[-5:]) if lines else "upload failed"
        if log_cb and tail:
            try:
                log_cb(f"obsutil 错误: {tail[-200:]}")
            except Exception:
                pass
        return False, tail
    except Exception as e:
        return False, str(e)


# ---------------------------------------------------------------------------
# 下载
# ---------------------------------------------------------------------------

def run_download_cmd(
    obs_path: str,
    local: str,
    download_script: Optional[str] = None,
    timeout: int = 300,
) -> Tuple[bool, str]:
    """执行下载脚本，将 OBS 文件/目录下载到本地。"""
    if download_script is None:
        download_script = DEFAULT_DOWNLOAD_SCRIPT
    else:
        p = Path(download_script)
        if not p.is_absolute():
            download_script = str((PROJECT_ROOT / p).resolve())

    cmd = [download_script, obs_path, local]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        if result.returncode == 0:
            return True, result.stdout.strip()
        return False, (result.stderr or result.stdout).strip()
    except Exception as e:
        return False, str(e)


# ---------------------------------------------------------------------------
# 目录/文件列表
# ---------------------------------------------------------------------------

_DU_RE = re.compile(r"\[DU\]\s+Total prefix\s+\[[^\]]*\]\s+size:\s*([0-9.]+)\s*([KMGT]?B)", re.I)


def obsutil_prefix_size(obs_url: str, timeout: int = 600) -> Optional[int]:
    """递归统计某个 OBS 前缀的实际总字节数。失败返回 None。

    这是**删除前的强校验**：拿 OBS 上真实存在的体积与本地记录比对。
    代价高（要递归列举所有对象，单叶几十万文件要几十秒到几分钟），
    所以只在「保留期已到、准备删除」时逐库调用，不做实时展示。

    输出形如：`[DU] Total prefix [raw/.../]: size: 59.73GB`
    """
    if not obs_url or not os.path.isfile(OBSUTIL_BIN):
        return None
    url = obs_url if obs_url.endswith("/") else obs_url + "/"
    try:
        r = subprocess.run([OBSUTIL_BIN, "ls", url, "-du"],
                           capture_output=True, text=True, timeout=timeout)
        if r.returncode != 0:
            return None
    except Exception:
        return None
    m = _DU_RE.search(r.stdout or "")
    if not m:
        return None
    return _parse_size_bytes(m.group(1) + m.group(2).upper())


def obsutil_ls(path: str, show_dirs: bool = False, limit: int = 1000) -> List[dict]:
    """执行 obsutil ls -d，只列出当前一层的子目录和文件（不递归）。

    obsutil ls -d 输出分两段：
        Folder list:
        obs://.../subdir/
        ...
        Object list:
        key   LastModified   Size   StorageClass   ETag
        obs://.../file.json
                          2026-...   472B   standard   "..."
    文件的元数据（大小等）在文件 key 的下一行，需要跨行解析。
    参数 show_dirs 保留兼容，实际始终 -d。
    """
    if not os.path.isfile(OBSUTIL_BIN):
        return []
    if not path.endswith("/"):
        path += "/"
    args = [OBSUTIL_BIN, "ls", path, "-d", f"-limit={limit}"]
    try:
        result = subprocess.run(args, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            return []
    except Exception:
        return []

    dirs: List[dict] = []
    files: List[dict] = []
    section = None  # "folder" | "object"
    lines = result.stdout.splitlines()
    i = 0
    while i < len(lines):
        raw = lines[i]
        line = raw.strip()
        i += 1

        low = line.lower()
        if low.startswith("folder list"):
            section = "folder"
            continue
        if low.startswith("object list"):
            section = "object"
            continue
        if not line:
            continue

        if section == "folder":
            if not line.startswith("obs://"):
                continue
            full_path = line
            name = full_path.replace(path, "", 1).strip("/")
            if not name:  # 当前目录自身，跳过
                continue
            dirs.append({"name": name, "path": full_path, "is_dir": True, "size": None})

        elif section == "object":
            if line.startswith("key") and "LastModified" in raw:
                continue  # 表头
            if not line.startswith("obs://"):
                continue
            full_path = line
            name = full_path.replace(path, "", 1).strip("/")
            if not name:  # 目录占位对象（path 自身），跳过
                continue
            # 元数据在下一行：<空格>LastModified  Size  StorageClass  ETag
            size = None
            if i < len(lines):
                meta = lines[i].strip()
                if meta and not meta.startswith("obs://"):
                    for tok in meta.split():
                        if any(tok.endswith(u) for u in ("B", "KB", "MB", "GB", "TB")):
                            size = tok
                            break
                    i += 1  # 消费元数据行
            files.append({"name": name, "path": full_path, "is_dir": False, "size": size})

    return dirs + files


# ---------------------------------------------------------------------------
# 失败报告解析 + 单文件补传（"补同步"）
# ---------------------------------------------------------------------------

DEFAULT_OBSUTIL_OUTPUT_DIR = str(Path.home() / ".obsutil_output")

# 匹配失败报告数据行的 status/error message，用于记录失败原因
_STATUS_RE = re.compile(r"status \[(\d+)\]")
_ERRMSG_RE = re.compile(r"error message \[([^\]]*)\]")

# obsutil ls 的对象大小字段：如 "472B" / "1.2MB" / "391.68GB"
_SIZE_RE = re.compile(r"^(\d+(?:\.\d+)?)(B|KB|MB|GB|TB)$")
_SIZE_UNITS = {"B": 1, "KB": 1024, "MB": 1024 ** 2, "GB": 1024 ** 3, "TB": 1024 ** 4}


def _parse_size_bytes(tok: str) -> Optional[int]:
    m = _SIZE_RE.match(tok or "")
    if not m:
        return None
    return int(float(m.group(1)) * _SIZE_UNITS[m.group(2)])


def obsutil_object_size(obs_url: str, timeout: int = 30) -> Optional[int]:
    """查单个 OBS 对象的大小（字节）。不存在/查询失败返回 None。

    用于删除本地前的轻量复核：对**单个对象**跑一次 `obsutil ls`，秒级返回。
    不要用 `ls -du` 递归统计整个叶子 —— 实测单叶 80 万个对象要跑几分钟。

    必须带 `-bf=raw`：默认输出把大小格式化成 2 位小数的可读值（如 "659.75KB"），
    反推字节数会引入舍入误差（659.75*1024=675584，真实值 675579，差 5 字节）。
    实测这会让每一个 >1KB 的哨兵都误判为「大小不符」，而文件本身其实完全一致。
    `-bf=raw` 输出 "675579B"，精确无歧义。
    """
    if not obs_url or not os.path.isfile(OBSUTIL_BIN):
        return None
    url = obs_url
    if url.endswith("/"):
        # 目录 URL 会命中该前缀下的第一个对象，语义不对；单对象查询不接受尾部 /
        return None
    try:
        result = subprocess.run([OBSUTIL_BIN, "ls", url, "-limit=1", "-bf=raw"],
                                capture_output=True, text=True, timeout=timeout)
        if result.returncode != 0:
            return None
    except Exception:
        return None

    # 只有「对象列表里出现了与目标 key 完全一致的 obs:// 行」才算命中。
    # 不能退化成全文扫第一个大小 token：对象不存在时 obsutil 只打印
    # `Total size of prefix [...]: 0B`，会被当成「大小为 0」，从而把
    # 上传失败误判成校验通过（实测踩到）。
    target = url.rstrip("/")
    lines = result.stdout.splitlines()
    for i, raw in enumerate(lines):
        line = raw.strip()
        if not line.startswith("obs://") or line.rstrip("/") != target:
            continue
        for tok in line.split():
            b = _parse_size_bytes(tok)
            if b is not None:
                return b
        # 元数据在下一行：<空格>LastModified  Size  StorageClass  ETag
        if i + 1 < len(lines):
            for tok in lines[i + 1].split():
                b = _parse_size_bytes(tok)
                if b is not None:
                    return b
        return None
    return None


def find_failed_report(task_id: str, output_dir: str = "") -> Optional[str]:
    """在 output_dir 下查找 obsutil 某次 cp 的失败报告，返回最新匹配文件路径。

    obsutil 命名：cp_failed_report_<timestamp>_<task_id>.txt
    """
    if not task_id:
        return None
    base = output_dir or DEFAULT_OBSUTIL_OUTPUT_DIR
    pattern = os.path.join(base, f"cp_failed_report_*_{task_id}.txt")
    matches = glob.glob(pattern)
    if not matches:
        return None
    return max(matches, key=os.path.getmtime)


def parse_failed_report(report_path: str) -> List[Tuple[str, str, str]]:
    """解析 obsutil 失败报告，返回 [(local_path, obs_path, error), ...]。

    数据行形如：
        <ts> <size>, <local_src> --> <obs_dst>, cost [N], status [500], error code [..], error message [..], request id [..]
    表头行是 `[file size, src --> dst, ...]`（含 `-->` 但被方括号包裹），需排除。
    """
    result: List[Tuple[str, str, str]] = []
    try:
        with open(report_path, "r", encoding="utf-8", errors="replace") as f:
            for raw in f:
                line = raw.strip()
                if " --> " not in line:
                    continue
                # 排除表头：形如 "... [file size, src --> dst, cost(ms), ...]"
                if "[file size" in line or "src --> dst" in line:
                    continue
                # 切出 src：--> 左侧，取最后一个 ", " 之后（前面是时间戳+大小）
                left, _, right = line.partition(" --> ")
                # left 末尾是 "<ts> <size>, <local_src>"，local 从第一个 ", " 后开始
                if ", " in left:
                    local_path = left.split(", ", 1)[1].strip()
                else:
                    local_path = left.strip()
                # 切出 dst：--> 右侧到 ", cost [" 之前
                obs_path = right.split(", cost [", 1)[0].strip()
                if not obs_path:
                    obs_path = right.strip()
                # 错误信息
                status_m = _STATUS_RE.search(line)
                errmsg_m = _ERRMSG_RE.search(line)
                parts = []
                if status_m:
                    parts.append(f"status {status_m.group(1)}")
                if errmsg_m:
                    parts.append(errmsg_m.group(1))
                error = " - ".join(parts) if parts else "upload failed"
                if local_path and obs_path.startswith("obs://"):
                    result.append((local_path, obs_path, error))
    except OSError:
        return []
    return result


def reupload_file(local_path: str, obs_path: str, timeout: int = 120) -> Tuple[bool, str]:
    """单文件补传：obsutil cp <local> <obs> -f（非 -r，幂等覆盖）。"""
    if not os.path.isfile(OBSUTIL_BIN):
        return False, "obsutil not found"
    if not os.path.isfile(local_path):
        return False, f"本地文件不存在: {local_path}"
    cmd = [OBSUTIL_BIN, "cp", local_path, obs_path, "-f"]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        if result.returncode == 0:
            return True, (result.stdout or "").strip()[-200:]
        return False, ((result.stderr or result.stdout) or "").strip()[-200:]
    except subprocess.TimeoutExpired:
        return False, f"补传超时（{timeout}s）"
    except Exception as e:
        return False, str(e)

