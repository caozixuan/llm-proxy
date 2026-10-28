import re
import os
import json
import time
import httpx
import hmac
import asyncio
import logging
import threading
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from datetime import datetime
from dotenv import load_dotenv
from fastapi import FastAPI, Request, Query
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from typing import Any, Dict, List, Optional, AsyncIterator
from fastapi.responses import JSONResponse, StreamingResponse, Response, FileResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.gzip import GZipMiddleware
from starlette.middleware.sessions import SessionMiddleware

from utils.token_index import query_token_stats, query_key_stats, query_channel_stats, query_channel_keys, query_api_keys
from utils.auth import validate_api_key, resolve_upstream_channel
from utils.key_store import init_db as init_key_db, find_key as _find_key
from utils.key_config import init_key_config
from utils.channel_store import init_db as init_channel_db
from utils.custom_models import resolve_custom_model, load_custom_models
from utils.http_client import shared_client, get_client, close_all as close_http_clients
from utils import inflight
from utils.metrics import (
    get_metrics_snapshot,
    get_rate_history,
    get_metrics_storage_info,
    start_metrics_scanner,
)
from utils.log_paths import build_index_path, get_log_dir, get_log_task_tag, get_upstream_key_prefix, get_service_log_dir, STARTUP_DATE_TAG
from utils.logs_config import get_stats_roots
from utils.log_routes import register_log_routes
from utils.session_routes import register_session_routes
from utils.key_routes import register_key_routes
from utils.export_routes import register_export_routes, resolve_export_key, verify_access_key
from utils.channel_routes import register_channel_routes
from utils.user_routes import register_user_routes
from utils.export_store import init_db as init_export_db
from utils.backup_store import init_db as init_backup_db
from utils.logdir_store import init_db as init_logdir_db
from utils.backup_routes import register_backup_routes
from utils.obs_routes import register_obs_routes
from utils.logs_routes import register_logs_routes
from utils.user_store import init_db as init_user_db, verify_user, create_user, get_user_permissions
from utils.session_store import init_db as init_session_db
from utils.message_common import build_chain_key, get_first_user_text, get_text_from_content
from utils.debug_logs import write_debug, write_debug_async, debug_filename, register_debug_routes
from src.thinking_reflection import register_reflection_routes
from utils.req_index import (
    append_index_anthropic, append_index_openai, append_index_responses,
    append_index_anthropic_async, append_index_openai_async, append_index_responses_async,
    load_index, get_index_counts,
)

load_dotenv(os.environ.get("ENV_FILE", ".env"), override=True)

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)

# 全局默认：是否屏蔽 Task 工具里的 "- Explore:" 行
BAN_EXPLORE = os.getenv("BAN_EXPLORE", "false").lower() == "true"
BAN_STREAM = os.getenv("BAN_STREAM", "false").lower() == "true"
EXPOSE_THINKING = os.getenv("EXPOSE_THINKING", "true").lower() == "true"
TRUST_ENV = os.getenv("TRUST_ENV", "true").lower() == "true"

# 全局默认：重试次数（不从环境变量读取）
# 20 次几乎不会比 6 次多救回请求，只会在上游故障时拖长失败等待、占用连接。
MAX_RETRIES = 6

# 重试退避上限（秒）：指数退避 + 抖动，避免上游过载时的同步重试风暴
_RETRY_BACKOFF_CAP = 4.0


def _retry_delay(attempt: int) -> float:
    """第 attempt 次失败后的等待：0.3 * 2^attempt，封顶 _RETRY_BACKOFF_CAP，
    叠加 0~0.25s 抖动，打散并发请求的重试时刻。"""
    import random
    base = min(0.3 * (2 ** attempt), _RETRY_BACKOFF_CAP)
    return base + random.uniform(0, 0.25)

MONITOR_AUTH_EXACT_PATHS = {
    "/",
    "/query",
    "/history",
    "/sessions",
    "/keys",
    "/thinking",
    "/channels",
    "/users",
    "/backup",
    "/logs-admin",
    "/docs",
    "/redoc",
    "/openapi.json",
}
MONITOR_AUTH_PREFIX_PATHS = (
    "/api/statistic",
    "/api/switch-user",
    "/api/accounts",
    "/api/users",
    "/metrics",
    "/logs",
    "/sessions/",
    "/keys/",
    "/channels/",
    "/api/keys/",
    "/api/channels/",
    "/api/export/",
    "/api/backup/",
    "/api/reflection/",
    "/api/logs-admin/",
)
MONITOR_AUTH_PUBLIC_PATHS = {
    "/hi",
    "/login",
    "/logout",
    "/register",
    "/invite",
    "/export/view",
    "/export/submit",
    "/export/status",
}

MONITOR_ADMIN_ONLY_PATHS = {"/users", "/logs-admin"}
MONITOR_ADMIN_ONLY_PREFIXES = ("/api/users", "/api/logs-admin")

_PERM_PATH_MAP = {
    "/keys": "keys",
    "/channels": "channels",
    "/backup": "backup",
}
_PERM_PREFIX_MAP = (
    ("/keys/export", "export"),
    ("/api/export/", "export"),
    ("/keys/", "keys"),
    ("/api/keys/", "keys"),
    ("/channels/", "channels"),
    ("/api/channels/", "channels"),
    ("/api/backup/", "backup"),
    ("/thinking", "thinking"),
    ("/api/reflection/", "thinking"),
)

LOGS_DIR = get_log_dir("logs_all")
ENV_DIR = os.path.dirname(LOGS_DIR)


def _stats_roots():
    """统计聚合的所有根目录：活跃 env_dir + 配置的历史路径。"""
    return get_stats_roots(ENV_DIR)

SERVICE_LOG_DIR = get_service_log_dir()


def _build_debug_dir() -> str:
    return os.path.join(SERVICE_LOG_DIR, "debug", STARTUP_DATE_TAG)


LOGS_DEBUG = _build_debug_dir()

app = FastAPI(title="Anthropic+OpenAI Proxy (FastAPI)")


@app.on_event("shutdown")
async def _shutdown_http_clients():
    """进程退出时关闭所有按 base_url 缓存的共享 httpx client。"""
    await close_http_clients()

class MonitorAuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if not _is_monitor_auth_enabled() or not _is_monitor_path(request.url.path):
            return await call_next(request)

        if _is_monitor_authenticated(request):
            path = request.url.path
            role = request.session.get("monitor_role", "user")
            if role == "admin":
                return await call_next(request)
            if _is_admin_only_path(path):
                return JSONResponse({"detail": "Admin access required"}, status_code=403)
            required_perm = _required_permission(path)
            if required_perm:
                user_perms = (request.session.get("monitor_permissions") or "").split(",")
                if required_perm not in user_perms:
                    return JSONResponse({"detail": "权限不足"}, status_code=403)
            return await call_next(request)

        if request.url.path in MONITOR_AUTH_EXACT_PATHS:
            next_path = _normalize_next_path(request.url.path)
            if request.url.query:
                next_path = f"{next_path}?{request.url.query}"
            return RedirectResponse(url=f"/login?next={next_path}", status_code=303)

        return JSONResponse(
            {"detail": "Monitor login required"},
            status_code=401,
            headers={"Cache-Control": "no-store"},
        )


app.add_middleware(MonitorAuthMiddleware)
app.add_middleware(
    SessionMiddleware,
    secret_key=os.getenv("MONITOR_SESSION_SECRET") or os.getenv("API_KEY") or "change-this-monitor-session-secret",
    session_cookie="monitor_session",
    same_site="lax",
    https_only=os.getenv("MONITOR_COOKIE_SECURE", "false").lower() == "true",
    max_age=max(300, int(os.getenv("MONITOR_SESSION_MAX_AGE", "43200"))),
)
# GZip 最后添加 → 位于最外层，压缩内层（含鉴权后）的全部响应。
# Starlette 的 DEFAULT_EXCLUDED_CONTENT_TYPES 已含 text/event-stream，
# 不会缓冲/压缩 SSE 流式透传（见 _filter_response_headers 处的说明）。
app.add_middleware(GZipMiddleware, minimum_size=1024)

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# templates = Jinja2Templates(directory="api/templates")

async def get_x_auth_token(request) -> str:
    keys = ['authorization', 'x-api-key']
    for key in keys:
        ack = request.headers.get(key)
        if isinstance(ack, str) and ack.startswith('Bearer '):
            ack = ack.split('Bearer ')[1].strip()
            return ack
    return ''


def _append_query(url: str, req: "Request") -> str:
    """把客户端原始请求的 query string 原样拼到上游 URL 上。

    实现「原样全部透传」：客户端发 /v1/messages?beta=true，转发到上游时也带上
    ?beta=true。上游 URL 一般不含 query，用 ? 拼接；若已带 query 则用 &。
    """
    q = req.url.query
    if not q:
        return url
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}{q}"


# ---------------------------------------------------------------------------
# 未定义路由 catch-all 透传：客户端打代理上没定义的路径（如 /test/、/v1/models）
# 时，原样反向代理到「上游域名根 + 该路径」。仍走客户端鉴权 + 渠道选择 + 换上游 key。
# ---------------------------------------------------------------------------

# 逐跳（hop-by-hop）头：只在单跳连接内有意义，转发时必须剔除，否则会破坏转发。
_HOP_BY_HOP_HEADERS = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailer", "transfer-encoding", "upgrade",
    # host / content-length 让 httpx 按目标重新计算；client 鉴权头由上游 key 覆盖
    "host", "content-length",
}

# catch-all 不该转发的「本地功能」路径前缀。已定义的具体路由本就优先于 catch-all
# 不会落到这里；这里兜底那些「本地功能的未定义子路径」（如 /query/x、/export/x）。
# 后台 /api/*、/metrics/*、/keys/* 等已被 MonitorAuthMiddleware 在更前面拦下。
_PASSTHROUGH_EXCLUDE_PREFIXES = (
    "query", "history", "register", "login", "logout",
    "invite", "hi", "docs", "redoc", "openapi.json", "favicon.ico",
    "static", "api", "metrics", "logs", "sessions", "keys", "channels",
    "users", "backup", "thinking", "export",
)


def _should_passthrough(full_path: str) -> bool:
    """判断未匹配路径是否应转发上游（True）还是返回本地 404（False）。"""
    if not full_path:
        return False  # 根路径由 "/" 路由处理，不该到这
    first = full_path.split("/", 1)[0].split("?", 1)[0]
    if first in _PASSTHROUGH_EXCLUDE_PREFIXES:
        return False
    if _is_monitor_path("/" + full_path):
        return False
    return True


def _upstream_root(channel: Optional[dict]) -> str:
    """从渠道或 env 的 upstream_url 提取「域名根」(scheme://netloc)，去掉 /v1 等后缀。"""
    base = ""
    if channel and channel.get("upstream_url"):
        base = channel["upstream_url"]
    else:
        base = os.environ.get("UPSTREAM_URL", "")
    p = urlparse(base)
    if p.scheme and p.netloc:
        return f"{p.scheme}://{p.netloc}"
    # 兜底：upstream_url 不规范时，退回原字符串去掉尾部路径
    return base.rstrip("/")


def _filter_forward_headers(headers, upstream_key: str) -> dict:
    """透传客户端请求头，剔除逐跳头，并把鉴权头换成上游 key。"""
    out = {}
    for k, v in headers.items():
        if k.lower() in _HOP_BY_HOP_HEADERS:
            continue
        if k.lower() in ("authorization", "x-api-key"):
            continue  # 下面统一用上游 key 覆盖
        out[k] = v
    if upstream_key:
        out["Authorization"] = f"Bearer {upstream_key}"
        out["x-api-key"] = upstream_key
    return out


def _filter_response_headers(headers) -> dict:
    """透传上游响应头，剔除逐跳头。

    我们用 aiter_raw() 原样转发未解压的响应体，所以要**保留** content-encoding
    （如 gzip），让客户端自己解压。但 content-length / transfer-encoding 要剔除：
    流式回传由 Starlette 用 chunked 编码，长度不再固定。
    """
    return {k: v for k, v in headers.items() if k.lower() not in _HOP_BY_HOP_HEADERS}


def _env_enabled(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _is_monitor_auth_enabled() -> bool:
    explicit = os.getenv("MONITOR_AUTH_ENABLED")
    if explicit is not None:
        return _env_enabled("MONITOR_AUTH_ENABLED")
    return bool(os.getenv("MONITOR_USERNAME", "").strip())


def _is_monitor_login_valid(username: str, password: str) -> tuple[bool, str, str]:
    expected_user = os.getenv("MONITOR_USERNAME", "").strip()
    expected_password = os.getenv("MONITOR_PASSWORD", "")
    if expected_user and expected_password:
        if hmac.compare_digest(username, expected_user) and hmac.compare_digest(password, expected_password):
            return True, "admin", ""
    user = verify_user(username, password)
    if user:
        return True, user.get("role", "user"), user.get("permissions", "")
    return False, "", ""


def _is_admin_only_path(path: str) -> bool:
    if path in MONITOR_ADMIN_ONLY_PATHS:
        return True
    return any(path.startswith(prefix) for prefix in MONITOR_ADMIN_ONLY_PREFIXES)


def _required_permission(path: str) -> str:
    if path in _PERM_PATH_MAP:
        return _PERM_PATH_MAP[path]
    for prefix, perm in _PERM_PREFIX_MAP:
        if path.startswith(prefix):
            return perm
    return ""


def _ctx(request: Request, active_page: str, **extra) -> dict:
    if _is_monitor_auth_enabled():
        perms_raw = request.session.get("monitor_permissions") or ""
        perms_list = [p.strip() for p in perms_raw.split(",") if p.strip()]
        role = request.session.get("monitor_role", "user")
        user_name = request.session.get("monitor_user", "")
    else:
        perms_list = []
        role = "admin"
        user_name = ""
    ctx = {
        "active_page": active_page,
        "user_role": role,
        "user_name": user_name,
        "user_permissions": perms_list,
    }
    ctx.update(extra)
    return ctx


def _normalize_next_path(next_path: str) -> str:
    if not next_path or not next_path.startswith("/") or next_path.startswith("//"):
        return "/"
    if next_path in {"/login", "/logout"}:
        return "/"
    return next_path


def _is_monitor_path(path: str) -> bool:
    if path in MONITOR_AUTH_PUBLIC_PATHS or path.startswith("/static/"):
        return False
    if path in MONITOR_AUTH_EXACT_PATHS:
        return True
    return any(path.startswith(prefix) for prefix in MONITOR_AUTH_PREFIX_PATHS)


def _is_monitor_authenticated(request: Request) -> bool:
    return bool(request.session.get("monitor_authenticated"))


def _resolve_model_name(raw_model: Any) -> str:
    """
    统一处理 model 名：
    - "byenv" 或空值 -> 从环境变量 MODEL_ID 读取
    - 其他值 -> 原样返回
    """
    if raw_model == "byenv" or not raw_model:
        return os.environ.get("MODEL_ID") or "unknown"
    return raw_model


def _strip_task_explore_line(
        tools: Optional[List[Dict[str, Any]]],
        ban_explore: Optional[bool] = None,
) -> Optional[List[Dict[str, Any]]]:
    """
    根据 ban_explore 决定是否从 Task 工具描述中移除 "- Explore:" 行。
    - ban_explore 为 None 时，采用全局 BAN_EXPLORE 开关。
    """

    def _remove_explore_from_desc(desc: Any) -> Optional[str]:
        if not isinstance(desc, str):
            return None
        lines = desc.splitlines()
        filtered_lines = []
        changed = False
        for line in lines:
            if line.lstrip().startswith("- Explore:") or line.lstrip().startswith("- **Explore**:"):
                changed = True
                continue
            filtered_lines.append(line)
        return "\n".join(filtered_lines) if changed else None

    if ban_explore is None:
        ban_explore = BAN_EXPLORE

    if not ban_explore or not tools:
        return tools

    cleaned: List[Any] = []
    for t in tools:
        if not isinstance(t, dict):
            cleaned.append(t)
            continue
        # Anthropic tools: {name, description, input_schema}
        if t.get("name") == "Task":
            new_desc = _remove_explore_from_desc(t.get("description"))
            if new_desc is not None:
                cleaned.append({**t, "description": new_desc})
            else:
                cleaned.append(t)
            continue

        # OpenAI tools: {type:"function", function:{name, description, parameters}}
        if t.get("type") == "function":
            func = t.get("function")
            if isinstance(func, dict) and func.get("name") == "Task":
                new_desc = _remove_explore_from_desc(func.get("description"))
                if new_desc is not None:
                    cleaned.append({**t, "function": {**func, "description": new_desc}})
                else:
                    cleaned.append(t)
                continue

        cleaned.append(t)

    return cleaned


def _parse_extra_headers() -> Dict[str, str]:
    raw = os.getenv("UPSTREAM_EXTRA_HEADERS", "").strip()
    if not raw:
        return {}
    headers = {}
    for pair in raw.split(","):
        pair = pair.strip()
        if ":" not in pair:
            continue
        k, v = pair.split(":", 1)
        k, v = k.strip(), v.strip()
        if k:
            headers[k] = v
    return headers


_EXTRA_HEADERS = _parse_extra_headers()


def build_upstream_headers(x_auth_token: str, model_id: str, upstream_key_override: str = "") -> Dict[str, str]:
    ack = upstream_key_override or os.getenv('UPSTREAM_API_KEY') or x_auth_token
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {ack}",
        "x-api-key": ack,
        "Model-Id": os.environ.get("MODEL_ID") or model_id,
    }
    if _EXTRA_HEADERS:
        headers.update(_EXTRA_HEADERS)
    return headers


def _channel_response_headers(channel_key: str, fallback: str, channel: dict = None) -> dict:
    h = {}
    if channel_key:
        h["X-Channel-Key"] = channel_key
    if fallback:
        h["X-Channel-Fallback"] = fallback
    if channel:
        if channel.get("id"):
            h["X-Channel-Id"] = str(channel["id"])
        if channel.get("name"):
            h["X-Channel-Name"] = channel["name"]
        if channel.get("upstream_url"):
            h["X-Channel-Upstream"] = channel["upstream_url"]
    return h


def _ssl_verify() -> bool:
    return os.getenv("SSL_VERIFY", "true").lower() != "false"


# -----------------------------
# Rate limit helpers
# -----------------------------
# 所有需要视为「限流且可重试」的上游状态码，统一维护在这里，便于后续扩展（如再加入 503 等）
RATE_LIMIT_STATUS_CODES = {406, 429}


def is_rate_limit_status(status_code: int) -> bool:
    """
    判断上游响应码是否属于「限流/可重试」错误。
    所有调用处统一依赖本函数，而不是直接写死 (406, 429)。
    """
    return status_code in RATE_LIMIT_STATUS_CODES


_VALID_CONTENT_TYPES = {"text", "thinking", "tool_use"}


def _has_valid_content(resp_json: dict) -> bool:
    """非流式：content 数组中至少有一个有效内容块（text/thinking/tool_use 非空）。"""
    for block in resp_json.get("content", []):
        btype = block.get("type")
        if btype == "text" and block.get("text"):
            return True
        if btype == "thinking" and block.get("thinking"):
            return True
        if btype == "tool_use" and block.get("input") is not None:
            return True
    return False


def _has_valid_sse_content(chunks: list) -> bool:
    """流式：SSE chunks 中至少有一个有效 delta（text/thinking/tool_use input）。"""
    for chunk in chunks:
        if chunk.get("type") == "content_block_delta":
            delta = chunk.get("delta", {})
            dtype = delta.get("type")
            if dtype == "text_delta" and delta.get("text"):
                return True
            if dtype == "thinking_delta" and delta.get("thinking"):
                return True
            if dtype == "input_json_delta" and delta.get("partial_json") is not None:
                return True
    return False


# -----------------------------
# Endpoints
# -----------------------------
@app.get("/hi")
async def health():
    return {"LLM_PROXY": "hello !!!"}


@app.get("/metrics/live")
async def metrics_live():
    """实时在途请求快照：当前并发数 + 各阶段分布（connecting/waiting_upstream/streaming）。
    受 /metrics 监控鉴权保护。单 worker 反映本进程；多 worker 需在读取端聚合各进程。"""
    return inflight.snapshot()


@app.get("/login")
async def monitor_login_page(request: Request, next: str = "/", add: str = ""):
    if not _is_monitor_auth_enabled():
        return RedirectResponse(url="/", status_code=303)
    if _is_monitor_authenticated(request) and not add:
        return RedirectResponse(url=_normalize_next_path(next), status_code=303)
    registered = request.query_params.get("registered")
    return templates.TemplateResponse(
        request,
        "login.html",
        context={
            "next_path": _normalize_next_path(next),
            "error": "",
            "success": "注册成功，请登录" if registered else "",
        },
        headers={"Cache-Control": "no-store"},
    )


@app.post("/login")
async def monitor_login_submit(request: Request):
    if not _is_monitor_auth_enabled():
        return RedirectResponse(url="/", status_code=303)

    body_bytes = await request.body()
    form = parse_qs(body_bytes.decode("utf-8"), keep_blank_values=True)
    username = (form.get("username", [""])[0]).strip()
    password = form.get("password", [""])[0]
    next_path = _normalize_next_path(form.get("next", ["/"])[0])

    valid, role, permissions = _is_monitor_login_valid(username, password)
    if valid:
        accounts = request.session.get("monitor_accounts") or []
        if not any(a.get("username") == username for a in accounts):
            accounts.append({"username": username, "role": role})
        request.session["monitor_authenticated"] = True
        request.session["monitor_user"] = username
        request.session["monitor_role"] = role
        request.session["monitor_permissions"] = permissions
        request.session["monitor_accounts"] = accounts
        return RedirectResponse(url=next_path, status_code=303)

    return templates.TemplateResponse(
        request,
        "login.html",
        context={
            "next_path": next_path,
            "error": "用户名或密码错误",
        },
        status_code=401,
        headers={"Cache-Control": "no-store"},
    )


@app.get("/logout")
async def monitor_logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/login", status_code=303)


@app.post("/api/switch-user")
async def switch_user(request: Request):
    body = await request.json()
    target = body.get("username", "").strip()
    if not target:
        return JSONResponse({"detail": "username required"}, status_code=400)
    accounts = request.session.get("monitor_accounts") or []
    acct = next((a for a in accounts if a.get("username") == target), None)
    if not acct:
        return JSONResponse({"detail": "account not found"}, status_code=404)
    request.session["monitor_authenticated"] = True
    request.session["monitor_user"] = acct["username"]
    request.session["monitor_role"] = acct["role"]
    perms = get_user_permissions(acct["username"])
    request.session["monitor_permissions"] = ",".join(perms)
    return JSONResponse({"ok": True, "username": acct["username"], "role": acct["role"]})


@app.get("/api/accounts")
async def list_accounts(request: Request):
    accounts = request.session.get("monitor_accounts") or []
    current = request.session.get("monitor_user", "")
    return JSONResponse({"accounts": accounts, "current": current})


@app.get("/register")
async def register_page(request: Request):
    if not _is_monitor_auth_enabled():
        return RedirectResponse(url="/", status_code=303)
    from utils.user_store import has_users
    no_db_users = not has_users()
    return templates.TemplateResponse(
        request,
        "register.html",
        context={"error": "", "is_first_user": no_db_users},
        headers={"Cache-Control": "no-store"},
    )


@app.post("/register")
async def register_submit(request: Request):
    if not _is_monitor_auth_enabled():
        return RedirectResponse(url="/", status_code=303)
    body_bytes = await request.body()
    form = parse_qs(body_bytes.decode("utf-8"), keep_blank_values=True)
    username = (form.get("username", [""])[0]).strip()
    password = form.get("password", [""])[0]
    password2 = form.get("password2", [""])[0]

    from utils.user_store import has_users
    no_db_users = not has_users()

    def _render_error(msg, code=400):
        return templates.TemplateResponse(
            request, "register.html",
            context={"error": msg, "is_first_user": False},
            status_code=code, headers={"Cache-Control": "no-store"},
        )

    if not username or not password:
        return _render_error("用户名和密码不能为空")

    if password != password2:
        return _render_error("两次输入的密码不一致")

    if len(username) < 2 or len(username) > 32:
        return _render_error("用户名长度应在 2-32 个字符之间")

    if len(password) < 6:
        return _render_error("密码长度至少 6 个字符")

    role = "user"
    user = create_user(username, password, role=role)
    if user is None:
        return _render_error("该用户名已被注册", 409)

    return RedirectResponse(url="/login?registered=1", status_code=303)


# ---------- Anthropic Messages ----------
def _dump_json(path: str, obj):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")


async def _dump_json_async(path: str, obj):
    """_dump_json 的异步包装：把同步的 JSON 序列化+磁盘写入丢到线程池，
    不阻塞事件循环。请求体常达数百 KB，indent+sort_keys 序列化+落盘的耗时
    在高并发下会冻结整个 worker，导致其它请求超时断连。"""
    try:
        await asyncio.to_thread(_dump_json, path, obj)
    except Exception as ex:
        logging.warning(f"Failed to dump json async ({path}): {ex}")


def _resp_to_obj(r):  # httpx.Response -> dict
    base = {"status_code": r.status_code, "headers": dict(r.headers)}
    try:
        base["json"] = r.json()
    except Exception:
        base["text"] = r.text
    return base


# 启动时初始化
init_key_db(SERVICE_LOG_DIR)
# 全局路径注册表,供各模块快速查询
ALL_PATH = os.path.join(SERVICE_LOG_DIR, "log_paths.json")

init_key_config(SERVICE_LOG_DIR)
init_channel_db(SERVICE_LOG_DIR)
init_export_db(SERVICE_LOG_DIR)
init_backup_db(SERVICE_LOG_DIR)
init_logdir_db(SERVICE_LOG_DIR)

# 写入路径注册表 log_paths.json,供 worker 快速读取,免解析环境变量
def _write_log_paths_registry():
    """写入全局路径注册表到 logs/port{PORT}/log_paths.json。"""
    import json
    registry = {
        "service_log_dir": SERVICE_LOG_DIR,
        "export_db": os.path.join(SERVICE_LOG_DIR, "export_session_record.db"),
        "logdir_db": os.path.join(SERVICE_LOG_DIR, "log_dir.db"),
        "logs_all": ENV_DIR,
        "backfill_log": os.path.join(SERVICE_LOG_DIR, "backfill.log"),
        "export_log": os.path.join(SERVICE_LOG_DIR, "export_log"),
    }
    registry_path = os.path.join(SERVICE_LOG_DIR, "log_paths.json")
    try:
        with open(registry_path, "w", encoding="utf-8") as f:
            json.dump(registry, f, indent=2, ensure_ascii=False)
    except Exception:
        pass  # 写失败不影响启动,worker 回退到解析环境变量

_write_log_paths_registry()

# 注意：leaf_status 的 building→pending 复位与 backfill_requests 的队列清空，已随回填
# 执行一起剥离到 backfill_worker._init_env。app 启动不再复位，避免误清 worker 正在跑的
# 叶子（app/worker 各自重启互不干扰）。见 utils/backfill_worker.py。
# 「数据管理」以 log_dir.db 的 sources 表为唯一数据源，无 YAML 导入。
# 每次启动刷新活跃目录行（root_id='default'）root_path，空库时补插。
from utils.logdir_store import _refresh_active_row
_refresh_active_row(ENV_DIR)
from utils.newapi_backfill import init_backfill_logger
init_backfill_logger(SERVICE_LOG_DIR)
init_user_db(SERVICE_LOG_DIR)
init_session_db(SERVICE_LOG_DIR)
load_custom_models()
# 被重启打断的导出/质检任务不再直接判失败，而是在 register_export_routes() 内
# （路由闭包就绪后）由 _requeue_interrupted() 自动重新入队，见 utils/export_routes.py。
load_index(LOGS_DIR)
# 多 worker：仅 leader 跑 metrics 扫描线程，避免 N 个进程重复写 rpm.log/scanner_state.json
from utils.leader_lock import try_acquire_leader, is_leader
_IS_LEADER = try_acquire_leader(os.path.join(SERVICE_LOG_DIR, "scanner.lock"))
if _IS_LEADER:
    start_metrics_scanner(os.path.dirname(LOGS_DIR))
else:
    logging.info("metrics scanner skipped (not leader worker, pid=%s)", os.getpid())
# 多 worker：启用 in-flight 跨进程心跳，让 /metrics/live 聚合所有 worker 的在途请求。
# 单 worker 下也无害（只是多写一份 {pid}.json 心跳文件）。
inflight.enable_cross_process(SERVICE_LOG_DIR)

# new-api 叶子的实时增量索引已剥离为独立进程 src/watch/worker.py：
# 每源一个发现线程（只 stat index.jsonl）+ 8 线程同步池（ingest→enrich→sync_sessions）。
# app 进程不再内嵌 watcher：内嵌方案会与大叶子首建抢锁、把专职回填 worker 饿死，
# 且单线程啃 10w 行级叶子会堵住后续所有叶子。
# 启动：`./app watch start`（`./app start` 会一并拉起）。


def _sanitize_messages(messages: Any) -> Any:
    """
    清洗 messages 列表，过滤掉空 text content block。
    Anthropic API 要求 text content blocks 必须非空，否则返回 400。
    - content 为 list 时：过滤 type==text 且 text 为空/空白的块；
      若过滤后 list 为空，用单个空格占位块替代，避免整条消息丢失。
    - content 为字符串时：原样保留（空字符串同理由上游决定，非我们的问题范围）。
    """
    if not isinstance(messages, list):
        return messages
    cleaned = []
    for msg in messages:
        if not isinstance(msg, dict):
            cleaned.append(msg)
            continue
        content = msg.get("content")
        if not isinstance(content, list):
            cleaned.append(msg)
            continue
        # thinking/redacted_thinking blocks in assistant messages must remain exactly
        # as they were in the original response; skip sanitization for such messages.
        if msg.get("role") == "assistant" and any(
            isinstance(b, dict) and b.get("type") in ("thinking", "redacted_thinking")
            for b in content
        ):
            cleaned.append(msg)
            continue
        def _is_empty_text_block(b: Any) -> bool:
            return isinstance(b, dict) and b.get("type") == "text" and not (b.get("text") or "").strip()

        def _sanitize_block(b: Any) -> Any:
            """对 tool_result 块递归清理其嵌套 content 中的空 text 块。"""
            if not isinstance(b, dict) or b.get("type") != "tool_result":
                return b
            nested = b.get("content")
            if not isinstance(nested, list):
                return b
            filtered_nested = [nb for nb in nested if not _is_empty_text_block(nb)]
            if len(filtered_nested) == len(nested):
                return b
            return {**b, "content": filtered_nested if filtered_nested else [{"type": "text", "text": " "}]}

        new_blocks = [_sanitize_block(b) for b in content if not _is_empty_text_block(b)]
        if len(new_blocks) == len(content) and all(new_blocks[i] is content[i] for i in range(len(content))):
            cleaned.append(msg)
        elif new_blocks:
            cleaned.append({**msg, "content": new_blocks})
        else:
            # 所有块都被过滤掉了，用占位符保留消息结构
            cleaned.append({**msg, "content": [{"type": "text", "text": " "}]})
    return cleaned


@app.post("/v1/messages")
async def anthropic_messages(req: Request):
    """anthropic透传"""
    _api_key = await validate_api_key(req)
    _channel = resolve_upstream_channel(_api_key)
    if _channel and _channel.get("_error"):
        return JSONResponse({"error": {"type": "channel_error", "message": "所有绑定渠道均已离线"}}, status_code=503)
    _channel_key = _channel.get("upstream_key", "")[-4:] if _channel else ""
    _channel_fallback = _channel.get("_fallback", "") if _channel else ""
    body = await req.json()
    stream = bool(body.get("stream", False))
    resolve_custom_model(body)
    body_model = body.get("model")
    ban_explore = BAN_EXPLORE

    model_from_body: Optional[str] = body_model if isinstance(body_model, str) else None
    suffix = "--ban_explore"
    if model_from_body and model_from_body.endswith(suffix):
        # 任何带 "--ban_explore" 后缀的模型名，都强制开启屏蔽 Explore
        ban_explore = True
        base_model = model_from_body[: -len(suffix)]
        model = _resolve_model_name(base_model or "byenv")
    else:
        model = _resolve_model_name(body_model)
    # session相关
    session_id = None
    session_metadata = body.get("metadata")
    if isinstance(session_metadata, dict):
        user_id = session_metadata.get("user_id") or ""
        m = re.search(r"session_([A-Za-z0-9-]+)", str(user_id))
        if m:
            session_id = m.group(1)
    else:
        session_id = req.headers.get("X-Session-Id")

    # 保存请求/响应日志（anthropic 直通）
    os.makedirs(LOGS_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S_%f")[:-3] + f"-{os.getpid()}"  # 毫秒+PID，防多worker并发同毫秒撞名
    _t_start = time.perf_counter()  # 链路计时起点
    req_path = os.path.join(LOGS_DIR, f"{ts}-req.json")
    res_path = os.path.join(LOGS_DIR, f"{ts}-res.json")
    head_path = os.path.join(LOGS_DIR, f"{ts}-headers.json")

    _upstream_base = _channel["upstream_url"].rstrip("/") if _channel and _channel.get("upstream_url") else os.environ['UPSTREAM_URL'].rstrip('/')
    upstream_url = f"{_upstream_base}/messages"
    upstream_url = _append_query(upstream_url, req)  # 原样透传客户端 query（如 ?beta=true）
    verify = _ssl_verify()

    x_auth_token = await get_x_auth_token(req)
    _key_override = _channel.get("upstream_key", "") if _channel else ""
    upstream_headers = build_upstream_headers(x_auth_token, model, upstream_key_override=_key_override)
    body["model"] = upstream_headers['Model-Id']

    # 根据当前请求是否开启 ban_explore 来处理 Task 工具描述
    tools = _strip_task_explore_line(body.get("tools"), ban_explore=ban_explore)
    if tools is not None:
        body["tools"] = tools
    elif "tools" in body:
        body.pop("tools", None)

    # 清洗空 text content block，避免上游 400 "text content blocks must be non-empty"
    if "messages" in body:
        body["messages"] = _sanitize_messages(body["messages"])

    # headers = dict(req.headers)
    headers = dict()
    headers.update(upstream_headers)
    await _dump_json_async(head_path, headers)
    await _dump_json_async(req_path, body)
    # ---- non-stream ----
    if not stream:
        r = None
        last_exception = None
        success = False
        final_valid = False
        upstream_attempts = 0
        _t_upstream_ms = None
        _tracker = inflight.Tracker()
        try:
            async with shared_client(_upstream_base, verify, TRUST_ENV) as client:
                for attempt in range(MAX_RETRIES):
                    upstream_attempts += 1
                    try:
                        _t_up0 = time.perf_counter()
                        _tracker.enter("waiting_upstream")
                        r = await client.post(upstream_url, headers=upstream_headers, json=body)
                        _tracker.done()
                        _t_upstream_ms = int((time.perf_counter() - _t_up0) * 1000)
                        last_exception = None
                        if r.status_code == 200:
                            try:
                                resp_json = r.json()
                                if _has_valid_content(resp_json):
                                    success = True
                                    final_valid = True
                                    break
                                logging.warning(f"Attempt {attempt} empty content (anthropic non-stream), retrying: {r.text[:200]}")
                                _dbg = await write_debug_async(LOGS_DEBUG, ts, attempt, model, "empty_content", r.text[:2000])
                                if _dbg: logging.warning(f"  -> debug: {_dbg}")
                            except Exception:
                                # JSON 解析失败：透传原始响应
                                success = True
                                break
                        else:
                            # 非 200 一律重试，最大次数后透传
                            _dbg = await write_debug_async(LOGS_DEBUG, ts, attempt, model, f"http_{r.status_code}", r.text[:2000])
                            logging.warning(f"Attempt {attempt} non-200 (anthropic non-stream): {r.status_code} {r.text[:200]}" + (f" -> debug: {_dbg}" if _dbg else ""))
                    except Exception as e:
                        _tracker.done()  # 释放在途计数，避免异常泄漏
                        last_exception = e
                        _t_up_type = type(e).__name__  # 记录异常类型，空消息异常也能区分
                        logging.warning(f"Attempt {attempt} upstream error (anthropic non-stream): {_t_up_type}: {e}")

                    if attempt < MAX_RETRIES - 1:
                        await asyncio.sleep(_retry_delay(attempt))
                        x_auth_token = await get_x_auth_token(req)
                        upstream_headers = build_upstream_headers(x_auth_token, model, upstream_key_override=_key_override)
        except Exception as e:
            _tracker.done()
            last_exception = e
            logging.error(f"Failed to create httpx client (anthropic non-stream): {e}")

        _timing = {
            "t_upstream_ms": _t_upstream_ms,
            "t_total_ms": int((time.perf_counter() - _t_start) * 1000),
        }

        if not success:
            if r is not None:
                # 透传上游最后一次错误响应
                error_msg = f"HTTP {r.status_code}"
                logging.error(f"All retries exhausted (anthropic non-stream), passing through: {error_msg}")
                await _dump_json_async(res_path, _resp_to_obj(r))
                await append_index_anthropic_async(ts, req_path, upstream_attempts, False, LOGS_DIR, model, api_key=_api_key, messages=body.get("messages", []), channel_key=_channel_key, debug_file=debug_filename(ts, model), timing=_timing)
                return Response(
                    content=r.content,
                    status_code=r.status_code,
                    media_type=r.headers.get("content-type", "application/json"),
                )
            else:
                error_msg = str(last_exception) if last_exception else "unknown"
                logging.error(f"All retries exhausted (anthropic non-stream): {error_msg}")
                await _dump_json_async(res_path, {"error": "max_retries_exceeded", "detail": error_msg})
                await append_index_anthropic_async(ts, req_path, upstream_attempts, False, LOGS_DIR, model, api_key=_api_key, messages=body.get("messages", []), channel_key=_channel_key, debug_file=debug_filename(ts, model), timing=_timing)
                return JSONResponse(
                    status_code=502,
                    content={"type": "error", "error": {"type": "max_retries_exceeded", "message": f"上游多次失败({MAX_RETRIES}次): {error_msg}"}},
                )

        await _dump_json_async(res_path, _resp_to_obj(r))
        tok_in, tok_out, cache_in = 0, 0, 0
        usage = {}
        try:
            resp_json = r.json()
            usage = resp_json.get("usage", {})
            tok_in = usage.get("input_tokens") or usage.get("prompt_tokens") or 0
            tok_out = usage.get("output_tokens") or usage.get("completion_tokens") or 0
            cache_in = (usage.get("cache_read_input_tokens") or 0) + (usage.get("cache_creation_input_tokens") or 0)
        except Exception:
            pass
        await append_index_anthropic_async(ts, req_path, upstream_attempts, final_valid, LOGS_DIR, model, tok_in, tok_out, cache_in=cache_in, api_key=_api_key, messages=body.get("messages", []), channel_key=_channel_key, usage=usage, timing=_timing)
        return Response(
            content=r.content,
            status_code=r.status_code,
            media_type=r.headers.get("content-type", "application/json"),
            headers=_channel_response_headers(_channel_key, _channel_fallback, _channel),
        )

    # ---- stream SSE (pure pass-through) ----
    async def anthropic_sse_passthrough() -> AsyncIterator[bytes]:
        up_chunks: List[Any] = []
        connection_established = False
        upstream_attempts = 0
        last_exception = None
        last_retry_status = None
        retry_headers = upstream_headers
        retry_token = x_auth_token
        _t_ttfb_ms = None          # 首字节(message_start)耗时
        _tracker = inflight.Tracker()

        try:
            async with shared_client(_upstream_base, verify, TRUST_ENV) as client:
                # Retry loop: only retries BEFORE any bytes are yielded to the client
                for attempt in range(MAX_RETRIES):
                    upstream_attempts += 1
                    try:
                        _tracker.switch("waiting_upstream")
                        async with client.stream("POST", upstream_url, headers=retry_headers, json=body) as r:
                            up_chunks.append({
                                "type": "anthropic_passthrough_sse_meta",
                                "status_code": r.status_code,
                                "headers": dict(r.headers),
                            })

                            if is_rate_limit_status(r.status_code):
                                err = await r.aread()
                                last_retry_err_text = err.decode("utf-8", errors="replace")
                                last_retry_status = r.status_code
                                up_chunks.append({"type": "error_body", "body": last_retry_err_text})
                                _dbg = await write_debug_async(LOGS_DEBUG, ts, attempt, model, "rate_limit", last_retry_err_text[:2000])
                                logging.warning(f"Attempt {attempt} rate limit (anthropic stream): {r.status_code}" + (f" -> debug: {_dbg}" if _dbg else ""))
                                if attempt < MAX_RETRIES - 1:
                                    await asyncio.sleep(_retry_delay(attempt))
                                    retry_token = await get_x_auth_token(req)
                                    retry_headers = build_upstream_headers(retry_token, model, upstream_key_override=_key_override)
                                continue

                            # Connection established — from here we yield directly, no more retries
                            connection_established = True

                            if r.status_code >= 400:
                                err = await r.aread()
                                err_text = err.decode("utf-8", errors="replace")
                                up_chunks.append({"type": "error_body", "body": err_text})
                                error_data = {"type": "error", "error": {"type": "api_error", "message": err_text}}
                                yield f"event: error\ndata: {json.dumps(error_data, ensure_ascii=False)}\n\n".encode("utf-8")
                                return

                            # Stream with early commit on message_start:
                            # Buffer until we see message_start, then flush + pass-through directly.
                            # If stream ends without message_start → no valid response → retry.
                            log_buf = bytearray()   # full capture for logging
                            raw_buf = bytearray()   # pre-commit buffer only
                            committed = False
                            line_buf = ""

                            async for raw in r.aiter_bytes():
                                log_buf.extend(raw)
                                if committed:
                                    yield raw
                                else:
                                    raw_buf.extend(raw)
                                    line_buf += raw.decode("utf-8", errors="replace")
                                    while "\n" in line_buf:
                                        line, line_buf = line_buf.split("\n", 1)
                                        if line.startswith("data:"):
                                            data_part = line[5:].strip()
                                            if data_part and data_part != "[DONE]":
                                                try:
                                                    if json.loads(data_part).get("type") == "message_start":
                                                        committed = True
                                                        connection_established = True
                                                        if _t_ttfb_ms is None:
                                                            _t_ttfb_ms = int((time.perf_counter() - _t_start) * 1000)
                                                        _tracker.switch("streaming")  # 进入透传阶段
                                                        yield bytes(raw_buf)
                                                        raw_buf = bytearray()
                                                        break
                                                except json.JSONDecodeError:
                                                    pass

                            if not committed:
                                raw_text = raw_buf.decode("utf-8", errors="replace")
                                if attempt < MAX_RETRIES - 1:
                                    _dbg = await write_debug_async(LOGS_DEBUG, ts, attempt, model, "no_message_start", raw_text[:4000])
                                    logging.warning(f"Attempt {attempt} no message_start in SSE (anthropic stream), retrying" + (f" -> debug: {_dbg}" if _dbg else ""))
                                    connection_established = False
                                    up_chunks.clear()
                                    await asyncio.sleep(_retry_delay(attempt))
                                    retry_token = await get_x_auth_token(req)
                                    retry_headers = build_upstream_headers(retry_token, model, upstream_key_override=_key_override)
                                    continue
                                else:
                                    connection_established = True
                                    yield bytes(raw_buf)

                            # Parse full log_buf for logging
                            for line in log_buf.decode("utf-8", errors="replace").splitlines():
                                if line.startswith("data:"):
                                    data_part = line[5:].strip()
                                    if data_part and data_part != "[DONE]":
                                        try:
                                            up_chunks.append(json.loads(data_part))
                                        except json.JSONDecodeError:
                                            pass
                            return

                    except Exception as e:
                        if connection_established:
                            # Already streaming to client, can't retry
                            logging.warning(f"Stream interrupted (anthropic stream): {e}")
                            return
                        last_exception = e
                        logging.warning(f"Attempt {attempt} upstream error (anthropic stream): {e}")
                        if attempt < MAX_RETRIES - 1:
                            await asyncio.sleep(_retry_delay(attempt))
                            retry_token = await get_x_auth_token(req)
                            retry_headers = build_upstream_headers(retry_token, model, upstream_key_override=_key_override)

                # All retries exhausted without connecting
                error_msg = str(last_exception) if last_exception else (f"HTTP {last_retry_status}" if last_retry_status else "unknown")
                logging.error(f"All retries exhausted (anthropic stream): {error_msg}")
                err_event = {"type": "error", "error": {"type": "max_retries_exceeded", "message": f"上游多次失败({MAX_RETRIES}次): {error_msg}"}}
                yield f"event: error\ndata: {json.dumps(err_event, ensure_ascii=False)}\n\n".encode("utf-8")

        except Exception as e:
            logging.error(f"Failed to create httpx client (anthropic stream): {e}")
            err_event = {"type": "error", "error": {"type": "connection_error", "message": str(e)}}
            yield f"event: error\ndata: {json.dumps(err_event, ensure_ascii=False)}\n\n".encode("utf-8")
        finally:
            _tracker.done()  # 释放在途计数（正常结束 / 客户端断开 / 异常都会走到）
            await _dump_json_async(res_path, {"type": "anthropic_passthrough_sse_capture", "chunks": up_chunks})
            _tok_in, _tok_out, _cache_in = 0, 0, 0
            _usage_raw = {}
            for _c in up_chunks:
                if isinstance(_c, dict):
                    _u = _c.get("message", {}).get("usage") or _c.get("usage") or {}
                    if _u:
                        _usage_raw.update(_u)
                        _tok_in = _u.get("input_tokens") or 0
                        _tok_out = _u.get("output_tokens") or 0
                        _cache_in = (_u.get("cache_read_input_tokens") or 0) + (_u.get("cache_creation_input_tokens") or 0)
            _timing = {
                "t_ttfb_ms": _t_ttfb_ms,
                "t_total_ms": int((time.perf_counter() - _t_start) * 1000),
            }
            await append_index_anthropic_async(ts, req_path, upstream_attempts, connection_established, LOGS_DIR, model, _tok_in, _tok_out, cache_in=_cache_in, api_key=_api_key, messages=body.get("messages", []), channel_key=_channel_key, usage=_usage_raw, debug_file=debug_filename(ts, model) if not connection_established else "", timing=_timing)


    return StreamingResponse(
        anthropic_sse_passthrough(),
        media_type="text/event-stream",
        headers={**{"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}, **_channel_response_headers(_channel_key, _channel_fallback, _channel)},
    )


# ---------- OpenAI Chat Completions ----------
@app.post("/v1/chat/completions")
@app.post("/chat/completions")
async def openai_chat_completions(req: Request):
    """
    OpenAI-compatible endpoint:
      - non-stream: upstream JSON pass-through
      - stream: upstream OpenAI SSE pass-through
    """
    _api_key = await validate_api_key(req)
    _channel = resolve_upstream_channel(_api_key)
    if _channel and _channel.get("_error"):
        return JSONResponse({"error": {"type": "channel_error", "message": "所有绑定渠道均已离线"}}, status_code=503)
    _channel_key = _channel.get("upstream_key", "")[-4:] if _channel else ""
    _channel_fallback = _channel.get("_fallback", "") if _channel else ""
    body = await req.json()
    stream = bool(body.get("stream", False))
    resolve_custom_model(body)
    body_model = body.get("model")
    ban_explore = BAN_EXPLORE

    model_from_body: Optional[str] = body_model if isinstance(body_model, str) else None
    suffix = "--ban_explore"
    if model_from_body and model_from_body.endswith(suffix):
        # 任何带 "--ban_explore" 后缀的模型名，都强制开启屏蔽 Explore
        ban_explore = True
        base_model = model_from_body[: -len(suffix)]
        model = _resolve_model_name(base_model or "byenv")
    else:
        model = _resolve_model_name(body_model)

    # session相关
    session_id = None
    session_metadata = body.get("metadata")
    if isinstance(session_metadata, dict):
        user_id = session_metadata.get("user_id") or ""
        m = re.search(r"session_([A-Za-z0-9-]+)", str(user_id))
        if m:
            session_id = m.group(1)
    else:
        session_id = req.headers.get("X-Session-Id")

    # 保存请求/响应日志（OpenAI 直通）
    os.makedirs(LOGS_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S_%f")[:-3] + f"-{os.getpid()}"  # 毫秒+PID，防多worker并发同毫秒撞名
    _t_start = time.perf_counter()  # 链路计时起点
    req_path = os.path.join(LOGS_DIR, f"{ts}-req.json")
    res_path = os.path.join(LOGS_DIR, f"{ts}-res.json")
    head_path = os.path.join(LOGS_DIR, f"{ts}-headers.json")

    _upstream_base = _channel["upstream_url"].rstrip("/") if _channel and _channel.get("upstream_url") else os.environ['UPSTREAM_URL'].rstrip('/')
    upstream_url = f"{_upstream_base}/chat/completions"
    upstream_url = _append_query(upstream_url, req)  # 原样透传客户端 query
    verify = _ssl_verify()

    x_auth_token = await get_x_auth_token(req)
    _key_override = _channel.get("upstream_key", "") if _channel else ""
    upstream_headers = build_upstream_headers(x_auth_token, model, upstream_key_override=_key_override)
    body["model"] = upstream_headers['Model-Id']
    # 根据当前请求是否开启 ban_explore 来处理 Task 工具描述
    tools = _strip_task_explore_line(body.get("tools"), ban_explore=ban_explore)
    if tools is not None:
        body["tools"] = tools
    elif "tools" in body:
        body.pop("tools", None)

    headers = dict(req.headers)
    headers.update(upstream_headers)
    await _dump_json_async(head_path, headers)
    await _dump_json_async(head_path, dict(req.headers))
    await _dump_json_async(req_path, body)

    # ---- non-stream ----
    if not stream:
        r = None
        last_exception = None
        success = False
        upstream_attempts = 0
        _t_upstream_ms = None
        _tracker = inflight.Tracker()
        try:
            async with shared_client(_upstream_base, verify, TRUST_ENV) as client:
                for attempt in range(MAX_RETRIES):
                    upstream_attempts += 1
                    try:
                        _t_up0 = time.perf_counter()
                        _tracker.enter("waiting_upstream")
                        r = await client.post(upstream_url, headers=upstream_headers, json=body)
                        _tracker.done()
                        _t_upstream_ms = int((time.perf_counter() - _t_up0) * 1000)
                        last_exception = None
                        if r.status_code == 200:
                            success = True
                            break
                        # 非 200 一律重试，最大次数后透传
                        _dbg = await write_debug_async(LOGS_DEBUG, ts, attempt, model, f"http_{r.status_code}", r.text[:2000])
                        logging.warning(f"Attempt {attempt} non-200 (openai non-stream): {r.status_code} {r.text[:200]}" + (f" -> debug: {_dbg}" if _dbg else ""))
                    except Exception as e:
                        _tracker.done()
                        last_exception = e
                        logging.warning(f"Attempt {attempt} upstream error (openai non-stream): {type(e).__name__}: {e}")

                    if attempt < MAX_RETRIES - 1:
                        await asyncio.sleep(_retry_delay(attempt))
                        x_auth_token = await get_x_auth_token(req)
                        upstream_headers = build_upstream_headers(x_auth_token, model, upstream_key_override=_key_override)
        except Exception as e:
            _tracker.done()
            last_exception = e
            logging.error(f"Failed to create httpx client (openai non-stream): {e}")

        _timing = {"t_upstream_ms": _t_upstream_ms, "t_total_ms": int((time.perf_counter() - _t_start) * 1000)}

        if not success:
            if r is not None:
                # 透传上游最后一次错误响应
                error_msg = f"HTTP {r.status_code}"
                logging.error(f"All retries exhausted (openai non-stream), passing through: {error_msg}")
                await _dump_json_async(res_path, _resp_to_obj(r))
                await append_index_openai_async(ts, req_path, LOGS_DIR, model=model, success=False, api_key=_api_key, messages=body.get("messages", []), channel_key=_channel_key, debug_file=debug_filename(ts, model), timing=_timing)
                return Response(
                    content=r.content,
                    status_code=r.status_code,
                    media_type=r.headers.get("content-type", "application/json"),
                )
            else:
                error_msg = str(last_exception) if last_exception else "unknown"
                logging.error(f"All retries exhausted (openai non-stream): {error_msg}")
                await _dump_json_async(res_path, {"error": "max_retries_exceeded", "detail": error_msg})
                await append_index_openai_async(ts, req_path, LOGS_DIR, model=model, success=False, api_key=_api_key, messages=body.get("messages", []), channel_key=_channel_key, debug_file=debug_filename(ts, model), timing=_timing)
                return JSONResponse(
                    status_code=502,
                    content={"error": {"message": f"上游多次失败({MAX_RETRIES}次): {error_msg}", "type": "max_retries_exceeded"}},
                )

        await _dump_json_async(res_path, _resp_to_obj(r))
        tok_in, tok_out = 0, 0
        usage = {}
        try:
            resp_json = r.json()
            usage = resp_json.get("usage", {})
            tok_in = usage.get("input_tokens") or usage.get("prompt_tokens") or 0
            tok_out = usage.get("output_tokens") or usage.get("completion_tokens") or 0
        except Exception:
            pass
        await append_index_openai_async(ts, req_path, LOGS_DIR, model=model, tok_in=tok_in, tok_out=tok_out, success=r.status_code < 400, api_key=_api_key, messages=body.get("messages", []), channel_key=_channel_key, usage=usage, timing=_timing)
        return Response(
            content=r.content,
            status_code=r.status_code,
            media_type=r.headers.get("content-type", "application/json"),
            headers=_channel_response_headers(_channel_key, _channel_fallback, _channel),
        )

    # ---- stream SSE (OpenAI SSE pass-through) ----
    async def sse_passthrough() -> AsyncIterator[bytes]:
        up_chunks: List[Any] = []
        connection_established = False
        upstream_attempts = 0
        last_exception = None
        last_retry_status = None
        retry_headers = upstream_headers
        retry_token = x_auth_token
        _t_ttfb_ms = None
        _tracker = inflight.Tracker()

        try:
            async with shared_client(_upstream_base, verify, TRUST_ENV) as client:
                for attempt in range(MAX_RETRIES):
                    upstream_attempts += 1
                    try:
                        _tracker.switch("waiting_upstream")
                        async with client.stream("POST", upstream_url, headers=retry_headers, json=body) as r:
                            up_chunks.append({
                                "type": "openai_passthrough_sse_meta",
                                "status_code": r.status_code,
                                "headers": dict(r.headers),
                            })

                            if r.status_code != 200:
                                # 非 200 一律重试，最大次数后透传
                                err = await r.aread()
                                last_retry_err_text = err.decode("utf-8", errors="replace")
                                last_retry_status = r.status_code
                                up_chunks.append({"type": "error_body", "body": last_retry_err_text})
                                _dbg = await write_debug_async(LOGS_DEBUG, ts, attempt, model, f"http_{r.status_code}", last_retry_err_text[:2000])
                                logging.warning(f"Attempt {attempt} non-200 (openai stream): {r.status_code}" + (f" -> debug: {_dbg}" if _dbg else ""))
                                if attempt < MAX_RETRIES - 1:
                                    await asyncio.sleep(_retry_delay(attempt))
                                    retry_token = await get_x_auth_token(req)
                                    retry_headers = build_upstream_headers(retry_token, model, upstream_key_override=_key_override)
                                continue

                            connection_established = True

                            # Pure pass-through: tee raw bytes to client and capture for logging
                            raw_buf = bytearray()
                            async for raw in r.aiter_bytes():
                                if _t_ttfb_ms is None:
                                    _t_ttfb_ms = int((time.perf_counter() - _t_start) * 1000)
                                    _tracker.switch("streaming")
                                raw_buf.extend(raw)
                                yield raw

                            # Parse captured SSE for logging (best-effort)
                            for line in raw_buf.decode("utf-8", errors="replace").splitlines():
                                if line.startswith("data:"):
                                    data_part = line[5:].strip()
                                    if data_part and data_part != "[DONE]":
                                        try:
                                            up_chunks.append(json.loads(data_part))
                                        except json.JSONDecodeError:
                                            pass
                            return

                    except Exception as e:
                        if connection_established:
                            logging.warning(f"Stream interrupted (openai stream): {type(e).__name__}: {e}")
                            return
                        last_exception = e
                        logging.warning(f"Attempt {attempt} upstream error (openai stream): {type(e).__name__}: {e}")
                        if attempt < MAX_RETRIES - 1:
                            await asyncio.sleep(_retry_delay(attempt))
                            retry_token = await get_x_auth_token(req)
                            retry_headers = build_upstream_headers(retry_token, model, upstream_key_override=_key_override)

                # All retries exhausted
                error_msg = str(last_exception) if last_exception else (f"HTTP {last_retry_status}" if last_retry_status else "unknown")
                logging.error(f"All retries exhausted (openai stream): {error_msg}")
                error_data = {"error": {"message": f"上游多次失败({MAX_RETRIES}次): {error_msg}", "type": "max_retries_exceeded"}}
                yield f"data: {json.dumps(error_data, ensure_ascii=False)}\n\n".encode("utf-8")
                yield b"data: [DONE]\n\n"

        except Exception as e:
            logging.error(f"Failed to create httpx client (openai stream): {e}")
            error_data = {"error": {"message": str(e), "type": "connection_error"}}
            yield f"data: {json.dumps(error_data, ensure_ascii=False)}\n\n".encode("utf-8")
            yield b"data: [DONE]\n\n"
        finally:
            _tracker.done()
            # 无论正常/异常/客户端断开，尽最大努力落盘
            await _dump_json_async(res_path, {"type": "openai_passthrough_sse_capture", "chunks": up_chunks})
            # 统计 token（从 usage chunk 提取）
            _tok_in, _tok_out = 0, 0
            _usage_raw = {}
            for _c in up_chunks:
                if isinstance(_c, dict):
                    _u = _c.get("usage") or {}
                    if _u:
                        _usage_raw.update(_u)
                    _tok_in = _tok_in or (_u.get("prompt_tokens") or 0)
                    _tok_out = _tok_out or (_u.get("completion_tokens") or 0)
            _timing = {"t_ttfb_ms": _t_ttfb_ms, "t_total_ms": int((time.perf_counter() - _t_start) * 1000)}
            await append_index_openai_async(ts, req_path, LOGS_DIR, model=model, tok_in=_tok_in, tok_out=_tok_out, success=connection_established, api_key=_api_key, messages=body.get("messages", []), channel_key=_channel_key, usage=_usage_raw, debug_file=debug_filename(ts, model) if not connection_established else "", timing=_timing)

    return StreamingResponse(
        sse_passthrough(),
        media_type="text/event-stream",
        headers={**{"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}, **_channel_response_headers(_channel_key, _channel_fallback, _channel)},
    )


# ---------- OpenAI Responses API ----------
@app.post("/v1/responses")
async def openai_responses(req: Request):
    """
    OpenAI Responses API endpoint:
      - non-stream: upstream JSON pass-through
      - stream: upstream SSE pass-through (event: xxx\ndata: {...})
    """
    _api_key = await validate_api_key(req)
    _channel = resolve_upstream_channel(_api_key)
    if _channel and _channel.get("_error"):
        return JSONResponse({"error": {"type": "channel_error", "message": "所有绑定渠道均已离线"}}, status_code=503)
    _channel_key = _channel.get("upstream_key", "")[-4:] if _channel else ""
    _channel_fallback = _channel.get("_fallback", "") if _channel else ""
    body = await req.json()
    stream = bool(body.get("stream", False))
    resolve_custom_model(body)
    body_model = body.get("model")
    ban_explore = BAN_EXPLORE

    model_from_body: Optional[str] = body_model if isinstance(body_model, str) else None
    suffix = "--ban_explore"
    if model_from_body and model_from_body.endswith(suffix):
        ban_explore = True
        base_model = model_from_body[: -len(suffix)]
        model = _resolve_model_name(base_model or "byenv")
    else:
        model = _resolve_model_name(body_model)

    # session相关
    session_id = None
    session_metadata = body.get("metadata")
    if isinstance(session_metadata, dict):
        user_id = session_metadata.get("user_id") or ""
        m = re.search(r"session_([A-Za-z0-9-]+)", str(user_id))
        if m:
            session_id = m.group(1)
    else:
        session_id = req.headers.get("X-Session-Id")

    # 保存请求/响应日志
    os.makedirs(LOGS_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S_%f")[:-3]
    req_path = os.path.join(LOGS_DIR, f"{ts}-req.json")
    res_path = os.path.join(LOGS_DIR, f"{ts}-res.json")
    head_path = os.path.join(LOGS_DIR, f"{ts}-headers.json")

    _upstream_base = _channel["upstream_url"].rstrip("/") if _channel and _channel.get("upstream_url") else os.environ['UPSTREAM_URL'].rstrip('/')
    upstream_url = f"{_upstream_base}/responses"
    upstream_url = _append_query(upstream_url, req)  # 原样透传客户端 query
    verify = _ssl_verify()

    x_auth_token = await get_x_auth_token(req)
    _key_override = _channel.get("upstream_key", "") if _channel else ""
    upstream_headers = build_upstream_headers(x_auth_token, model, upstream_key_override=_key_override)
    body["model"] = upstream_headers['Model-Id']

    # ban_explore 处理 tools
    tools = _strip_task_explore_line(body.get("tools"), ban_explore=ban_explore)
    if tools is not None:
        body["tools"] = tools
    elif "tools" in body:
        body.pop("tools", None)

    headers = dict()
    headers.update(upstream_headers)
    await _dump_json_async(head_path, headers)
    await _dump_json_async(req_path, body)

    input_data = body.get("input")

    # ---- non-stream ----
    if not stream:
        r = None
        last_exception = None
        success = False
        upstream_attempts = 0
        _t_upstream_ms = None
        _tracker = inflight.Tracker()
        try:
            async with shared_client(_upstream_base, verify, TRUST_ENV) as client:
                for attempt in range(MAX_RETRIES):
                    upstream_attempts += 1
                    try:
                        _t_up0 = time.perf_counter()
                        _tracker.enter("waiting_upstream")
                        r = await client.post(upstream_url, headers=upstream_headers, json=body)
                        _tracker.done()
                        _t_upstream_ms = int((time.perf_counter() - _t_up0) * 1000)
                        last_exception = None
                        if r.status_code == 200:
                            success = True
                            break
                        _dbg = await write_debug_async(LOGS_DEBUG, ts, attempt, model, f"http_{r.status_code}", r.text[:2000])
                        logging.warning(f"Attempt {attempt} non-200 (responses non-stream): {r.status_code} {r.text[:200]}" + (f" -> debug: {_dbg}" if _dbg else ""))
                    except Exception as e:
                        _tracker.done()
                        last_exception = e
                        logging.warning(f"Attempt {attempt} upstream error (responses non-stream): {type(e).__name__}: {e}")

                    if attempt < MAX_RETRIES - 1:
                        await asyncio.sleep(_retry_delay(attempt))
                        x_auth_token = await get_x_auth_token(req)
                        upstream_headers = build_upstream_headers(x_auth_token, model, upstream_key_override=_key_override)
        except Exception as e:
            _tracker.done()
            last_exception = e
            logging.error(f"Failed to create httpx client (responses non-stream): {e}")

        _timing = {"t_upstream_ms": _t_upstream_ms, "t_total_ms": int((time.perf_counter() - _t_start) * 1000)}

        if not success:
            if r is not None:
                error_msg = f"HTTP {r.status_code}"
                logging.error(f"All retries exhausted (responses non-stream), passing through: {error_msg}")
                await _dump_json_async(res_path, _resp_to_obj(r))
                await append_index_responses_async(ts, req_path, LOGS_DIR, model=model, success=False, api_key=_api_key, input_data=input_data, channel_key=_channel_key, debug_file=debug_filename(ts, model), timing=_timing)
                return Response(
                    content=r.content,
                    status_code=r.status_code,
                    media_type=r.headers.get("content-type", "application/json"),
                )
            else:
                error_msg = str(last_exception) if last_exception else "unknown"
                logging.error(f"All retries exhausted (responses non-stream): {error_msg}")
                await _dump_json_async(res_path, {"error": "max_retries_exceeded", "detail": error_msg})
                await append_index_responses_async(ts, req_path, LOGS_DIR, model=model, success=False, api_key=_api_key, input_data=input_data, channel_key=_channel_key, debug_file=debug_filename(ts, model), timing=_timing)
                return JSONResponse(
                    status_code=502,
                    content={"error": {"message": f"上游多次失败({MAX_RETRIES}次): {error_msg}", "type": "max_retries_exceeded"}},
                )

        await _dump_json_async(res_path, _resp_to_obj(r))
        tok_in, tok_out = 0, 0
        usage = {}
        try:
            resp_json = r.json()
            usage = resp_json.get("usage", {})
            tok_in = usage.get("input_tokens") or usage.get("prompt_tokens") or 0
            tok_out = usage.get("output_tokens") or usage.get("completion_tokens") or 0
        except Exception:
            pass
        await append_index_responses_async(ts, req_path, LOGS_DIR, model=model, tok_in=tok_in, tok_out=tok_out, success=r.status_code < 400, api_key=_api_key, input_data=input_data, channel_key=_channel_key, usage=usage, timing=_timing)
        return Response(
            content=r.content,
            status_code=r.status_code,
            media_type=r.headers.get("content-type", "application/json"),
            headers=_channel_response_headers(_channel_key, _channel_fallback, _channel),
        )

    # ---- stream SSE (Responses API pass-through) ----
    async def responses_sse_passthrough() -> AsyncIterator[bytes]:
        up_chunks: List[Any] = []
        connection_established = False
        upstream_attempts = 0
        last_exception = None
        last_retry_status = None
        retry_headers = upstream_headers
        retry_token = x_auth_token
        _t_ttfb_ms = None
        _tracker = inflight.Tracker()

        try:
            async with shared_client(_upstream_base, verify, TRUST_ENV) as client:
                for attempt in range(MAX_RETRIES):
                    upstream_attempts += 1
                    try:
                        _tracker.switch("waiting_upstream")
                        async with client.stream("POST", upstream_url, headers=retry_headers, json=body) as r:
                            up_chunks.append({
                                "type": "responses_passthrough_sse_meta",
                                "status_code": r.status_code,
                                "headers": dict(r.headers),
                            })

                            if r.status_code != 200:
                                err = await r.aread()
                                last_retry_err_text = err.decode("utf-8", errors="replace")
                                last_retry_status = r.status_code
                                up_chunks.append({"type": "error_body", "body": last_retry_err_text})
                                _dbg = await write_debug_async(LOGS_DEBUG, ts, attempt, model, f"http_{r.status_code}", last_retry_err_text[:2000])
                                logging.warning(f"Attempt {attempt} non-200 (responses stream): {r.status_code}" + (f" -> debug: {_dbg}" if _dbg else ""))
                                if attempt < MAX_RETRIES - 1:
                                    await asyncio.sleep(_retry_delay(attempt))
                                    retry_token = await get_x_auth_token(req)
                                    retry_headers = build_upstream_headers(retry_token, model, upstream_key_override=_key_override)
                                continue

                            connection_established = True

                            # Pure pass-through: tee raw bytes to client and capture for logging
                            raw_buf = bytearray()
                            async for raw in r.aiter_bytes():
                                if _t_ttfb_ms is None:
                                    _t_ttfb_ms = int((time.perf_counter() - _t_start) * 1000)
                                    _tracker.switch("streaming")
                                raw_buf.extend(raw)
                                yield raw

                            # Parse captured SSE for logging (best-effort)
                            for line in raw_buf.decode("utf-8", errors="replace").splitlines():
                                if line.startswith("data:"):
                                    data_part = line[5:].strip()
                                    if data_part and data_part != "[DONE]":
                                        try:
                                            up_chunks.append(json.loads(data_part))
                                        except json.JSONDecodeError:
                                            pass
                            return

                    except Exception as e:
                        if connection_established:
                            logging.warning(f"Stream interrupted (responses stream): {type(e).__name__}: {e}")
                            return
                        last_exception = e
                        logging.warning(f"Attempt {attempt} upstream error (responses stream): {type(e).__name__}: {e}")
                        if attempt < MAX_RETRIES - 1:
                            await asyncio.sleep(_retry_delay(attempt))
                            retry_token = await get_x_auth_token(req)
                            retry_headers = build_upstream_headers(retry_token, model, upstream_key_override=_key_override)

                # All retries exhausted
                error_msg = str(last_exception) if last_exception else (f"HTTP {last_retry_status}" if last_retry_status else "unknown")
                logging.error(f"All retries exhausted (responses stream): {error_msg}")
                error_data = {"error": {"message": f"上游多次失败({MAX_RETRIES}次): {error_msg}", "type": "max_retries_exceeded"}}
                yield f"data: {json.dumps(error_data, ensure_ascii=False)}\n\n".encode("utf-8")
                yield b"data: [DONE]\n\n"

        except Exception as e:
            logging.error(f"Failed to create httpx client (responses stream): {e}")
            error_data = {"error": {"message": str(e), "type": "connection_error"}}
            yield f"data: {json.dumps(error_data, ensure_ascii=False)}\n\n".encode("utf-8")
            yield b"data: [DONE]\n\n"
        finally:
            _tracker.done()
            await _dump_json_async(res_path, {"type": "responses_passthrough_sse_capture", "chunks": up_chunks})
            _tok_in, _tok_out = 0, 0
            _usage_raw = {}
            for _c in up_chunks:
                if isinstance(_c, dict):
                    _u = _c.get("usage") or {}
                    if _u:
                        _usage_raw.update(_u)
                    _tok_in = _tok_in or (_u.get("input_tokens") or _u.get("prompt_tokens") or 0)
                    _tok_out = _tok_out or (_u.get("output_tokens") or _u.get("completion_tokens") or 0)
            _timing = {"t_ttfb_ms": _t_ttfb_ms, "t_total_ms": int((time.perf_counter() - _t_start) * 1000)}
            await append_index_responses_async(ts, req_path, LOGS_DIR, model=model, tok_in=_tok_in, tok_out=_tok_out, success=connection_established, api_key=_api_key, input_data=input_data, channel_key=_channel_key, usage=_usage_raw, debug_file=debug_filename(ts, model) if not connection_established else "", timing=_timing)

    return StreamingResponse(
        responses_sse_passthrough(),
        media_type="text/event-stream",
        headers={**{"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}, **_channel_response_headers(_channel_key, _channel_fallback, _channel)},
    )


# ===============================================
# 以下为新增的统计功能

@app.get("/")
async def index_statistic(request: Request):
    return templates.TemplateResponse(request, "dashboard.html", context=_ctx(request, "dashboard"))


@app.get("/overview")
async def overview_stat_page(request: Request):
    return templates.TemplateResponse(request, "overview_stat.html", context=_ctx(request, "overview"))


@app.get("/query")
async def query_page(request: Request):
    return templates.TemplateResponse(request, "query.html", context=_ctx(request, "query"))


@app.get("/history")
async def chat_viewer(request: Request):
    return templates.TemplateResponse(
        request,
        "chat-viewer.html",
        context=_ctx(request, "history"),
    )


# --- 公开导出浏览（access-key 验证，风格对齐「对话浏览」） ---


def _check_public_view(access_key: str, key: str = ""):
    """导出浏览身份验证：access_key 取 .env 的 ACCESS_KEY（无默认值，未配置恒拒）；key 必须是日志命名空间里能解析出的完整/后四位 key。
    返回 (err, resolved_key)：err 非空则直接返回；否则 resolved_key 为完整 key。"""
    if not verify_access_key(access_key):
        return JSONResponse({"detail": "Invalid access-key"}, status_code=403), None
    if not key:
        return JSONResponse({"detail": "Key not found"}, status_code=404), None
    roots = [r for r in _stats_roots() if Path(r).is_dir()]
    resolved = resolve_export_key(key, roots, str(ENV_DIR))
    if resolved is None:
        return JSONResponse({"detail": "Key not found"}, status_code=404), None
    if resolved == -1:
        return JSONResponse({"detail": "Key not unique (ambiguous suffix)"}, status_code=404), None
    return None, resolved


@app.get("/export/view")
async def export_view_page(request: Request, key: str = "", model: str = "",
                           access_key: str = Query(default="", alias="access-key")):
    err, resolved_key = _check_public_view(access_key, key)
    if err:
        return err
    return templates.TemplateResponse(
        request,
        "chat-viewer.html",
        context={
            "active_page": "history",
            "user_role": "shared",
            "user_name": "",
            "user_permissions": [],
            "shared_api_key": resolved_key,
            "shared_code": access_key,
            "export_view_mode": True,
            "export_key": resolved_key,
            "export_model": model,
        },
    )


@app.get("/export/view/api/aggregate")
def export_view_api_aggregate(key: str = "", model: str = "", min_messages: int = 1,
                              offset: int = 0, limit: int = 50, refresh: bool = False,
                              search: str = "", q1search: str = "",
                              access_key: str = Query(default="", alias="access-key")):
    err, resolved_key = _check_public_view(access_key, key)
    if err:
        return err
    from utils.log_routes import export_view_aggregate_payload
    return JSONResponse(export_view_aggregate_payload(
        _stats_roots(), ENV_DIR, min_messages, offset, limit, resolved_key, model, search, q1search, refresh))


@app.get("/export/view/api/aggregate/stream/start")
def export_view_api_aggregate_stream_start(key: str = "", model: str = "", min_messages: int = 1,
                                           refresh: bool = False,
                                           access_key: str = Query(default="", alias="access-key")):
    """开启聚合流式任务：后台逐叶收集，前端轮询 /aggregate/stream/{task_id} 拿增量。"""
    err, resolved_key = _check_public_view(access_key, key)
    if err:
        return err
    from utils.log_routes import export_view_aggregate_stream_start
    tid = export_view_aggregate_stream_start(
        _stats_roots(), ENV_DIR, min_messages, resolved_key, model, refresh=refresh)
    return JSONResponse({"task_id": tid})


@app.get("/export/view/api/aggregate/stream/{task_id}")
def export_view_api_aggregate_stream_poll(task_id: str,
                                          access_key: str = Query(default="", alias="access-key")):
    """取流式任务自上次轮询以来的增量 items；status=done 时前端停止轮询。

    任务已在 start 时与 key 绑定并校验过 access-key，这里只验 access-key 有效，
    不再对 key 做解析（task_id 本身即可定位任务）。
    """
    from utils.export_routes import verify_access_key
    if not verify_access_key(access_key):
        return JSONResponse({"detail": "Invalid access-key"}, status_code=403)
    from utils.log_routes import export_view_aggregate_stream_poll
    return JSONResponse(export_view_aggregate_stream_poll(task_id))


@app.get("/export/view/api/list")
def export_view_api_list(key: str = "", model: str = "", min_messages: int = 1,
                         offset: int = 0, limit: int = 50, refresh: bool = False,
                         search: str = "", q1search: str = "",
                         access_key: str = Query(default="", alias="access-key")):
    err, resolved_key = _check_public_view(access_key, key)
    if err:
        return err
    from utils.log_routes import export_view_list_payload
    return JSONResponse(export_view_list_payload(
        _stats_roots(), ENV_DIR, min_messages, offset, limit, resolved_key, model, search, q1search, refresh))


@app.get("/export/view/api/dirs")
def export_view_api_dirs(key: str = "", access_key: str = Query(default="", alias="access-key")):
    err, _ = _check_public_view(access_key, key)
    if err:
        return err
    return JSONResponse({"dirs": [], "current": ""})


@app.get("/export/view/api/file")
def export_view_api_file(key: str = "", filename: str = "", log_dir: str = "",
                         access_key: str = Query(default="", alias="access-key")):
    err, resolved_key = _check_public_view(access_key, key)
    if err:
        return err
    from utils.log_routes import _export_view_find_file, _load_conversation_file
    path = _export_view_find_file(_stats_roots(), ENV_DIR, filename, api_key=resolved_key)
    if not path:
        return JSONResponse({"error": "file not found"}, status_code=404)
    data = _load_conversation_file(path)
    if data is None:
        return JSONResponse({"error": "parse failed or invalid filename"}, status_code=500)
    return JSONResponse(data)


@app.get("/export/view/api/session-traces")
def export_view_api_session_traces(key: str = "", filename: str = "", chain_lookup: str = "",
                                   access_key: str = Query(default="", alias="access-key")):
    """按会话取 trace 明细（「关联列表」）。列表接口只回 file_count，点开才拉明细。

    chain_lookup 由列表项带过来：跨小时合并后的一条对话在多个小时叶子里各有 session 行，
    带上它才能把各段的 trace 都取回来，否则关联列表只列出最后一个小时的文件。
    """
    err, resolved_key = _check_public_view(access_key, key)
    if err:
        return err
    from utils.log_routes import session_traces_payload
    return JSONResponse(session_traces_payload(_stats_roots(), ENV_DIR, resolved_key, filename,
                                               chain_lookup=chain_lookup))


@app.get("/export/view/api/file/download")
def export_view_api_file_download(key: str = "", filename: str = "", log_dir: str = "",
                                  access_key: str = Query(default="", alias="access-key")):
    err, resolved_key = _check_public_view(access_key, key)
    if err:
        return err
    from utils.log_routes import _export_view_find_file
    path = _export_view_find_file(_stats_roots(), ENV_DIR, filename, api_key=resolved_key)
    if not path:
        return JSONResponse({"error": "file not found"}, status_code=404)
    return FileResponse(path, filename=filename, media_type="application/json")


@app.get("/export/view/api/file/raw")
def export_view_api_file_raw(key: str = "", filename: str = "", log_dir: str = "",
                             access_key: str = Query(default="", alias="access-key")):
    err, resolved_key = _check_public_view(access_key, key)
    if err:
        return err
    from utils.log_routes import _export_view_find_file
    path = _export_view_find_file(_stats_roots(), ENV_DIR, filename, api_key=resolved_key)
    if not path:
        return JSONResponse({"error": "file not found"}, status_code=404)
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return JSONResponse(data)


@app.get("/api/statistic")
def statistic_tokens_web(model: str = '', date_start: str = '', date_end: str = '', status: str = '全部', refresh: str = '', channel_key: str = '', api_key: str = ''):
    res = query_token_stats(
        _stats_roots(),
        model=model,
        date_start=date_start or '2000-01-01',
        date_end=date_end or '9999-12-31',
        status=status,
        channel_key=channel_key,
        api_key=api_key,
        force=bool(refresh),
    )
    res["synced_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
    return JSONResponse(res)


@app.get("/api/statistic/keys")
def statistic_keys_web(date_start: str = '', date_end: str = '', refresh: str = ''):
    res = query_key_stats(
        _stats_roots(),
        date_start=date_start or '2000-01-01',
        date_end=date_end or '9999-12-31',
        force=bool(refresh),
    )
    res["synced_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
    return JSONResponse(res)


@app.get("/api/statistic/channels")
def statistic_channels_web(date_start: str = '', date_end: str = '', refresh: str = ''):
    res = query_channel_stats(
        _stats_roots(),
        date_start=date_start or '2000-01-01',
        date_end=date_end or '9999-12-31',
        force=bool(refresh),
    )
    res["synced_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
    return JSONResponse(res)


@app.get("/api/statistic/channel-keys")
def statistic_channel_keys_list():
    keys = query_channel_keys(_stats_roots())
    return JSONResponse({"channel_keys": keys})


@app.get("/api/statistic/api-keys")
def statistic_api_keys_list():
    keys = query_api_keys(_stats_roots())
    return JSONResponse({"api_keys": keys})


@app.get("/metrics/realtime")
def metrics_realtime(hours: int = 2):
    """返回最近 N 小时的 RPM/TPM 数据，默认 2 小时。"""
    safe_hours = max(1, min(hours, 24))
    snapshot = get_metrics_snapshot()
    keep = safe_hours * 60
    if len(snapshot) > keep:
        snapshot = snapshot[-keep:]
    return JSONResponse(snapshot)


@app.get("/metrics/index-stats")
def index_stats():
    """返回请求的首次/总体/有效次数及成功率。"""
    first_count, total_count, valid_count = get_index_counts()
    rate = (valid_count / total_count) if total_count > 0 else 0.0
    metrics_info = get_metrics_storage_info()
    return JSONResponse({
        "first_count": first_count,
        "total_count": total_count,
        "valid_count": valid_count,
        "success_rate": round(rate, 4),
        "index_file": build_index_path(LOGS_DIR),
        "debug_dir": LOGS_DEBUG,
        "rpm_log": metrics_info["rpm_log"],
        "rate_log": metrics_info["rate_log"],
        "metrics_window_minutes": metrics_info["metrics_window_minutes"],
        "rate_window_minutes": metrics_info["rate_window_minutes"],
        "scanner_alive": metrics_info.get("scanner_alive", False),
    })


@app.get("/metrics/rate-history")
def rate_history(hours: int = 2):
    """返回最近 N 小时的有效率时序数据。"""
    safe_hours = max(1, min(hours, 24))
    history = get_rate_history()
    keep = safe_hours * 60
    if len(history) > keep:
        history = history[-keep:]
    return JSONResponse(history)


register_log_routes(app)
register_session_routes(app, LOGS_DIR)
register_key_routes(app, templates)
register_channel_routes(app, templates)
register_export_routes(app, LOGS_DIR)
register_backup_routes(app, LOGS_DIR, port=os.getenv("PROXY_PORT", "4000"))
register_obs_routes(app, templates)
register_logs_routes(app, templates, active_env_dir=ENV_DIR)
register_user_routes(app, templates)
register_debug_routes(app, LOGS_DEBUG, STARTUP_DATE_TAG)
register_reflection_routes(app, templates)


# ---------------------------------------------------------------------------
# catch-all 透传路由：必须在所有具体路由注册之后。FastAPI「先注册先匹配」，
# 已定义的具体路径会优先命中，只有未定义路径落到这里。
# ---------------------------------------------------------------------------
@app.api_route(
    "/{full_path:path}",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"],
)
async def passthrough(full_path: str, req: Request):
    """未定义路径 → 原样反向代理到上游域名根 + 该路径（仍鉴权 + 换上游 key）。"""
    if not _should_passthrough(full_path):
        return JSONResponse({"detail": "Not Found"}, status_code=404)

    # 1) 客户端鉴权（与 /v1/messages 一致，不过返回 401）
    _api_key = await validate_api_key(req)
    # 2) 选渠道 + 离线回退
    _channel = resolve_upstream_channel(_api_key)
    if _channel and _channel.get("_error"):
        return JSONResponse(
            {"error": {"type": "channel_error", "message": "所有绑定渠道均已离线"}},
            status_code=503,
        )

    # 3) 上游域名根 + 原始路径 + 原始 query
    root = _upstream_root(_channel)
    target = f"{root}/{full_path}"
    target = _append_query(target, req)

    # 4) 原样透传 method + body + headers（换上游 key、剔逐跳头）
    upstream_key = _channel.get("upstream_key", "") if _channel else os.getenv("UPSTREAM_API_KEY", "")
    fwd_headers = _filter_forward_headers(req.headers, upstream_key)
    body = await req.body()
    verify = _ssl_verify()

    ts = datetime.now().strftime("%H:%M:%S")
    _t0 = time.perf_counter()
    try:
        client = await get_client(root, verify, TRUST_ENV)
        upstream_req = client.build_request(
            req.method, target, headers=fwd_headers, content=body or None,
        )
        r = await client.send(upstream_req, stream=True)
    except Exception as ex:
        logging.warning("passthrough %s %s -> upstream error: %s", req.method, full_path, ex)
        return JSONResponse(
            {"error": {"type": "upstream_error", "message": str(ex)}}, status_code=502,
        )

    async def _body_iter():
        try:
            async for chunk in r.aiter_raw():
                yield chunk
        finally:
            await r.aclose()
            logging.info(
                "passthrough %s /%s -> %s [%s] %dms",
                req.method, full_path, target, r.status_code,
                int((time.perf_counter() - _t0) * 1000),
            )

    return StreamingResponse(
        _body_iter(),
        status_code=r.status_code,
        headers=_filter_response_headers(r.headers),
    )


if __name__ == "__main__":
    import uvicorn
    import argparse

    parser = argparse.ArgumentParser(description="Start the LLM proxy")
    parser.add_argument("--ban_explore", action="store_true",
        help="Remove '- Explore:' line from Task tool descriptions in /v1/messages")
    parser.add_argument("--ban_stream", action="store_true",
        help="Disable stream requests for anthropic api /v1/messages")

    args = parser.parse_args()

    if args.ban_explore:
        os.environ["BAN_EXPLORE"] = "true"

    if args.ban_stream:
        os.environ["BAN_STREAM"] = "true"

    host = os.getenv("PROXY_HOST", "127.0.0.1")
    port = int(os.getenv("PROXY_PORT", "4000"))

    os.makedirs(LOGS_DIR, exist_ok=True)
    os.makedirs(SERVICE_LOG_DIR, exist_ok=True)

    _meta_path = os.path.join(SERVICE_LOG_DIR, "app-meta.json")
    _previous_logs_dir = None
    try:
        with open(_meta_path, "r", encoding="utf-8") as _mf:
            _old_meta = json.load(_mf)
        _previous_logs_dir = _old_meta.get("logs_dir")
    except (FileNotFoundError, json.JSONDecodeError):
        pass
    _meta_content = {"logs_dir": LOGS_DIR, "previous_logs_dir": _previous_logs_dir}
    try:
        with open(_meta_path, "w", encoding="utf-8") as _mf:
            json.dump(_meta_content, _mf)
    except Exception:
        pass
    # COMPAT: 旧版 CLI 从 logs/app-meta-port{port}.json 读取，可移除整段
    _legacy_meta_path = os.path.join("logs", f"app-meta-port{port}.json")
    try:
        os.makedirs("logs", exist_ok=True)
        with open(_legacy_meta_path, "w", encoding="utf-8") as _mf:
            json.dump(_meta_content, _mf)
    except Exception:
        pass

    # 多 worker：PROXY_WORKERS 控制进程数，默认 1（行为与单 worker 完全一致）。
    # >1 时 uvicorn 用 import string "app:app" fork 多个 worker 进程，吃满多核。
    workers = int(os.getenv("PROXY_WORKERS", "1"))
    if workers > 1:
        logging.info("starting with %d workers", workers)

    # 连接阀门（默认不限制，行为与旧版一致；配置后才生效）：
    #   PROXY_MAX_CONNS       并发连接上限，超出的新连接直接 503，防止无限收连接拖垮事件循环
    #   PROXY_KEEPALIVE_TIMEOUT 空闲 keep-alive 连接回收秒数（默认 uvicorn 为 5s）
    run_kwargs = {}
    _max_conns = os.getenv("PROXY_MAX_CONNS", "").strip()
    if _max_conns.isdigit() and int(_max_conns) > 0:
        run_kwargs["limit_concurrency"] = int(_max_conns)
        logging.info("limit_concurrency=%s", _max_conns)
    _ka = os.getenv("PROXY_KEEPALIVE_TIMEOUT", "").strip()
    if _ka.isdigit() and int(_ka) > 0:
        run_kwargs["timeout_keep_alive"] = int(_ka)
        logging.info("timeout_keep_alive=%s", _ka)

    uvicorn.run("app:app", host=host, port=port, log_level="info", workers=workers, **run_kwargs)
