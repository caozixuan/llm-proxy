# 公开导出/浏览 — URL 驱动端点说明（README_export_public.md）

一组**公开、URL 驱动**的导出/浏览端点，用 `access-key` 做身份验证，无需登录即可访问。
风格对齐「对话浏览」页面（`/history`）；这是目前唯一的公开入口（旧的 share 模式已移除）。

> **命名注意**：查询参数拼写为 **`access-key`**（连字符）。旧版曾用 `acesskey`（拼写错误），
> 已全局改名为 `access-key`。改名前老链接中的 `acesskey` 会校验失败（403），需更新为 `access-key`。

## 端点总览

| 端点 | 方法 | 作用 |
|------|------|------|
| `/export/view` | GET | 导出浏览页（按 key 聚合会话，风格同「对话浏览」） |
| `/export/view/api/{aggregate,list,file,file/download,file/raw,dirs}` | GET | 浏览页后端数据接口 |
| `/export/submit` | GET | 正式导出提交（建真实 export_records 任务，可选导出类型 mode） |
| `/export/status` | GET | 按 export_id 查询导出状态 |
| `/keys/export/browse` | GET | **内部**鉴权的 key 选择落地页（侧栏「导出浏览」入口） |

URL 形态（示例）：

```
http://<host>:<port>/export/view?key=<完整key>&model=<model>&access-key=<ak>
http://<host>:<port>/export/submit?key=<完整key>&model=<model>&export_name=<名称>&mode=<类型>&access-key=<ak>
http://<host>:<port>/export/status?export_id=<id>&access-key=<ak>
```

## 身份验证

- `access-key` 用 `hmac.compare_digest(access_key, os.getenv("ACCESS_KEY",""))` 校验。
  **无默认值**：`.env` 未配置 `ACCESS_KEY` 时恒拒。错/缺 → 403 `Invalid access-key`。
- `key` 用 `resolve_export_key(key, roots, env_dir)` 在**日志命名空间**里解析（见「key 匹配规则」），
  不再用 `find_key` 查 `api_keys` 表；解析不到返回 404。
- 校验顺序：先 `access-key`（错 → 403 `Invalid access-key`），再 `key`（无/解析不到 → 404 `Key not found`）。

代码位置：
- `/export/view` 系列：`app.py` 的 `_check_public_view(access_key, key)`（约 `app.py:1762`）。
- `/export/submit`：`utils/export_routes.py` 的 `_check_public_export(key, access_key)`。
- `/export/status`：`export_routes.py` 内联校验（仅 `access-key`，URL 带 `key` 时再校验归属一致）。

## key 匹配规则（`resolve_export_key`）

`utils/export_routes.py:213` 把 URL 传入的 `key` 解析成**日志命名空间**里的完整 api_key。可接受三种形态：

1. **全量 key**：`sk-...mQOk`（严格区分大小写精确命中）。
2. **后四位后缀**：`mQOk`（唯一命中则返回该完整 key）。
3. **slot 形式**：`key-mQOk`（即「导出概览」里显示的 `key_slot`，自动剥掉 `key-` 前缀再按后缀匹配）。

决策规则：
- 归一化：去空白、剥 `key-` 前缀；空串 → `None`（404）。
- 先按完整值精确命中；否则按后四位后缀匹配。
- 后缀命中**多个** → 返回 `-1`，调用方回 `Key not unique (ambiguous suffix)`（404）。
- 后缀命中**零个** → 返回 `None`，调用方回 `Key not found`（404）。
- 键集来自 `_collect_log_keys`（`utils/export_routes.py:170`），以 **`build_stats_multi` 扫盘结果**为真相源，
  **不是** `api_keys` 表——日志 key 与签发 key 本就是不同命名空间（签发 key 很少，日志里绝大多数
  key 从未被签发）。
- `_collect_log_keys` 带 TTL 缓存（`EXPORT_KEY_TTL`，默认 **30s**，见 `export_routes.py:167`），
  避免同一页面的多个 `/export/view/*` 串行请求各自重扫。

## 1. `/export/view` — 导出浏览页

- 页面路由（`app.py:1779`）渲染 `templates/chat-viewer.html`，注入 `export_view_mode=True` 上下文
  （`export_key` / `export_model`），复用「对话浏览」页的前端逻辑。
- 与「对话浏览」的差异：跨该 key 落到的叶子聚合，隐藏来源/时间目录下拉（前端 `_exportViewMode`
  分支）；**顶栏不再显示 `key=… · model=…`**（该提示条已移除，key/model 仍作为请求参数内部使用）。
- 后端接口（`app.py:1803` 起）落到 `utils/log_routes.py` 的模块级聚合函数：
  - `export_view_aggregate_payload(...)`（`log_routes.py:871`）— 分页聚合（q1search / 深度 search / model 过滤）
  - `export_view_list_payload(...)`（`log_routes.py:968`）— 单会话文件行展开
  - `_export_view_find_file(...)`（`log_routes.py:1052`）/ `_load_conversation_file(...)` — 文件定位与加载
  - newapi 叶子走 `_NidbBackend`，native 叶子走 `_ss`（`utils.session_store`）；后端由 `_read_backend` 按 `_root_format` 选择。

### 叶子集加载（关键性能优化）

浏览页的叶子集由 `_export_key_leaves(roots, env_dir, api_key)`（`log_routes.py:826`）决定：

- **按 key 的预计算分布取叶子**：直接读 `build_stats_multi(rows[].mtime_cells)` —— 这正是
  「该 key 落在哪些 mtime 目录」的表（与 `/export/submit` 取 mtime_dirs 同源），再用
  `_resolve_mt_for` 把每个 mtime key（`<root_id>/<rel>` 或裸 `<rel>`）解析回叶子绝对路径。
  只加载该 key 真正落到的叶子（实测目标 key 落 13 个 mtime 目录），而非遍历全部 1900+ 叶子。
- 逐叶打开 `index.db` 的总时间由**分钟级**降至 **~2.5-2.7s 热响应**（NFS 上的 index.db 读取是剩余主延迟）。
- `api_key` 为空 / 分布未命中 / 解析失败时，退回 `_export_view_leaves`（`log_routes.py:773`）：
  - 该函数**只**从 `leaf_status` 表枚举叶子（同步产物），**不再**用 `iter_index_dirs` 扫盘。
  - 用 `get_source_by_path` 取实际存储的 root_id（**不是** `get_root_id` 的 active-dir 折叠 ——
    后者把活跃 env 目录折叠成 `default`，但它的叶子的 root_id 是 md5 哈希如 `1e7ae809` / `01cfd4b2`，
    折叠后 `bulk_get('default')` 为空，曾导致旧代码回退到扫盘）。
- `known_keys` 语义：由于叶子集被限定到目标 key，聚合/列表的 `known_keys` 只含当前 key
  （导出浏览页本就锁定单个 key，前端仅用它填充下拉，正确）；`known_models` 仍覆盖该 key 会话涉及的所有模型。

### 聚合流式加载（大 key 优化）

当 key 的会话散布在**大量** mtime 叶子（如 90+ 个）时，`/aggregate` 一次性收齐要数十秒，
前端会长时间白屏。导出浏览页改为「后台任务 + 增量 poll」：

- **`GET /export/view/api/aggregate/stream/start`**（`app.py:1815` → `export_view_aggregate_stream_start`，
  `log_routes.py:1064`）— 立即返回 `{"task_id":"agg-<pid>-<seq>"}`，后台 daemon 线程
  `_run()` 逐叶收集（mtime 倒序，最新会话先推）。
- **`GET /export/view/api/aggregate/stream/{task_id}`**（`app.py:1829` → `export_view_aggregate_stream_poll`，
  `log_routes.py:1174`）— 返回自上次 poll 以来的**增量** `items`，外加：
  - `status`：`running` / `done` / `gone`
  - `total`：已收集到的 item 数（累计即精确总数，不再另发 COUNT）
  - `leaves_total` / `leaves_done`：叶子进度（前端展示「目录 x/y」）
  - `cached`：本次是否为缓存命中
  - `known_keys` / `known_models`：用于填充下拉
- **轮询接口只验 access-key**（`verify_access_key`），不再对 key 做解析——任务在 start 时已与
  key 绑定并校验过。**必须带 `?access-key=...` 且带 `?` 前缀**：poll 的 task_id 是路径段，
  access-key 是查询参数，若拼成 `.../{task_id}&access-key=...` 会让 `&access-key=...` 落进路径，
  导致 403。
- **结果缓存**（`_AGG_CACHE`，`log_routes.py:1046`）：同 `(api_key, model, min_messages)` 收集完成
  后写入缓存（TTL 900s）。再次打开同 key+model 时 start 直接命中缓存、立刻返回一个
  `status=done, cached=true` 的任务，跳过对 tens of thousands 会话的重复收集。
  `refresh=true` / 带 `search` / `q1search` 时**不**缓存、**不**查缓存（保证强制刷新与搜索词新鲜）。
- 前端只把收集到的条目放进内存 `aggChains`，**DOM 只保留 PAGE_SIZE(50) 的倍数窗口**；
  滚动到底 `loadMore()` 从内存补下一段（`_renderAggWindow`），不发网络请求。状态栏在
  running 时显示「加载中… 已收集 N 条（目录 x/y）」，done 后切回「已显示 x / 共 y 条」。
  URL 的 `model` 参数会被注入「模型」下拉并选中，且随 start 传给后台（只收集该模型会话）。
- 任务 TTL 300s（`_AGG_STREAM_TTL`）：超时未被 poll 即清理；poll 到 `gone` 时前端退回一次
  全量 `/aggregate`。

## 2. `/export/submit` — 正式导出提交

`export_routes.py:1741`。等价于在「导出」页手工建一个正式导出任务：

1. 校验 `access-key` + `key`。
2. `build_stats_multi(roots, active_env_dir=env_dir)` 找该 `key` 的全部 mtime 目录。
   查不到任何 mtime → 404 `No session data found for this key`。
3. 校验 `mode`（见下表）；非法 → 400 `Invalid mode ...`。
4. `create_record(api_key, key_slot, mtime_dirs, mode, key_name=...)` 建记录。
5. `export_name` 非空时 `set_manage_name(record_id, export_name)` —— 该名字显示在正式「导出」列表的 `manage_name`。
6. `persist_params(...)` 落库任务参数 + `_enqueue_task(...)` 入队后台执行。

返回：
```json
{"export_id": <id>, "session_path": "<obs_dst>", "status": "queued", "success": true,
 "mode": "<类型>", "export_name": "<名称>"}
```

### 导出类型（`mode` 参数）

`mode` 指定导出类型，默认 `export`（不传 = 现有行为）。合法值及 OBS 输出子目录
（映射见 `export_routes.py:1731` 的 `_PUBLIC_MODE_OBS_SUB`，与 `_run_task_inner` 的 `obs_sub` 分支一致）：

| mode | OBS 子目录 | 说明 |
|------|-----------|------|
| `export`（默认） | `session/` | 原「合并导出」 |
| `reformat` | `session_analysis/` | 合并 + 落盘，不质检 |
| `eval` | `session_analysis/` | 质检（合并 + analyze，出报告） |
| `reconstruct` | `session_reconstruct/` | hermes 聚合去重 + 回填 reasoning，仅 new-api |
| `full_reformat` | `session_analysis_full/` | 全量导出：session 全部 trace 文件合并，**仅允许单个 mtime 目录** |
| `jiuwen_reformat` | `session_analysis_jiuwen/` | 九问导出：`reasoning_content` 回填重构 |

白名单 = `settings/export_modes.yaml` 里 `public: true` 的模式，随配置变化（不再硬编码在
路由里）；`model` 等未知参数不影响白名单。`_default_mode()` 兜底值 = 配置里首个
`pipeline: copy` 的模式（即 `export`）。

- `full_reformat` 与内部 `/api/export/run` 一致：只允许单个 mtime 目录。若该 key 落到多个
  mtime 目录 → 400 `全量导出(full_reformat)仅支持单个 mtime 目录，该 key 落到 N 个目录`（不静默取第一个）。
- `model` 参数仅为兼容 URL 语义保留：导出按已解析的 `mtime_dirs` 全量执行，不做按 model 的会话过滤
  （与既有「导出」页行为一致）。
- `obs_dst` 公式：`{obs_prefix}/{obs_sub}/{env_key_name}/{slot}/ex-{now_tag}/`（`obs_prefix` 空则为空串）。
- `slot = "key-" + api_key[-4:]`；`now_tag = %y%m%d%H%M%S`。

## 3. `/export/status` — 状态查询

`export_routes.py:1809`。仅需 `access-key`（URL 可不带 `key`）：

1. 校验 `access-key`（错 → 403）。
2. `get_record_resolved(export_id)` 读记录（自动解析外部文件）；无 → 404 `Not found`。
3. URL 若带 `key` 且与 `rec["api_key"]` 不一致 → 403 `Access denied`。

返回：
```json
{"export_id": <id>, "status": "<state>", "session_path": "<obs_dst>",
 "total_sessions": <n>, "error_message": "<msg>", "mode": "<类型>"}
```

- `status` 取值：`queued` / `running` / `success` / `failed` / `draft`。
- `mode` 回带任务实际执行的导出类型（提交时指定的）。

## 4. `/keys/export/browse` — 侧栏「导出浏览」落地页（内部）

为把 `/export/view` 接入 Session 模块新增。因为 `/export/view` 强制要 `key`+`access-key`、
侧栏菜单项无法预先提供，故新增一个**内部鉴权**的 key 选择落地页。

- 侧栏项：`templates/_layout.html` 的 Session 分区、「导出」下方，受同一 `show_sess_export`
  （admin 或 `export` 权限）门控，`href="/keys/export/browse"`，`active_page='export_browse'`。
- 路由：`export_routes.py:940` `export_browse_page`，渲染 `templates/export_browse.html`，
  服务端注入 `access_key = os.getenv("ACCESS_KEY","")`（不出现在侧栏 href）。
- 权限：命中既有 `_PERM_PREFIX_MAP` 的 `("/keys/export","export")` 前缀，自动要求 `export` 权限；
  **不**加入 `MONITOR_AUTH_PUBLIC_PATHS`（内部页，不公开）。
- 前端（`export_browse.html`，Vue 2）：`mounted` 拉 `/api/export/keys`（复用现有接口，带
  `X-Requested-With: XMLHttpRequest`），列出所有 key（名称 / api_key / 会话数 + 搜索框）；
  每个 key 渲染「全部模型」+ 各 model 按钮；点击 →
  `window.location = /export/view?key=<api_key>&model=<model>&access-key=<注入值>`。

## 白名单/权限改动

`app.py`：
- `MONITOR_AUTH_PUBLIC_PATHS` 加入 `/export/view`、`/export/submit`、`/export/status`（公开）。
- `_PASSTHROUGH_EXCLUDE_PREFIXES` 加入 `"export"`（不转发上游）。
- `/keys/export/browse` **不**在公开白名单，走内部 `export` 权限。

---

## 案例分析：单个 key 的完整导出流程（实测）

以一个真实 key 为例（`sk-wv1LmZ8TEQdEgtaSmNYwIArJKGMmxVmBYCUSNXNFs2SMjuo5`，记为 `K`；
`ak-28ccd680-a1fb-11f1-8d86-3510ad01d4a2` 为 `.env` 的 `ACCESS_KEY`，记为 `AK`），
演示从预览、导出到状态查询的完整链路，并列出**预期返回**（成功 + 异常）。

该 key 在 `build_stats_multi` 里落 **13 个 mtime 目录**（全在 `240fa79b` / proxy-004 源），共 **843 个会话**。

### ① 预览（对话浏览页）

```
http://<host>:8084/export/view?key=<K>&model=&access-key=<AK>
```

- **成功(200)**：返回 HTML 页面（渲染 `chat-viewer.html`），浏览器进入对话浏览视图。
  顶栏**不**显示 `key=… · model=…`（该提示条已移除）。页面加载后前端调用
  `/export/view/api/aggregate` 与 `/export/view/api/list`，内部经 `_export_key_leaves` 按 key
  分布取 13 个叶子。
  - `aggregate` 预期：`total=843`（该 key 会话数）、`known_keys=[K]`、`known_models` 为该 key 会话涉及的模型。
  - `list` 预期：`total=34860`（文件行数，每会话可展开多个 trace 文件）。
- **异常**：

| 情况 | 状态码 | 预期返回 |
|------|--------|----------|
| `access-key` 缺失/错误 | 403 | `{"detail":"Invalid access-key"}` |
| `key` 缺失或解析不到 | 404 | `{"detail":"Key not found"}` |
| `key` 后四位后缀命中多个 | 404 | `{"detail":"Key not unique (ambiguous suffix)"}` |

### ② 导出提交

```
http://<host>:8084/export/submit?key=<K>&model=&export_name=preview-0827&access-key=<AK>
```

- **成功(200)**：立即返回 `status:"queued"`，任务进入后台队列：

```json
{"export_id": 126,
 "session_path": "obs://s3-asset-b-hd-cce-aifm-nlp-exp/raw/proxy/session/env-99oR/key-juo5/ex-260827200000/",
 "status": "queued",
 "success": true,
 "mode": "export",
 "export_name": "preview-0827"}
```

  - `export_id` 是后续状态查询要用的 id。
  - `session_path` 的 `ex-260827200000` = `ex-` + `now_tag`（每次提交不同）；`key-juo5` = `key-` + key 后四位。
  - 任务在 `export_worker` 执行，通常数秒内完成（实测上一个任务首次轮询即 `success`，843 会话）。
- **指定类型**：在 `export_name` 后加 `&mode=<type>`，可选 `reformat` / `eval` / `reconstruct` /
  `full_reformat`；`mode` 非法 → 400。不同 mode 的 `session_path` 子目录不同（见上文「导出类型」表）。
- **异常**：

| 情况 | 状态码 | 预期返回 |
|------|--------|----------|
| `access-key` 错误 | 403 | `{"detail":"Invalid access-key"}` |
| `key` 解析不到 | 404 | `{"detail":"Key not found"}` |
| 该 key 无会话数据 | 404 | `{"detail":"No session data found for this key"}` |
| `mode` 非法（如 `bogus`） | 400 | `{"detail":"Invalid mode 'bogus' (allowed: export, reformat, reconstruct, full_reformat, jiuwen_reformat, eval)"}` |
| `full_reformat` 且 key 落多目录 | 400 | `{"detail":"全量导出(full_reformat)仅支持单个 mtime 目录，该 key 落到 13 个目录"}` |
| URL 含未编码中文 | 400 | `Invalid HTTP request received` |

### ③ 查询导出状态

```
http://<host>:8084/export/status?export_id=126&key=<K>&access-key=<AK>
```

- **成功(200)**（任务已完成时）：

```json
{"export_id": 126,
 "status": "success",
 "session_path": "obs://s3-asset-b-hd-cce-aifm-nlp-exp/raw/proxy/session/env-99oR/key-juo5/ex-260827200000/",
 "total_sessions": 843,
 "error_message": "",
 "mode": "export"}
```

  - `status` 取值：`queued`（排队）→ `running`（执行中）→ `success` / `failed`。
  - `total_sessions` 成功时 = 该 key 会话数（实测 843）；`running` 时通常为 0。
  - `mode` 回带提交时指定的导出类型。
- **异常**：

| 情况 | 状态码 | 预期返回 |
|------|--------|----------|
| `access-key` 错误 | 403 | `{"detail":"Invalid access-key"}` |
| `export_id` 不存在 | 404 | `{"detail":"Not found"}` |
| URL 带 `key` 但与记录归属不一致 | 403 | `{"detail":"Access denied"}` |
| 任务失败 | 200 | `status:"failed"` + `error_message:"<原因>"`（`total_sessions` 可能是部分完成数） |

### 实测参考

用 `export_id=121` 实测完整跑过一遍：提交返回
`{"export_id":121,"session_path":"obs://.../session/env-99oR/key-juo5/ex-260827195343/","status":"queued","success":true,"mode":"export","export_name":"实测导出-0827"}`，
首次轮询即 `success`，`total_sessions=843`，`error_message` 为空；连续多次轮询稳定返回 `success`。

> **注意**：含中文 `export_name`（如 `实测导出-0827`）或超长 key 的 URL 需做 URL 编码，否则直接
> 拼进地址栏会报 `Invalid HTTP request received`（400）。建议 `export_name` 用纯英文；状态查询链接
> （无中文、已含 `export_id`）可避免此问题。

---

## 已修复：key 校验数据源与「上游 key」不匹配

此前 `/export/view|submit|status` 的 `key` 校验用 **`find_key(key)` 查 `api_keys` 表**
（本项目 key 管理页**签发**的 key），而实际请求日志（`index.jsonl` / `session_cache.db` 的
`api_key` 字段）记录的是**上游请求实际携带的 key**，与 `api_keys` 表是**不同命名空间**。
后果是浏览页列出的 key 到 view 页校验会 404。

**现已在 `resolve_export_key` 中统一到日志命名空间**（见上文「key 匹配规则」）：

- 三个 `/export/*` 端点 + `/keys/export/browse` 的跳转全部消费日志 api_key。
- 支持全量 key / 后四位后缀 / `key-slot` 三种形态匹配。
- `_collect_log_keys` 以 `build_stats_multi` 扫盘结果为准（覆盖 index.jsonl / 原始 req 文件聚合），
  **不再**依赖 `api_keys` 表。

### 已修复：导出浏览慢（全量叶子扫描）

此前的浏览页对**全部登记根的所有叶子**逐叶打开 `index.db`（1900+ 叶子，跨 `/mnt/sfs_turbo`），
单次请求分钟级。现改为：

- `_export_key_leaves` 用 `build_stats_multi` **已算好的** key→`mtime_cells` 分布，只加载目标 key
  落到的叶子（实测 13 个），消除逐叶扫描。响应降至 ~2.5-2.7s（NFS 上 index.db 读取为主）。
- `_export_view_leaves`（无 key / 回退路径）只从 `leaf_status` 表枚举叶子，移除 `iter_index_dirs` 扫盘；
  并用 `get_source_by_path` 取真实 root_id，修复活跃环境叶子因 `default` 折叠查不到的问题
  （active env leaves 的 root_id 是 md5 哈希 `1e7ae809` / `01cfd4b2`）。

### 仍待确认

- `api_keys` 表（Web 签发）与日志 api_key 命名空间仍不同；签发 key 很少，
  日志里绝大多数 key 从未被签发。若后续要「浏览页也显示签发 key 的名字」，需在
  `_collect_log_keys` 里把 `api_keys` 表的 key 合并进去（当前只做日志侧的 key 校验）。
- `find_key` 的 `isdigit()` 数字分支（按 id 查）不再被 `/export/*` 使用；纯数字后缀（如
  `1234`）现在按后四位后缀语义匹配，行为已明确。
- `full_reformat` 需 key 落到**单个** mtime 目录——若目标 key 跨多目录（本环境该 key 跨 13 个），
  会 400。如需在这种 key 上跑全量导出，需先择一个 mtime 目录（内部 API `/api/export/run` 可选单个目录）。
