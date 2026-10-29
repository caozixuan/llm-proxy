# Session 导出与质检 — 设计文档

> ⚠️ 本文档为**设计文档**，部分内容已落后于当前实现。实际代码自 2026-08 起重构：
> 任务执行剥离到独立 `export_worker` 进程、模式从 2 种扩到 6 种并改为配置驱动、
> OBS 上传改为
> 收尾统一上传。带「当前实现」标注的段落是已核实的最新行为，其余段落为设计初衷，
> 以代码为准。公开 URL 导出见 [README_export_public.md](README_export_public.md)。

## 概览

Session 导出/质检模块提供 Web 界面，用于将 LLM 代理日志从本地 mtime 目录导出到结构化的 session 文件，并可选地进行质量检测（reformat + 分析 + 报告）。**当前实现**支持 7 种模式（export / reformat / reconstruct / full_reformat / jiuwen_reformat / jiuwenswarm_dev / eval），共享统一的后端任务引擎和前端轮询机制。

> **模式清单已配置化**：全部模式的 id / 名称 / OBS 子前缀 / 本地落盘目录 / processor /
> analyze / 格式限制 / 公开可见性都来自 `settings/export_modes.yaml`，网页端
> （`utils/export_routes.py` + `templates/export.html`）与离线 CLI
> （`tools/offline_reformat_export.py`）共用同一份配置，新增模式只改 YAML。

**入口（内部）**: `/keys/export` （与 Key 管理共享认证）
**入口（公开 URL）**: `/export/view` `/export/submit` `/export/status`（`acesskey` 认证，见 [README_export_public.md](README_export_public.md)）

## 架构

```
┌─────────────────────────────────────────────────────────────┐
│  前端 (templates/export.html)                                │
│  Vue 2 + Bootstrap 5 (无构建)                                │
│                                                             │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌─────────────┐ │
│  │ Key 列表  │  │ 导出Modal │  │ 进度日志  │  │ 报告面板     │ │
│  │ + 记录历史 │  │ mtime选择 │  │ 实时轮询  │  │ Markdown 内联│ │
│  └──────────┘  └──────────┘  └──────────┘  └─────────────┘ │
│        │              │              │              │        │
│        │   POST /run  │  GET /status │  GET /report │        │
└────────┼──────────────┼──────────────┼──────────────┼────────┘
         ▼              ▼              ▼              ▼
┌─────────────────────────────────────────────────────────────┐
│  后端 (utils/export_routes.py) —— 只负责建记录 + 入队          │
│                                                             │
│  register_export_routes(app, logs_dir)                      │
│    ├─ GET  /api/export/config        服务配置                │
│    ├─ GET  /api/export/keys          Key列表 + 历史记录       │
│    ├─ POST /api/export/run           创建任务(导出/质检/…)     │
│    ├─ POST /api/export/start         启动草稿(draft)记录       │
│    ├─ POST /api/export/eval          手动质检(基于已有导出)    │
│    ├─ POST /api/export/retry         重试失败/取消任务         │
│    ├─ POST /api/export/upload_retry  仅重试 OBS 上传          │
│    ├─ GET  /api/export/status/:id    任务状态 + 日志          │
│    ├─ GET  /api/export/records       记录列表                │
│    ├─ GET  /api/export/obs/ls        OBS 目录浏览             │
│    └─ GET  /api/export/eval/report/:id  质检报告(Markdown)    │
│                                                             │
│  _enqueue_task(record_id) → status=queued 入队              │
└─────────────────────────────────────────────────────────────┘
         │  队列 (export_session_record.db, status='queued')
         ▼
┌─────────────────────────────────────────────────────────────┐
│  独立 worker 进程 (utils/export_worker.py)                    │
│  python -m utils.export_worker  —— 长驻，轮询 DB 领任务        │
│                                                             │
│  _init_env(): init_db(svc_dir) + cancel_interrupted()        │
│  主循环: 抢槽位锁(slot.lock) → claim_next_queued() →          │
│         _run_one(rec) 按 task_json 重建参数执行              │
└─────────────────────────────────────────────────────────────┘
         │                          │
         ▼                          ▼
┌─────────────────┐    ┌──────────────────────┐
│ export_store.py  │    │ eval/                 │
│ SQLite 持久化     │    │  reformat.py  重组日志 │
│ export_records   │    │  eval.py      质检分析 │
│  + mode 字段     │    │  quality_rules.py     │
└─────────────────┘    └──────────────────────┘
```

## 任务模式

> **当前实现**：以下为已核实的 6 种模式（由 DB 记录的 `mode` 字段区分），
> 比早期设计的 2 种（export/eval）扩展了 4 种合并类模式。前端在「新建导出任务」Modal
> 里选「导出方式」，不再只是勾选「质检模式」。旧 `auto_eval` 布尔已降级为兼容参数
> （仅当显式 `mode` 非法时兜底 `eval` / 配置里首个 `pipeline=copy` 的模式）。
>
> **模式真相在 `settings/export_modes.yaml`**，不在代码里。每项字段：
>
> | 字段 | 含义 |
> |---|---|
> | `label` / `mode_label` / `step` | Modal 下拉长名 / 列表徽章短名 / 进度日志里的步骤名 |
> | `obs_sub` | OBS 子前缀，**对外契约**（下游消费方按此路径取数，不可随意改） |
> | `local_dir` | 本地落盘目录名（`{temp_path}/{local_dir}/{env_key}/{slot}/ex-{tag}/`） |
> | `pipeline` | `copy`（原始三元组复制）/ `reformat`（合并落盘）/ `eval`（合并 + 质检报告） |
> | `processor` | `"模块路径:函数名"`，web 与 CLI 都用 `importlib` 解析 |
> | `analyze` | 三态：`true` / `false` / 缺省。缺省表示该 processor 不接受 `analyze` 形参 |
> | `formats` | 可选，限定支持的目录格式（如 `["newapi"]`）；缺省=不限 |
> | `max_mtimes` | 可选，限制单次只能选几个 mtime 目录（如 `1`） |
> | `public` | 是否出现在公开 `/export/submit` 的 mode 白名单 |
>
> 顶层 `temp_path` 是唯一的落盘根（留空或 `"."` 回退 `env_dir.parent.parent` = 项目根）。
> 配置按 mtime+size 缓存，改完 YAML 无需重启即生效；YAML 缺失/损坏时回退到
> `utils/export_modes.py` 里的内置 `_DEFAULT_MODES` 表并记 warning，导出不会整体瘫痪。

### 导出模式 (mode="export")

用途：将原始 session 三元组文件（request/response/trace）复制到本地目录并同步到 OBS。

```
用户选 Key + mtime目录 → POST /api/export/run (mode="export")
  → 创建 record (mode="export")
  → _enqueue_task 入队（status=queued）
  → export_worker 领取执行:
      for each mtime_dir:
        1. export_session_index()  — 生成 session_index.jsonl
        2. sync_session_index()    — 按 key 过滤，复制三元组到共享 local_base
      → 收尾（循环外）统一上传整个 local_base 到 OBS
      → 更新 status = success/failed
```

本地输出: `logs_session/{env_key}/{slot}/ex-{now_tag}/`
OBS 路径: `{obs_prefix}/session/{env_key}/{slot}/ex-{now_tag}/`

**当前实现（关键变更）**: OBS 上传**不再**在 `sync_session_index()` 内部逐 mtime 目录上传，
而是把全部 mtime 目录复制到共享 `local_base` 后**收尾统一上传一次**（避免多目录并行时
重复上传 + 写入中上传的竞态）。

### 质检模式 (mode="eval")

用途：将三元组合并为完整对话，分析消息/工具/质量指标，生成 Markdown 报告。

```
触发方式 A: 新建任务选「质检」→ POST /api/export/run (mode="eval")
触发方式 B: 在已有导出记录上点 [质检] → POST /api/export/eval
  → 创建 record (mode="eval", source_export_id=原记录)
  → _enqueue_task 入队
  → export_worker 领取执行:
      for each mtime_dir:
        1. _load_session_index()     — 读取已有索引
        2. reformat_and_analyze()    — 合并三元组 + 分析 (analyze=True)
      post-loop:
        3. evaluate_sessions()       — 聚合统计 + 生成 session_report.md
        4. 统一上传整个分析目录到 OBS
      → 更新 status
```

本地输出: `logs_session_analysis/{env_key}/{slot}/ex-{now_tag}/`
OBS 路径: `{obs_prefix}/session_analysis/{env_key}/{slot}/ex-{now_tag}/`

**关键**: OBS 上传在循环外统一做一次（报告需所有 session 数据汇总后才能生成）。

### 合并导出 (mode="reformat")

`reformat_and_analyze(analyze=False)`，不跑质检，把每个 session 的 `latest_file` 合并为
单个 session JSON（无分析步骤），落 `logs_session_analysis/`，OBS `session_analysis/`。

### 重构导出 (mode="reconstruct")

hermes 重构聚合：`reconstruct_and_export` 逐 session 聚合**多个** trace 文件（去重 +
保留分支 + 回填 reasoning），落 `logs_session_reconstruct/`，OBS `session_reconstruct/`。
**仅支持 new-api 目录**（native 三元组叶子无合并文件实物，生成 session_index 前即跳过）。

### 全量导出 (mode="full_reformat")

`full_reformat_export` 把每个 session 的 `trace_list` 全部文件合并落盘（无 analyze），
落 `logs_session_analysis_full/`，OBS `session_analysis_full/`。
**单次只导一个 mtime 目录**（海量文件，限制避免放大资源占用）。

### 九问导出 (mode="jiuwen_reformat")

`jiuwen_reformat_export` 对九问格式（`reasoning_content`）的会话做 reasoning 回填重构，
落 `logs_session_analysis_jiuwen/`，OBS `session_analysis_jiuwen/`。与其他合并类模式平级，
网页下拉、公开 `/export/submit`、离线 CLI 均已放开。

### Jiuwenswarm 开发导出 (mode="jiuwenswarm_dev")

`jiuwenswarm_export` 把 trace 文件转成**带 assistant 权重的 OpenAI chat 训练记录**：
保留最大可证明快照分支、从原始 response 回填历史 reasoning，产物 schema 为
`jiuwen.segment.weighted.openai.v3`。落 `logs_session_analysis_jiuwenswarm/`，
OBS `session_analysis_jiuwenswarm/`。与其他合并类模式平级，网页下拉、公开
`/export/submit`、离线 CLI 均已放开。

产物布局（聚合模式）：

```
<local_base>/segments/<namespace>/<sha256>.json   每条会话一个加权记录
<local_base>/manifests/<namespace>.jsonl          逐条清单
<local_base>/manifests/<namespace>.summary.json   aggregation_stats / trajectory_types
<local_base>/session_index.jsonl                  引擎收尾补写的会话清单
```

处理器声明了 `file_dirs` 形参，因此引擎的签名探测会放行**跨小时聚合**：整点切分只是
存储桶、不是会话边界，勾选多个 mtime 目录 + `merge_hours` 时先合并会话再统一产出
（`/export/submit?...&merge_hours=1` 同样支持）。被复制的采集会去重，独立重试保留。
权重是候选权重（`task_outcome=unknown`），不代表任务成功判定。

### 手动质检 (POST /api/export/eval)

从已有的导出记录创建 **新的** eval 记录，复制 `api_key`、`key_slot`、`mtime_dirs`，
并在 `source_export_id` 里记下原记录 id 以便溯源：

```python
new_record_id = create_record(
    api_key=rec["api_key"], key_slot=rec["key_slot"],
    mtime_dirs=rec["mtime_dirs"], mode="eval",
    source_export_id=src_record_id,
)
```

这样每次质检都有独立的状态追踪和日志，不会污染原始导出记录。

## 统一任务引擎 (_run_task)

`_run_task` 是全部模式的统一执行入口。**当前实现**：它不再在 app 进程的
后台线程里运行，而是由独立的 `utils/export_worker.py` 进程执行。app 只负责
建记录 + `_enqueue_task` 入队（`status=queued`）；worker 用
`claim_next_queued()` 领取后调用 `_run_task_from_record()`，从 DB 记录 +
`task_json` 参数重建执行上下文并跑 `_run_task()`。

**为什么 task_json 必须持久化**：`env_dir` / `env_key_name` 是传给
`register_export_routes(app, logs_dir)` 的绝对路径，无法由
`SERVICE_LOG_DIR`（`logs/port<P>/<seg>`）反推——app 侧日志根是
`get_log_dir("logs_all")`（`logs_all/<env>`），base 与日期段都不同。
worker 是独立进程，拿不到 app 的内存闭包，故这些参数必须随任务落库
（旧记录缺 `env_dir` 时兜底用 `get_log_dir("logs_all").parent`）。

```
_run_task(record_id, env_dir, env_key_name, obs_prefix, now_tag, mode, force=False)
  │
  ├─ 按 mode 从 settings/export_modes.yaml 解析 processor 并注入 _run_task_inner
  │    （resolve_processor 用 importlib 按 "模块:函数" 解析；缺失则任务直接报错）
  │    export                              → 无 processor（pipeline=copy，走 export_session_index）
  │    reformat / eval                     → reformat_and_analyze   (analyze=false / true)
  │    reconstruct                         → reconstruct_and_export  (逐 session 聚合多个 trace)
  │    full_reformat                       → full_reformat_export    (trace_list 全部文件合并)
  │    jiuwen_reformat                     → jiuwen_reformat_export  (reasoning_content 回填)
  │    jiuwenswarm_dev                    → jiuwenswarm_export      (快照分支聚合 + assistant 权重)
  │
  ├─ _run_task_inner(..., processor, evaluate_sessions, _run_upload_cmd, _log)
  │    ├─ local_base = temp_path/local_dir/env_key/slot/ex-tag，obs_sub 均取自配置
  │    ├─ 设置协作式取消回调 _should_cancel()（每 ~2s 节流查一次 DB，取消后永久缓存）
  │    ├─ 并发：workers（每目录线程数）× dir_workers（并行 mtime 目录数），
  │    │    记录未设时回退全局 sync 配置默认（workers=8, dir_workers=8）
  │    ├─ 遍历 mtime_dirs 执行 processor
  │    ├─ 收尾按 pipeline 分支：copy→直接上传；reformat→写 session_index.jsonl 后上传；
  │    │    eval→写 session_index.jsonl + evaluate_sessions() 出报告后上传
  │    └─ _run_upload_cmd() → 循环外统一上传整个产物目录到 OBS（全部模式）
  │
  └─ 统一状态更新:
       update_status(record_id, "success"/"failed"/"cancelled", ...)
```

> 收尾分支由 `pipeline` 驱动而非按 mode 名硬编码——历史 bug 就是 jiuwen 之类的
> 新模式落到 `mode == "eval"` 分支，被误跑 `evaluate_sessions`。

## 数据库 (export_store.py)

SQLite, 路径: `{service_log_dir}/export_session_record.db`

### export_records 表

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INTEGER PK | 自增 |
| api_key | TEXT | 筛选的 API key（空=全量） |
| key_slot | TEXT | `"all"` 或 `"key-xxxx"`（`"key-" + api_key[-4:]`） |
| mtime_dirs | TEXT (JSON) | `["26061500", "26061415"]` |
| status | TEXT | 见下方**状态机** |
| mode | TEXT | `"export"`/`"eval"`/`"reformat"`/`"reconstruct"`/`"full_reformat"`（默认 `"export"`） |
| error_message | TEXT | 失败原因 |
| total_sessions | INTEGER | 匹配/处理的 session 数 |
| files_uploaded | INTEGER | 上传文件数（export 模式） |
| files_skipped | INTEGER | 跳过文件数（export 模式） |
| obs_dst | TEXT | 云端目标路径 |
| local_copy_dir | TEXT | 本地产物目录 |
| progress_log | TEXT (JSON) | 实时日志 `[{"ts":"...", "msg":"..."}]` |
| eval_report_path | TEXT | 报告文件路径（eval 模式） |
| eval_status | TEXT | **旧字段**，兼容旧记录，新代码不写 |
| analysis_json | TEXT | 分析结果 JSON |
| source_export_id | INTEGER | 手动质检溯源（eval 记录指向原导出记录） |
| in_manage | INTEGER | 是否在管理列表 |
| manage_name | TEXT | 管理列表自定义显示名（空=回退 key_slot） |
| key_name | TEXT | key 显示名 |
| workers | INTEGER | 每目录线程数（0=回退全局默认 8） |
| dir_workers | INTEGER | 并行 mtime 目录数（0=回退默认 8） |
| leaves_cache | TEXT | 匹配叶子缓存 |
| task_json | TEXT (JSON) | **执行参数**，供独立 worker 进程重建上下文（env_dir/env_key_name/obs_prefix/now_tag/mode/force） |
| is_delete | INTEGER | 软删除标记（1=已删，列表默认过滤；行与产物元数据保留，可 restore） |
| created_at | TEXT | 创建时间 |
| started_at | TEXT | 开始执行时间 |
| finished_at | TEXT | 完成时间 |

### 状态机 (status)

```
queued   → pending → running → success
                        ↑         └→ failed
                        └─(取消)→ cancelled
draft    → (前端点「启动」) → 同一链路 queued→pending→running→...

说明：
- queued:   app 已入队，等 worker 领取
- pending:  worker 已 claim（原子置为 pending），开始执行前的中态
- running:  执行中（写 started_at）
- success / failed / cancelled: 终态（写 finished_at）
- draft:    草稿（start=False 建记录时置），由前端手动「启动」后才入队；重启不取消
- 软删除: delete → is_delete=1（仅允许 draft/success/failed/cancelled 终态或草稿）
- 重启: cancel_interrupted() 把遗留 running→failed("服务重启中断")、queued→cancelled("服务重启取消")
```

## 质检报告 (Markdown)

报告由 `utils/eval/eval.py` 的 `write_markdown()` 生成，输出为 `session_report.md`，前端通过 `<pre>` 标签内联展示（monospace 下 markdown 表格天然对齐）。

### 报告结构

```markdown
# Session 质检报告

> 总 Sessions: 10 | 工具调用: 58 | 成功: 52 (89.7%)
> 模型: claude-3-opus: 8, gpt-4: 2 | 多轮: 6 | API错误: 1

## Session 详情
| Session | Q1 | 时长(s) | 轮次 | 消息 | 工具调用 | 成功率 | 模型 | 质检 |
（精简 9 列，Q1 截断 80 字符）

## 分布统计
### 对话轮次分布 / 消息总数分布 / API Call次数 / ...
（6 个分布表）

## 技能统计
（技能名称 + 使用次数）
```

### 报告 API

```
GET /api/export/eval/report/{record_id}
→ {"report_md": "# Session 质检报告\n...", "record_id": 123}
```

认证: 使用 session cookie 检查（非 XHR header 检查），因为需要支持直接访问。

## 前端交互

### 状态轮询

统一使用 `_startPoll()`，每 2 秒轮询 `/api/export/status/{id}`：

```javascript
var done = (d.status === 'success' || d.status === 'failed');
if (done) {
    clearInterval(self.pollTimer);
    self.fetchKeys();  // 刷新列表
}
```

无论导出还是质检，每个任务都是独立的 record，完成条件相同。

### 记录行渲染（按 mode 区分）

```
export + success → 显示 "N sessions / M files" + [质检] 按钮
reformat 类       → 显示 "N sessions" + "合并导出/全量导出/…" badge（短名取自配置 mode_label）
eval + success   → 显示 "N sessions" + [报告] 按钮 + "质检" badge
running          → 显示 "导出中" 或 "质检中"（根据 mode）
```

徽章文案与「是否是 reformat 类」都由后端 `/api/export/config` 下发的 `modes` 元数据决定，
前端不再硬编码模式名。

### 操作流

```
[导出] 按钮 → 打开 Modal → 选 mtime + 选「导出方式」→ POST /run
  下拉选项 = settings/export_modes.yaml 里的全部模式（v-for 渲染，顺序即 YAML 顺序）
  ├─ 导出     → mode=export          → 轮询 → 完成后显示 [质检] 按钮
  ├─ 合并导出 → mode=reformat        → 轮询
  ├─ 重构导出 → mode=reconstruct     → 轮询
  ├─ 全量导出 → mode=full_reformat   → 轮询（只能选 1 个 mtime 目录）
  ├─ 九问导出 → mode=jiuwen_reformat → 轮询
  └─ 质检     → mode=eval            → 轮询 → 完成后显示 [报告] 按钮

Modal 里可选「保存为草稿」（start=False）→ 只建 draft 记录，点 [启动] 才入队。

[质检] 按钮（在已有导出记录上）→ POST /eval → 创建新 eval 记录（source_export_id 溯源）→ 轮询 → [报告]
[报告] 按钮 → fetch /report → 报告面板内联显示 Markdown
[详情] 按钮 → fetch /status → 日志面板显示进度
[重试] 按钮 → POST /retry（失败/取消记录重新入队）;POST /upload_retry 仅重试 OBS 上传
[删除] 按钮 → POST 删除接口（默认软删除 is_delete=1，可 restore 恢复）
```

## 认证

与 Key 管理共享 `key_state.yaml` 密码认证：

- 页面访问: `request.session["key_authenticated"]`
- API 调用: `_require_key_api()` 检查 `X-Requested-With: XMLHttpRequest` + session
- 报告端点: 仅检查 session（不要求 XHR header）

## OBS 上传

使用 `tools/obs_upload.sh` 包装 `obsutil cp -f -r`：

- 不使用 `-flat`，利用 obsutil 递归上传自动携带本地目录名的特性
- `obs_dst` 保持完整路径（含 `ex-{now_tag}/`），用于 DB 存储和前端 OBS 浏览
- 上传时将 `obs_dst` 去掉最后一层，本地路径不带尾 `/`，obsutil 自动附加目录名还原完整路径
- **当前实现**: OBS 上传在 `_run_task_inner` 的 post-loop（循环外）**统一做一次**，
  覆盖全部 6 种模式——`_run_upload_cmd()` 把整个产物目录（local_base）递归上传，
  不再逐 mtime 目录上传。`/api/export/upload_retry` 可单独重试 OBS 上传失败。

## 文件清单

| 文件 | 职责 |
|------|------|
| `settings/export_modes.yaml` | **模式真相**：id/名称/OBS 子前缀/落盘目录/pipeline/processor/公开性 |
| `utils/export_modes.py` | YAML 加载 + `ModeConfig` 建模 + processor 解析（mtime+size 缓存，改完即生效） |
| `utils/export_routes.py` | 路由注册 + 建记录/入队 + _run_task 统一引擎 |
| `utils/export_store.py` | SQLite CRUD（export_records 表，状态机/软删除） |
| `utils/export_worker.py` | **独立 worker 进程**：轮询 DB 领任务（claim_next_queued）并执行 |
| `utils/export_jobs.py` | task_json 任务参数重建（供 worker 进程恢复执行上下文） |
| `utils/export_sync.py` | session_index 生成 + 文件同步 |
| `utils/eval/reformat.py` | 三元组合并 + 单 session 分析（多进程） |
| `utils/eval/reconstruct.py` | 重构聚合（多 trace 去重/保留分支/回填 reasoning） |
| `utils/eval/reformat_full.py` | 全量导出（trace_list 全部文件合并） |
| `utils/eval/reformat_jiuwen.py` | 九问导出（reasoning_content 回填重构） |
| `utils/eval/reformat_jiuwenswarm.py` | Jiuwenswarm 开发导出（快照分支聚合 + 权重 + reasoning 回填） |
| `utils/eval/eval.py` | 统计聚合 + Markdown 报告生成 |
| `utils/eval/quality_rules.py` | 质量检测规则 |
| `utils/obs_sync.py` | OBS 上传工具函数 |
| `tools/obs_upload.sh` | obsutil cp 包装脚本 |
| `tools/offline_reformat_export.py` | 离线批量导出 CLI，与 Web 共用同一份 YAML |
| `templates/export.html` | 前端页面（Vue 2 单文件，模式下拉由配置渲染） |

## key 命名空间与匹配规则

导出模块涉及**两套互不相同的 key 命名空间**，这是历史 bug 的根源，必须先分清：

| 来源 | 内容 | 谁消费 |
|------|------|--------|
| **`api_keys` 表**（keys.db） | Web 端**签发/管理**的 key（本环境仅 2 个） | key 管理页 |
| **日志 api_key**（`session_cache.db` / newapi `index.db`） | 调用方**实际发来的**上游 key（本环境 169 个） | 「导出概览」列表、`/export/*` 校验、`/export/submit` 匹配 |

「导出概览」「导出浏览」页列出的 key 来自**日志统计**（`build_stats_multi` / `_collect_log_keys`），
显示为 `key_slot = "key-" + api_key[-4:]`（如 `key-1114`）。这些 key 大多**从未被签发**，
`api_keys` 表里没有，所以旧逻辑用 `find_key` 校验时必然 404——这就是「浏览页列出的 key
到 view 页走不通」的根因。

### resolve_export_key（公开 URL 的统一 key 解析）

`utils/export_routes.py` 的 `resolve_export_key(key, roots, env_dir)` 在**日志命名空间**里
把传入的 key 解析成完整 api_key，接受三种形态：

1. **全量 key**：`sk-...mQOk`（区分大小写精确命中）。
2. **后四位后缀**：`mQOk`（唯一命中即返回完整 key）。
3. **slot 形式**：`key-mQOk`（即概览/浏览页显示的 `key_slot`，剥掉 `key-` 前缀再按后缀匹配）。

规则：去掉 `/` `key-` 前缀 → 空串返回 `None`；先精确命中，否则按后四位后缀匹配；后缀命中
多个 → `-1`（`Key not unique`）；零个 → `None`（`Key not found`）。键集来自
`_collect_log_keys`（native DISTINCT + newapi known_keys），带 5s TTL 缓存。

三处口径统一消费该解析后的完整 key：
- `/export/view` 系列：`app.py` `_check_public_view` → 传 `resolved_key` 下推聚合过滤。
- `/export/submit`：`_check_public_export` → 用完整 key 匹配 `build_stats_multi` 的 `row["api_key"]`。
- `/export/status`：URL 带 `key` 时校验与 `rec["api_key"]` 一致。

## 公开访问（无需登录）

公开入口统一为 `/export/view` `/export/submit` `/export/status`（`access-key` 鉴权），
字段/流程见 [README_export_public.md](README_export_public.md)。

> 旧的 share 模式（`/history/shared` + `/api/shared/logs/*` + `/api/shared/export[/status]`，
> `key + code` 认证，依赖 `.env` 的 `SHARED_CODE`）已整体移除，相关路由不再存在（404）。
