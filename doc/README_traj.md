# Hermes 代理轨迹导出管道 — 调研与整合设计

## 概览

Hermes 代理的轨迹快照存在两个层面：

- **上游 new-api 合并文件**：每个请求一个 JSON，含 `req`/`resp`/`up_req`/`up_resp` 等字段——这是 `hermes_traj.py` 聚合器读的形态。
- **本仓库实际日志**：native 三元组（`-req.json`/`-res.json`/`-headers.json`）为主，目前**没有合并文件实物**。

`hermes_traj.py` 解决的核心问题：时间上相邻的代理记录不一定属于同一条线性对话——Hermes 可以同时跑前台回合和后台 memory/skill 检查，provider 重试会重放完全相同的请求。它把记录按「最后一个 user 消息锚点」分组、保留每个极大请求分支、去重精确重放、并从早期响应回填缺失的 `reasoning_content`。

本文档归拢：合并文件格式、`hermes_traj.py` 算法、`trace_list` 来源、导出管道并行框架，以及「把 hermes 聚合器接入导出管道」的整合方案。

## 1. Hermes 合并文件格式

new-api 每个请求写一个合并 JSON 文件（`utils/newapi_format.py:2-9`）：

```json
{
  "ts": "2026-08-06_19-57-09_230-833514",
  "rid": "...", "uid": "...",
  "model": "claude-opus-4-7",
  "api_key": "sk-xxxx",
  "usage": {"token_in": 0, "token_out": 0},
  "req": "<请求体 JSON 字符串>",
  "resp": "<原始响应文本：Anthropic SSE 或 OpenAI 流式 chunk / 非流式整块>",
  "up_req": "<上游请求 JSON 字符串>",
  "up_resp": "<上游响应文本>"
}
```

| 字段 | 说明 |
|---|---|
| `ts` | 时间戳（下划线格式 `YYYY-MM-DD_HH-MM-SS_SSS` 或 ISO） |
| `req` | **请求体 JSON 字符串**（含 `messages`、`tools`、`model` 等） |
| `resp` | 原始响应文本（SSE `data:` 行 / 非流式整块 JSON） |
| `up_req` / `up_resp` | 上游代理层包装后的请求 / 响应（`req`/`resp` 是下游可见的） |
| `usage` | `{token_in, token_out}`，成功判定 = `token_out > 0` |

**关键事实（现场核实，2026-08-07 更新）**：`logs_all/env-99oR/` 下 `26080600/`、`26080622/`、`26080711/` 三个叶子是 native 三元组，`index.jsonl` 的 `req_file` 指向 `...-req.json`（如 `logs_all/env-99oR/26080600/2026-08-06_19-57-09_232-833514-req.json`），**不是合并文件**。但上游 new-api 源（`log_dir.db` 的 `sources` 表，如 `f1563a05` = `.../jumper-001/data/newapi/logs/details`，模板 `["{日6}/{时8}", "details/{日6}/{时8}"]`）有**大量合并文件实物**：每个叶子 `index.db`（requests/sessions/traces/chain_index 表）+ 每请求一个合并 JSON（`ts`/`req`/`resp`/`up_req`/`up_resp`/`usage`），叶子下还有 `session_index.jsonl`（`_load_session_index` 读的就是它，含 `trace_list`）。**格式实况**：响应侧以 Anthropic SSE 为主（~94%，`event: message_start` / `content_block_delta` / `thinking_delta` / `input_json_delta`），另有 OpenAI SSE（`choices[].delta`，GLM 等模型，`reasoning_content` 在 `delta` 里）；同一文件 `resp` 与 `up_resp` 可能分属两种格式（如 `tkfly_glm-5.2`：`resp` OpenAI、`up_resp` Anthropic）。请求历史的 assistant 消息**混合形态**：OpenAI 形态（`content` 字符串 + `tool_calls` + 可能缺 `reasoning_content`）与 Anthropic 形态（`content` 为 `thinking`/`text`/`tool_use` block 列表）可共存于同一 `req`/`up_req`。

## 2. hermes_traj.py 算法（`src/export/hermes_traj.py`）

处理流水线（入口 `process_directory`，第二个定义 [hermes_traj.py:645](src/export/hermes_traj.py#L645)；第一个 [hermes_traj.py:551](src/export/hermes_traj.py#L551) 是死代码）：

### 2.1 加载 — `load_records`（[hermes_traj.py:273](src/export/hermes_traj.py#L273)）

按文件名排序读取目录下所有 `.json`，解析顶层 `req`，重汇编流式响应得到 `response_message`，并预先算好每个 assistant 消息的「签名」（content + tool_calls，排除 reasoning）。

### 2.2 分组 — `last_user_anchor`（[hermes_traj.py:350](src/export/hermes_traj.py#L350)）

每条记录以「最后一个 user 消息及其之前的历史」作为锚点分到同一个 run。这比按时间切分更抗并发和重试——前台/后台/恢复 run 各归各的组。

### 2.3 选分支 — `select_branch_records`（[hermes_traj.py:416](src/export/hermes_traj.py#L416)）

- 组内先按完整消息序列去重（`request_variants`），精确重放只留一份；
- `is_strict_prefix`（[hermes_traj.py:367](src/export/hermes_traj.py#L367)）找所有「极大」分支（不是任何其他变体前缀的那些），每个分支都保留；
- 响应相同的重放用 `response_quality`（[hermes_traj.py:371](src/export/hermes_traj.py#L371)）打分择优：**最终文本 > 有可见输出 > token_out>0 > 输出长 > 后出现**；
- **最后一条记录无条件保留**（[hermes_traj.py:473](src/export/hermes_traj.py#L473)）。

### 2.4 回填 reasoning — `repair_nested_request`（[hermes_traj.py:512](src/export/hermes_traj.py#L512)）

对每个被选中的记录，用**之前**所有记录的响应签名做索引（`response_registry` [hermes_traj.py:287](src/export/hermes_traj.py#L287)），给请求历史里缺 `reasoning_content` 的 assistant 消息补上最近的匹配值。`req` 和 `up_req` 都会修；嵌套 JSON 以字符串形式存储的会保持字符串形式重新序列化（`encode_like_original` [hermes_traj.py:71](src/export/hermes_traj.py#L71)）。

### 2.5 写 manifest — `manifest_json_lines`（[hermes_traj.py:582](src/export/hermes_traj.py#L582)）

`_manifest.jsonl` 记录每个 run 的分组依据（run_kind / user_message_preview / 首末 ts）、选择了哪些文件、reasoning 回填了多少，可审计。

### 2.6 关键设计

| 机制 | 位置 | 说明 |
|---|---|---|
| 签名匹配抗噪 | `assistant_signature` [hermes_traj.py:193](src/export/hermes_traj.py#L193) | 身份 = content + tool_calls（参数键排序归一化），明确排除 `reasoning_content`——「推理不同、内容相同」的消息仍能匹配上；Anthropic block 形态的 content 会先抽出 text/tool_use block 再算，thinking 同样不参与 |
| 流式重汇编 | `assemble_streamed_message` [hermes_traj.py:104](src/export/hermes_traj.py#L104) / `assemble_anthropic_stream` [hermes_traj.py:166](src/export/hermes_traj.py#L166) | OpenAI SSE（`delta`/完整 `message`）与 Anthropic SSE（`content_block_start`/`content_block_delta` 的 `thinking_delta`/`text_delta`/`signature_delta`/`input_json_delta`/`content_block_stop`，含非流式单对象）都装配；两种装配器归一化到同一内部形态（`content` 字符串 + `reasoning_content` + `tool_calls` + `thinking_signature`），`resp`/`up_resp` 都试、取 reasoning 更长的一侧（`extract_response_message`） |
| 回填双形态 | `backfill_messages` [hermes_traj.py:439](src/export/hermes_traj.py#L439) | 目标消息是 OpenAI 形态 → 写 `reasoning_content` 键；Anthropic block 形态 → 插入/就地填充 `thinking` block（带 `signature`）。`canonical_message` 也会剥 content 里的 thinking block，去重/分组不被推理有无干扰 |
| 原子写 | `atomic_write_json` [hermes_traj.py:537](src/export/hermes_traj.py#L537) | tmp + `os.replace`，`utf-8-sig` 读入容错 BOM，`ensure_ascii=False` 保留中文 |

**死代码已清理（2026-08-07）**：第一个 3 元组 `process_directory`（原 [hermes_traj.py:551-568]）与 `detect_pre_compaction_records`（上下文压缩检测启发式）已删除；`atomic_write_text` / `manifest_json_lines` 保留（活跃 4 元组 `process_directory` 与 `reconstruct.py` 仍用）。

## 3. trace_list 的来源与字段

### 3.1 两条来源路径

| 来源 | 函数 | 位置 |
|---|---|---|
| native | `session_store.export_sessions` → `to_unified_record` | [session_store.py:519](utils/session_store.py#L519) / [session_store.py:559](utils/session_store.py#L559) |
| new-api | `newapi_index_db.export_sessions` → `get_traces_batch` | [newapi_index_db.py:876](utils/newapi_index_db.py#L876) / [newapi_index_db.py:823](utils/newapi_index_db.py#L823) |

### 3.2 字段

| 字段 | 说明 |
|---|---|
| `filename` | **纯文件名（basename）**，不含子目录前缀 |
| `model` | 实际路由到的模型 |
| `msg_count` | 该请求的消息数（含响应侧 +1） |
| `ts` | 时间戳，**保留下划线格式**（对应真实文件名，改了对不上磁盘文件） |
| `success=False` | 失败时追加；native 还带 `total_attempts`，有 debug 时带 `debug_file` |

**filename 为何是 basename**：`_agg_one`（[newapi_index_db.py:550](utils/newapi_index_db.py#L550)）`os.path.basename(r["req_file"])`；native 的 trace 直接来自 DB 里的 filename。所以 `Path(src_dir) / filename` 与 new-api 的 `_resolve_combined_path`（[newapi_consumer.py:24](utils/newapi_consumer.py#L24)）一致——**合并文件路径 = 叶子目录 / trace.filename**。

## 4. 导出管道并行框架（要仿照的结构）

### 4.1 Web 版 `_run_task_inner`（[export_routes.py:357](utils/export_routes.py#L357)，reformat 分支 [453-479](utils/export_routes.py#L453-L479)）

```
按 mtime 目录迭代
  ├─ dir_workers 个目录并行（线程池，[493-510](utils/export_routes.py#L493-L510)）
  │   每目录内部再 workers 线程
  │   new-api 叶子前置：detect_format=="newapi" 且 needs_build → 跳过（warning 不判失败，[417-433](utils/export_routes.py#L417-L433)）
  │   reformat 分支：_load_session_index(mt_src) → 按 api_key 过滤 → reformat_and_analyze(analyze=False)
  └─ 各目录结果【串行合并】保持 mtime_dirs 原顺序（[513-525](utils/export_routes.py#L513-L525)）
收尾：
  写 local_base/session_index.jsonl（all_entries，已按 key 过滤、不含 _meta 行）
  整目录 _run_upload_cmd 上传到 obs_dst 的父目录（obs_dst.rstrip("/").rsplit("/",1)[0] + "/"）
```

路径规则（reformat 模式，[export_routes.py:381-384](utils/export_routes.py#L381-L384)）：
- `local_base = logs_session_analysis/{env_key_name}/{slot}/ex-{now_tag}`
- `obs_dst = {obs_prefix}/session_analysis/{env_key_name}/{slot}/ex-{now_tag}/`

### 4.2 离线版 `tools/offline_reformat_export.py`

- 自动探测 `SERVICE_LOG_DIR`（扫描 `logs/port*/*/export_session_record.db`）与 `ENV_DIR`（读 `logs/app-meta-port*.json` 的 `logs_dir` 推出，[offline_reformat_export.py:57-103](tools/offline_reformat_export.py#L57-L103)）。
- 逐 key：`build_stats_multi` 拿 key + mtime 目录 → `create_record(mode="reformat")` → 与 Web 完全一致的流程 → `update_status("success")`。
- `_run_one_reformat`（[offline_reformat_export.py:286-438](tools/offline_reformat_export.py#L286-L438)）复刻 Web 的 `_run_task_inner` reformat 分支，`local_base` / `obs_dst` 路径规则对齐。

### 4.3 reformat_and_analyze（[reformat.py:358](utils/eval/reformat.py#L358)）

- `workers` 硬上限 32（[reformat.py:384](utils/eval/reformat.py#L384)）；
- 每个 session 只处理 `latest_file`（[reformat.py:405](utils/eval/reformat.py#L405)、[409](utils/eval/reformat.py#L409)）；
- `_process_one`（[reformat.py:131](utils/eval/reformat.py#L131)）里 `_load_merged`（[reformat.py:37](utils/eval/reformat.py#L37)）优先三元组，new-api 合并单文件则 `src_dir / latest_file` 直接拼接（[reformat.py:68](utils/eval/reformat.py#L68)）；
- `analyze=False`：reformat-only，`_reformat_only_record`（[reformat.py:78](utils/eval/reformat.py#L78)）评估字段占位（键集合与 `_process_one` 对齐，下游读取方无需判 key 存在性），不走 `_eval` 缓存复用；
- **线程池并行而非进程池**（[reformat.py:432-437](utils/eval/reformat.py#L432-L437) 注释：spawn 会重 import app.py 跑模块级启动、worker 挂死变孤儿堆积等历史顽疾）；
- `log_dir` 由 `_log_dir_key_for`（[export_routes.py:275](utils/export_routes.py#L275)）把叶子绝对路径解析为相对 root 的 `"260728/26072813"` 形式；
- 返回 `{"total_sessions", "total_files", "errors", "results"}`。

## 5. 整合方案：平行第二导出路径

用户拍板的方向：**不是「reformat 后串行接 hermes」，而是平行第二条导出路径**：

```
offline_reformat_export.py → hermes_traj → _process_one_hermes
```

共享同一「session 迭代并行」框架（`_run_one_reformat` 的 mtime 循环 + `ThreadPoolExecutor` 逐 session 并行），但导出方式不同：

| 维度 | reformat（现有） | hermes 聚合（新） |
|---|---|---|
| 每 session 处理 | 只落盘 `latest_file` 一个文件 | 聚合该 session 的**多个** trace 文件 |
| 输出 | 1 个合并 JSON | **多个**聚合后的新文件（可能多个） |
| 上传 | 整目录一次 | 整目录一次（统一上传链路不变） |
| 语义 | 取最后一个文件 | 去重 + 保留分支 + 回填 reasoning |

### 5.1 已确认的关键决策

1. **聚合边界 = session 级**：trace_list 已按 `chain_key`（Q1 哈希）归好，只聚合组内 trace。
2. **导出方式可不同**：reformat 取最后一个文件，hermes 聚合取多个文件。
3. 聚合器**读合并文件本身**（`load_combined` 按 `Path(src_dir)/filename` 定位），而非依赖 trace_list 携带 req/resp 内容。

### 5.2 待定点 / 注意（如实记录）

- ~~现场日志目录没有 Hermes 合并文件实物~~ **已解决（2026-08-07）**：上游 new-api 源有大量合并文件实物（见 §1 关键事实更新），且真实格式是 Anthropic SSE 为主 + OpenAI SSE + 混合请求形态；`hermes_traj.py` 已补 `assemble_anthropic_stream` 与双形态签名/回填，用 `4T8e` key（`f1563a05/details/260729/26072921` 叶子，1287 sessions）全量验证：0 错误、1873→294 调用去重、回填 1222/751 条。
- `hermes_traj.py` 目前是独立 CLI（`main` / `discover_input_dirs` / `build_parser`）；接管道时**保留纯函数核心**（`load_records` / `select_branch_records` / `response_registry` / `repair_nested_request` 等），去掉 CLI 外壳或保留为调试入口。
- trace 的 `filename` 是 basename，`Path(src_dir)/filename` 与 new-api 的 `_resolve_combined_path` 一致（见 §3.2）。

## 6. 当前处理流程（--mode reconstruct 全链路）

整合已完成，`--mode reconstruct` 是平行于 reformat 的第二条导出路径。**Web 端与离线端都支持**：

- **离线**：`tools/offline_reformat_export.py --mode reconstruct`（下方全链路图）
- **Web**：`utils/export_routes.py` 的 `export_run` 用 `settings/export_modes.yaml` 的模式清单校验 mode（网页下拉、公开 `/export/submit` 白名单、离线 CLI 全部由它驱动）；`_run_task` 用 `resolve_processor` 按配置注入 processor（reconstruct → `reconstruct_and_export`，因配置无 `analyze` 字段故不传该形参）；`_run_task_inner` 的 local_base / obs_sub 全部取自配置（重构走 `logs_session_reconstruct/` + OBS `session_reconstruct/`）；reconstruct 配置了 `formats: ["newapi"]`，非 new-api 目录在 `detect_format` 处提前跳过（warning，不生成 session_index）

完整链路：

```
tools/offline_reformat_export.py --mode reconstruct
│
├─ 自动探测：SERVICE_LOG_DIR = logs/port8084/env-99oR，ENV_DIR = logs_all/env-99oR
│
├─ 逐 key 循环（build_stats_multi 从 log_dir.db 的 sources 表拿叶子目录）
│    │
│    └─ 按 mtime 目录迭代（叶子，如 f1563a05/details/260729/26072921）
│         ├─ detect_format != "newapi" → 跳过（重构仅支持合并文件，native 叶子被拦）
│         ├─ export_session_index → 生成/复用 session_index.jsonl（每行含 trace_list）
│         ├─ _load_session_index → entries，按 api_key 过滤（如 sk-ZMi2a...4T8e → 1287 sessions）
│         │
│         └─ reconstruct_and_export(src_dir, out_dir, entries, api_key, workers)
│              │  ThreadPoolExecutor（≤32 线程，IO 密集；进程池会重 import app.py
│              │  跑模块级启动、worker 挂死变孤儿堆积，历史顽疾不复用）
│              │
│              └─ 每 session 一个 worker：_process_one_hermes
│                   ├─① _resolve_trace_paths：trace_list 的 filename（basename）→ Path(src_dir)/filename
│                   ├─② read_record：解析合并 JSON，req.messages 校验
│                   │    响应装配（extract_response_message）：resp/up_resp 各试
│                   │    OpenAI(assemble_streamed_message) + Anthropic(assemble_anthropic_stream)
│                   │    两种装配器，取 reasoning 更长的一侧，归一化到统一内部形态
│                   ├─③ select_branch_records（hermes 核心）：
│                   │    · last_user_anchor：按「最后一个 user 消息及其历史」分组 → run
│                   │    · 组内按 canonical_messages 去重（精确重放只留一份）
│                   │    · is_strict_prefix 找所有「极大」请求分支（非他人前缀），每个都保留
│                   │    · 同签名（response_signature）的重放用 response_quality 择优
│                   │      （最终文本 > 可见输出 > token_out>0 > 输出长 > 后出现）
│                   │    · 最后一条记录无条件保留
│                   ├─④ 对每个选中记录：response_registry(之前所有记录的响应签名)
│                   │    → repair_nested_request 修 req 和 up_req（双修）
│                   │    · OpenAI 形态 assistant → 写 reasoning_content 键
│                   │    · Anthropic block 形态 → 插/填 thinking block（带 signature）
│                   └─⑤ 原子写：out_dir/{first_ts}/<原文件名>.json + _manifest.jsonl
│
├─ 各目录结果【串行合并】保持 mtime 原顺序
│
└─ 收尾：写 session_index.jsonl 清单 → 整目录 run_upload_cmd 上传 OBS（session_reconstruct/）
```

### 6.1 关键点

| 环节 | 说明 |
|---|---|
| 聚合边界 | session 级——trace_list 已按 `chain_key`（Q1 哈希）归好，只聚合组内 trace |
| 读什么 | 合并文件本身（`Path(src_dir)/filename`），不依赖 trace_list 携带 req/resp 内容 |
| 输出 | 每个选中记录**原文件名**写入 session 目录（原生格式保留），`req`/`up_req` 已回填；外加 `_manifest.jsonl` 审计 |
| 失败语义 | 任一合并文件损坏 → 该 session 严格失败返回 None（fail-fast），不静默产出残缺数据 |
| 双格式 | 响应装配 OpenAI/Anthropic SSE 都支持；请求历史混合形态（OpenAI 键 / Anthropic block）都回填 |
| 与 reformat 区别 | reformat 每 session 只落 1 个 `latest_file`；reconstruct 落**多个**聚合文件 + manifest，语义是去重 + 保留分支 + 回填 reasoning |
| 路径规则 | 本地 `logs_session_reconstruct/{env}/{slot}/ex-{tag}`，OBS 走 `session_reconstruct/`（平行于 reformat 的 `session_analysis/`，互不混放） |

### 6.2 产物结构

```
hermes_recon_full/
├─ 2026-07-29T21:02:34.483+08:00/        ← session 目录（first_ts 命名）
│    ├─ 2026-07-29_21-13-36_000-1676776.json   ← 选中的合并文件（原样名，req/up_req 已修）
│    ├─ 2026-07-29_21-39-06_000-1703415.json
│    └─ _manifest.jsonl                        ← 每 run 的分组依据/选中/回填计数
└─ …（共 1287 个 session 目录）
```

每 session 的 result 记录带统一评估 schema（对齐 `_reformat_only_record` 的键集合，下游无需判 key 存在性）+ hermes 特有字段（`hermes_run_count` / `hermes_selected_count` / `hermes_req_filled` / `hermes_up_req_filled` / `hermes_output_files`），最终汇总成 `{"total_sessions", "total_files", "errors", "results"}` 返回，写入 `export_session_record.db` 供网页显示「成功」。

### 6.3 各环节代码位置

| 环节 | 位置 |
|---|---|
| 离线入口 + 逐 key / mtime 迭代 + 上传 | `tools/offline_reformat_export.py` `_run_one_export` |
| Web 入口（mode 注入 + 路径 + 守卫） | `utils/export_routes.py:313` `_run_task` / `:357` `_run_task_inner` / `export_run` mode 白名单 |
| newapi 格式守卫 | 离线 `tools/offline_reformat_export.py`；Web `utils/export_routes.py` `_process_mtime`（`detect_format != "newapi"` 跳过） |
| 线程池逐 session 并行 | `utils/eval/reconstruct.py:185` `reconstruct_and_export` |
| 单 session 聚合（读 trace → 选分支 → 回填 → 写） | `utils/eval/reconstruct.py:127` `_process_one_hermes` |
| trace filename → 合并文件路径 | `utils/eval/reconstruct.py:45` `_resolve_trace_paths` |
| 合并文件解析 + 双格式响应装配 | `src/export/hermes_traj.py:463` `read_record` / `:334` `extract_response_message` / `:271` `assemble_streamed_message` / `:156` `assemble_anthropic_stream` |
| 分组 / 选分支 / 择优 | `src/export/hermes_traj.py:530` `last_user_anchor` / `:596` `select_branch_records` |
| reasoning 回填（双形态） | `src/export/hermes_traj.py:509` `response_registry` / `:664` `backfill_messages` / `:739` `repair_nested_request` |
| manifest / 原子写 | `src/export/hermes_traj.py:852` `process_directory`（`_manifest.jsonl` 生成） |

## 关键文件位置汇总

| 内容 | 位置 |
|---|---|
| hermes_traj 聚合脚本 | `src/export/hermes_traj.py` |
| reconstruct 并行处理（新） | `utils/eval/reconstruct.py:185` `reconstruct_and_export` |
| 合并文件格式定义 | `utils/newapi_format.py:2-9`（docstring） |
| `parse_combined_file` / `load_combined` | `utils/newapi_format.py:102` / `:76` |
| trace 来源（native） | `utils/session_store.py:519` `export_sessions` |
| trace 来源（new-api） | `utils/newapi_index_db.py:876` `export_sessions` |
| Web 导出主循环 | `utils/export_routes.py:357-636` |
| 离线导出（`--mode reformat/reconstruct`） | `tools/offline_reformat_export.py` |
| reformat 并行处理 | `utils/eval/reformat.py:358` |
| 双格式合成测试 | `test/test_hermes_anthropic.py` |
| session 归并（chain_key） | `utils/log_routes.py:193` `_process_req_row` / `utils/newapi_consumer.py:87` `_process_entry` |
