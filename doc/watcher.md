# new-api 实时索引服务（watcher）

把 new-api 数据源的 `index.jsonl` 增量同步进每个叶子自己的 `index.db`，
让浏览器能在**秒级**看到新写入的记录，而不是等小时级批量回填。

---

## 为什么需要它

new-api 格式的叶子目录长这样：

```
<root>/<天 261022>/<小时 26102217>/
    index.jsonl      # 每行一条请求的元数据（ts/model/api_key/req_file/usage）
    2026-...json     # 每个请求一个合并文件（~1MB，含完整 req/resp）
    index.db         # 由本项目构建的富索引（视图只读它）
```

浏览页（`/export/view`、`/sessions`）只读 `index.db`。而 `index.jsonl` 里的每条记录
还缺会话聚合所需的字段（`q1_hash` / `msg_count` / `user_turns`），必须解析那个
~1MB 的合并文件才能补上。

`index.db` 的三段式构建：

| 阶段 | 做什么 | 代价特征 |
|---|---|---|
| **ingest** | 从 `ingest_offset` 读 `index.jsonl` 新增行，写入 `requests` 表 | 极廉价，只读文件尾部 |
| **enrich** | 把 `requests` 里 `q1_hash IS NULL` 的行补齐 meta（解析合并文件） | 昂贵，NFS 读 + JSON 解析 |
| **sync_sessions** | 把 `requests` 按 rowid 水位折叠进 `sessions`/`traces`/`chain_index` | 廉价，纯 SQLite |

改造前这三段只由小时级回填 worker 触发（`AUTO_SYNC_INTERVAL=3600`），所以新记录
最长要 1 小时才可见。本服务把它压到秒级。

---

## 架构

```
src/watch/worker.py            （独立进程，由 ./app watch start 拉起）
├── 发现线程 × N                 每个 new-api 来源一个，只 stat index.jsonl
│    └─ 变了 → enqueue 到 watch_queue
├── dispatcher                 从 watch_queue 按 priority 领取
└── 同步池 (8 线程)             每个任务: ingest → enrich → sync_sessions
```

**关键设计：发现与构建彻底解耦。**

发现线程永远只做 `os.stat()`（微秒级），绝不打开 SQLite、绝不解析合并文件。
构建全部落到同步池，再慢也慢不到发现。这样即使某个叶子有 10 万行积压要跑几分钟，
其他来源的新数据依然能在 5 秒内被发现并入队。

### 为什么不能把 watcher 塞进 app 进程

曾经试过在主应用里开 daemon 线程，实测失败：

- 大叶子首建极慢（实测单叶 411s / 125k 行，另一叶 634s / 201k 行），单线程硬啃
  会把后续所有叶子堵死；
- 长时间持 `.indexer.lock` 会把专职的回填 worker 饿死。

独立进程 + 多线程池 + 优先级调度才能让「当前小时的新数据」不被「历史积压」挡住。

---

## 队列：`watch_queue` 表

存在 `${LOG_DIR}/log_dir.db`，与应用共用同一个库。

```sql
CREATE TABLE watch_queue (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    root_id    TEXT,         -- 来源标识
    root_path  TEXT,         -- 来源根目录
    leaf_path  TEXT,         -- 叶子绝对路径（唯一性以此为准）
    dir_key    TEXT,         -- 展示用，如 261022/26102217
    priority   INTEGER,      -- 越大越先处理
    reason     TEXT,         -- new | grown | rebuild
    idx_size   INTEGER,      -- index.jsonl 最新大小（持续刷新）
    offset_at  INTEGER,      -- 入队时已摄取到的偏移（算剩余量）
    status     TEXT,         -- pending | running | done | failed
    ...
);

-- 同一叶子同时只允许一条活跃任务
CREATE UNIQUE INDEX watch_queue_active
    ON watch_queue(leaf_path) WHERE status IN ('pending','running');
```

### 不会重复入队

唯一索引只在 `pending/running` 上生效（部分索引），所以：

- 任务在排队或执行中 → 再次入队被 `INSERT OR IGNORE` 静默忽略；
- 任务完成（`done`）后 → 可以正常入队下一条。

同一个叶子任意时刻最多一条活跃任务，**天然串行**，不会出现两个线程同时写一个
`index.db`。跨进程（与回填 worker 之间）另有一把 `flock`：`<leaf>/.indexer.lock`。

### 队列语义：提交的是「叶子变脏了」，不是「第几行」

发现线程不提交行数，只提交「这个叶子的 `index.jsonl` 比已摄取的水位更大」。
任务真正执行时 `ingest()` 读的是 **DB 里的 `ingest_offset` 到文件 EOF**，
不是入队时记的快照。所以：

> 20 行时入队，排队期间涨到 40 行 —— 执行时会把 40 行一起吃掉，不会漏。

### 水位推进规则（重要）

**「已入队」不等于「已同步」。** 判定水位一律以 `index.db` 里的 `ingest_offset`
为准；内存缓存只用于「文件大小完全没变」的快速短路。

历史上有个因此产生的漏同步缺陷：任务 running 时文件增长，入队被唯一索引忽略
（这是对的），但内存水位照样推进 → 任务结束后 `ingest_offset` 停在旧位置，
而下一轮扫描认为「没长过」不再入队 → 最新几行要等文件下次增长才补；
若恰好此时叶子停写，就永久挂着。

现在三处独立兜底：

1. 判定基准是 DB 的 `ingest_offset`，不是内存值；
2. 入队被忽略时**不**推进内存水位，下一轮自然重试；
3. 任务收尾做权威复核（`_requeue_if_lagging`）：文件仍大于 offset 就补一条。

---

## 发现逻辑

发现线程每轮做两档扫描：

| 模式 | 周期 | 范围 | 目的 |
|---|---|---|---|
| **快扫** | `WATCH_POLL_SEC`（默认 5s） | 最近 `FAST_SCAN_DAYS` 天 × 最近 `FAST_SCAN_HOURS` 小时 | 保证当前数据的时效 |
| **全量扫** | `WATCH_FULL_SCAN_EVERY`（默认 60s） | 该来源下所有含 `index.jsonl` 的叶子 | 兜底，防漏老叶子/回溯写入 |

代价差异很大：某来源有 1256 个叶子，全量扫要 1256 次 stat（NFS 上数秒），
而真正会变的只有当前小时那个目录。两档结合既保时效又不压垮 NFS。

### 优先级

```
当前小时        PRIO_NOW     (100)  ← 新记录落在这里，必须最先可见
最近 N 小时     PRIO_RECENT  (50)
其余积压        PRIO_OLD     (0)    ← 老叶子慢慢补，绝不挡新数据
```

这是「历史积压不阻塞实时数据」的核心机制。

---

## 配置

全部集中在 **`utils/watch_config.py`**，用 `.env` 覆盖，**不填就用默认值**。
非法值只告警并回退默认，不会让 worker 起不来。

| 变量 | 默认 | 说明 |
|---|---|---|
| `WATCH_CONCURRENCY` | 8 | 叶子间并发：同时处理多少个**不同**叶子。看板「并发 N/8」即此值 |
| `WATCH_POLL_SEC` | 5 | 发现线程轮询周期（秒） |
| `WATCH_ENRICH_WORKERS` | 4 | 单叶 enrich 并发子进程数 |
| `WATCH_ENRICH_BUDGET` | 12 | 全局 enrich 子进程上限（跨任务共享，**非阻塞**分配） |
| `WATCH_ENRICH_INLINE_THRESHOLD` | 1000 | 积压低于此值走进程内单进程快路径（不起 spawn 池） |
| `WATCH_FAST_SCAN_DAYS` | 2 | 快扫最近几天 |
| `WATCH_FAST_SCAN_HOURS` | 3 | 快扫每天最近几小时 |
| `WATCH_FULL_SCAN_EVERY` | 60 | 全量扫描兜底周期（秒） |
| `WATCH_PRIO_NOW` | 100 | 当前小时叶子优先级 |
| `WATCH_PRIO_RECENT` | 50 | 最近 N 小时叶子优先级 |
| `WATCH_PRIO_OLD` | 0 | 其余积压优先级 |
| `WATCH_RECENT_HOURS` | 3 | 「最近」的判定阈值（小时） |
| `WATCH_SYNC_BATCH` | 500 | ingest 分批行数（避免长事务长期占写锁） |
| `WATCH_ENRICH_STALL_TIMEOUT` | 60 | enrich 判定卡死的超时（秒），多因 NFS 读死等 |

相关但不在本模块的：

| 变量 | 默认 | 说明 |
|---|---|---|
| `BACKFILL_AUTO_SYNC_INTERVAL` | 0 | 回填 worker 的小时级 auto_sync 周期。0=关闭（默认），实时索引已覆盖增量 |

### 关于「并发」的三种不同含义

| 概念 | 数量 | 可配 |
|---|---|---|
| **发现线程** | = 已注册 new-api 来源数（当前 7 个） | 随数据源增减自动变化 |
| **同步并发** | `WATCH_CONCURRENCY`（默认 8） | ✅ 即「同时处理几个叶子」 |
| **worker 进程** | 1 | 固定。并发靠进程内线程，多进程无收益 |

> 注意：实测活跃叶子的 `pending` 常为 0（enrich 无活可干），所以**稳态滞后不是
> enrich 并发限制的**。加大 `WATCH_ENRICH_WORKERS` 只在积压追赶时有明显收益；
> 真正决定时效的是叶子间并发 + 发现周期。

---

## 运维

```bash
# 启停（app start 也会一并拉起）
./app watch start  --env .env
./app watch stop   --env .env
./app watch status
./app watch logs -f

# 看板
curl -s http://127.0.0.1:8084/api/watch/status -H "x-requested-with: XMLHttpRequest"
# 浏览器：/watch
```

看板返回：

```json
{
  "running": true, "pid": 747385, "uptime_sec": 1800,
  "pool": {"size": 8, "busy": 8, "pending": 0},
  "totals": {"pending": 0, "running": 8, "done": 412, "failed": 0,
             "remaining_bytes": 32554915, "eta_sec": 281},
  "current": [{"leaf": "...", "elapsed_sec": 12, "percent": 42}],
  "sources": [{"name": "jumper-003", "pending": 0, "running": 2, "done": 120,
               "bytes_per_sec": 1200000, "eta_sec": 283}]
}
```

`pool.busy == pool.size` 表示**已满负荷**（8/8 全量实时同步中）。

### ETA 怎么算

按**来源历史吞吐率**（`watch_source_stats` 表，EMA α=0.3）：

```
单源 ETA = 剩余字节 / 该源 bytes_per_sec
```

用字节而不是行数，因为每个叶子 `index.jsonl` 的大小入队时已知，`idx_size - offset_at`
能精确算出剩余量，比猜行数准。新源无历史时回退全局平均，再没有则显示「估算中」。

`idx_size` 会被发现线程**持续刷新**（`touch_watch_size`），所以看板上的剩余量
和 ETA 跟着文件实时走，不会冻在入队那一刻。

---

## 数据一致性

`sync_sessions` 是**增量折叠**，不是全量重建：只处理 rowid 水位之后的行，
按会话做 upsert。它复刻了 `rebuild_sessions` 的聚合逻辑，包括会话切分
（`user_turns` 回落时另起 `##session_N`）。

验证方法：对任一已增量同步的叶子，把库拷贝一份跑一次全量 `rebuild_sessions()`，
逐列比对 `sessions` / `traces` / `chain_index`，三者应完全一致。

`traces` 表上有 `(session_key, filename)` 唯一索引，保证增量追加时重放不会产生重复行。

### 保序停止

`sync_sessions` 按 rowid 顺序扫描，**遇到第一行 meta 未补齐（`q1_hash IS NULL`）
就停下**，水位只推进到最后一行成功折叠处。这样既不会跳过还没 enrich 完的行，
也不会打乱 `index.jsonl` 的追加顺序。

---

## 故障排查

| 现象 | 排查方向 |
|---|---|
| 看板 `running: false` | worker 没起来：`./app watch status`、`./app watch logs -n 50` |
| 队列一直有 `pending` 不减少 | dispatcher 卡住 / 全部槽位被慢叶子占满；看 `current` 里的 elapsed |
| 某叶子反复 `failed` | 单独跑一次 `./app watch logs -f` 看 traceback；常见是合并文件损坏或孤立代理字符（已在 enrich 写回路径清洗） |
| 数据始终不更新 | 该叶子的 `ingest_offset` 是否追上文件大小；`pending` 是否长期 >0 |
| 看板 ETA 不准 | `watch_source_stats` 样本太少（新源/刚重启），跑几分钟后自动收敛 |
| 与回填 worker 抢锁 | 检查 `BACKFILL_AUTO_SYNC_INTERVAL` 是否为 0；`.indexer.lock` 是否会长期被持有 |

### 手工清空队列

```bash
# 停 worker 后操作，避免并发写
./app watch stop --env .env
sqlite3 "$LOG_DIR/log_dir.db" "DELETE FROM watch_queue;"
./app watch start --env .env
```

启动时会把上次遗留的 `running` 自动复位为 `pending`（`reset_watch_on_startup`），
所以正常重启不需要手工干预。

---

## 相关文件

| 文件 | 职责 |
|---|---|
| `src/watch/worker.py` | 进程入口：发现线程编排 + dispatcher + 同步池 |
| `src/watch/discover.py` | 每源发现逻辑、两档扫描、优先级 |
| `utils/watch_config.py` | 全部可调参数（唯一配置入口） |
| `utils/newapi_watcher.py` | `flush_leaf()`（ingest→enrich→sync 一趟）+ 跨进程 `flock` |
| `utils/newapi_index_db.py` | 三段式实现的实体：`ingest` / `enrich` / `sync_sessions` |
| `utils/logdir_store.py` | `watch_queue` / `watch_source_stats` 表与增删查 |
| `utils/logs_routes.py` | `/watch` 页面、`/api/watch/status` |
| `templates/watch.html` | 看板前端（5s 轮询） |

参考文档：`doc/README_newapi_index_db.md`（三段式的设计背景与成本实测）。
