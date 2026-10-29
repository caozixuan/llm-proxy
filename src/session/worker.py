"""
src/session/worker.py — session 统计预热独立进程

背景：
  leaf_status 里每个叶子缓存着两份聚合结果：
    - stats_json        （/sessions 用：按 api_key×date 的会话数）
    - token_stats_json  （/query 用：按 model/key/channel 的 token 归属）
  它们的失效签名只校验 index.jsonl 的 size+mtime。但 watcher 把新 session 折叠
  进 index.db 时**不改 index.jsonl**，于是快照会「名义有效」却落后于 index.db
  （实测某叶 sessions 表 26 个 key、stats_json 只有 24 个）。

  为此 leaf_status 增加了两个水位列：
    - sync_rowid         watcher 本次同步到达的 index.db rowid
    - stats_sync_rowid   stats_json 快照是基于哪个水位算出来的
  两者不等 = 快照落后，需要重算。

为什么放在独立进程：
  重算一个叶子要 1.4~7.7s（get_session_stats + NFS I/O）。若在 Web 请求里顺手
  重算，用户会撞上几秒的毛刺；4689 个叶子集中补一次更会把主进程拖住。
  这里做成后台慢速预热，用户请求路径永远只读现成缓存。

职责：
  1. 周期性扫描 leaf_status 里「快照落后」的叶子，逐叶重算并回写
  2. 顺带刷新 token_stats_json（同样的落后判定暂按 index.jsonl 签名）
  3. 空闲时低频空转，不占 CPU

与其它 worker 的分工：
  - watch worker    ：发现增量并同步 index.db（秒级）
  - backfill worker ：手动触发的批量回填（力大砖飞）
  - session worker  ：把已同步进 index.db 的数据**聚合成统计快照**（本进程）
  三者共用同一份 log_dir.db（WAL + busy_timeout），互不阻塞。
"""

import argparse
import logging
import os
import signal
import sys
import time
from pathlib import Path

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from utils.log_paths import get_service_log_dir
import utils.logdir_store as logdir_store

logger = logging.getLogger("session_worker")

_shutdown_requested = False

# 一轮最多处理多少个落后叶子（每轮之间 sleep，避免长时间占满 NFS）。
BATCH = int(os.getenv("SESSION_WORKER_BATCH", "20") or 20)
# 轮与轮之间的间隔（秒）。落后叶子清空后也按这个节奏空转。
INTERVAL = float(os.getenv("SESSION_WORKER_INTERVAL", "15") or 15)
# 单个叶子的重算超时保护：超过判定为卡住（多为 NFS 读死等），跳过并留待下轮。
LEAF_TIMEOUT = float(os.getenv("SESSION_WORKER_LEAF_TIMEOUT", "300") or 300)
# 统计口径的「合格会话」阈值，与 stats_index 保持一致。
THRESHOLD = int(os.getenv("SESSION_WORKER_THRESHOLD", "5") or 5)


def _handle_signal(signum, frame):
    global _shutdown_requested
    _shutdown_requested = True
    logger.info("收到信号 %s，准备优雅退出（当前叶子算完后退出）", signum)


def _sleep(seconds: float) -> None:
    """分片 sleep，收到停机信号立即醒来。"""
    end = seconds
    while end > 0:
        if _shutdown_requested:
            return
        time.sleep(min(0.5, end))
        end -= 0.5


def _setup_logging() -> None:
    fmt = "%(asctime)s [%(levelname)s] %(message)s"
    log_file = os.path.join(get_service_log_dir(), "session_worker.log")
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    # 只挂 FileHandler：launcher 已把 stdout/stderr 重定向到同一文件，
    # 再加 StreamHandler 会让每条日志写两遍（与 watch_worker 一致）。
    logging.basicConfig(level=logging.INFO, format=fmt,
                        handlers=[logging.FileHandler(log_file, encoding="utf-8")])


def _init_env() -> None:
    svc_dir = get_service_log_dir()
    logdir_store.init_db(svc_dir)
    # native 格式叶子的会话数据在全局 session_cache.db，按 root_dir 过滤。
    # 不初始化的话，所有 native 源都会报 "session_store not initialized"。
    try:
        import utils.session_store as _ss
        _ss.init_db(svc_dir)
    except Exception as ex:  # noqa: BLE001
        logger.warning("session_store 初始化失败（native 源将无法补算）: %s", ex)
    logger.info("session_worker 启动 svc_dir=%s batch=%d interval=%.1fs threshold=%d",
                svc_dir, BATCH, INTERVAL, THRESHOLD)
    try:
        na = logdir_store.count_archived_leaves()
        if na:
            # 已归档（上传 OBS 后本地目录已删）的叶子不在补算范围内，
            # 打印出来便于确认备份侧的通知标记已生效。
            logger.info("跳过 %d 个已归档叶子（本地已删，内容在 OBS）", na)
    except Exception as ex:  # noqa: BLE001
        logger.warning("已归档叶子数查询失败: %s", ex)
    try:
        n = logdir_store.count_stale_stats_leaves()
        if n:
            logger.info("发现 %d 个统计快照落后的叶子，开始后台补算", n)
        else:
            logger.info("没有落后的叶子，进入空转")
    except Exception as ex:  # noqa: BLE001
        logger.warning("统计落后叶子数失败: %s", ex)


def _leaf_is_newapi(leaf_path: str) -> bool:
    """判断叶子格式。newapi 有 index.db 且无 -req.json；native 反之。

    优先看磁盘事实（index.db 是否存在且含 sessions 表），再回退到
    log_scan.detect_format 抽样 index.jsonl 首条。两种格式的会话数据存放在
    完全不同的地方，判错就会读不到数据。
    """
    import sqlite3
    db_file = os.path.join(leaf_path, "index.db")
    if os.path.isfile(db_file):
        try:
            c = sqlite3.connect("file:%s?mode=ro" % db_file, uri=True, timeout=10)
            try:
                names = {r[0] for r in c.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'")}
            finally:
                c.close()
            if "sessions" in names and "traces" in names:
                return True
        except sqlite3.Error:
            pass
    try:
        from utils.log_scan import detect_format
        return detect_format(leaf_path) == "newapi"
    except Exception:  # noqa: BLE001
        return False


def _newapi_stats(leaf_path: str):
    """newapi 叶子的统计桶。缺表时抛错，由调用方记录并跳过。"""
    import utils.newapi_index_db as nidb
    return nidb.get_session_stats(leaf_path, THRESHOLD)


def recompute_leaf(row: dict) -> bool:
    """重算一个叶子的统计快照并回写 leaf_status。返回是否成功。

    按叶子格式择数据源（两种格式的会话数据根本不在同一个地方）：
      - newapi 叶子：读叶子内的 index.db（sessions / traces 表）
      - native 叶子：读全局 session_cache.db（按 root_dir 过滤），**没有 index.db**
    把 native 当成「index.db 缺失的坏叶子」跳过是错的 —— 那 36 个叶子有真实数据，
    只是格式不同（实测某叶 session_cache 里 19429 个会话，与 leaf_status 一致）。
    """
    import json
    import utils.newapi_index_db as nidb

    root_id = row.get("root_id") or ""
    dir_key = row.get("dir_key") or ""
    leaf_path = row.get("leaf_path") or ""
    if not leaf_path or not os.path.isdir(leaf_path):
        return False

    is_newapi = _leaf_is_newapi(leaf_path)
    t0 = time.monotonic()
    try:
        if is_newapi:
            stats = _newapi_stats(leaf_path)
        else:
            import utils.session_store as _ss
            stats = _ss.get_session_stats(leaf_path, THRESHOLD)
    except Exception as ex:  # noqa: BLE001
        logger.warning("读取统计失败 %s: %s", leaf_path, ex)
        return False
    if stats is None:
        return False
    elapsed = time.monotonic() - t0
    if elapsed > LEAF_TIMEOUT:
        logger.warning("叶子重算超时 %.0fs，跳过：%s", elapsed, leaf_path)
        return False

    idx_size = row.get("idx_size")
    idx_mtime = row.get("idx_mtime")
    if idx_size is None or idx_mtime is None or idx_size < 0:
        try:
            st = os.stat(os.path.join(leaf_path, "index.jsonl"))
            idx_size, idx_mtime = st.st_size, st.st_mtime
        except OSError:
            idx_size, idx_mtime = 0, 0.0

    # 回写时盖上新水位：stats_json 现在确实反映了 sync_rowid 时刻的 index.db。
    #
    # sessions 必须一并回写：stale_stats_leaves 的判据之一是「stats_json 的
    # total 之和 != leaf_status.sessions」。若这里只写 stats_json 而不写 sessions，
    # 两者永远对不上，该叶子会被每轮反复挑中重算——实测 240fa79b/260805/26080517
    # 每 2 秒重算一次、日志刷屏（真实会话 2826，leaf_status.sessions 却停在 1796）。
    # ingested/pending 同理顺手刷新，看板「待同步」靠它算欠账。
    sessions_total = None
    ingested = pending = None
    if isinstance(stats, dict):
        try:
            sessions_total = sum(int(v.get("total", 0)) for v in stats.values())
        except (TypeError, ValueError, AttributeError):
            sessions_total = None
    if is_newapi:
        try:
            st = nidb.status(leaf_path)
            ingested = st.get("ingested", 0)
            pending = st.get("pending", 0)
            if sessions_total is None:
                sessions_total = st.get("sessions")
        except Exception as ex:  # noqa: BLE001
            logger.debug("读叶子 status 失败 %s: %s", leaf_path, ex)

    logdir_store.set_leaf_stats(
        root_id, dir_key, leaf_path=leaf_path,
        idx_size=int(idx_size), idx_mtime=float(idx_mtime),
        stats_json=json.dumps(stats, ensure_ascii=False),
        stats_threshold=THRESHOLD,
        stats_sync_rowid=int(row.get("sync_rowid") or 0),
        sessions=sessions_total, ingested=ingested, pending=pending,
    )
    logger.info("补算完成 %s/%s buckets=%d 用时 %.2fs 水位=%s",
                root_id, dir_key, len(stats), elapsed, row.get("sync_rowid"))
    return True


def run_once() -> int:
    """处理一批落后的叶子，返回本轮成功数。"""
    rows = logdir_store.stale_stats_leaves(limit=BATCH)
    if not rows:
        return 0
    done = 0
    for row in rows:
        if _shutdown_requested:
            break
        try:
            if recompute_leaf(row):
                done += 1
        except Exception as ex:  # noqa: BLE001
            logger.warning("叶子补算失败 %s/%s: %s",
                           row.get("root_id"), row.get("dir_key"), ex)
    return done


def main() -> int:
    parser = argparse.ArgumentParser(description="session 统计预热 worker")
    parser.add_argument("--once", action="store_true",
                        help="把当前落后的叶子跑空后退出（调试 / cron）")
    args = parser.parse_args()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    _setup_logging()
    _init_env()

    try:
        _prev_remaining = None
        while not _shutdown_requested:
            n = run_once()
            if n:
                _prev_remaining = logdir_store.count_stale_stats_leaves()
                logger.info("本轮补算 %d 个叶子，剩余 %d 个落后", n, _prev_remaining)
            elif _prev_remaining:
                # 上一轮还有欠账、这轮一个都没捞到 → 说明剩余的已被归档过滤掉。
                # 打一条，便于从日志确认备份侧打的 archived 标记已被本进程感知。
                _prev_remaining = logdir_store.count_stale_stats_leaves()
                logger.info("落后叶子已清空（含已归档叶子跳过），剩余 %d 个",
                            _prev_remaining)
            if args.once:
                break
            # 有活就立刻继续下一批（缩短总耗时）；没活就按 INTERVAL 空转。
            _sleep(1.0 if n else INTERVAL)
    except Exception:
        logger.exception("session_worker 主循环异常退出")
        return 1
    logger.info("session_worker 退出")
    return 0


if __name__ == "__main__":
    sys.exit(main())
