"""
src/watch/discover.py — 每源发现线程

职责：轮询 index.jsonl 是否增长（或叶子是否新建）→ 入队 watch_queue。
绝不写 index.db，也不解析合并文件——只做 os.stat。
"""

import logging
import os
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple

logger = logging.getLogger("watch.discover")

# 全部可调项来自 utils/watch_config（.env 可覆盖，不填用默认）。
from utils import watch_config as _cfg

# 当前小时/最近 N 小时的优先级
_PRIO_NOW = _cfg.PRIO_NOW
_PRIO_RECENT = _cfg.PRIO_RECENT
_PRIO_OLD = _cfg.PRIO_OLD
_RECENT_HOURS = _cfg.RECENT_HOURS

# 两档扫描节奏
_FAST_SCAN_DAYS = _cfg.FAST_SCAN_DAYS    # 最近 N 天：每 POLL_SEC 扫
_FAST_SCAN_HOURS = _cfg.FAST_SCAN_HOURS  # 最近 N 小时叶子
_FULL_SCAN_EVERY = _cfg.FULL_SCAN_EVERY  # 全量扫描周期（秒）


def _current_hour_tag() -> str:
    return datetime.now().strftime("%y%m%d%H")


def _leaf_priority(leaf_name: str) -> int:
    """根据叶子名（小时标签格式 YYMMDDHH）判断优先级。"""
    now_tag = _current_hour_tag()
    try:
        now_ts = int(now_tag)
        leaf_ts = int(leaf_name[-8:])  # 取最后8位
        diff = now_ts - leaf_ts
        if diff == 0:
            return _PRIO_NOW
        if diff <= _RECENT_HOURS:
            return _PRIO_RECENT
    except (ValueError, IndexError):
        pass
    return _PRIO_OLD


class SourceWatcher:
    """单个 new-api 数据源的发现逻辑。在独立线程里循环运行。"""

    def __init__(self, root_id: str, root_path: str, templates,
                 poll_sec: float, stop_evt: threading.Event):
        self.root_id = root_id
        self.root_path = root_path
        self.templates = templates
        self.poll_sec = poll_sec
        self.stop_evt = stop_evt
        # {leaf_path: (idx_size, mtime)}  内存缓存
        self._known: Dict[str, Tuple[int, float]] = {}
        self._last_full_scan = 0.0

    def _enqueue(self, leaf_path: str, dir_key: str, reason: str,
                 idx_size: int, offset_at: int) -> bool:
        """尝试入队。返回 True 表示新增了一条任务；
        False 表示该叶子已有 pending/running 任务（被唯一索引忽略）——调用方
        必须据此**不要**推进本地水位，否则会吞掉这次唤醒（见 _scan_leaf 注释）。
        """
        leaf_name = os.path.basename(leaf_path.rstrip("/"))
        priority = _leaf_priority(leaf_name)
        try:
            import utils.logdir_store as lds
            enqueued = lds.enqueue_watch(
                self.root_id, self.root_path, leaf_path, dir_key,
                priority=priority, reason=reason,
                idx_size=idx_size, offset_at=offset_at,
            )
            if enqueued:
                logger.debug("入队 %s pri=%d reason=%s idx=%d",
                             dir_key, priority, reason, idx_size)
            return bool(enqueued)
        except Exception as ex:  # noqa: BLE001
            logger.warning("enqueue_watch 失败 %s: %s", dir_key, ex)
            return False

    def _scan_leaf(self, leaf: Path, dir_key: str) -> None:
        """检查一个叶子，必要时入队。

        核心原则：**「已入队」不等于「已同步」**。
        判定水位一律以 index.db 的 ingest_offset 为准；内存 _known 只用于
        「文件连大小都没变」的快速短路（省一次 SQLite 打开）。

        曾经的 bug：无条件把 _known 推进到当前大小。当任务 running 时文件增长，
        enqueue 被唯一索引忽略（这是对的），但 _known 照样前进 → 任务结束后
        ingest_offset 停在旧位置，而下一轮扫描认为「没长过」不再入队 → 最新几行
        要等文件下次增长才补；若恰好此时叶子停写，就永久挂着。
        """
        from utils.newapi_index_db import _db_exists, _meta_get, _connect
        idx = leaf / "index.jsonl"
        try:
            st = os.stat(idx)
            idx_size = st.st_size
            idx_mtime = st.st_mtime
        except OSError:
            return

        leaf_str = str(leaf)
        prev = self._known.get(leaf_str)

        # 快速短路：文件大小与上次完全相同 → 什么都没发生，不必打开 index.db。
        # 注意这里只跳过「完全相同」，文件变小（轮转/截断）要让下游处理。
        if prev is not None and idx_size == prev[0]:
            return

        # 读真实水位（廉价 meta 点查）
        no_db = not _db_exists(leaf_str)
        offset_at = 0
        if not no_db:
            try:
                c = _connect(leaf_str)
                offset_at = int(_meta_get(c, "ingest_offset", "0") or 0)
                c.close()
            except Exception:  # noqa: BLE001
                # db 半成品/打不开：当作需要重建
                no_db = True
                offset_at = 0

        # 权威判定：还欠多少字节
        lagging = no_db or idx_size > offset_at

        if not lagging:
            # 已追平，更新快速短路缓存即可
            self._known[leaf_str] = (idx_size, idx_mtime)
            return

        # 已有任务在跑/在排队 → 只刷新 size（看板 ETA 实时前进），不入队。
        # 这次「增长」不会被吞：任务结束时 worker 会做收尾复核补一条；
        # 而且 _known 不推进，下一轮扫描仍会判定 lagging 并重试。
        try:
            import utils.logdir_store as lds
            if lds.touch_watch_size(leaf_str, idx_size):
                self._known[leaf_str] = (idx_size, idx_mtime)
                return
        except Exception as ex:  # noqa: BLE001
            logger.debug("touch_watch_size 失败 %s: %s", leaf_str, ex)

        from utils.log_scan import dir_key_for
        dk = dir_key or dir_key_for(Path(self.root_path), leaf)
        reason = "new" if no_db else "grown"
        # 入队成功才推进缓存；被忽略（极小概率：touch 与 enqueue 之间的竞态）时
        # 保持旧值，下一轮自然重试。绝不因「已入队」而推进。
        if self._enqueue(leaf_str, dk, reason, idx_size, offset_at):
            self._known[leaf_str] = (idx_size, idx_mtime)

    def _get_leaves(self, fast_only: bool):
        """枚举叶子：fast 模式只看最近 _FAST_SCAN_DAYS 天的最近 _FAST_SCAN_HOURS 小时。"""
        root = Path(self.root_path)
        if not root.is_dir():
            return

        if fast_only:
            try:
                days = sorted((d for d in root.iterdir() if d.is_dir()),
                              key=lambda p: p.name)
                for day in days[-_FAST_SCAN_DAYS:]:
                    try:
                        hours = sorted((h for h in day.iterdir() if h.is_dir()),
                                       key=lambda p: p.name)
                        for hour in hours[-_FAST_SCAN_HOURS:]:
                            if (hour / "index.jsonl").is_file():
                                yield hour
                    except OSError:
                        continue
            except OSError:
                return
        else:
            # 全量扫：复用 fast_scan_leaf_dirs_by_templates 枚举所有叶子
            try:
                from utils.log_scan import fast_scan_leaf_dirs_by_templates, default_templates
                tpls = self.templates or default_templates("newapi")
                yield from fast_scan_leaf_dirs_by_templates(root, tpls)
            except Exception as ex:  # noqa: BLE001
                logger.warning("全量扫描失败 %s: %s", self.root_path, ex)

    def run_once(self) -> None:
        """运行一轮扫描。"""
        now = time.monotonic()
        do_full = (now - self._last_full_scan) >= _FULL_SCAN_EVERY
        if do_full:
            self._last_full_scan = now

        for leaf in self._get_leaves(fast_only=not do_full):
            if self.stop_evt.is_set():
                return
            try:
                self._scan_leaf(leaf, "")
            except Exception as ex:  # noqa: BLE001
                logger.debug("scan_leaf 异常 %s: %s", leaf, ex)

    def run_forever(self) -> None:
        logger.info("发现线程启动：%s (poll=%.1fs)", self.root_path, self.poll_sec)
        while not self.stop_evt.is_set():
            try:
                self.run_once()
            except Exception as ex:  # noqa: BLE001
                logger.warning("发现循环异常 %s: %s", self.root_path, ex)
            self.stop_evt.wait(self.poll_sec)
        logger.info("发现线程退出：%s", self.root_path)


def start_discover_threads(stop_evt: threading.Event,
                           poll_sec: float = 5.0) -> list:
    """为所有已注册的 newapi 源启动发现线程，返回线程列表。"""
    import utils.logdir_store as lds
    import utils.logs_config as lc

    sources = lc._list_sources()
    threads = []
    for src in sources:
        if src.get("format") != "newapi":
            continue
        root_path = (src.get("root_path") or "").strip()
        if not root_path or not os.path.isdir(root_path):
            continue
        root_id = src.get("root_id") or ""
        try:
            templates_raw = src.get("templates") or ""
            import json
            templates = json.loads(templates_raw) if templates_raw else []
        except Exception:
            templates = []

        watcher = SourceWatcher(root_id, root_path, templates, poll_sec, stop_evt)
        t = threading.Thread(
            target=watcher.run_forever,
            name=f"watch-discover-{root_id[:8]}",
            daemon=True,
        )
        t.start()
        threads.append(t)
        logger.info("发现线程已启动 root_id=%s path=%s", root_id, root_path)

    if not threads:
        logger.warning("未找到 newapi 格式的已注册数据源，发现线程为零")
    return threads
