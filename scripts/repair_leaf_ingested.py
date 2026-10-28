#!/usr/bin/env python3
"""
一次性脚本：修复 leaf_status.ingested 停在旧值的叶子（看板「待同步」虚高的根因）。

背景：看板「待同步叶子」按 `entry_count - leaf_status.ingested` 算欠账。而
`ingested` 只有 watcher 和回填会写：
  - 8 月初登记那批源在这两者引入之前就建好了 → `ingested` 一直是 0（实测 382 片、
    虚报 2700 万行）；
  - 另一批是「中途停更」：`ingested` 有值但已陈旧（实测 8 片正差，如 141,242 对
    真实的 197,238）。它们字节已追平，watcher 不会再碰，于是这个值永远不刷新。

本脚本对目标叶子逐个读 `nidb.status()` 并写回 `ingested` / `pending`。
幂等：已经是真值的叶子写回同一个数，可重复执行。

用法：
    python scripts/repair_leaf_ingested.py                # 只修 ingested=0
    python scripts/repair_leaf_ingested.py --mismatch     # 连「部分陈旧」一起修
    python scripts/repair_leaf_ingested.py --dry-run      # 只报告，不写库
    python scripts/repair_leaf_ingested.py --limit 20     # 小批量试跑（先看耗时）

注意 --mismatch 也会挑到「负差」的叶子（token_stats 快照比 ingested 旧），
那些叶子其实是对的，脚本会把同一个真值再写一遍，无副作用。

实测 status() 单叶约 0.6s（NFS 上 27w 行的叶子），382 叶约 4 分钟。
"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.log_paths import get_service_log_dir  # noqa: E402
import utils.logdir_store as lds  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description="修复 leaf_status.ingested 停留在旧值的已建叶子")
    ap.add_argument("--dry-run", action="store_true", help="只报告，不写库")
    ap.add_argument("--limit", type=int, default=0, help="最多处理多少个（0=全部）")
    ap.add_argument("--root", default="", help="只处理某个 root_path（默认全部）")
    ap.add_argument("--mismatch", action="store_true",
                    help="同时修 ingested 与 token_stats.entry_count 不一致的叶子"
                         "（只修 ingested=0 会漏掉「部分陈旧」的那批）")
    args = ap.parse_args()

    svc_dir = get_service_log_dir()
    lds.init_db(svc_dir)
    print(f"LOG_DIR={svc_dir}")

    import sqlite3  # noqa: F401 — 由 lds 的连接使用
    from utils import newapi_index_db as nidb

    with lds._lock:
        # 只处理 newapi 源：native 源没有 index.db，nidb.status() 对它恒返回
        # built=False，照抄回库会把 native 叶子的 built=1 覆盖成 0（native 的
        # built 判定走 session 聚合进度，根本不该由这个脚本碰）。
        #
        # 默认只挑 ingested=0（安全、精确）。--mismatch 时放宽到「ingested 与
        # token_stats.entry_count 不一致」——那批「部分陈旧」的叶子 ingested 非 0，
        # 光按 =0 筛永远漏掉（实测 8 片正差、合计 8.9 万行虚报）。
        if args.mismatch:
            cur = lds._conn.execute(
                "SELECT l.root_id, l.dir_key, l.leaf_path, l.ingested "
                "FROM leaf_status l "
                "JOIN sources s ON s.root_id = l.root_id "
                "WHERE l.built = 1 AND l.leaf_path != '' AND s.format = 'newapi' "
                "AND l.token_stats_json != '' "
                "AND COALESCE(json_extract(l.token_stats_json, '$.entry_count'), -1)"
                "    != l.ingested "
                "ORDER BY l.root_id, l.dir_key"
            )
        else:
            cur = lds._conn.execute(
                "SELECT l.root_id, l.dir_key, l.leaf_path FROM leaf_status l "
                "JOIN sources s ON s.root_id = l.root_id "
                "WHERE l.built = 1 AND l.ingested = 0 AND l.leaf_path != '' "
                "AND s.format = 'newapi' "
                "ORDER BY l.root_id, l.dir_key"
            )
        rows = [dict(r) for r in cur.fetchall()]

    if args.root:
        norm = os.path.normpath(args.root)
        rows = [r for r in rows if (r.get("leaf_path") or "").startswith(norm)]
    if args.limit:
        rows = rows[: args.limit]

    print(f"候选叶子 {len(rows)} 个" + ("（dry-run，不写库）" if args.dry_run else ""))
    if not rows:
        return 0

    fixed = failed = skipped = 0
    t_start = time.time()
    for i, r in enumerate(rows, 1):
        leaf_path = r.get("leaf_path") or ""
        root_id, dir_key = r.get("root_id") or "", r.get("dir_key") or ""
        if not leaf_path or not os.path.isdir(leaf_path):
            skipped += 1
            continue
        try:
            st = nidb.status(leaf_path)
        except Exception as ex:  # noqa: BLE001
            failed += 1
            print(f"  [{i}/{len(rows)}] 读取失败 {root_id}/{dir_key}: {ex}")
            continue
        ingested = int(st.get("ingested") or 0)
        pending = int(st.get("pending") or 0)
        built = bool(st.get("built"))
        if args.dry_run:
            print(f"  [{i}/{len(rows)}] {root_id}/{dir_key}: ingested "
                  f"{r.get('ingested', 0):,} → {ingested:,}"
                  f" pending={pending} built={built}")
        else:
            try:
                lds.upsert_leaf(root_id, dir_key, leaf_path=leaf_path,
                                ingested=ingested, pending=pending, built=built)
            except Exception as ex:  # noqa: BLE001
                failed += 1
                print(f"  [{i}/{len(rows)}] 写库失败 {root_id}/{dir_key}: {ex}")
                continue
        fixed += 1
        if i % 25 == 0:
            el = time.time() - t_start
            print(f"  ...{i}/{len(rows)}  已用 {el:.0f}s  预计剩余 "
                  f"{el / i * (len(rows) - i):.0f}s")

    print(f"完成：修复 {fixed}，跳过 {skipped}，失败 {failed}，"
          f"用时 {time.time() - t_start:.0f}s")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
