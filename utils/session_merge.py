"""
跨小时叶子的会话段合并（浏览页 / 导出共用）

背景：newapi 的数据按「日/时」分叶子目录存放，每个叶子有自己独立的 index.db，
chain_index（会话链索引）也在叶子内部。新建会话的 session_key 就是它的 ts，
所以同一段真实对话只要跨过整点，就会在新叶子里重新开一条 session。
结果：一条对话被切成 N 段，各自带着自己那一小时的首末时间。

实测（sk-YDyR... 2026-10-28）：20 条真实对话 / 50 条 session 行，20/20 全部跨叶。

本模块只做纯数据的合并，不碰 IO，供两条链路复用：
  - utils/log_routes.py  /export/view 浏览页
  - utils/export_routes.py 导出（reformat 系 / copy）
"""

import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

# 弱身份（回退键）允许的段间最大间隔（秒）。强身份不受此限制。
DEFAULT_MAX_GAP_SEC = 90 * 60


def parse_ts(value: str) -> Optional[float]:
    """解析时间戳为 epoch 秒，失败返回 None。

    兼容两种格式：
      - 原始 ISO：'2026-10-28T15:25:50.030+08:00'
      - 展示格式：'2026:10:28T15:25:50.030+08:00'（_format_time 把日期里的 '-' 换成了 ':'）
    """
    if not value:
        return None
    raw = str(value).strip()
    if len(raw) > 4 and raw[:4].isdigit() and raw[4] == ":":
        head, sep, tail = raw.partition("T")
        head = head.replace(":", "-")
        raw = head + sep + tail
    try:
        return datetime.datetime.fromisoformat(raw).timestamp()
    except (ValueError, TypeError):
        return None


def group_key(rec: Dict[str, Any], identity_field: str = "chain_lookup",
              fallback_key: Optional[Callable[[Dict[str, Any]], str]] = None
              ) -> Optional[Tuple[str, str]]:
    """返回合并分组键；返回 None 表示这条记录不参与合并。

    强身份 = identity_field（chain_lookup，即 api_key||q1_hash，同 q1 被判为新会话时
    带 ##session_N 后缀）。它是同步阶段用内容哈希算出来的，同一 key 的同一段对话跨叶
    必然一致，可以无条件合并——实测有对话 15:41 结束、17:21 才续上，间隔 1h40m。

    弱身份（fallback_key）默认**不启用**。原因：q1 文本在一个大叶子里最多有 5749 条
    session 共用同一段文本（都是系统提示词开头的压测/agent 请求），按 q1 分组会把它们
    全并成一条。老数据若确实缺 chain_lookup，宁可不合并（保持每段一条），也不能错并。
    """
    ident = (rec.get(identity_field) or "").strip()
    if ident:
        return ("strong", ident)
    if fallback_key is not None:
        weak = fallback_key(rec)
        if weak:
            return ("weak", weak)
    return None


def merge_segments(
    records: Sequence[Dict[str, Any]],
    first_field: str = "first_time",
    last_field: str = "last_time",
    count_field: str = "message_count",
    file_count_field: Optional[str] = "file_count",
    models_field: str = "models",
    identity_field: str = "chain_lookup",
    trace_field: Optional[str] = None,
    trace_ts_field: str = "ts",
    trace_name_field: str = "filename",
    latest_file_field: Optional[str] = "latest_file",
    fallback_key: Optional[Callable[[Dict[str, Any]], str]] = None,
    max_gap_sec: float = DEFAULT_MAX_GAP_SEC,
    guard_msg_count_reset: bool = False,
) -> List[Dict[str, Any]]:
    """把同一段逻辑对话跨叶子的多段合并成一条。

    合并语义：
      first_field     取最早段的
      last_field      取最晚段的
      count_field     取**最大** —— 它是「会话累计消息数」，不是增量，求和会翻倍
      file_count_field 各段**求和** —— 每段文件都真实存在于各自的叶子里
      models_field    并集（保序）
      has_failure     任一段有则保留
      trace_field     跨叶并集：按 trace_name_field 去重，按 trace_ts_field 升序
      latest_file_field 取最晚段的

    保护：
      1. 强身份直接合并；弱身份才按 max_gap_sec 切簇。
      2. guard_msg_count_reset（**默认关**）：曾想用「msg_count 回落 = 用户重开一轮」来
         额外切分，实测不成立——跨叶相邻段的 msg_count 本来就不单调（同一段对话
         97→90 这种小幅回落很常见，233 个回落边界里中位比值只有 0.565）。更关键的是
         「重开一轮」在同步阶段已经用 user_turns 判过，判定成立时会给新段加
         ##session_N 后缀，chain_lookup 本身就不同了。这里再加阈值只会把同一段对话
         错误切开（实测把一条 167 个 trace 的对话切成 109+58 两段）。保留开关仅供
         调试，不做默认行为。

    输出按 last_field 倒序（新的在前）。
    """
    if not records:
        return []

    items = [dict(r) for r in records]

    groups: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    passthrough: List[Dict[str, Any]] = []
    for rec in items:
        gk = group_key(rec, identity_field, fallback_key)
        if gk is None:
            passthrough.append(rec)
        else:
            groups.setdefault(gk, []).append(rec)

    merged: List[Dict[str, Any]] = list(passthrough)

    for gk, segs in groups.items():
        if len(segs) == 1:
            merged.append(segs[0])
            continue
        segs.sort(key=lambda x: parse_ts(x.get(first_field, "")) or 0.0)
        strong = gk[0] == "strong"

        clusters: List[List[Dict[str, Any]]] = []
        for seg in segs:
            if clusters:
                prev = clusters[-1]
                if strong:
                    if not guard_msg_count_reset or _count_ok(prev, seg, count_field):
                        prev.append(seg)
                        continue
                else:
                    prev_end = parse_ts(prev[-1].get(last_field, "")) or 0.0
                    cur_start = parse_ts(seg.get(first_field, "")) or 0.0
                    if prev_end and cur_start and cur_start - prev_end <= max_gap_sec:
                        prev.append(seg)
                        continue
            clusters.append([seg])

        for cluster in clusters:
            if len(cluster) == 1:
                merged.append(cluster[0])
            else:
                merged.append(_merge_one(cluster, first_field, last_field, count_field,
                                         file_count_field, models_field, trace_field,
                                         trace_ts_field, trace_name_field, latest_file_field))

    merged.sort(key=lambda x: (x.get(last_field) or x.get(first_field) or ""), reverse=True)
    return merged


def _count_ok(prev: List[Dict[str, Any]], seg: Dict[str, Any], count_field: str) -> bool:
    """段间累计消息数不应回落。回落 = 同一 q1_hash 下重开了新一轮对话。"""
    prev_count = int(prev[-1].get(count_field) or 0)
    cur_count = int(seg.get(count_field) or 0)
    return cur_count >= prev_count


def _merge_one(cluster: List[Dict[str, Any]], first_field, last_field, count_field,
               file_count_field, models_field, trace_field, trace_ts_field,
               trace_name_field, latest_file_field) -> Dict[str, Any]:
    base = dict(cluster[0])
    last_seg = cluster[-1]

    base[last_field] = last_seg.get(last_field, "") or base.get(last_field, "")
    if latest_file_field:
        base[latest_file_field] = (last_seg.get(latest_file_field, "")
                                   or base.get(latest_file_field, ""))
    base[count_field] = max(int(s.get(count_field) or 0) for s in cluster)
    if file_count_field:
        base[file_count_field] = sum(int(s.get(file_count_field) or 0) for s in cluster)

    models: List[str] = []
    for seg in cluster:
        for m in (seg.get(models_field) or []):
            if m and m not in models:
                models.append(m)
    base[models_field] = models

    if any(seg.get("has_failure") for seg in cluster):
        base["has_failure"] = True
    else:
        base.pop("has_failure", None)

    if trace_field:
        base[trace_field] = _merge_traces(cluster, trace_field,
                                          trace_ts_field, trace_name_field)

    base["segment_count"] = len(cluster)
    return base


def _merge_traces(cluster, trace_field, trace_ts_field, trace_name_field) -> List[Dict[str, Any]]:
    """跨叶拼 trace：按文件名去重，按时间升序。

    文件名在同一天内基本唯一；真撞名时保留先出现的那条并附带其所属叶子
    （_src_dir），避免下游拿错路径。
    """
    seen: Dict[str, Dict[str, Any]] = {}
    for seg in cluster:
        for tr in (seg.get(trace_field) or []):
            if not isinstance(tr, dict):
                continue
            key = str(tr.get(trace_name_field) or tr.get(trace_ts_field) or id(tr))
            if key in seen:
                continue
            item = dict(tr)
            if "_src_dir" not in item and "_src_dir" in seg:
                item["_src_dir"] = seg["_src_dir"]
            seen[key] = item
    out = list(seen.values())
    out.sort(key=lambda t: (str(t.get(trace_ts_field) or "")))
    return out
