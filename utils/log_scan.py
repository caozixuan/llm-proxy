"""
utils/log_scan.py — 可变深度日志目录发现 + 索引字段归一化

支持两种目录布局，统一以「含 index.jsonl 的叶子目录」为扫描单元：
  - 本项目:  {root}/{hour}/index.jsonl              (root = logs_all/env-xxx)
  - new-api: {root}/{day}/{hour}/index.jsonl        (root = .../logs/details)

叶子目录的稳定标识 dir_key = 相对 root 的 posix 路径（如 "26072520" 或 "260727/26072717"）。

索引条目字段在两种格式间有差异，normalize_entry 统一为:
  ts, model, api_key, tok_in, tok_out, cache_in, success, channel_key,
  q1_hash, chain_key, req_file
"""

import json
import re
from pathlib import Path
from typing import Iterator, Optional

_INDEX_NAME = "index.jsonl"
_MAX_DEPTH = 3  # root 自身算 depth 0；new-api 需要 depth 2

# 层级模板占位符（compile_leaf_template 用）：目录名的定长数字段。
# 用户注册数据源时按占位符写法填层级模板（如 details/{日6}/{时8}），
# {日6}=6 位数字（YYMMDD）、{时8}=8 位数字（YYMMDDHH）、{数N}=N 位数字（通用）；
# 其余字面段（如 details）按目录名精确匹配。
_PLACEHOLDERS = {
    "{日6}": re.compile(r"^\d{6}$"),
    "{时8}": re.compile(r"^\d{8}$"),
}
_RE_NUM_N = re.compile(r"^\{数(\d+)\}$")   # {数N} → ^\d{N}$

# 按 fmt 的默认层级模板：用户未填模板时回退（保证老源不填也能同步）。
# 与旧 iter_leaf_dirs_explicit 的硬规则等价（含 details/ 中间层变体）。
_DEFAULT_TEMPLATES = {
    "newapi": ["{日6}/{时8}", "details/{日6}/{时8}"],
    "native": ["{时8}"],
}


def iter_index_dirs(root: Path, max_depth: int = _MAX_DEPTH) -> Iterator[Path]:
    """递归查找 root 下所有直接包含 index.jsonl 的目录（含 root 自身）。

    找到含 index.jsonl 的目录后不再深入其子目录。
    """
    if not root.is_dir():
        return

    def _walk(d: Path, depth: int) -> Iterator[Path]:
        if (d / _INDEX_NAME).is_file():
            yield d
            return
        if depth >= max_depth:
            return
        try:
            children = sorted(d.iterdir())
        except OSError:
            return
        for sub in children:
            if sub.is_dir() and not sub.name.startswith("."):
                yield from _walk(sub, depth + 1)

    yield from _walk(root, 0)


def default_templates(fmt: str) -> list:
    """某格式的默认层级模板列表（用户未填模板时回退）。未知格式返回空。"""
    return list(_DEFAULT_TEMPLATES.get(fmt, []))


def compile_leaf_template(tpl: str):
    """把一行占位符层级模板编译成逐段匹配器列表。

    段以 `/` 分隔；每段编译为一个 (kind, matcher)：
      - 占位符 {日6}/{时8}/{数N} → ("re", 已编译正则)，按目录名匹配
      - 字面段（如 details）      → ("lit", 段名)，按目录名精确相等匹配
    空模板 / 段全空 → 抛 ValueError；未知占位符（{...} 但非上述）→ 抛 ValueError。
    返回段匹配器列表，供 iter_leaf_dirs_by_templates 逐层下降。
    """
    if not tpl or not tpl.strip():
        raise ValueError("模板不能为空")
    segs = [s for s in tpl.strip().strip("/").split("/") if s]
    if not segs:
        raise ValueError(f"模板无有效层级: {tpl!r}")
    compiled = []
    for seg in segs:
        if seg in _PLACEHOLDERS:
            compiled.append(("re", _PLACEHOLDERS[seg]))
        elif _RE_NUM_N.match(seg):
            n = int(_RE_NUM_N.match(seg).group(1))
            compiled.append(("re", re.compile(rf"^\d{{{n}}}$")))
        elif seg.startswith("{") and seg.endswith("}"):
            raise ValueError(f"未知占位符 {seg!r}（支持 {{日6}} {{时8}} {{数N}}）")
        else:
            compiled.append(("lit", seg))
    return compiled


def iter_leaf_dirs_by_templates(root: Path, templates: list) -> Iterator[Path]:
    """按用户注册的占位符层级模板列表枚举叶子目录（逐段 iterdir，不递归 walk 全树）。

    「数据管理」的「同步」专用，替代 iter_index_dirs 在网络盘上的递归 walk。
    每个模板（见 compile_leaf_template）逐层下降：占位符段按正则筛数字目录名、
    字面段按目录名精确匹配。最内层校验含 index.jsonl 才 yield（含「同名折叠层」
    兜底：叶/叶/index.jsonl，dir_key_for 会折回标准层）。多模板取并集、按 resolved
    path 去重。非法模板（compile 抛错）跳过该模板、继续其余。

    templates 为空或全非法 → 不产出（调用方应传默认模板 default_templates(fmt)）。
    """
    if not root.is_dir() or not templates:
        return

    def _leaf_or_folded(d: Path) -> Optional[Path]:
        """d 或其同名折叠子目录 d/<d.name> 含 index.jsonl 则返回该叶子，否则 None。"""
        if (d / _INDEX_NAME).is_file():
            return d
        nested = d / d.name
        if (nested / _INDEX_NAME).is_file():
            return nested
        return None

    def _descend(base: Path, segs: list, i: int) -> Iterator[Path]:
        """按段匹配器 segs 从 base 逐层下降；到最后一段时校验叶子。"""
        kind, m = segs[i]
        last = (i == len(segs) - 1)
        if kind == "lit":
            # 字面段：直接取同名子目录，不必列全目录
            sub = base / m
            if not sub.is_dir():
                return
            candidates = [sub]
        else:
            try:
                children = sorted(base.iterdir())
            except OSError:
                return
            candidates = [c for c in children
                          if not c.name.startswith(".") and c.is_dir() and m.match(c.name)]
        for c in candidates:
            if last:
                leaf = _leaf_or_folded(c)
                if leaf is not None:
                    yield leaf
            else:
                yield from _descend(c, segs, i + 1)

    seen = set()
    for tpl in templates:
        try:
            segs = compile_leaf_template(tpl)
        except ValueError:
            continue
        for leaf in _descend(root, segs, 0):
            key = str(leaf)
            if key in seen:
                continue
            seen.add(key)
            yield leaf


def fast_scan_leaf_dirs_by_templates(root: Path, templates: list) -> Iterator[Path]:
    """按模板枚举叶子的快速版：逐段下降，只 stat 目标 index.jsonl，
    绝不列举叶子目录里的海量小文件。

    老版 iter_leaf_dirs_by_templates 慢的根因：占位符段用 `Path.iterdir()`
    列目录后对**每个条目**调 `c.is_dir()`。当模板某段匹配到「平叶子」（内含
    几万~上百万个 *.json）时，会去列这个海量目录并逐个 stat，网络盘上卡死。

    本版关键：**下钻某目录前，先看它是不是已经是叶子（含 index.jsonl），
    是则不再往下列**。因为本源里平叶子的目录名也符合 `{时8}`，第一段会把它
    收进候选；到第二段若不短路，就会去 scandir 平叶子而卡死。短路后：
      - 平叶子在它该出现的那一段就被当作叶子产出，从不被列举内部文件。
      - 只有「非叶子的层级目录」才 scandir（层级目录只含少量子目录，很快）。
    占位符段 scandir 时目录名正则先筛（纯字符串），再用 DirEntry.is_dir()
    （scandir 自带类型，零额外 IO）。字面段用路径构造 + 一次 is_dir()。
    多模板并集去重；非法模板跳过；templates 空则不产出。
    """
    import os as _os
    if not root.is_dir() or not templates:
        return
    seen = set()

    def _emit(d):
        """d 或其同名折叠子目录 d/<basename> 含 index.jsonl 则登记并返回真实叶子；否则 None。

        与慢扫 iter_leaf_dirs_by_templates 的 _leaf_or_folded 对齐：某些 native 源把真叶子
        多套一层同名子目录（.../26071620/26071620/index.jsonl），单段模板 {时8} 匹配到外层
        空壳后，只 stat 外层 index.jsonl 会漏掉整个折叠叶子。此处补一次 folded stat（仍只
        stat index.jsonl，不列举叶子内海量小文件，保持快扫的零列举保证）。
        """
        direct = _os.path.join(d, _INDEX_NAME)
        if _os.path.isfile(direct):
            if d not in seen:
                seen.add(d)
                return d
            return None
        base = _os.path.basename(_os.path.normpath(d))
        nested = _os.path.join(d, base)
        if _os.path.isfile(_os.path.join(nested, _INDEX_NAME)) and nested not in seen:
            seen.add(nested)
            return nested
        return None

    for tpl in templates:
        try:
            segs = compile_leaf_template(tpl)
        except ValueError:
            continue
        current = [str(root)]  # 走到本段为止的候选目录（字符串路径）
        for kind, m in segs:
            nxt = []
            if kind == "lit":
                for d in current:
                    sub = _os.path.join(d, m)
                    if _os.path.isdir(sub):     # 直接构造 + 一次 stat，不列目录
                        nxt.append(sub)
            else:  # 占位符段
                for d in current:
                    # 短路：d 已是叶子就不下钻（避免 scandir 平叶子的海量文件）
                    if _os.path.isfile(_os.path.join(d, _INDEX_NAME)):
                        continue
                    try:
                        with _os.scandir(d) as it:
                            for e in it:
                                name = e.name
                                if name.startswith("."):
                                    continue
                                if not m.match(name):   # 正则先行：刷掉文件名
                                    continue
                                if e.is_dir():          # scandir 自带类型，零额外 IO
                                    nxt.append(e.path)
                    except OSError:
                        continue
            current = nxt
            if not current:
                break
        # 终态目录：只 stat index.jsonl（含同名折叠层），绝不列叶子
        for d in current:
            leaf = _emit(d)
            if leaf is not None:
                yield Path(leaf)


def iter_leaf_dirs_explicit(root: Path, fmt: str) -> Iterator[Path]:
    """按 fmt 默认模板枚举叶子（compat 包装：= iter_leaf_dirs_by_templates + 默认模板）。

    保留此签名供未显式传模板的调用方（默认模板等价旧硬规则）。
    """
    yield from iter_leaf_dirs_by_templates(root, default_templates(fmt))


def dir_key_for(root: Path, leaf: Path) -> str:
    """叶子目录相对 root 的标识；用于索引缓存的键。

    折叠"末段与父段同名"的冗余层：某些 new-api 源把真叶子多套一层同名子目录
    （如 260727/26072717/26072717/index.jsonl），此处归一化为标准 天/小时
    （260727/26072717），与其它源口径一致。与 resolve_leaf 成对，可逆。

    实现用字符串前缀切片而非 Path.relative_to().as_posix()：后者在冷路径上是
    隐藏热点（4592 次调用累计 0.68s，占 token 索引冷构建 CPU 的 56%），因为它
    每叶都要走 pathlib 的 parts 解析链。输出与旧实现逐字节相同（含同名折叠）。
    """
    rs = str(root)
    ls = str(leaf)
    if ls.startswith(rs):
        rel = ls[len(rs):].lstrip("/")
    else:
        try:
            rel = leaf.relative_to(root).as_posix()
        except ValueError:
            return leaf.name
    parts = rel.split("/")
    if len(parts) >= 2 and parts[-1] == parts[-2]:
        rel = "/".join(parts[:-1])
    return rel


def resolve_leaf(root, dir_key: str) -> Path:
    """dir_key（可能已被 dir_key_for 折叠）反解为真实叶子绝对路径。

    先试 root/dir_key（标准结构直接命中）；若该目录不含 index.jsonl 但存在
    同名子目录 root/dir_key/<末段> 且含 index.jsonl，则补回被折叠的层。
    与 dir_key_for 成对。
    """
    root_path = Path(root)
    cand = root_path / dir_key
    if (cand / _INDEX_NAME).is_file():
        return cand
    if dir_key:
        last = dir_key.split("/")[-1]
        nested = cand / last
        if (nested / _INDEX_NAME).is_file():
            return nested
    return cand


def detect_format(root: str) -> str:
    """采样一个叶子的 index.jsonl 首条，判断 'native' / 'newapi' / 'unknown' / 'empty'。"""
    root_path = Path(root)
    if not root_path.is_dir():
        return "missing"
    for leaf in iter_index_dirs(root_path):
        entry = _first_entry(leaf / _INDEX_NAME)
        if entry is None:
            continue
        if "tok_in" in entry or "channel_key" in entry or "q1_hash" in entry:
            return "native"
        usage = entry.get("usage")
        # new-api 的 usage 可能用本项目风格(token_in/token_out)或 OpenAI 风格
        # (prompt_tokens/completion_tokens)；与下游 normalize_entry 的口径保持一致。
        if isinstance(usage, dict) and any(
            k in usage for k in ("token_in", "token_out", "prompt_tokens", "completion_tokens")
        ):
            return "newapi"
        return "unknown"
    return "empty"


def _first_entry(index_file: Path) -> Optional[dict]:
    try:
        with open(index_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if obj.get("_meta"):
                    continue
                return obj
    except OSError:
        return None
    return None


def normalize_entry(entry: dict) -> dict:
    """把两种格式的索引条目归一化为统一字段。

    - 本项目已有 tok_in/tok_out/success/valid/channel_key/q1_hash/chain_key。
    - new-api 只有 usage.token_in/token_out、req_file、model、api_key、ts；
      无 success/channel_key/会话字段。成功判定退化为 tok_out > 0。
    """
    usage = entry.get("usage") if isinstance(entry.get("usage"), dict) else {}

    tok_in = entry.get("tok_in")
    if tok_in is None:
        tok_in = usage.get("token_in") or usage.get("prompt_tokens") or 0
    tok_out = entry.get("tok_out")
    if tok_out is None:
        tok_out = usage.get("token_out") or usage.get("completion_tokens") or 0
    tok_in = tok_in or 0
    tok_out = tok_out or 0

    cache_in = entry.get("cache_in") or 0

    if "valid" in entry:
        success = bool(entry["valid"]) and tok_out > 0
    elif "success" in entry:
        success = bool(entry["success"]) and tok_out > 0
    else:
        # new-api：无显式成功标记，用 tok_out 判定
        success = tok_out > 0

    ts = entry.get("ts", "") or ""

    return {
        "ts": ts,
        "date": ts[:10] if len(ts) >= 10 else "unknown",
        "model": entry.get("model", "") or "",
        "api_key": entry.get("api_key", "") or "",
        "tok_in": tok_in,
        "tok_out": tok_out,
        "cache_in": cache_in,
        "success": success,
        "channel_key": entry.get("channel_key", "") or "",
        "q1_hash": entry.get("q1_hash", "") or "",
        "chain_key": entry.get("chain_key", "") or "",
        "req_file": entry.get("req_file", "") or "",
    }


def date_from_ts(ts: str) -> str:
    """ts 可能是 '2026-07-25_00-01-41_930' 或 '2026-07-27T17:43:38.096+08:00'。
    统一取前 10 位 YYYY-MM-DD。"""
    return ts[:10] if len(ts) >= 10 else "unknown"
