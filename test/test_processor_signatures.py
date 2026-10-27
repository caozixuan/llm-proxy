"""
test/test_processor_signatures.py — 导出处理器签名与调用方约定一致性

背景：#242 全量导出 4 秒失败，报
`full_reformat_export() got an unexpected keyword argument 'on_session_done'`。
根因是 export_routes 给所有处理器统一传 on_session_done（进度回调），
而 reformat_full 的**函数体**用了它、**签名**却漏声明。Python 在进入函数前
就抛 TypeError，py_compile 查不出这类问题。

本测试把所有注册在 settings/export_modes.yaml 的 processor 解析出来，
断言它们都接受 export_routes 实际传递的关键字参数，一次性挡住
「调用方加参数、某个处理器漏改」的整类回归。

运行：.venv/bin/python test/test_processor_signatures.py
"""

import inspect
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.export_modes import get_config  # noqa: E402

# export_routes._run_task_inner 调用 processor 时实际传递的关键字参数。
# 其中 analyze 是三态：只有声明了该形参的处理器才会收到（见 yaml 的 analyze 字段），
# 故不列入必需集合，单独在下面校验。
REQUIRED_KWARGS = {
    "src_dir",
    "out_dir",
    "session_entries",
    "api_key",
    "workers",
    "progress_cb",
    "log_dir",
    "should_cancel",
    "on_session_done",
}


def _iter_processors():
    """遍历注册表里所有声明了 processor 的导出模式，产出 (mode_id, 函数)。"""
    cfg = get_config()
    for mode in cfg.all():
        if not getattr(mode, "processor", None):
            continue
        fn = cfg.resolve_processor(mode.id)
        if fn is None:
            raise AssertionError(f"模式 {mode.id} 声明了 processor 但解析失败: {mode.processor}")
        yield mode.id, fn


def test_all_processors_accept_caller_kwargs():
    """每个处理器都必须接受 export_routes 传递的全部关键字参数。"""
    checked = 0
    for mode_id, fn in _iter_processors():
        params = set(inspect.signature(fn).parameters)
        missing = REQUIRED_KWARGS - params
        assert not missing, (
            f"模式 {mode_id} 的处理器 {fn.__name__} 缺少形参: {sorted(missing)}；"
            f"export_routes 会传这些关键字，缺一个就会在进入函数前抛 TypeError"
        )
        checked += 1
    assert checked > 0, "没有解析到任何 processor，检查 settings/export_modes.yaml"


def test_analyze_param_matches_registry():
    """注册表里 analyze 有值（true/false）的模式，处理器必须声明 analyze 形参。

    export_routes 按 yaml 的 analyze 三态决定传不传：写了就传、没写就不传。
    若某模式配了 analyze 但处理器没这个形参，同样会 TypeError。
    """
    cfg = get_config()
    for mode in cfg.all():
        if not getattr(mode, "processor", None):
            continue
        fn = cfg.resolve_processor(mode.id)
        params = set(inspect.signature(fn).parameters)
        if mode.analyze is None:
            # 不传 analyze：处理器可以有也可以没有，不约束
            continue
        assert "analyze" in params, (
            f"模式 {mode.id} 在 yaml 里配了 analyze={mode.analyze}，"
            f"但处理器 {fn.__name__} 没有 analyze 形参"
        )


def main() -> int:
    failures = 0
    for name, fn in sorted(globals().items()):
        if not name.startswith("test_") or not callable(fn):
            continue
        try:
            fn()
            print(f"PASS {name}")
        except AssertionError as e:
            print(f"FAIL {name}: {e}")
            failures += 1
    print(f"\n{'全部通过' if not failures else f'{failures} 个失败'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
