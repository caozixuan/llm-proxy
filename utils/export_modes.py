"""
utils/export_modes.py — 导出模式注册表加载器

从 settings/export_modes.yaml 读取导出模式定义（模式清单、处理器、落盘目录、
OBS 子前缀、公开可见性等）。网页端（utils/export_routes.py）与离线 CLI
（tools/offline_reformat_export.py）共用同一份配置，新增模式只需改 YAML。

对外接口：
    cfg = get_config()                 # 带 mtime 缓存，改 YAML 无需重启
    cfg.temp_path                      # 本地落盘根目录 Path
    cfg.get("jiuwen_reformat")         # -> ModeConfig | None
    cfg.ids()                          # -> ("export", "reformat", ...) 顺序同 YAML
    cfg.public_ids()                   # 暴露给公开入口的模式
    cfg.resolve_processor(mode)        # -> 可调用对象（pipeline=copy 时为 None）

设计要点：
  * 模块级缓存 + mtime 比对，YAML 改动即时生效（沿用 utils/custom_models.py 风格）。
  * YAML 缺失/损坏时回退到 _DEFAULT_MODES 内置表并记 warning —— 导出是核心链路，
    配置出错不能让功能整体不可用。
  * `analyze` 为三态：True/False 表示调用处理器时传 analyze=该值，None 表示
    处理器没有这个形参，调用时不传（reconstruct / full_reformat / jiuwen_reformat）。
"""

import importlib
import logging
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple

import yaml

_CONFIG_PATH = Path(__file__).resolve().parent.parent / "settings" / "export_modes.yaml"

# 回退默认表：任何一项与 settings/export_modes.yaml 不一致时以 YAML 为准，
# 这里只保证「YAML 丢了也能跑」。
_DEFAULT_TEMP_PATH = "."
_DEFAULT_MODES: Dict[str, dict] = {
    "export": {
        "label": "导出原始文件（三元组，不分析）",
        "mode_label": "导出",
        "obs_sub": "session",
        "local_dir": "logs_session",
        "pipeline": "copy",
        "public": True,
    },
    "reformat": {
        "label": "合并导出（三元组合并成单 JSON，不质检）",
        "mode_label": "合并导出",
        "obs_sub": "session_analysis",
        "local_dir": "logs_session_analysis",
        "pipeline": "reformat",
        "processor": "utils.eval.reformat:reformat_and_analyze",
        "analyze": False,
        "public": True,
    },
    "reconstruct": {
        "label": "重构导出（hermes 聚合去重+回填 reasoning，仅 new-api）",
        "mode_label": "重构导出",
        "step": "hermes 重构",
        "obs_sub": "session_reconstruct",
        "local_dir": "logs_session_reconstruct",
        "pipeline": "reformat",
        "processor": "utils.eval.reconstruct:reconstruct_and_export",
        "formats": ["newapi"],
        "public": True,
    },
    "full_reformat": {
        "label": "全量导出（session 全部 trace 文件合并，仅单目录，不质检）",
        "mode_label": "全量导出",
        "step": "全量合并",
        "obs_sub": "session_analysis_full",
        "local_dir": "logs_session_analysis_full",
        "pipeline": "reformat",
        "processor": "utils.eval.reformat_full:full_reformat_export",
        "max_mtimes": 1,
        "public": True,
    },
    "jiuwen_reformat": {
        "label": "九问导出（reasoning_content 回填重构）",
        "mode_label": "九问导出",
        "step": "九问重构",
        "obs_sub": "session_analysis_jiuwen",
        "local_dir": "logs_session_analysis_jiuwen",
        "pipeline": "reformat",
        "processor": "utils.eval.reformat_jiuwen:jiuwen_reformat_export",
        "public": True,
    },
    "jiuwenswarm_dev": {
        "label": "Jiuwenswarm 开发导出（快照分支聚合+权重+reasoning 回填）",
        "mode_label": "Jiuwenswarm 导出",
        "step": "Jiuwenswarm 聚合",
        "obs_sub": "session_analysis_jiuwenswarm",
        "local_dir": "logs_session_analysis_jiuwenswarm",
        "pipeline": "reformat",
        "processor": "utils.eval.reformat_jiuwenswarm:jiuwenswarm_export",
        "public": True,
    },
    "eval": {
        "label": "质检模式（reformat+分析，出报告）",
        "mode_label": "质检",
        "step": "reformat+analyze",
        "obs_sub": "session_analysis",
        "local_dir": "logs_session_analysis",
        "pipeline": "eval",
        "processor": "utils.eval.reformat:reformat_and_analyze",
        "analyze": True,
        "public": True,
    },
}


class ModeConfig:
    """单个导出模式的配置。"""

    __slots__ = (
        "id", "label", "mode_label", "step", "obs_sub", "local_dir",
        "pipeline", "processor", "analyze", "formats", "max_mtimes", "public",
    )

    def __init__(self, mode_id: str, raw: dict) -> None:
        self.id = mode_id
        self.label = str(raw.get("label") or raw.get("mode_label") or mode_id)
        self.mode_label = str(raw.get("mode_label") or self.label)
        self.step = str(raw.get("step") or self.mode_label)
        self.obs_sub = str(raw.get("obs_sub") or "").strip("/")
        self.local_dir = str(raw.get("local_dir") or mode_id).strip("/")
        self.pipeline = str(raw.get("pipeline") or ("copy" if not raw.get("processor") else "reformat"))

        proc = raw.get("processor")
        if isinstance(proc, str) and ":" in proc:
            mod, _, func = proc.partition(":")
            self.processor: Optional[Tuple[str, str]] = (mod.strip(), func.strip())
        else:
            self.processor = None

        # 三态：True / False / None（None = 处理器没有 analyze 形参）
        analyze = raw.get("analyze", None)
        self.analyze: Optional[bool] = None if analyze is None else bool(analyze)

        fmts = raw.get("formats")
        self.formats: Optional[Tuple[str, ...]] = (
            tuple(str(f) for f in fmts) if isinstance(fmts, (list, tuple)) and fmts else None
        )

        mm = raw.get("max_mtimes")
        try:
            self.max_mtimes: Optional[int] = int(mm) if mm is not None else None
        except (TypeError, ValueError):
            self.max_mtimes = None

        self.public = bool(raw.get("public", False))

    def supports_format(self, fmt: str) -> bool:
        return self.formats is None or fmt in self.formats

    def __repr__(self) -> str:  # pragma: no cover - 调试用
        return f"<ModeConfig {self.id} obs_sub={self.obs_sub} pipeline={self.pipeline}>"


class ExportModesConfig:
    """整份配置：本地根目录 + 有序模式表。"""

    def __init__(self, temp_path: str, modes: Dict[str, ModeConfig]) -> None:
        self.temp_path_raw = temp_path
        self.temp_path = Path(temp_path)
        self.modes: Dict[str, ModeConfig] = modes

    def get(self, mode_id: str) -> Optional[ModeConfig]:
        return self.modes.get(mode_id)

    def ids(self) -> Tuple[str, ...]:
        return tuple(self.modes.keys())

    def public_ids(self) -> Tuple[str, ...]:
        return tuple(m for m, c in self.modes.items() if c.public)

    def all(self):
        return tuple(self.modes.values())

    def label_of(self, mode_id: str, default: str = "") -> str:
        cfg = self.modes.get(mode_id)
        return cfg.label if cfg else (default or mode_id)

    def mode_label_of(self, mode_id: str, default: str = "") -> str:
        cfg = self.modes.get(mode_id)
        return cfg.mode_label if cfg else (default or mode_id)

    def resolve_processor(self, mode_id: str) -> Optional[Callable]:
        """按配置 import 处理器函数。模块必须已可导入，失败返回 None。"""
        cfg = self.modes.get(mode_id)
        if cfg is None or cfg.processor is None:
            return None
        mod_name, func_name = cfg.processor
        try:
            mod = importlib.import_module(mod_name)
            return getattr(mod, func_name)
        except Exception as e:  # pragma: no cover - 配置写错时兜底
            logging.warning(
                "export_modes: resolve processor %s:%s failed: %s", mod_name, func_name, e
            )
            return None

    def to_public_list(self) -> list:
        """给前端 /api/export/config 用的模式清单。

        含前端渲染所需的全部字段（下拉文案、徽章文案、OBS 子目录预览、
        mtime 目录数上限、是否产合并文件），使新增模式不必改模板。
        """
        out = []
        for m in self.modes.values():
            item: Dict[str, Any] = {
                "id": m.id,
                "label": m.label,
                "mode_label": m.mode_label,
                "obs_sub": m.obs_sub,
                "pipeline": m.pipeline,
                # 前端据此给任务表的「导出方式」徽章着色（非 copy = 产合并文件）
                "reformat": m.pipeline != "copy",
                "max_mtimes": m.max_mtimes,
                "formats": list(m.formats) if m.formats else None,
                "public": m.public,
            }
            out.append(item)
        return out


_config: Optional[ExportModesConfig] = None
_config_mtime: Optional[float] = None
_config_sig: Optional[Tuple[float, int]] = None


def _build(data: Any, path: Path) -> ExportModesConfig:
    if not isinstance(data, dict):
        raise ValueError("top-level YAML is not a mapping")
    raw_modes = data.get("modes")
    if not isinstance(raw_modes, dict) or not raw_modes:
        raise ValueError("`modes` is missing or empty")

    modes: Dict[str, ModeConfig] = {}
    for mode_id, raw in raw_modes.items():
        if not isinstance(raw, dict):
            continue
        modes[str(mode_id)] = ModeConfig(str(mode_id), raw)
    if not modes:
        raise ValueError("no usable mode entries")

    temp_path = data.get("temp_path")
    if not isinstance(temp_path, str) or not temp_path.strip():
        temp_path = _DEFAULT_TEMP_PATH
    return ExportModesConfig(temp_path.strip(), modes)


def _fallback(reason: str) -> ExportModesConfig:
    logging.warning("export_modes: %s; falling back to built-in defaults", reason)
    return _build({"temp_path": _DEFAULT_TEMP_PATH, "modes": _DEFAULT_MODES}, _CONFIG_PATH)


def get_config(config_path: Optional[str] = None, force: bool = False) -> ExportModesConfig:
    """读取配置。默认带缓存，按 (mtime, size) 判失效，改 YAML 无需重启。"""
    global _config, _config_mtime, _config_sig

    path = Path(config_path) if config_path else _CONFIG_PATH
    try:
        st = path.stat()
        sig: Optional[Tuple[float, int]] = (st.st_mtime, st.st_size)
    except OSError:
        sig = None

    if _config is not None and not force and sig is not None and sig == _config_sig:
        return _config

    if sig is None:
        _config = _fallback(f"config not found: {path}")
        _config_sig = None
        return _config

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        cfg = _build(data, path)
        logging.info(
            "export_modes: loaded %d mode(s) from %s (temp_path=%s)",
            len(cfg.modes), path, cfg.temp_path_raw,
        )
        _config = cfg
        _config_sig = sig
        return cfg
    except Exception as e:
        # 解析失败：保留上一次可用配置（若有），否则回退内置表
        if _config is not None:
            logging.warning("export_modes: reload %s failed (%s); keeping previous config", path, e)
            _config_sig = sig
            return _config
        _config = _fallback(f"failed to load {path}: {e}")
        _config_sig = sig
        return _config


def get_mode(mode_id: str) -> Optional[ModeConfig]:
    return get_config().get(mode_id)


def mode_ids() -> Tuple[str, ...]:
    return get_config().ids()


def public_mode_ids() -> Tuple[str, ...]:
    return get_config().public_ids()


def temp_path() -> Path:
    return get_config().temp_path


def local_dir_for(mode_id: str, fallback: Optional[str] = None) -> str:
    cfg = get_config().get(mode_id)
    if cfg:
        return cfg.local_dir
    return fallback or mode_id


def obs_sub_for(mode_id: str, fallback: Optional[str] = None) -> str:
    cfg = get_config().get(mode_id)
    if cfg:
        return cfg.obs_sub
    return fallback or ""
