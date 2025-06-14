from __future__ import annotations


def _get_config_level(config: dict | None) -> str:
    """Return the configured debug level from *config*."""
    if not config:
        return "low"

    cfg_val = config.get("debug", False)
    if isinstance(cfg_val, bool):
        return "high" if cfg_val else "low"
    try:
        level = str(cfg_val).lower()
    except Exception:
        return "low"
    if level in {"low", "medium", "high"}:
        return level
    return "low"


def debug_log(message: str, config: dict | None = None, *, level: str = "high") -> None:
    """Print debug *message* when the configured level is >= ``level``."""
    current = _get_config_level(config)
    ranks = {"low": 1, "medium": 2, "high": 3}
    if ranks.get(current, 1) >= ranks.get(level, 3):
        print("[DEBUG]", message)
