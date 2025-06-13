from __future__ import annotations


def debug_log(message: str, config: dict | None = None) -> None:
    """Print *message* prefixed with [DEBUG] when ``config['debug']`` is True."""
    if config and config.get("debug", False):
        print("[DEBUG]", message)
