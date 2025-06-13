"""Load YAML configuration and resolve environment variables."""

import yaml
import os
from dotenv import load_dotenv

from utils.logger import debug_log

load_dotenv()


def get_env_var(name: str) -> str:
    """Return the value of ``name`` from the OS environment or raise."""
    value = os.getenv(name)
    if value is None:
        raise EnvironmentError(f"Environment variable '{name}' is missing or unset.")
    return value


def resolve_env_vars(env_map: dict, debug: bool = False) -> dict:
    """Expand a mapping of variable names to actual environment values."""
    debug_log(f"Resolving environment variables for: {env_map}", {"debug": debug})
    return {k: get_env_var(v) for k, v in env_map.items()}


def load_config(path: str = "config/config.yaml") -> dict:
    """Load configuration from *path* and inject resolved credentials."""
    with open(path, "r") as f:
        raw_config = yaml.safe_load(f)

    debug = raw_config.get("debug", False)
    raw_config["debug"] = debug

    # Resolve and store separately for clarity
    source_env = raw_config["source"].get("env", {})
    raw_config["source"]["resolved_env"] = resolve_env_vars(source_env, debug)

    dest_env = raw_config["destination"].get("env", {})
    raw_config["destination"]["resolved_env"] = resolve_env_vars(dest_env, debug)

    # Normalize column definitions
    src_cols = raw_config["source"].get("columns", {})
    if isinstance(src_cols, list):
        # Auto-generate identity mapping: {col: col}
        src_cols = {col: col for col in src_cols}
        raw_config["source"]["columns"] = src_cols

    # If dest columns are missing, mirror from source
    if "columns" not in raw_config["destination"]:
        raw_config["destination"]["columns"] = src_cols
    else:
        # Remove any destination-only fields (not in source logical names)
        dest_cols = raw_config["destination"]["columns"]
        raw_config["destination"]["columns"] = {
            k: v for k, v in dest_cols.items() if k in src_cols
        }

    def _lower_map(col_map: dict) -> dict:
        return {k.lower(): v.lower() for k, v in col_map.items()}

    raw_config["source"]["columns"] = _lower_map(raw_config["source"]["columns"])
    raw_config["destination"]["columns"] = _lower_map(raw_config["destination"]["columns"])

    return raw_config
