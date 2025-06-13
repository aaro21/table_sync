"""Load YAML configuration and resolve environment variables."""

import yaml
import os
from dotenv import load_dotenv

load_dotenv()


def get_env_var(name: str) -> str:
    """Return the value of ``name`` from the OS environment or raise."""
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Environment variable '{name}' is not set.")
    return value


def resolve_env_vars(env_map: dict) -> dict:
    """Expand a mapping of variable names to actual environment values."""
    return {k: get_env_var(v) for k, v in env_map.items()}


def load_config(path: str = "config/config.yaml") -> dict:
    """Load configuration from *path* and inject resolved credentials."""
    with open(path, "r") as f:
        raw_config = yaml.safe_load(f)

    # Resolve source DB env vars
    source_env = raw_config["source"].get("env", {})
    source_conn = resolve_env_vars(source_env)
    raw_config["source"]["connection"] = source_conn

    # Resolve destination DB env vars
    dest_env = raw_config["destination"].get("env", {})
    dest_conn = resolve_env_vars(dest_env)
    raw_config["destination"]["connection"] = dest_conn

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

    return raw_config
