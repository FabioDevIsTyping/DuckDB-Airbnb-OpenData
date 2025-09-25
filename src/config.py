"""
Loads configuration from a TOML file and resolves paths relative to the project root.
"""
import sys
from pathlib import Path

if sys.version_info < (3, 11):
    import tomli as tomllib
else:
    import tomllib

BASE_DIR = Path(__file__).resolve().parents[1]

def load_config():
    """Load configuration from a TOML file and resolve paths."""
    cfg_path = BASE_DIR / "config.toml"
    with open(cfg_path, "rb") as fh:
        config = tomllib.load(fh)
    config["duckdb"]["path"] = str(BASE_DIR / config["duckdb"]["path"])
    config["input"]["csv_path"] = str(BASE_DIR / config["input"]["csv_path"])
    return config