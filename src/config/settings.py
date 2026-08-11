"""
Centralized configuration loading.

Non-secret settings come from config/config.yaml.
Secrets (DB credentials, JDBC jar path) come from environment variables,
loaded from a local .env file via python-dotenv.

This module is the single place the rest of the codebase should import
configuration from -- no other module should read config.yaml or os.environ
directly.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

# Repository root = two levels up from this file (src/config/settings.py)
REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = REPO_ROOT / "config" / "config.yaml"

load_dotenv(REPO_ROOT / ".env")


def _load_yaml_config(path: Path) -> dict[str, Any]:
    """Load and parse the YAML config file. Raises if the file is missing."""
    if not path.exists():
        raise FileNotFoundError(
            f"Missing config file at {path}. Did you rename or move config.yaml?"
        )
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


_raw_config = _load_yaml_config(CONFIG_PATH)


@dataclass(frozen=True)
class PathsConfig:
    incoming_dir: Path
    processed_dir: Path
    checkpoint_dir: Path
    performance_dir: Path
    log_dir: Path


@dataclass(frozen=True)
class GeneratorConfig:
    batch_size: int
    interval_seconds: int
    num_users: int
    num_products: int
    purchase_probability: float


@dataclass(frozen=True)
class SparkConfig:
    app_name: str
    shuffle_partitions: int
    trigger_interval: str


@dataclass(frozen=True)
class DatabaseConfig:
    host: str
    port: int
    name: str
    user: str
    password: str
    table_name: str
    jdbc_driver_class: str
    jdbc_jar_path: str

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.name}"


def _resolve_path(relative: str) -> Path:
    p = REPO_ROOT / relative
    p.mkdir(parents=True, exist_ok=True)
    return p


def get_paths_config() -> PathsConfig:
    paths = _raw_config["paths"]
    return PathsConfig(
        incoming_dir=_resolve_path(paths["incoming_dir"]),
        processed_dir=_resolve_path(paths["processed_dir"]),
        checkpoint_dir=_resolve_path(paths["checkpoint_dir"]),
        performance_dir=_resolve_path(paths["performance_dir"]),
        log_dir=_resolve_path(paths["log_dir"]),
    )


def get_generator_config() -> GeneratorConfig:
    gen = _raw_config["generator"]
    return GeneratorConfig(
        batch_size=int(gen["batch_size"]),
        interval_seconds=int(gen["interval_seconds"]),
        num_users=int(gen["num_users"]),
        num_products=int(gen["num_products"]),
        purchase_probability=float(gen["purchase_probability"]),
    )


def get_spark_config() -> SparkConfig:
    spark = _raw_config["spark"]
    return SparkConfig(
        app_name=spark["app_name"],
        shuffle_partitions=int(spark["shuffle_partitions"]),
        trigger_interval=spark["trigger_interval"],
    )


def get_database_config() -> DatabaseConfig:
    db = _raw_config["database"]

    required_env = ["POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD"]
    missing = [var for var in required_env if not os.environ.get(var)]
    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {missing}. "
            f"Copy .env.example to .env and fill in real values."
        )

    return DatabaseConfig(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        name=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        table_name=db["table_name"],
        jdbc_driver_class=db["jdbc_driver_class"],
        jdbc_jar_path=os.environ.get("POSTGRES_JDBC_JAR_PATH", ""),
    )
