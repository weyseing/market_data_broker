"""Runtime configuration.

Single source of truth for runtime values (ports, URLs, timeouts). Replaces
ad-hoc ``os.environ.get(...)`` calls scattered across modules.

Precedence (highest wins):

1. **Environment variables** — ``MDB_WS_PORT=9000`` etc. See :data:`_ENV_OVERRIDES`
   for the full list.
2. **YAML file** — ``config/default.yaml`` at the repo root.
3. **Pydantic model defaults** — the values declared on the model classes below.

Module-level ``DEFAULT_*`` constants on individual modules (bus, ws server,
ingest, …) stay as test-time defaults so unit tests can construct components
without loading a Config. Production code paths flow values from the loaded
:class:`Config` into those constructors via existing kwargs.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError

# Strict by default: a typo in YAML or env var path produces a clear error
# rather than a silent ignore. The cost is that adding a new field requires
# updating the model — which is what we want.
_STRICT = ConfigDict(extra="forbid")


class CoinbaseReconnectConfig(BaseModel):
    model_config = _STRICT

    initial_backoff_seconds: float = 1.0
    max_backoff_seconds: float = 30.0


class CoinbaseConfig(BaseModel):
    model_config = _STRICT

    ws_url: str = "wss://ws-feed.exchange.coinbase.com"
    # Informational: the channels this hub knows how to ingest. Wired by the
    # topic catalog work in Step 8; today it documents intent.
    channels: list[str] = Field(default_factory=lambda: ["ticker", "matches", "level2_batch"])
    reconnect: CoinbaseReconnectConfig = Field(default_factory=CoinbaseReconnectConfig)
    heartbeat_timeout_seconds: float = 15.0


class BusConfig(BaseModel):
    model_config = _STRICT

    consumer_queue_size: int = 1000
    sustained_overflow_drops: int = 100


class WSServerConfig(BaseModel):
    model_config = _STRICT

    host: str = "0.0.0.0"
    port: int = 8765


class StatusServerConfig(BaseModel):
    """Reserved for the /status HTTP endpoint (Step 10b)."""

    model_config = _STRICT

    host: str = "0.0.0.0"
    port: int = 8080


class Config(BaseModel):
    model_config = _STRICT

    log_level: str = "INFO"
    coinbase: CoinbaseConfig = Field(default_factory=CoinbaseConfig)
    bus: BusConfig = Field(default_factory=BusConfig)
    ws_server: WSServerConfig = Field(default_factory=WSServerConfig)
    status_server: StatusServerConfig = Field(default_factory=StatusServerConfig)


# (env var, dotted path into Config). Pydantic handles string→int/float coercion
# at validation time, so the override layer doesn't need to know the field type.
_ENV_OVERRIDES: tuple[tuple[str, str], ...] = (
    ("LOG_LEVEL", "log_level"),
    ("MDB_COINBASE_WS_URL", "coinbase.ws_url"),
    ("MDB_COINBASE_HEARTBEAT_TIMEOUT_SECONDS", "coinbase.heartbeat_timeout_seconds"),
    ("MDB_COINBASE_INITIAL_BACKOFF_SECONDS", "coinbase.reconnect.initial_backoff_seconds"),
    ("MDB_COINBASE_MAX_BACKOFF_SECONDS", "coinbase.reconnect.max_backoff_seconds"),
    ("MDB_BUS_CONSUMER_QUEUE_SIZE", "bus.consumer_queue_size"),
    ("MDB_BUS_SUSTAINED_OVERFLOW_DROPS", "bus.sustained_overflow_drops"),
    ("MDB_WS_HOST", "ws_server.host"),
    ("MDB_WS_PORT", "ws_server.port"),
    ("MDB_STATUS_HOST", "status_server.host"),
    ("MDB_STATUS_PORT", "status_server.port"),
)


REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_CONFIG_PATH = REPO_ROOT / "config" / "default.yaml"


class ConfigError(RuntimeError):
    """Raised when config can't be loaded or validated."""


def load_config(
    yaml_path: Path | None = None,
    *,
    env: Mapping[str, str] | None = None,
) -> Config:
    """Load YAML, apply env-var overrides, validate against :class:`Config`.

    ``yaml_path`` defaults to ``config/default.yaml`` at the repo root. The
    file is required — its absence is a hard error because it ships in the
    repo. Tests can pass any path.

    ``env`` lets tests inject a fake environment; production callers use
    ``os.environ`` (the default).
    """
    path = yaml_path if yaml_path is not None else DEFAULT_CONFIG_PATH
    if not path.exists():
        raise ConfigError(f"config file not found: {path}")
    try:
        with path.open() as f:
            raw = yaml.safe_load(f)
    except yaml.YAMLError as exc:
        raise ConfigError(f"malformed yaml in {path}: {exc}") from exc

    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise ConfigError(
            f"yaml root in {path} must be a mapping, got {type(raw).__name__}"
        )

    env_dict: Mapping[str, str] = env if env is not None else os.environ
    for var, dotted in _ENV_OVERRIDES:
        if var in env_dict:
            _set_nested(raw, dotted, env_dict[var])

    try:
        return Config.model_validate(raw)
    except ValidationError as exc:
        raise ConfigError(f"invalid config: {exc}") from exc


def _set_nested(d: dict[str, Any], dotted: str, value: str) -> None:
    """Set ``d[a][b][c] = value`` for dotted path ``a.b.c``, creating
    intermediate dicts. Raises if an intermediate exists but isn't a mapping
    (means the YAML and the override path disagree on shape)."""
    parts = dotted.split(".")
    cur: Any = d
    for part in parts[:-1]:
        nxt = cur.get(part)
        if nxt is None:
            nxt = {}
            cur[part] = nxt
        elif not isinstance(nxt, dict):
            raise ConfigError(
                f"cannot apply override {dotted!r}: "
                f"intermediate {part!r} is {type(nxt).__name__}, not a mapping"
            )
        cur = nxt
    cur[parts[-1]] = value
