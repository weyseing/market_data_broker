"""Config loader tests.

Pinned behaviour:
    - Env vars override YAML, YAML overrides model defaults.
    - Type coercion (string env → int / float) happens at validation.
    - Missing yaml file → hard ConfigError.
    - Malformed yaml / wrong root type / extra keys → ConfigError.
    - The shipped ``config/default.yaml`` validates against the model.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from market_data_broker.config import (
    DEFAULT_CONFIG_PATH,
    Config,
    ConfigError,
    load_config,
)


def _write_yaml(tmp_path: Path, content: str) -> Path:
    p = tmp_path / "config.yaml"
    p.write_text(content)
    return p


# ---------------------------------------------------------------------------
# Happy paths
# ---------------------------------------------------------------------------


def test_loads_minimal_yaml(tmp_path: Path) -> None:
    p = _write_yaml(tmp_path, "log_level: DEBUG\n")
    cfg = load_config(p, env={})
    assert cfg.log_level == "DEBUG"
    # Defaults still applied for unspecified sections.
    assert cfg.ws_server.port == 8765
    assert cfg.coinbase.ws_url.startswith("wss://")


def test_empty_yaml_uses_all_defaults(tmp_path: Path) -> None:
    p = _write_yaml(tmp_path, "")
    cfg = load_config(p, env={})
    assert cfg == Config()


def test_yaml_only_no_env(tmp_path: Path) -> None:
    p = _write_yaml(
        tmp_path,
        """
log_level: WARNING
ws_server:
  port: 9999
""",
    )
    cfg = load_config(p, env={})
    assert cfg.log_level == "WARNING"
    assert cfg.ws_server.port == 9999


def test_repo_default_yaml_validates() -> None:
    """The shipped ``config/default.yaml`` must validate against the model
    (with no env overrides). Catches drift between yaml and model."""
    cfg = load_config(DEFAULT_CONFIG_PATH, env={})
    assert cfg.log_level
    assert cfg.coinbase.ws_url.startswith("wss://")
    assert cfg.ws_server.port > 0
    assert cfg.status_server.port > 0
    assert cfg.bus.consumer_queue_size > 0


# ---------------------------------------------------------------------------
# Env override precedence + type coercion
# ---------------------------------------------------------------------------


def test_env_var_overrides_yaml(tmp_path: Path) -> None:
    p = _write_yaml(tmp_path, "ws_server:\n  port: 8765\n")
    cfg = load_config(p, env={"MDB_WS_PORT": "9000"})
    assert cfg.ws_server.port == 9000


def test_env_takes_precedence_over_yaml_for_log_level(tmp_path: Path) -> None:
    p = _write_yaml(tmp_path, "log_level: ERROR\n")
    cfg = load_config(p, env={"LOG_LEVEL": "DEBUG"})
    assert cfg.log_level == "DEBUG"


def test_env_var_int_coercion(tmp_path: Path) -> None:
    p = _write_yaml(tmp_path, "")
    cfg = load_config(p, env={"MDB_BUS_CONSUMER_QUEUE_SIZE": "500"})
    assert cfg.bus.consumer_queue_size == 500
    assert isinstance(cfg.bus.consumer_queue_size, int)


def test_env_var_float_coercion(tmp_path: Path) -> None:
    p = _write_yaml(tmp_path, "")
    cfg = load_config(p, env={"MDB_COINBASE_HEARTBEAT_TIMEOUT_SECONDS": "30.5"})
    assert cfg.coinbase.heartbeat_timeout_seconds == 30.5
    assert isinstance(cfg.coinbase.heartbeat_timeout_seconds, float)


def test_nested_env_override_into_reconnect_subsection(tmp_path: Path) -> None:
    p = _write_yaml(tmp_path, "")
    cfg = load_config(p, env={"MDB_COINBASE_INITIAL_BACKOFF_SECONDS": "0.5"})
    assert cfg.coinbase.reconnect.initial_backoff_seconds == 0.5


def test_multiple_env_overrides_compose(tmp_path: Path) -> None:
    p = _write_yaml(tmp_path, "")
    cfg = load_config(
        p,
        env={
            "LOG_LEVEL": "DEBUG",
            "MDB_WS_PORT": "9001",
            "MDB_STATUS_PORT": "9002",
            "MDB_COINBASE_MAX_BACKOFF_SECONDS": "60",
        },
    )
    assert cfg.log_level == "DEBUG"
    assert cfg.ws_server.port == 9001
    assert cfg.status_server.port == 9002
    assert cfg.coinbase.reconnect.max_backoff_seconds == 60.0


def test_unrelated_env_vars_ignored(tmp_path: Path) -> None:
    """Env vars not in _ENV_OVERRIDES must not influence config."""
    p = _write_yaml(tmp_path, "")
    cfg = load_config(p, env={"PATH": "/usr/local/bin", "FOO": "bar"})
    assert cfg == Config()


# ---------------------------------------------------------------------------
# Failure modes
# ---------------------------------------------------------------------------


def test_missing_yaml_raises(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="not found"):
        load_config(tmp_path / "nope.yaml", env={})


def test_malformed_yaml_raises(tmp_path: Path) -> None:
    p = _write_yaml(tmp_path, "key: value\n  bad_indent: oops\nkey2: [unterminated")
    with pytest.raises(ConfigError, match="yaml"):
        load_config(p, env={})


def test_yaml_root_must_be_mapping(tmp_path: Path) -> None:
    p = _write_yaml(tmp_path, "- a list at root\n- is not allowed\n")
    with pytest.raises(ConfigError, match="mapping"):
        load_config(p, env={})


def test_invalid_field_value_raises(tmp_path: Path) -> None:
    p = _write_yaml(tmp_path, "ws_server:\n  port: not_a_number\n")
    with pytest.raises(ConfigError, match="invalid config"):
        load_config(p, env={})


def test_extra_yaml_key_rejected(tmp_path: Path) -> None:
    """Strict model rejects unknown fields — typos surface as errors."""
    p = _write_yaml(tmp_path, "ws_server:\n  port: 8765\n  hsot: typo\n")
    with pytest.raises(ConfigError, match="invalid config"):
        load_config(p, env={})


def test_env_override_into_non_mapping_raises(tmp_path: Path) -> None:
    """If yaml puts a scalar where the override expects a mapping, fail loudly
    instead of silently corrupting state."""
    p = _write_yaml(tmp_path, "coinbase: scalar_not_mapping\n")
    with pytest.raises(ConfigError, match="not a mapping"):
        load_config(p, env={"MDB_COINBASE_WS_URL": "wss://other"})
