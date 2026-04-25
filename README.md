# Market Data Broker

Real-time crypto market data hub. Ingests from Coinbase, distributes via pub/sub, exposes itself via an MCP server for LLM agents.

Status: scaffolding (Step 1 of [progress/20260425_implementation_plan.txt](progress/20260425_implementation_plan.txt)).

## Quickstart

Requires Python 3.11+.

```sh
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

python -m market_data_broker
```

Send SIGINT (Ctrl-C) to stop. Logs are JSON on stdout.

## Layout

```
src/market_data_broker/   package source
tests/                    pytest suite
config/default.yaml       runtime config
docs/                     context docs for LLM agents
progress/                 implementation log
```

## Conventions

See [CLAUDE.md](CLAUDE.md) for architecture and working conventions.
