# Project Context: Real-Time Market Data Hub with MCP Interface

## What This Project Is

A real-time crypto market data hub that ingests from Coinbase and distributes to multiple consumers through a subscribable pub/sub interface. The hub is natively AI-interfaceable via an MCP (Model Context Protocol) server, so internal tools and LLM agents consume the same data through one unified surface.

- **Scope:** Single venue (Coinbase), but architecture must make adding new venues a simple exercise.
- **Goal:** One centralized hub owns the upstream relationship and manages distribution to consumers.
- **Key constraint:** An LLM given only the context documentation must be able to use the system correctly on first attempt.

Source spec: [docs/technical_assessment.md](docs/technical_assessment.md)

## Core Architecture Shape

- **Single upstream stream, many downstream connections.** The hub is an intermediary between Coinbase and internal consumers.
- **Upstream:** A small number of WebSocket connections to Coinbase, kept healthy.
- **Downstream:** Any number of consumers connect, subscribe to topics, receive updates in real time.
- **Centralised registry:** Tracks both sides and reference-counts subscriptions. The hub only maintains an active Coinbase subscription while at least one consumer wants it.

Layer separation must be clear and clean: **ingestion**, **registry**, **pub/sub**, and **MCP** layers.

## Functional Requirements

### 1. Coinbase Ingestion
- Establish and maintain WebSocket connections to Coinbase WebSocket feed.
- Implement connection lifecycle management (reconnects, health checks).
- Respect Coinbase subscription constraints and message limits.
- Document all subscribed channels clearly and concisely.

### 2. Subscribable Topics
- Consistent naming convention: e.g. `BTC-USD`, `ETH-USD`, `SOL-USD`.
- Support multiple consumers per topic.
- Define and document a backpressure strategy for slow/overloaded consumers.
- Consumers can dynamically subscribe and unsubscribe.

### 3. Connection Registry
- Track every upstream (Coinbase) and downstream (consumer) connection.
- Reference-count topic subscriptions so upstream reflects actual downstream demand.
- Tear down cleanly on disconnect — no leaked subscriptions.
- Expose a status surface: active connections, uptime per connection, message rates.

### 4. MCP Server (Important)
Wrap the hub in an MCP server. At minimum, expose tools to:
- List available topics.
- Describe a topic's schema and example payload.
- Subscribe to a topic and stream messages.
- Query current snapshot state (top of book, last trade, best bid/ask).

**MCP tool design rules (LLM-first):**
- Action-verb names.
- Strongly typed arguments.
- Short descriptions that explain **when** to use the tool, not just what it does.

### 5. AI Context Documentation (Important)
Documentation is a first-class deliverable. Context docs must let an LLM correctly drive the MCP tools immediately. Required content:
- Purpose, scope, and non-goals up front in plain language.
- Every topic's name, schema, update cadence, and a real example payload.
- Worked examples (e.g. "agent wants current mid for BTC-USD → call X with Y → expect Z").
- Failure modes: what the agent sees when a topic is stale, connection drops, or symbol is unknown.

Context docs live in [docs/](docs/).

## Non-Functional Requirements

- **Language:** Python or TypeScript (fast development/iteration).
- **Production-shaped code:** tests, structured logs, configuration files, Dockerfile, README.
- **Packaging:** Single-container or single-binary deployable.
- **No database.** In-memory state is fine — but be explicit in docs about what is lost on restart.

## Deliverables

- Source repo.
- MCP server with clear local setup instructions.
- Context documentation markdown files in `/docs`.
- One-page architecture write-up (design choices and trade-offs).
- 10-minute walkthrough of the system and reasoning.

## Evaluation Criteria (what reviewers will look for)

1. **Architecture judgement** — clean separation between ingestion / registry / pub/sub / MCP layers; scalability, maintainability, reliability; structured to support both engineers and LLM agents.
2. **Correctness under stress** — reconnections and stale states are handled.
3. **LLM usability** — how far a fresh agent gets when handed only the docs and the MCP tool list.
4. **Code quality and ergonomics** — readability, maintainability, ease of extension.

## Ground Rules

- AI assistant usage is expected and encouraged — be transparent about how it was leveraged.
- Ask clarifying questions early to resolve ambiguity.

## Working Conventions for This Repo

- Treat `/docs` as a first-class deliverable, not an afterthought. Write docs as if an LLM will be the primary reader.
- When adding a new MCP tool, also update the context docs (topic list, schema, example payload, failure modes).
- Prefer reference-counting subscriptions over ad-hoc tracking — leaked upstream subs is a primary failure mode reviewers will probe.
- Keep ingestion, registry, pub/sub, and MCP boundaries clean — adding a second venue should not require touching the MCP or pub/sub layers.
- Be explicit in the README about what in-memory state is lost on restart.
