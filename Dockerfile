# syntax=docker/dockerfile:1
#
# Multi-stage build for the market-data-broker hub.
#
# Stage 1 (builder): install the package + runtime deps into an isolated
# prefix. Keeps the final image free of the build toolchain.
#
# Stage 2 (runtime): copy just the installed packages + config, run as a
# non-root user, and expose the WS + HTTP ports.

# ---- Stage 1: builder -----------------------------------------------------
FROM python:3.12-slim AS builder

WORKDIR /build

# Copy the metadata + source needed to install. pyproject.toml first so the
# layer cache survives source-only edits.
COPY pyproject.toml ./
COPY src/ ./src/

# Install into /install — copy this whole tree to the runtime stage.
RUN pip install --no-cache-dir --prefix=/install .

# ---- Stage 2: runtime -----------------------------------------------------
FROM python:3.12-slim AS runtime

# Non-root user. Hub doesn't need any host-level privileges; running as a
# numbered user makes it easy to map volumes safely later.
RUN useradd --create-home --uid 1000 mdb

WORKDIR /app

COPY --from=builder /install /usr/local
COPY config/ /app/config/

# Tell the config loader where to find the yaml inside the image. The
# package is installed under site-packages, so the repo-root-relative
# default path doesn't apply here.
ENV MDB_CONFIG_PATH=/app/config/default.yaml \
    PYTHONUNBUFFERED=1

USER mdb

# 8765 = downstream WebSocket; 8080 = HTTP /status + /healthz.
EXPOSE 8765 8080

# Liveness probe — uses /healthz, which always returns 200 if the process is
# alive. /status is the right thing for ingest health, but for container
# orchestration the simpler "is python still answering" check is enough.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://localhost:8080/healthz', timeout=3).getcode()==200 else 1)"

ENTRYPOINT ["python", "-m", "market_data_broker"]
