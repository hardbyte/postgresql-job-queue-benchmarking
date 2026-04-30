FROM python:3.12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*
RUN curl -LsSf https://astral.sh/uv/install.sh | sh

ENV PATH="/root/.local/bin:${PATH}"

WORKDIR /app
# Build context is REPO_ROOT (see bench_harness/adapters.py). Copy adapter +
# vendored pgque submodule (sql/pgque.sql is loaded at startup).
COPY pgque-bench ./pgque-bench

WORKDIR /app/pgque-bench
RUN uv sync

ENTRYPOINT ["sh", "-lc", "cd /app/pgque-bench && exec uv run python main.py"]
