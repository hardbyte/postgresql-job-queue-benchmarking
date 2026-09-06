FROM rust:bookworm AS builder
WORKDIR /app

# Build context is the bench repo root; awa crates come from the pinned Git revision
# (pinned in Cargo.toml), so no awa source tree is needed.
COPY awa-bench/Cargo.toml awa-bench/Cargo.lock ./
COPY awa-bench/src ./src

ENV SQLX_OFFLINE=true
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/awa-bench /usr/local/bin/awa-bench
ENTRYPOINT ["awa-bench"]
