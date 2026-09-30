ARG PG_IMAGE=postgres:16.15-bookworm

FROM rust:1.98.0-bookworm AS rust

FROM ${PG_IMAGE} AS builder
COPY --from=rust /usr/local/cargo /usr/local/cargo
COPY --from=rust /usr/local/rustup /usr/local/rustup
ENV CARGO_HOME=/usr/local/cargo \
    RUSTUP_HOME=/usr/local/rustup \
    PATH=/usr/local/cargo/bin:$PATH

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential ca-certificates clang libclang-dev pkg-config libssl-dev \
        "postgresql-server-dev-${PG_MAJOR}=${PG_VERSION}" \
    && rm -rf /var/lib/apt/lists/*

RUN cargo install --locked cargo-pgrx --version 0.16.1 \
    && cargo pgrx init "--pg${PG_MAJOR}" "/usr/lib/postgresql/${PG_MAJOR}/bin/pg_config"

WORKDIR /usr/src/kafgres
COPY vendor/kafgres/codec/Cargo.toml vendor/kafgres/codec/KAFKA_VERSION ./codec/
COPY vendor/kafgres/codec/src ./codec/src
COPY vendor/kafgres/extension/Cargo.toml vendor/kafgres/extension/Cargo.lock vendor/kafgres/extension/kafgres.control ./extension/
COPY vendor/kafgres/extension/src ./extension/src
COPY vendor/kafgres/extension/sql ./extension/sql

WORKDIR /usr/src/kafgres/extension
RUN cargo pgrx package --no-default-features --features "pg${PG_MAJOR}" \
        --pg-config "/usr/lib/postgresql/${PG_MAJOR}/bin/pg_config" \
    && mkdir -p /app/dist \
    && cp -r "target/release/kafgres-pg${PG_MAJOR}/usr" /app/dist/

FROM ${PG_IMAGE}
COPY --from=builder /app/dist/usr/ /usr/

RUN for sample in /usr/share/postgresql/postgresql.conf.sample /usr/share/postgresql/*/postgresql.conf.sample; do \
        [ -f "$sample" ] || continue; \
        printf '%s\n' \
            "shared_preload_libraries = 'kafgres'" \
            "kafgres.advertised_host = '127.0.0.1'" \
            "log_min_messages = warning" \
            "wal_level = logical" \
            "output_plugin_libraries = 'pgoutput, test_decoding, kafgres'" >> "$sample"; \
    done \
    && for hba in /usr/share/postgresql/pg_hba.conf.sample /usr/share/postgresql/*/pg_hba.conf.sample; do \
        [ -f "$hba" ] || continue; \
        printf '%s\n' \
            "local   all             all                                     trust" \
            "host    all             all             all                     trust" >> "$hba"; \
    done \
    && printf '%s\n' '#!/bin/bash' \
        'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "CREATE EXTENSION IF NOT EXISTS kafgres;"' \
        > /docker-entrypoint-initdb.d/01-create-extension.sh \
    && chmod +x /docker-entrypoint-initdb.d/01-create-extension.sh

EXPOSE 5432 9092
