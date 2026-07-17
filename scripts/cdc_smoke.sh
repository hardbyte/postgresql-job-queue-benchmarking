#!/usr/bin/env bash
# Fast CDC pipe check (~90s): postgres (logical WAL) → loadgen → pgoutput-raw
# baseline → receiver, one consumer-dead chaos phase, drain + ledger verify.
set -euo pipefail
cd "$(dirname "$0")/.."

cargo build --release --manifest-path cdc-receiver/Cargo.toml
docker compose -f docker-compose.yml -f docker-compose.cdc.yml up -d --wait postgres
uv run cdc --scenario smoke --skip-pg-setup "$@"
rc=$?
exit $rc
