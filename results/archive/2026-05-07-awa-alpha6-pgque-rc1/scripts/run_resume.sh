#!/usr/bin/env bash
# Resume runner — picks up where run_all.sh left off after the
# chaos_crash_recovery step hung pgque (multi-replica + shared consumer
# semantics — see SUMMARY.md notes).
#
# Per-step `timeout` is applied so a single hung adapter cannot wedge the
# whole sweep.
set -uo pipefail

OUT_DIR="results/2026-05-07-awa-alpha6-pgque-rc1"
RAW_DIR="$OUT_DIR/raw"
RUN_INDEX="$OUT_DIR/run_index.tsv"
RUN_LOG="$OUT_DIR/run.log"

mkdir -p "$RAW_DIR"

run_step() {
  local label="$1"; local step_timeout="$2"; shift 2
  local logf="$RAW_DIR/${label}.log"
  echo "=== $(date -u +%FT%TZ) BEGIN $label (timeout=${step_timeout}) ===" | tee -a "$RUN_LOG"
  echo "    cmd: timeout ${step_timeout} uv run bench run $*" | tee -a "$RUN_LOG"
  timeout --kill-after=30s "$step_timeout" uv run bench run "$@" >"$logf" 2>&1
  local rc=$?
  local rid
  rid="$(grep -oE 'results/[a-zA-Z0-9_/.-]+' "$logf" | tail -1 | sed 's@results/@@')"
  printf '%s\t%s\t%s\n' "$label" "$rid" "$rc" >>"$RUN_INDEX"
  echo "=== $(date -u +%FT%TZ) END $label rc=$rc rid=$rid ===" | tee -a "$RUN_LOG"
  # Cleanup orphan containers from a timed-out run
  docker ps --filter "ancestor=pgque-bench" -q | xargs -r docker stop >/dev/null 2>&1 || true
  docker ps --filter "ancestor=awa-bench"   -q | xargs -r docker stop >/dev/null 2>&1 || true
  docker compose down -v >/dev/null 2>&1 || true
  return 0
}

SKIP=(--skip-build --sample-every 2)

# Note: pgque under chaos_crash_recovery (replicas=2) hung — when replica 0
# is SIGKILLed mid-batch, replica 1 keeps re-reading the orphan batch_id
# until pgque.maint() reaps it. Run awa only at replicas=2, and run pgque
# separately at replicas=1 for an apples-to-its-own-shape view.
# chaos_crash_recovery_awa_2x already completed in earlier interleaved run.
run_step "chaos_crash_recovery_pgque_1x" "8m" "${SKIP[@]}" --systems pgque --replicas 1 --producer-rate 300 --worker-count 32 --scenario chaos_crash_recovery
run_step "chaos_repeated_kills_awa_2x"  "10m" "${SKIP[@]}" --systems awa   --replicas 2 --producer-rate 300 --worker-count 32 --scenario chaos_repeated_kills
run_step "chaos_repeated_kills_pgque_1x" "10m" "${SKIP[@]}" --systems pgque --replicas 1 --producer-rate 300 --worker-count 32 --scenario chaos_repeated_kills

# ── Bloat / pressure (custom shortened phases) ────────────────────────
run_step "idle_in_tx_short" "15m" \
  "${SKIP[@]}" --systems awa,pgque --replicas 1 --producer-rate 800 --worker-count 32 \
  --phase warmup=warmup:30s --phase clean_1=clean:120s \
  --phase idle_1=idle-in-tx:240s --phase recovery_1=recovery:180s

run_step "sustained_high_load_short" "15m" \
  "${SKIP[@]}" --systems awa,pgque --replicas 1 --producer-rate 800 --worker-count 32 \
  --phase warmup=warmup:30s --phase clean_1=clean:120s \
  --phase pressure_1=high-load:240s --phase recovery_1=clean:120s

run_step "active_readers_short" "15m" \
  "${SKIP[@]}" --systems awa,pgque --replicas 1 --producer-rate 800 --worker-count 32 \
  --phase warmup=warmup:30s --phase clean_1=clean:120s \
  --phase readers_1=active-readers:240s --phase recovery_1=clean:120s

run_step "event_delivery_burst_short" "20m" \
  "${SKIP[@]}" --systems awa,pgque --replicas 1 --producer-rate 1200 --worker-count 64 \
  --phase warmup=warmup:30s --phase clean_1=clean:120s \
  --phase pressure_1=high-load:300s --phase recovery_1=clean:240s

echo "RESUME DONE $(date -u +%FT%TZ)" | tee -a "$RUN_LOG"
