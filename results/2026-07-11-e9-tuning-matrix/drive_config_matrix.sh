#!/usr/bin/env bash
# E9 config-only matrix driver (E9.1 group commit + E9.4a wal lz4).
# Serial, interleaved off/on/off to absorb drift. Ref pre-built at e389168.
# Each cell: ~4-5 min. This driver runs unattended; progress in the per-cell
# logs and the master log below.
set -u
E9="/home/brian/dev/postgresql-job-queue-benchmarking/results/2026-07-11-e9-tuning-matrix"
OV="$E9/overrides"
RUN="$E9/run_cell.sh"
MASTER="$E9/logs/drive_config_matrix.log"

BASE="$OV/baseline.yml"
GC500="$OV/e91_groupcommit_500.yml"
GC2000="$OV/e91_groupcommit_2000.yml"
LZ4="$OV/e94a_wal_lz4.yml"

# gate cell: 800/s W=32 fixed; 5k cell: 5000/s W=128 fixed.
GATE_PH="--phase warmup=warmup:30s --phase clean=clean:180s"
FIVEK_PH="--phase warmup=warmup:30s --phase clean=clean:180s"

mlog() { echo "[$(date -u +%H:%M:%S)] $*" | tee -a "$MASTER"; }

cell() { # label override rate workers mode phases
  mlog ">>> $1"
  bash "$RUN" "$1" "$2" "$3" "$4" "$5" "$6" && mlog "<<< $1 OK" || mlog "<<< $1 FAIL rc=$?"
}

mlog "======== E9 CONFIG MATRIX START ========"

# --- E9.1 group commit @ gate (800/W32): off,500,off,2000,off ---
cell gc_gate_off1  "$BASE"   800 32 fixed "$GATE_PH"
cell gc_gate_500   "$GC500"  800 32 fixed "$GATE_PH"
cell gc_gate_off2  "$BASE"   800 32 fixed "$GATE_PH"
cell gc_gate_2000  "$GC2000" 800 32 fixed "$GATE_PH"
cell gc_gate_off3  "$BASE"   800 32 fixed "$GATE_PH"

# --- E9.1 group commit @ 5k (5000/W128): off,500,off,2000,off ---
cell gc_5k_off1    "$BASE"   5000 128 fixed "$FIVEK_PH"
cell gc_5k_500     "$GC500"  5000 128 fixed "$FIVEK_PH"
cell gc_5k_off2    "$BASE"   5000 128 fixed "$FIVEK_PH"
cell gc_5k_2000    "$GC2000" 5000 128 fixed "$FIVEK_PH"
cell gc_5k_off3    "$BASE"   5000 128 fixed "$FIVEK_PH"

# --- E9.4a wal lz4 @ gate: off,on,off ---
cell lz4_gate_off1 "$BASE" 800 32 fixed "$GATE_PH"
cell lz4_gate_on   "$LZ4"  800 32 fixed "$GATE_PH"
cell lz4_gate_off2 "$BASE" 800 32 fixed "$GATE_PH"

# --- E9.4a wal lz4 @ 5k: off,on,off ---
cell lz4_5k_off1   "$BASE" 5000 128 fixed "$FIVEK_PH"
cell lz4_5k_on     "$LZ4"  5000 128 fixed "$FIVEK_PH"
cell lz4_5k_off2   "$BASE" 5000 128 fixed "$FIVEK_PH"

mlog "======== E9 CONFIG MATRIX DONE ========"
