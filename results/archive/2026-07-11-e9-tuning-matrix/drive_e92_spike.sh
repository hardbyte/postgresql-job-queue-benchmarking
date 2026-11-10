#!/usr/bin/env bash
# E9.2 statistics-bundle spike A/B driver.
#
# The "off" arm is the e389168 baseline (already measured as gc_*_off /
# lz4_*_off cells). The "on" arm is the spike/e9-statistics build. This driver
# switches the PRIMARY awa checkout to the spike branch, rebuilds awa-bench
# against it, then runs interleaved on-cells at both standard cells. It leaves
# the primary checkout on the spike branch; the outer runbook restores it.
#
# Requires: no other owner of /home/brian/dev/awa, box otherwise idle.
set -u
BENCH="/home/brian/dev/postgresql-job-queue-benchmarking"
E9="$BENCH/results/2026-07-11-e9-tuning-matrix"
OV="$E9/overrides"
RUN="$E9/run_cell.sh"
MASTER="$E9/logs/drive_e92_spike.log"
AWA="/home/brian/dev/awa"
mlog() { echo "[$(date -u +%H:%M:%S)] $*" | tee -a "$MASTER"; }
cell() { mlog ">>> $1"; bash "$RUN" "$1" "$2" "$3" "$4" "$5" "$6" && mlog "<<< $1 OK" || mlog "<<< $1 FAIL rc=$?"; }

GATE_PH="--phase warmup=warmup:30s --phase clean=clean:180s"
FIVEK_PH="--phase warmup=warmup:30s --phase clean=clean:180s"

mlog "======== E9.2 SPIKE START ========"
# The spike branch is checked out in a worktree, so detach the primary to the
# spike commit (same tree) rather than checking out the branch name.
SPIKE_SHA=$(git -C "$AWA" rev-parse spike/e9-statistics)
mlog "detaching primary checkout $AWA -> $SPIKE_SHA (spike/e9-statistics tree)"
git -C "$AWA" checkout --detach "$SPIKE_SHA" >>"$MASTER" 2>&1 || { mlog "FATAL checkout"; exit 3; }
git -C "$AWA" log --oneline -1 | tee -a "$MASTER"

mlog "pre-building awa-bench against spike (heavy, once)"
( cd "$BENCH/awa-bench" && SQLX_OFFLINE=true cargo build --release ) >>"$MASTER" 2>&1
RC=$?
if [[ $RC -ne 0 ]]; then mlog "FATAL build rc=$RC"; exit 4; fi
mlog "build OK"

# Interleaved on-cells. The off baseline is the existing gc_*_off / lz4_*_off
# cells (same ref e389168, baseline.yml override). Two on-reps per cell to
# gauge within-arm variance against the knee.
cell stats_gate_on1 "$OV/baseline.yml" 800  32  fixed "$GATE_PH"
cell stats_5k_on1   "$OV/baseline.yml" 5000 128 fixed "$FIVEK_PH"
cell stats_gate_on2 "$OV/baseline.yml" 800  32  fixed "$GATE_PH"
cell stats_5k_on2   "$OV/baseline.yml" 5000 128 fixed "$FIVEK_PH"

mlog "======== E9.2 SPIKE DONE ========"
