#!/usr/bin/env python3
"""Paired AWA reference/saturation cells and fresh MVCC soaks.

Build/archive each executable with bench_harness.adapters.build_awa(False),
including its .build.json receipt. Select binaries explicitly: this script
never changes dependency pins or rebuilds while measuring.
"""
from __future__ import annotations
import argparse
import contextlib
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from bench_harness.adapters import pg_url
from bench_harness.orchestrator import drive, start_postgres, stop_postgres
from bench_harness.phases import parse_phase_spec
from bench_harness.pin_validation import validate_mvcc_pin
from bench_harness.versions import file_sha256, verify_awa_build
from bench_harness.writers import capture_pg_env


def check_offered_load(summary: dict, rate: int) -> None:
    """A fixed-rate gate is invalid if the producer cannot deliver its load."""
    phases = summary["systems"]["awa"]["phases"]
    for name, phase in phases.items():
        actual = phase.get("median_enqueue_rate_per_s")
        if actual is None or actual < rate * 0.95:
            raise RuntimeError(f"{name}: requested {rate}/s but measured {actual}/s; workload underdriven")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--protocol-bin", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--pg-image", default="postgres:18.3-alpine")
    parser.add_argument("--overnight", action="store_true", help="Run matched four-hour baseline/candidate MVCC soaks after the reference and saturation matrix")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    binaries = {"baseline":args.baseline.resolve(), "candidate":args.candidate.resolve()}
    for name in ("postgres.conf", "docker-compose.yml", "docker-compose.override.yml"):
        if (ROOT/name).exists():
            shutil.copy2(ROOT/name,args.output/name)
    campaign = {"started_at":datetime.now(timezone.utc).isoformat(), "status":"running",
                "builds":{label:verify_awa_build(binary, match_inputs=False) for label,binary in binaries.items()},
                "protocol_executable_sha256":file_sha256(args.protocol_bin), "overnight":args.overnight, "cells":[]}
    def save():
        (args.output / "campaign.json").write_text(json.dumps(campaign, indent=2)+"\n")
    save()
    try:
        # This is a control-plane transaction probe, separate from job throughput.
        campaign["active_cell"] = "cron-protocol"
        save()
        print("Running cron protocol fleet/schedule matrix", flush=True)
        with (args.output / "protocol-driver.log").open("w") as log, contextlib.redirect_stderr(log):
            start_postgres(args.pg_image)
            try:
                campaign["protocol_postgres"] = capture_pg_env(pg_url("awa_bench"))
                container = subprocess.check_output(["docker","compose","ps","-q","postgres"],cwd=ROOT,text=True).strip()
                info = json.loads(subprocess.check_output(["docker","inspect",container],text=True))[0]
                campaign["postgres_image_id"] = info["Image"]
                campaign["postgres_limits"] = {key:info["HostConfig"][key] for key in ("NanoCpus","Memory","ShmSize")}
                save()
                with (args.output / "cron-protocol.jsonl").open("w") as output:
                    subprocess.run([str(args.protocol_bin.resolve())], env={**os.environ,
                        "DATABASE_URL":pg_url("awa_bench"), "BENCH_DISPOSABLE_DATABASE":"yes"},
                        stdout=output, stderr=log, check=True)
            finally:
                stop_postgres(args.pg_image)
        cells = []
        for name, workers, rate, mode, clean in [("ref800",32,800,"fixed",300),
                ("sat-w64",64,50000,"depth-target",180),
                ("sat-w128",128,50000,"depth-target",180),
                ("sat-w256",256,50000,"depth-target",180)]:
            order = ["baseline","candidate"] if name in {"ref800","sat-w128"} else ["candidate","baseline"]
            for label in order:
                cells.append((f"{name}-{label}",label,workers,rate,mode,["warmup=warmup:60s",f"clean=clean:{clean}s"]))
        if args.overnight:
            for label in ("baseline", "candidate"):
                cells.append((f"mvcc-soak-{label}", label, 32, 800, "fixed", [
                    "warmup=warmup:10m", "clean=clean:30m", "pinned=idle-in-tx:120m", "recovery=recovery:80m"]))
        else:
            cells.append(("mvcc-soak-candidate","candidate",32,800,"fixed",[
                "warmup=warmup:10m","clean=clean:10m","pinned=idle-in-tx:60m","recovery=recovery:30m"]))
        for name,label,workers,rate,mode,specs in cells:
            campaign["active_cell"] = name
            save()
            print(f"Running {name}: {' -> '.join(specs)}", flush=True)
            os.environ["AWA_BENCH_EXECUTABLE"] = str(binaries[label])
            cli = ["env",f"AWA_BENCH_EXECUTABLE={binaries[label]}","bench","run","--systems","awa",
                "--skip-build","--pg-image",args.pg_image,"--worker-count",str(workers),
                "--producer-rate",str(rate),"--producer-mode",mode,"--target-depth","4000"]
            for spec in specs: cli.extend(["--phase",spec])
            with (args.output / f"{name}.log").open("w") as log, contextlib.redirect_stderr(log):
                result = drive(systems=["awa"],scenario=None,phases=[parse_phase_spec(s) for s in specs],
                    pg_image=args.pg_image,fast=False,skip_build=True,sample_every_s=5,
                    producer_rate=rate,producer_mode=mode,target_depth=4000,worker_count=workers,
                    high_load_multiplier=1.5,awa_completion_batch_size=None,replicas=1,cli_args=cli)
            measured = json.loads((result / "manifest.json").read_text())
            assert measured["adapters"]["awa"]["revision"]["runtime_storage"]["ring_authority"] == "ledger", "Release gate requires ledger authority"
            shutil.move(str(result),str(args.output/name))
            if mode == "fixed":
                check_offered_load(json.loads((args.output/name/"summary.json").read_text()), rate)
            if name.startswith("mvcc-soak-"):
                validate_mvcc_pin(args.output/name)
            campaign["cells"].append({"name":name,"path":name,"status":"complete"})
            save()
        campaign["status"]="complete"
        campaign.pop("active_cell",None)
    except BaseException as error:
        campaign["status"]="failed"
        campaign["error"]=repr(error)
        raise
    finally:
        campaign["updated_at"]=datetime.now(timezone.utc).isoformat()
        save()


if __name__ == "__main__":
    main()
