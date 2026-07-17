"""CDC adapter registry: how each system under test is launched and stopped.

Each adapter is one or more long-running processes (native or `docker run
--network host`) that read the source database's WAL and deliver to the
harness receiver. Readiness is uniform: the orchestrator polls
pg_replication_slots until every declared slot exists.
"""

from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

REPO_ROOT = Path(__file__).resolve().parent.parent

# Pin an explicit .Final tag. KNOWN ISSUE: the http sink sends one event
# per POST on 3.1.x (batch mode doesn't exist there), so per-request
# consumer handling latency caps a consumer at 1/latency events/s — smoke
# this system with --profiles Nxfast. 3.2.x has batch.enabled but in a
# quick trial delivered a near-empty stream (~31 events) — needs
# investigation before bumping the pin.
DEBEZIUM_IMAGE = os.environ.get("DEBEZIUM_IMAGE", "quay.io/debezium/server:3.1.3.Final")


@dataclass
class LaunchCtx:
    db_url: str
    db_host: str
    db_port: int
    db_user: str
    db_password: str
    db_name: str
    receiver_base: str
    consumer_count: int
    logs_dir: Path
    env: dict


@dataclass
class ManagedProc:
    """A launched adapter process. Docker containers get a stop hook so
    SIGTERM actually reaches PID 1 inside the container."""

    name: str
    proc: subprocess.Popen
    container: str | None = None

    def stop(self, grace_s: float = 10.0) -> None:
        if self.container is not None:
            subprocess.run(
                ["docker", "stop", "-t", str(int(grace_s)), self.container],
                capture_output=True,
            )
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=grace_s)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=5)


@dataclass
class CdcAdapter:
    system: str
    envelope: str  # receiver decoder: canonical | debezium | sequin
    topology: str
    db_name: str
    slot_prefix: str
    launch: Callable[["CdcAdapter", LaunchCtx], list[ManagedProc]]
    prepare: Callable[["CdcAdapter"], object] = field(default=lambda self: None)
    version: Callable[["CdcAdapter"], str] = field(default=lambda self: "in-repo")
    # Databases the SUT needs on the shared instance beyond db_name
    # (e.g. Sequin's config store). Created empty at preflight.
    extra_databases: tuple[str, ...] = ()
    # Buffer topologies (one shared slot) override the per-consumer default.
    slots: Callable[[int], list[str]] | None = None
    # Slots preflight should create with pgoutput before the SUT starts
    # (e.g. Sequin cancels its own slot-create call on a short timeout).
    precreate_slots: tuple[str, ...] = ()

    def slot_names(self, consumer_count: int) -> list[str]:
        if self.slots is not None:
            return self.slots(consumer_count)
        return [f"{self.slot_prefix}{i}" for i in range(consumer_count)]


def _docker_pull(image: str) -> None:
    have = subprocess.run(
        ["docker", "image", "inspect", image], capture_output=True
    )
    if have.returncode != 0:
        print(f"[cdc] docker pull {image}…", flush=True)
        subprocess.run(["docker", "pull", image], check=True)


# ── pgoutput-raw: in-repo SQL-polling baseline, N slots in one process ──


def _launch_pgoutput_raw(adapter: CdcAdapter, ctx: LaunchCtx) -> list[ManagedProc]:
    proc = subprocess.Popen(
        [sys.executable, str(REPO_ROOT / "pgoutput-raw-bench" / "main.py")],
        env={**ctx.env,
             "DATABASE_URL": ctx.db_url,
             "SINK_URL": ctx.receiver_base,
             "CONSUMER_COUNT": str(ctx.consumer_count)},
        stdout=subprocess.PIPE,
        stderr=(ctx.logs_dir / "pgoutput-raw.stderr.log").open("w"),
        text=True, cwd=REPO_ROOT,
    )
    return [ManagedProc(name="pgoutput-raw", proc=proc)]


# ── debezium-server: one container (JVM) per consumer, slot per consumer ──
#
# The no-broker Debezium arm: Debezium Server has exactly one sink, so
# fan-out means one server instance per consumer — honest slot-per-consumer
# topology with the same capture engine as the Kafka deployment.


def _debezium_env(adapter: CdcAdapter, ctx: LaunchCtx, cid: int) -> dict[str, str]:
    return {
        "DEBEZIUM_SINK_TYPE": "http",
        "DEBEZIUM_SINK_HTTP_URL": f"{ctx.receiver_base}/sink/{cid}",
        "DEBEZIUM_SINK_HTTP_TIMEOUT_MS": "30000",
        "DEBEZIUM_SINK_HTTP_RETRIES": "2147483647",  # retry forever: chaos phases must not kill the pipeline
        "DEBEZIUM_SINK_HTTP_RETRY_INTERVAL_MS": "500",
        "DEBEZIUM_SOURCE_CONNECTOR_CLASS": "io.debezium.connector.postgresql.PostgresConnector",
        "DEBEZIUM_SOURCE_OFFSET_STORAGE": "org.apache.kafka.connect.storage.FileOffsetBackingStore",
        "DEBEZIUM_SOURCE_OFFSET_STORAGE_FILE_FILENAME": "/debezium/data/offsets.dat",
        "DEBEZIUM_SOURCE_OFFSET_FLUSH_INTERVAL_MS": "1000",
        "DEBEZIUM_SOURCE_DATABASE_HOSTNAME": ctx.db_host,
        "DEBEZIUM_SOURCE_DATABASE_PORT": str(ctx.db_port),
        "DEBEZIUM_SOURCE_DATABASE_USER": ctx.db_user,
        "DEBEZIUM_SOURCE_DATABASE_PASSWORD": ctx.db_password,
        "DEBEZIUM_SOURCE_DATABASE_DBNAME": ctx.db_name,
        "DEBEZIUM_SOURCE_TOPIC_PREFIX": f"cdcbench{cid}",
        "DEBEZIUM_SOURCE_PLUGIN_NAME": "pgoutput",
        "DEBEZIUM_SOURCE_SLOT_NAME": f"{adapter.slot_prefix}{cid}",
        "DEBEZIUM_SOURCE_PUBLICATION_NAME": "cdc_pub",
        "DEBEZIUM_SOURCE_PUBLICATION_AUTOCREATE_MODE": "disabled",
        "DEBEZIUM_SOURCE_TABLE_INCLUDE_LIST": "cdc_bench.events",
        "DEBEZIUM_SOURCE_SNAPSHOT_MODE": "never",
        "DEBEZIUM_SOURCE_TOMBSTONES_ON_DELETE": "false",
        "DEBEZIUM_FORMAT_VALUE": "json",
        "DEBEZIUM_FORMAT_VALUE_SCHEMAS_ENABLE": "false",
        "DEBEZIUM_FORMAT_KEY": "json",
        "DEBEZIUM_FORMAT_KEY_SCHEMAS_ENABLE": "false",
        "QUARKUS_LOG_LEVEL": "WARN",
        # Host networking: each instance needs a distinct Quarkus HTTP
        # (health) port or instances 1..N-1 crash on bind.
        "QUARKUS_HTTP_PORT": str(8090 + cid),
        "JAVA_OPTS": "-Xms128m -Xmx512m",
    }


def _launch_debezium_server(adapter: CdcAdapter, ctx: LaunchCtx) -> list[ManagedProc]:
    procs: list[ManagedProc] = []
    for cid in range(ctx.consumer_count):
        container = f"cdcbench-debezium-{cid}"
        subprocess.run(["docker", "rm", "-f", container], capture_output=True)
        argv = ["docker", "run", "--rm", "--network", "host",
                "--name", container]
        for key, value in _debezium_env(adapter, ctx, cid).items():
            argv += ["-e", f"{key}={value}"]
        argv.append(DEBEZIUM_IMAGE)
        proc = subprocess.Popen(
            argv,
            stdout=(ctx.logs_dir / f"debezium-{cid}.stdout.log").open("w"),
            stderr=(ctx.logs_dir / f"debezium-{cid}.stderr.log").open("w"),
            text=True,
        )
        procs.append(ManagedProc(name=f"debezium-{cid}", proc=proc,
                                 container=container))
    return procs


# ── sequin: single container + Redis sidecar; ONE shared slot, per-sink
# cursors — the "buffer" topology. One webhook sink per consumer. ──────────

SEQUIN_IMAGE = os.environ.get("SEQUIN_IMAGE", "sequin/sequin:latest")
REDIS_IMAGE = "redis:7-alpine"
SEQUIN_REDIS_PORT = 16379


def _sequin_yaml(ctx: LaunchCtx) -> str:
    endpoints = "\n".join(
        f'  - name: "consumer-{i}"\n    url: "{ctx.receiver_base}/sink/{i}"'
        for i in range(ctx.consumer_count)
    )
    sinks = "\n".join(
        f'  - name: "sink-{i}"\n'
        f'    database: "source"\n'
        f"    source:\n"
        f'      include_tables: ["cdc_bench.events"]\n'
        f"    destination:\n"
        f'      type: "webhook"\n'
        f'      http_endpoint: "consumer-{i}"\n'
        f"    batch_size: 100"
        for i in range(ctx.consumer_count)
    )
    return f"""account:
  name: "cdc-bench"
databases:
  - name: "source"
    hostname: "{ctx.db_host}"
    port: {ctx.db_port}
    database: "{ctx.db_name}"
    username: "{ctx.db_user}"
    password: "{ctx.db_password}"
    slot:
      name: "sequin_slot"
      create_if_not_exists: true
    publication:
      name: "cdc_pub"
      create_if_not_exists: false
http_endpoints:
{endpoints}
sinks:
{sinks}
"""


def _launch_sequin(adapter: CdcAdapter, ctx: LaunchCtx) -> list[ManagedProc]:
    import base64

    procs: list[ManagedProc] = []
    subprocess.run(["docker", "rm", "-f", "cdcbench-redis", "cdcbench-sequin"],
                   capture_output=True)
    redis = subprocess.Popen(
        ["docker", "run", "--rm", "--network", "host", "--name", "cdcbench-redis",
         REDIS_IMAGE, "redis-server", "--port", str(SEQUIN_REDIS_PORT)],
        stdout=(ctx.logs_dir / "redis.stdout.log").open("w"),
        stderr=subprocess.STDOUT, text=True,
    )
    procs.append(ManagedProc(name="redis", proc=redis, container="cdcbench-redis"))

    config_b64 = base64.b64encode(_sequin_yaml(ctx).encode()).decode()
    (ctx.logs_dir / "sequin.yaml").write_text(_sequin_yaml(ctx))  # forensics
    env = {
        "PG_HOSTNAME": ctx.db_host,
        "PG_PORT": str(ctx.db_port),
        "PG_DATABASE": "sequin_config",
        "PG_USERNAME": ctx.db_user,
        "PG_PASSWORD": ctx.db_password,
        "PG_POOL_SIZE": "10",
        "REDIS_URL": f"redis://{ctx.db_host}:{SEQUIN_REDIS_PORT}",
        # Deterministic keys: the config DB may outlive a run, and a changed
        # VAULT_KEY makes Sequin crash decrypting its own stored config.
        # This is a throwaway bench instance — nothing sensitive is stored.
        "SECRET_KEY_BASE": base64.b64encode(b"cdc-bench-secret-key-base-0000000000000000000000").decode(),
        "VAULT_KEY": base64.b64encode(b"cdc-bench-vault-key-000000000000").decode(),
        "CONFIG_FILE_YAML": config_b64,
    }
    argv = ["docker", "run", "--rm", "--network", "host",
            "--name", "cdcbench-sequin"]
    for key, value in env.items():
        argv += ["-e", f"{key}={value}"]
    argv.append(SEQUIN_IMAGE)
    sequin = subprocess.Popen(
        argv,
        stdout=(ctx.logs_dir / "sequin.stdout.log").open("w"),
        stderr=(ctx.logs_dir / "sequin.stderr.log").open("w"),
        text=True,
    )
    procs.append(ManagedProc(name="sequin", proc=sequin,
                             container="cdcbench-sequin"))
    return procs


ADAPTERS: dict[str, CdcAdapter] = {
    "pgoutput-raw": CdcAdapter(
        system="pgoutput-raw",
        envelope="canonical",
        topology="slot-per-consumer",
        db_name="cdc_raw_bench",
        slot_prefix="cdc_raw_",
        launch=_launch_pgoutput_raw,
    ),
    "debezium-server": CdcAdapter(
        system="debezium-server",
        envelope="debezium",
        topology="slot-per-consumer",
        db_name="debezium_cdc_bench",
        slot_prefix="dbz_",
        launch=_launch_debezium_server,
        prepare=lambda self: _docker_pull(DEBEZIUM_IMAGE),
        version=lambda self: DEBEZIUM_IMAGE,
    ),
    "sequin": CdcAdapter(
        system="sequin",
        envelope="sequin",
        topology="buffer",
        db_name="sequin_cdc_bench",
        slot_prefix="sequin_",
        launch=_launch_sequin,
        prepare=lambda self: [_docker_pull(i) for i in (SEQUIN_IMAGE, REDIS_IMAGE)] and None,
        version=lambda self: SEQUIN_IMAGE,
        extra_databases=("sequin_config",),
        slots=lambda _n: ["sequin_slot"],
        precreate_slots=("sequin_slot",),
    ),
}
