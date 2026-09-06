import hashlib,json,time
from pathlib import Path
import psycopg
from bench_harness.adapters import pg_url
from bench_harness.metrics import _CLUSTER_SQL
output=Path('/tmp/awa-pin-verification.jsonl')
while True:
    with psycopg.connect(pg_url('awa_bench'),autocommit=True) as c:
        now,horizon=c.execute('SELECT clock_timestamp(),pg_snapshot_xmin(pg_current_snapshot())::text').fetchone()
        holder=c.execute("SELECT pid,backend_xid::text,backend_xmin::text,EXTRACT(epoch FROM clock_timestamp()-xact_start)::float8 FROM pg_stat_activity WHERE pid=4545 AND backend_xid='219063'::xid").fetchone()
        corrected=c.execute(_CLUSTER_SQL).fetchone()
    record={'observed_at':now.isoformat(),'snapshot_xmin':horizon,'holder':holder,'corrected_xmin_age_s':corrected[1],'metric_sql_sha256':hashlib.sha256(_CLUSTER_SQL.encode()).hexdigest()}
    with output.open('a') as f:f.write(json.dumps(record)+'\n')
    print(json.dumps(record),flush=True)
    if holder is None:break
    time.sleep(60)
