"""Exercise the real horizon-age SQL with controlled activity rows (read-only)."""
import os
import psycopg
import pytest
from bench_harness.metrics import _CLUSTER_SQL

@pytest.mark.parametrize('holder_field', ['backend_xid', 'backend_xmin'])
@pytest.mark.parametrize('epoch', [0, 1])
def test_horizon_age_includes_idle_xid_and_wraparound(holder_field, epoch):
    database_url = os.environ.get('BENCH_TEST_DATABASE_URL')
    if not database_url:
        pytest.skip('Set BENCH_TEST_DATABASE_URL for read-only PostgreSQL SQL checks')
    xid = "'500'::xid" if holder_field == 'backend_xid' else 'NULL::xid'
    xmin = "'500'::xid" if holder_field == 'backend_xmin' else 'NULL::xid'
    activity = f'''WITH pg_stat_activity AS (
        SELECT {xid} AS backend_xid, {xmin} AS backend_xmin,
               now() - interval '120 seconds' AS xact_start,
               'idle in transaction'::text AS state
        UNION ALL
        SELECT NULL::xid, '500'::xid, now() - interval '1 second', 'active'
    ), snap AS ('''
    original = 'SELECT pg_snapshot_xmin(pg_current_snapshot()) AS snapshot_xmin'
    assert original in _CLUSTER_SQL
    query = _CLUSTER_SQL.replace('WITH snap AS (', activity).replace(
        original, f"SELECT '{epoch * 2**32 + 500}'::xid8 AS snapshot_xmin")
    with psycopg.connect(database_url, autocommit=True) as connection:
        row = connection.execute(query).fetchone()
    assert row[1] == pytest.approx(120.0)
    assert row[3] == 1
