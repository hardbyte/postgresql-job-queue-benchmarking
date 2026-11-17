import contextlib

import psycopg

from bench_harness.metrics import bounded_query


class FakeCursor:
    def __init__(self, fail: bool) -> None:
        self.fail = fail
        self.executed: list[tuple[str, tuple | None]] = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        if self.fail and "set_config" not in sql:
            raise psycopg.errors.QueryCanceled("canceling statement due to statement timeout")

    def fetchone(self):
        return (42,)


class FakeConn:
    """Savepoints as psycopg runs them: an exception rolls back the block,
    and Rollback ends it quietly."""

    def __init__(self) -> None:
        self.rolled_back = 0

    @contextlib.contextmanager
    def transaction(self):
        try:
            yield
        except psycopg.Rollback:
            self.rolled_back += 1
        except Exception:
            self.rolled_back += 1
            raise


def test_bounded_query_sets_timeouts_and_rolls_back_its_savepoint():
    conn, cur = FakeConn(), FakeCursor(fail=False)
    assert bounded_query(conn, cur, "SELECT count(*) FROM t", statement_timeout_ms=2000) == (42,)
    sql, params = cur.executed[0]
    assert "statement_timeout" in sql and "lock_timeout" in sql
    assert params == ("2000", "500")
    assert conn.rolled_back == 1


def test_bounded_query_returns_none_on_timeout():
    conn, cur = FakeConn(), FakeCursor(fail=True)
    assert bounded_query(conn, cur, "SELECT count(*) FROM t", statement_timeout_ms=10) is None
    assert conn.rolled_back == 2
