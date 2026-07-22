from cdc_harness.orchestrator import verify_consumer


def source(*, txes: int = 0, events_per_tx: int | None = None) -> dict:
    return {
        "totals": {"txes": txes, "events_per_tx": events_per_tx},
        "tables": {
            "cdc_bench.accounts": {
                "1": [3, 100, False],
                "2": [2, None, True],
            }
        },
    }


def receiver(**overrides) -> dict:
    value = {
        "profile": "fast",
        "delivered": 5,
        "dups": 0,
        "order_violations": 0,
        "open_txs": 0,
        "txs_completed": 0,
        "tables": {
            "cdc_bench.accounts": {
                "1": {"seq": 3, "balance": 100, "deleted": False},
                "2": {"seq": 0, "balance": None, "deleted": True},
            }
        },
    }
    value.update(overrides)
    return value


def test_exact_final_state_passes() -> None:
    result = verify_consumer(source(), receiver())
    assert result["pass"]
    assert result["final_state_converged"]


def test_sequence_deficit_fails() -> None:
    got = receiver()
    got["tables"]["cdc_bench.accounts"]["1"]["seq"] = 2
    result = verify_consumer(source(), got)
    assert not result["pass"]
    assert result["sequence_deficit_at_drain"] == 1


def test_sequence_ahead_fails_without_negative_deficit() -> None:
    got = receiver()
    got["tables"]["cdc_bench.accounts"]["1"]["seq"] = 4
    result = verify_consumer(source(), got)
    assert not result["pass"]
    assert result["final_state_mismatched_live_keys"] == 1
    assert result["sequence_deficit_at_drain"] == 0


def test_balance_and_unexpected_key_fail() -> None:
    got = receiver()
    got["tables"]["cdc_bench.accounts"]["1"]["balance"] = 99
    got["tables"]["cdc_bench.accounts"]["3"] = {
        "seq": 1,
        "balance": 100,
        "deleted": False,
    }
    result = verify_consumer(source(), got)
    assert not result["pass"]
    assert result["balance_mismatches"] == 1
    assert result["unexpected_keys"] == 1


def test_missing_delete_tombstone_fails() -> None:
    got = receiver()
    del got["tables"]["cdc_bench.accounts"]["2"]
    result = verify_consumer(source(), got)
    assert not result["pass"]
    assert result["delete_tombstone_mismatches"] == 1


def test_incomplete_or_missing_transaction_groups_fail() -> None:
    result = verify_consumer(
        source(txes=2, events_per_tx=3),
        receiver(
            open_txs=1,
            complete_tx_groups=1,
            completed_tx_id_min=1,
            completed_tx_id_max=1,
        ),
    )
    assert not result["pass"]
    assert result["incomplete_tx_groups_at_drain"] == 1
    assert result["missing_complete_tx_groups"] == 1


def test_unexpected_transaction_id_fails_exact_range_check() -> None:
    result = verify_consumer(
        source(txes=2, events_per_tx=3),
        receiver(
            complete_tx_groups=2,
            completed_tx_id_min=1,
            completed_tx_id_max=3,
        ),
    )
    assert not result["pass"]
    assert not result["completed_tx_id_range_matches"]
