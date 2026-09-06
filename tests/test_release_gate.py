"""Reject a throughput label when the producer did not deliver that workload."""
import pytest
from scripts.run_awa_release_gate import check_offered_load


@pytest.mark.parametrize("actual", [450, None])
def test_underdriven_reference_is_not_a_pass(actual):
    with pytest.raises(RuntimeError, match="underdriven"):
        check_offered_load({"systems": {"awa": {"phases": {
            "clean": {"median_enqueue_rate_per_s": actual}
        }}}}, 800)


def test_delivered_reference_passes():
    check_offered_load({"systems": {"awa": {"phases": {
        "clean": {"median_enqueue_rate_per_s": 798.7}
    }}}}, 800)
