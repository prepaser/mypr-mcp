from mypr_mcp.timings import Timings


def test_timings_keep_bounded_samples_and_lifetime_counts():
    timings = Timings(limit=3, labels={"known"})
    for value in (0.001, 0.002, 0.003, 0.004):
        timings.observe("known", value)
    timings.observe("untrusted-label", 0.005)
    timings.observe("known", float("nan"))
    timings.observe("known", True)

    snapshot = timings.snapshot()
    assert snapshot["known"] == {
        "sample_count": 3,
        "total_count": 4,
        "p50": 3.0,
        "p95": 4.0,
        "max": 4.0,
        "unit": "ms",
    }
    assert snapshot["other"]["sample_count"] == 1
    assert "untrusted-label" not in snapshot


def test_timings_bound_dynamic_label_count():
    timings = Timings()
    for index in range(100):
        timings.observe(f"op-{index}", 0.001)

    snapshot = timings.snapshot()
    assert len(snapshot) <= timings.max_labels
    assert snapshot["other"]["total_count"] > 0
