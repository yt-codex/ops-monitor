from scripts.collect_probes import history_snapshot_key, median_duration


def test_history_snapshot_key_ignores_capture_time_only_changes():
    base = {
        "captured_at": "2026-03-12T19:52:33Z",
        "last_run_time": "2026-03-12T00:33:23Z",
        "status": "OK",
        "severity": "WARN",
        "duration_seconds": 45.0,
        "freshness_lag_seconds": 143.0,
        "schema_hash": "hash-1",
        "top_warning": "duration 45.0s exceeded baseline 1.0s",
    }
    later_capture = dict(base)
    later_capture["captured_at"] = "2026-03-12T20:52:33Z"

    assert history_snapshot_key(base) == history_snapshot_key(later_capture)


def test_history_snapshot_key_changes_when_signal_changes():
    earlier = {
        "captured_at": "2026-03-12T19:52:33Z",
        "last_run_time": "2026-03-12T00:33:23Z",
        "status": "OK",
        "severity": "OK",
        "duration_seconds": 45.0,
        "freshness_lag_seconds": 143.0,
        "schema_hash": "hash-1",
        "top_warning": "None",
    }
    later = dict(earlier)
    later["severity"] = "WARN"
    later["freshness_lag_seconds"] = 86_500.0
    later["top_warning"] = "freshness lag 1d 0h exceeds warn threshold"

    assert history_snapshot_key(earlier) != history_snapshot_key(later)


def test_median_duration_dedupes_repeated_captures_of_same_run():
    history = [
        {
            "captured_at": "2026-03-06T13:06:00Z",
            "last_run_time": "2026-03-06T12:34:24Z",
            "status": "OK",
            "duration_seconds": 1.0,
        },
        {
            "captured_at": "2026-03-06T19:25:20Z",
            "last_run_time": "2026-03-06T12:34:24Z",
            "status": "OK",
            "duration_seconds": 1.0,
        },
        {
            "captured_at": "2026-03-06T20:25:20Z",
            "last_run_time": "2026-03-06T12:34:24Z",
            "status": "OK",
            "duration_seconds": 1.0,
        },
        {
            "captured_at": "2026-03-08T17:58:57Z",
            "last_run_time": "2026-03-08T00:33:23Z",
            "status": "OK",
            "duration_seconds": 40.0,
        },
        {
            "captured_at": "2026-03-09T19:50:46Z",
            "last_run_time": "2026-03-09T00:33:38Z",
            "status": "OK",
            "duration_seconds": 43.0,
        },
        {
            "captured_at": "2026-03-10T19:51:19Z",
            "last_run_time": "2026-03-10T00:33:23Z",
            "status": "OK",
            "duration_seconds": 45.0,
        },
        {
            "captured_at": "2026-03-11T19:50:32Z",
            "last_run_time": "2026-03-11T00:33:15Z",
            "status": "OK",
            "duration_seconds": 59.0,
        },
    ]

    assert median_duration(history) == 43.0
