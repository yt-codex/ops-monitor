from scripts.send_telegram_alerts import truncate


def test_truncate_uses_ascii_ellipsis():
    assert truncate("abcdefghij", 8) == "abcde..."


def test_truncate_handles_tiny_limits():
    assert truncate("abcdefghij", 3) == "..."
