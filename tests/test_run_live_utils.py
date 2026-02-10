from collections import deque
from decimal import Decimal

from src.app.run_live import (
    _env_bool,
    _positions_to_net,
    _resolve_alert_webhook_url,
    _trim_recent_errors,
)


def test_env_bool_true_false(monkeypatch):
    monkeypatch.setenv("X_BOOL", "true")
    assert _env_bool("X_BOOL", False) is True

    monkeypatch.setenv("X_BOOL", "0")
    assert _env_bool("X_BOOL", True) is False


def test_env_bool_default(monkeypatch):
    monkeypatch.delenv("X_BOOL", raising=False)
    assert _env_bool("X_BOOL", True) is True
    assert _env_bool("X_BOOL", False) is False

    monkeypatch.setenv("X_BOOL", "not-bool")
    assert _env_bool("X_BOOL", True) is True
    assert _env_bool("X_BOOL", False) is False


def test_positions_to_net_empty():
    net, avg, rows = _positions_to_net([])
    assert net == Decimal("0")
    assert avg is None
    assert rows == 0


def test_positions_to_net_mixed():
    positions = [
        {"side": "BUY", "size": 0.2, "price": 100},
        {"side": "SELL", "size": 0.1, "price": 120},
        {"side": "BUY", "size": 0, "price": 999},  # 無効行
        {"side": "X", "size": 1, "price": 1},  # 無効行
    ]
    net, avg, rows = _positions_to_net(positions)
    assert net == Decimal("0.1")
    assert avg == Decimal("80")
    assert rows == 2


def test_resolve_alert_webhook_url_precedence(monkeypatch):
    keys = [
        "LIVE_ALERT_WEBHOOK_URL",
        "ALERT_WEBHOOK_URL",
        "DISCORD_WEBHOOK_URL",
        "SLACK_WEBHOOK_URL",
        "WEBHOOK_URL",
    ]
    for key in keys:
        monkeypatch.delenv(key, raising=False)
    assert _resolve_alert_webhook_url() == ""

    monkeypatch.setenv("WEBHOOK_URL", "https://example.test/webhook")
    assert _resolve_alert_webhook_url() == "https://example.test/webhook"

    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://example.test/slack")
    assert _resolve_alert_webhook_url() == "https://example.test/slack"

    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "https://example.test/discord")
    assert _resolve_alert_webhook_url() == "https://example.test/discord"

    monkeypatch.setenv("ALERT_WEBHOOK_URL", "https://example.test/alert")
    assert _resolve_alert_webhook_url() == "https://example.test/alert"

    monkeypatch.setenv("LIVE_ALERT_WEBHOOK_URL", "https://example.test/live")
    assert _resolve_alert_webhook_url() == "https://example.test/live"


def test_trim_recent_errors_windowed():
    samples = deque([1.0, 5.0, 7.5, 12.0])
    count = _trim_recent_errors(samples, now_mono=12.0, window_sec=5.0)
    assert count == 2
    assert list(samples) == [7.5, 12.0]


def test_trim_recent_errors_no_window():
    samples = deque([1.0, 2.0, 3.0])
    count = _trim_recent_errors(samples, now_mono=100.0, window_sec=0.0)
    assert count == 3
    assert list(samples) == [1.0, 2.0, 3.0]
