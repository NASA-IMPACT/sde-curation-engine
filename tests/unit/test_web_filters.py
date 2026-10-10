"""Jinja filters registered on the web app."""

from datetime import UTC, datetime, timedelta

from sde_curation.web.app import since


def test_since_formats_compact_durations():
    now = datetime.now(UTC)
    assert since(now - timedelta(seconds=45)) in ("45s", "46s")
    assert since(now - timedelta(minutes=12)) == "12m"
    assert since(now - timedelta(hours=3, minutes=10)) == "3h 10m"
    assert since(now - timedelta(days=2, hours=4)) == "2d 4h"
    assert since(now + timedelta(hours=1)) == "0s", "clock skew never shows a negative wait"
    assert since((now - timedelta(minutes=5)).replace(tzinfo=None)) == "5m", "naive timestamps are UTC"
