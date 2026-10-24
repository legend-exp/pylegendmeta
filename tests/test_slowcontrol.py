from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest
from dbetto import AttrsDict

from legendmeta import LegendSlowControlDB


class _Row:
    def asdict(self):
        return {"vmon": 1.0}


class _Result:
    def first(self):
        return (_Row(),)


class _Session:
    def __init__(self):
        self.times = []

    def execute(self, stmt):
        params = stmt.compile().params
        self.times += [v for k, v in params.items() if k.startswith("tstamp")]
        return _Result()


def _queried_times(on):
    scdb = LegendSlowControlDB()
    scdb.session = _Session()
    channel = AttrsDict(
        {"system": "geds", "voltage": {"card": {"id": 1}, "channel": 2}}
    )
    scdb.status(channel, on=on)
    return scdb.session.times


def test_status_utc_string():
    times = _queried_times("20250701T080000Z")
    assert times
    assert all(t == datetime(2025, 7, 1, 8, tzinfo=timezone.utc) for t in times)
    assert all(t.tzinfo is not None for t in times)


@pytest.mark.parametrize(
    "on",
    [
        datetime(2025, 7, 1, 8, tzinfo=timezone.utc),
        datetime(2025, 7, 1, 10, tzinfo=ZoneInfo("Europe/Rome")),
        datetime(2025, 7, 1, 10),  # noqa: DTZ001
    ],
)
def test_status_datetime_unchanged(on):
    times = _queried_times(on)
    assert times
    assert all(t == on and t.tzinfo == on.tzinfo for t in times)


def test_status_default_now():
    times = _queried_times(None)
    now = datetime.now(timezone.utc)
    assert times
    assert all(t.tzinfo is not None for t in times)
    assert all(abs(t - now) < timedelta(minutes=1) for t in times)


def test_status_bad_timestamp():
    with pytest.raises(TypeError):
        _queried_times(20250701)
