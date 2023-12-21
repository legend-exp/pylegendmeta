from __future__ import annotations

from datetime import datetime

from legendmeta import to_datetime


def test_to_datetime():
    assert to_datetime("20230501T205951Z") == datetime(2023, 5, 1, 20, 59, 51)
