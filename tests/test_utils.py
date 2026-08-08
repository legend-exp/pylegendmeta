from __future__ import annotations

from datetime import datetime

import pytest

from legendmeta import FileKey
from legendmeta.utils import expand_runs


def test_filekey_parsing():
    key = FileKey("l200-p10-r001-phy-20230101T000000Z")
    assert key.experiment == "l200"
    assert key.period == "p10"
    assert key.run == "r001"
    assert key.category == "phy"
    assert key.timestamp == "20230101T000000Z"


def test_filekey_datetime():
    key = FileKey("l200-p03-r000-cal-20230501T205951Z")
    assert key.datetime.replace(tzinfo=None) == datetime(2023, 5, 1, 20, 59, 51)


def test_filekey_str_roundtrip():
    raw = "l200-p10-r001-phy-20230101T000000Z"
    assert str(FileKey(raw)) == raw


def test_filekey_immutable():
    key = FileKey("l200-p10-r001-phy-20230101T000000Z")
    with pytest.raises((AttributeError, TypeError)):
        key.experiment = "l60"  # type: ignore[misc]


def test_filekey_equality_and_hash():
    raw = "l200-p10-r001-phy-20230101T000000Z"
    assert FileKey(raw) == FileKey(raw)
    assert hash(FileKey(raw)) == hash(FileKey(raw))
    assert FileKey(raw) != FileKey("l200-p10-r002-phy-20230101T000000Z")
    assert len({FileKey(raw), FileKey(raw)}) == 1


@pytest.mark.parametrize(
    "bad",
    [
        "bad-key",
        "l200-p10-r001-phy",
        "l200-p10-r001-phy-20230101T000000Z-extra",
        "l200-p10-r001-phy-2023-01-01T000000Z",
        "l200-p10-r001-phy-notatimestamp",
        "l200-p10-r001-phy-20230230T000000Z",  # Feb 30 matches the regex but is not a real date
        "",
    ],
)
def test_filekey_invalid(bad):
    with pytest.raises(ValueError, match="invalid file key format"):
        FileKey(bad)


def test_expand_runs_single():
    assert expand_runs("r001") == ["r001"]


def test_expand_runs_range():
    assert expand_runs("r000..r003") == ["r000", "r001", "r002", "r003"]
    assert expand_runs("r005..r005") == ["r005"]


def test_expand_runs_list():
    assert expand_runs(["r001", "r003"]) == ["r001", "r003"]
    assert expand_runs(["r000..r002", "r005"]) == ["r000", "r001", "r002", "r005"]


def test_expand_runs_invalid():
    assert expand_runs("foo") == []
    assert expand_runs("p03") == []
    assert expand_runs("") == []
    assert expand_runs(42) == []
    assert expand_runs([]) == []
