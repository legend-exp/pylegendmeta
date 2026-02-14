from __future__ import annotations

import copy

from legendmeta import LegendMetadata
from legendmeta.slowcontrol import LegendSlowControlDB


def test_copy_legend_metadata() -> None:
    meta = LegendMetadata(path="tests/testdb", lazy=True)

    shallow = copy.copy(meta)
    assert isinstance(shallow, LegendMetadata)
    assert shallow is not meta
    assert list(shallow.keys()) == list(meta.keys())

    deep = copy.deepcopy(meta)
    assert isinstance(deep, LegendMetadata)
    assert deep is not meta
    assert list(deep.keys()) == list(meta.keys())


def test_copy_legend_slowcontrol_db() -> None:
    scdb = LegendSlowControlDB()

    shallow = copy.copy(scdb)
    assert isinstance(shallow, LegendSlowControlDB)
    assert shallow is not scdb

    deep = copy.deepcopy(scdb)
    assert isinstance(deep, LegendSlowControlDB)
    assert deep is not scdb
