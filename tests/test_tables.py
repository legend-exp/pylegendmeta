from __future__ import annotations

import os
import tempfile

import polars as pl
import pytest
from dbetto import AttrsDict
from git import GitCommandError

from legendmeta import LegendMetadata

pytestmark = [
    pytest.mark.xfail(run=True, reason="requires access to legend-metadata"),
    pytest.mark.needs_metadata,
]

tmpdir = tempfile.mkdtemp()


@pytest.fixture
def metadb():
    if os.getenv("LEGEND_METADATA_TESTDIR") is not None:
        mdata = LegendMetadata(os.getenv("LEGEND_METADATA_TESTDIR"), lazy=True)
    else:
        mdata = LegendMetadata(str(tmpdir), lazy=True)
    mdata.checkout("main")
    return mdata


@pytest.fixture
def metadb_v056():
    """Metadata at v0.5.6 — uses dataprod.runinfo and dataprod.config fallback paths."""
    if os.getenv("LEGEND_METADATA_TESTDIR") is not None:
        mdata = LegendMetadata(os.getenv("LEGEND_METADATA_TESTDIR"), lazy=True)
    else:
        mdata = LegendMetadata(str(tmpdir), lazy=True)
    try:
        mdata.checkout("v0.5.6")
    except GitCommandError:
        pytest.skip("cannot checkout v0.5.6 (missing submodule history)")
    return mdata


def test_runinfo(metadb):
    metadb.scan()
    df = metadb.tables.runinfo
    assert isinstance(df, pl.DataFrame)
    assert len(df) > 0
    assert {"period", "run", "datatype"}.issubset(df.columns)


def test_statuses(metadb):
    metadb.scan()
    df = metadb.tables.statuses
    assert isinstance(df, pl.DataFrame)
    assert len(df) > 0
    assert {"period", "run", "datatype", "name"}.issubset(df.columns)


def test_channelmaps(metadb):
    metadb.scan()
    chmaps = metadb.tables.channelmaps
    assert isinstance(chmaps, AttrsDict)
    assert "geds" in chmaps
    assert isinstance(chmaps.geds, pl.DataFrame)
    assert chmaps.geds is chmaps["geds"]
    assert {"name", "period", "rawid"}.issubset(chmaps.geds.columns)


def test_detector_tables(metadb):
    metadb.scan()
    for name in ("crystals", "diodes", "sipms", "fibers"):
        df = getattr(metadb.tables, name)
        assert isinstance(df, pl.DataFrame)


def test_tables_cached(metadb):
    metadb.scan()
    assert metadb.tables is metadb.tables
    assert metadb.tables.runinfo is metadb.tables.runinfo


def test_runinfo_v056(metadb_v056):
    metadb_v056.scan()
    df = metadb_v056.tables.runinfo
    assert isinstance(df, pl.DataFrame)
    assert len(df) > 0
    assert {"period", "run", "datatype"}.issubset(df.columns)


def test_statuses_v056(metadb_v056):
    metadb_v056.scan()
    df = metadb_v056.tables.statuses
    assert isinstance(df, pl.DataFrame)
    assert len(df) > 0
    assert {"period", "run", "datatype", "name"}.issubset(df.columns)


def test_channelmaps_v056(metadb_v056):
    metadb_v056.scan()
    chmaps = metadb_v056.tables.channelmaps
    assert isinstance(chmaps, AttrsDict)
    assert "geds" in chmaps
    assert isinstance(chmaps.geds, pl.DataFrame)
    assert {"name", "period", "rawid"}.issubset(chmaps.geds.columns)
