from __future__ import annotations

import os
import tempfile

import polars as pl
import pytest
from dbetto import AttrsDict
from git import GitCommandError

from legendmeta import LegendMetadata
from legendmeta.tables import Tables

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
    df = Tables(metadb).runinfo
    assert isinstance(df, pl.DataFrame)
    assert len(df) > 0
    assert {"period", "run", "datatype"}.issubset(df.columns)


def test_statuses(metadb):
    metadb.scan()
    df = Tables(metadb).statuses
    assert isinstance(df, pl.DataFrame)
    assert len(df) > 0
    assert {"period", "run", "datatype", "name"}.issubset(df.columns)


def test_channelmaps(metadb):
    metadb.scan()
    tables = Tables(metadb)
    chmaps = tables.channelmaps
    assert isinstance(chmaps, AttrsDict)
    assert "geds" in chmaps
    assert isinstance(chmaps.geds, pl.DataFrame)
    assert chmaps.geds is chmaps["geds"]
    assert {"name", "period", "run", "datatype", "rawid"}.issubset(chmaps.geds.columns)

    # one channelmap per runinfo row: (period, run, datatype, name) is unique
    keys = chmaps.geds.select("period", "run", "datatype", "name")
    assert keys.is_duplicated().sum() == 0

    # every runinfo row is covered by at least one system (not necessarily
    # geds: e.g. p13 is SiPM-only commissioning and has no geds entries)
    all_keys = pl.concat(
        [df.select("period", "run", "datatype") for df in chmaps.values()]
    ).unique()
    runinfo_keys = tables.runinfo.select("period", "run", "datatype").unique()
    covered = runinfo_keys.join(all_keys, on=["period", "run", "datatype"])
    assert covered.height == runinfo_keys.height


def test_detector_tables(metadb):
    metadb.scan()
    tables = Tables(metadb)
    for name in ("crystals", "sipms", "fibers"):
        df = getattr(tables, name)
        assert isinstance(df, pl.DataFrame)

    # diodes contains a "simulation" sub-directory, skipped with a warning
    with pytest.warns(UserWarning, match="simulation"):
        df = tables.diodes
    assert isinstance(df, pl.DataFrame)


def test_tables_cached(metadb):
    metadb.scan()
    tables = Tables(metadb)
    assert tables.runinfo is tables.runinfo


def test_runinfo_v056(metadb_v056):
    metadb_v056.scan()
    df = Tables(metadb_v056).runinfo
    assert isinstance(df, pl.DataFrame)
    assert len(df) > 0
    assert {"period", "run", "datatype"}.issubset(df.columns)


def test_statuses_v056(metadb_v056):
    metadb_v056.scan()
    df = Tables(metadb_v056).statuses
    assert isinstance(df, pl.DataFrame)
    assert len(df) > 0
    assert {"period", "run", "datatype", "name"}.issubset(df.columns)


def test_channelmaps_v056(metadb_v056):
    metadb_v056.scan()
    chmaps = Tables(metadb_v056).channelmaps
    assert isinstance(chmaps, AttrsDict)
    assert "geds" in chmaps
    assert isinstance(chmaps.geds, pl.DataFrame)
    assert {"name", "period", "run", "datatype", "rawid"}.issubset(chmaps.geds.columns)
