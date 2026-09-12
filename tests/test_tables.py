from __future__ import annotations

import os
import tempfile

import polars as pl
import pytest
from dbetto import AttrsDict
from git import GitCommandError

from legendmeta import LegendMetadata
from legendmeta.tables import LegendMetadataTables

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
    df = LegendMetadataTables(metadb).runinfo
    assert isinstance(df, pl.DataFrame)
    assert len(df) > 0
    assert {"period", "run", "datatype"}.issubset(df.columns)


def test_runlists(metadb):
    metadb.scan()
    df = LegendMetadataTables(metadb).runlists
    assert isinstance(df, pl.DataFrame)
    assert len(df) > 0
    assert {"runlist", "period", "run", "datatype"}.issubset(df.columns)

    # single inclusive range expands fully: p03 cal "valid" -> r000..r005
    valid_p03_cal = df.filter(
        (pl.col("runlist") == "valid")
        & (pl.col("period") == 3)
        & (pl.col("datatype") == "cal")
    )
    assert set(valid_p03_cal["run"].to_list()) == {0, 1, 2, 3, 4, 5}

    # multiple ranges concatenate and gaps are preserved: p08 phy "valid"
    # is "r000..r004" + "r006..r014", so r005 is excluded
    valid_p08_phy = df.filter(
        (pl.col("runlist") == "valid")
        & (pl.col("period") == 8)
        & (pl.col("datatype") == "phy")
    )
    assert set(valid_p08_phy["run"].to_list()) == {0, 1, 2, 3, 4, *range(6, 15)}

    # expansion/flattening never double-counts a run
    assert df.is_duplicated().sum() == 0

    # runlist keys mostly resolve to real runinfo rows; a few upstream
    # entries legitimately have no runinfo row (e.g. a phy run listed where
    # runinfo only records cal), but a key-parsing bug would flip this ratio
    runinfo_keys = (
        LegendMetadataTables(metadb)
        .runinfo.select("period", "run", "datatype")
        .unique()
    )
    runlist_keys = df.select("period", "run", "datatype").unique()
    matched = runlist_keys.join(runinfo_keys, on=["period", "run", "datatype"])
    orphans = runlist_keys.join(
        runinfo_keys, on=["period", "run", "datatype"], how="anti"
    )
    assert matched.height > orphans.height


def test_ignored_daq_cycles(metadb):
    metadb.scan()
    df = LegendMetadataTables(metadb).ignored_daq_cycles
    assert isinstance(df, pl.DataFrame)
    assert len(df) > 0
    assert {"category", "key", "period", "run", "datatype", "timestamp"}.issubset(
        df.columns
    )

    # the compound key is split into typed columns
    assert df["period"].dtype == pl.Int64
    assert df["run"].dtype == pl.Int64
    assert set(df["category"].unique()).issubset({"unprocessable", "removed"})


def test_statuses(metadb):
    metadb.scan()
    df = LegendMetadataTables(metadb).statuses
    assert isinstance(df, pl.DataFrame)
    assert len(df) > 0
    assert {"period", "run", "datatype", "name"}.issubset(df.columns)


def test_channelmaps(metadb):
    metadb.scan()
    tables = LegendMetadataTables(metadb)
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


def test_channelmaps_respect_datatype_category(metadb):
    """Regression test for #149: rows must be resolved with their datatype as
    the validity category, so datatype-specific overlays are applied."""
    metadb.scan()
    tables = LegendMetadataTables(metadb)

    # p03 has a cal-only overlay (l200-p03-r%-T%-cal-config.yaml) setting
    # PULS01.rate_in_Hz to 0.5; the base config has 0.05
    puls = tables.channelmaps.puls.filter(
        (pl.col("period") == 3) & (pl.col("run") == 0) & (pl.col("name") == "PULS01")
    )
    rate = dict(zip(puls["datatype"], puls["rate_in_Hz"], strict=True))
    assert rate["cal"] == 0.5
    assert rate["phy"] == 0.05

    # and it must agree with a direct category-aware query
    start = tables.runinfo.filter(
        (pl.col("period") == 3) & (pl.col("run") == 0) & (pl.col("datatype") == "cal")
    )["start_key"][0]
    direct = metadb.hardware.configuration.channelmaps.on(start, category="cal")
    assert rate["cal"] == direct.PULS01.rate_in_Hz


def test_detector_tables(metadb):
    metadb.scan()
    tables = LegendMetadataTables(metadb)
    for name in ("crystals", "sipms", "fibers"):
        df = getattr(tables, name)
        assert isinstance(df, pl.DataFrame)

    # diodes contains a "simulation" sub-directory, skipped with a warning
    with pytest.warns(UserWarning, match="simulation"):
        df = tables.diodes
    assert isinstance(df, pl.DataFrame)


def test_tables_cached(metadb):
    metadb.scan()
    tables = LegendMetadataTables(metadb)
    assert tables.runinfo is tables.runinfo


def test_runinfo_v056(metadb_v056):
    metadb_v056.scan()
    df = LegendMetadataTables(metadb_v056).runinfo
    assert isinstance(df, pl.DataFrame)
    assert len(df) > 0
    assert {"period", "run", "datatype"}.issubset(df.columns)


def test_statuses_v056(metadb_v056):
    metadb_v056.scan()
    df = LegendMetadataTables(metadb_v056).statuses
    assert isinstance(df, pl.DataFrame)
    assert len(df) > 0
    assert {"period", "run", "datatype", "name"}.issubset(df.columns)


def test_channelmaps_v056(metadb_v056):
    metadb_v056.scan()
    chmaps = LegendMetadataTables(metadb_v056).channelmaps
    assert isinstance(chmaps, AttrsDict)
    assert "geds" in chmaps
    assert isinstance(chmaps.geds, pl.DataFrame)
    assert {"name", "period", "run", "datatype", "rawid"}.issubset(chmaps.geds.columns)
