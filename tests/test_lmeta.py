from __future__ import annotations

from datetime import datetime

import pytest
from dbetto import AttrsDict, TextDB
from git import GitCommandError

from legendmeta import LegendMetadata

pytestmark = [
    pytest.mark.xfail(run=True, reason="requires access to legend-metadata"),
    pytest.mark.needs_metadata,
]


@pytest.fixture
def metadb():
    mdata = LegendMetadata(lazy=True)
    mdata.checkout("main")
    return mdata


def test_checkout(metadb):
    metadb.checkout("v0.5.6")
    metadb.checkout("v0.5.7")
    metadb.checkout("main")
    metadb.checkout("1c36c84b")

    assert isinstance(metadb.hardware, TextDB)
    assert list(metadb.keys()) == ["hardware"]

    metadb.checkout("main")
    assert list(metadb.keys()) == []


def test_version(metadb):
    metadb.checkout("63b789e")
    assert metadb.__version__ == "v0.5.9-3-g63b789e"

    metadb.show_metadata_version()


def test_get_file(metadb):
    metadb.checkout("63b789e")
    assert isinstance(metadb["hardware/detectors/germanium/diodes/B00000A"], AttrsDict)


def test_get_directory(metadb):
    metadb.checkout("63b789e")
    assert isinstance(metadb["hardware"], TextDB)
    assert isinstance(metadb.hardware, TextDB)


def test_file_not_found(metadb):
    with pytest.raises(FileNotFoundError):
        metadb["non-existing-file.ext"]


def test_git_ref_not_found(metadb):
    with pytest.raises(GitCommandError):
        metadb.checkout("non-existent-ref")


def test_nested_get(metadb):
    metadb.checkout("63b789e")
    assert (
        metadb["hardware"]["detectors"]["germanium"]["diodes"]["B00000A"]["name"]
        == "B00000A"
    )
    assert metadb.hardware.detectors.germanium.diodes.B00000A.name == "B00000A"


def test_chmap_remapping(metadb):
    date = datetime(2024, 7, 1)
    metadb.checkout("63b789e")
    metadb.scan()
    assert (
        "daq"
        in metadb.hardware.configuration.channelmaps.on(date).map("daq.rawid")[1027200]
    )

    assert "daq" in metadb.channelmap(date).map("daq.rawid")[1027200]


def test_channelmap(metadb):
    date = datetime(2024, 7, 1)
    metadb.checkout("63b789e")
    metadb.scan()
    assert isinstance(metadb, LegendMetadata)
    assert isinstance(metadb.channelmap(date), AttrsDict)
    channel = metadb.channelmap(on=date).V02160A
    assert isinstance(channel, AttrsDict)
    assert "geometry" in channel
    assert hasattr(channel, "geometry")
    assert "analysis" in channel

    channel = metadb.channelmap(on=date, system="cal").V02160A
    assert "analysis" in channel
