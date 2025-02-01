from __future__ import annotations

from datetime import datetime

import pytest
from git import GitCommandError

from legendmeta import LegendMetadata
from legendmeta.textdb import AttrsDict

pytestmark = pytest.mark.xfail(run=True, reason="requires access to legend-metadata")


@pytest.fixture(scope="module")
def metadb():
    mdata = LegendMetadata(lazy=True)
    mdata.checkout("refactor")
    return mdata


def test_checkout(metadb):
    metadb.checkout("v0.5.6")
    metadb.checkout("v0.5.7")
    metadb.checkout("main")
    metadb.checkout("1c36c84b")


def test_get_version(metadb):
    metadb.metadata_version()


def test_get_file(metadb):
    assert metadb["hardware/detectors/germanium/diodes/B00000A.json"]


def test_get_directory(metadb):
    assert metadb["hardware"]
    assert metadb.hardware


def test_file_not_found(metadb):
    with pytest.raises(FileNotFoundError):
        assert metadb["non-existing-file.ext"]


def test_git_ref_not_found(metadb):
    with pytest.raises(GitCommandError):
        metadb.checkout("non-existent-ref")


def test_nested_get(metadb):
    assert (
        metadb["hardware"]["detectors"]["germanium"]["diodes"]["B00000A"]["name"]
        == "B00000A"
    )
    assert metadb.hardware.detectors.germanium.diodes.B00000A.name == "B00000A"


def test_chmap_remapping(metadb):
    metadb.scan()
    assert (
        "daq"
        in metadb.hardware.configuration.channelmaps.on(datetime.now()).map(
            "daq.rawid"
        )[1080000]
    )

    assert "daq" in metadb.channelmap().map("daq.rawid")[1080000]


def test_channelmap(metadb):
    channel = metadb.channelmap(on=datetime.now()).V02162B
    assert isinstance(channel, AttrsDict)
    assert "geometry" in channel
    assert hasattr(channel, "geometry")
    assert "analysis" in channel

    channel = metadb.channelmap(on=datetime.now(), system="cal").V02162B
    assert "analysis" in channel
