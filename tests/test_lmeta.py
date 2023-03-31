from datetime import datetime

import pytest
from git import GitCommandError

from legendmeta import LegendMetadata
from legendmeta.jsondb import AttrsDict

pytestmark = pytest.mark.xfail(run=True, reason="requires access to legend-metadata")


@pytest.fixture(scope="module")
def metadb():
    mdata = LegendMetadata()
    mdata.checkout("98c00f0")
    return mdata


def test_get_file(metadb):
    metadb["hardware/detectors/germanium/diodes/B00000A.json"]


def test_get_directory(metadb):
    metadb["hardware"]
    metadb.hardware


def test_file_not_found(metadb):
    with pytest.raises(FileNotFoundError):
        metadb["non-existing-file.ext"]


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
