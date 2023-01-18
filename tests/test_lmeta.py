from datetime import datetime

import pytest
from git import GitCommandError

from legendmeta import LegendMetadata

pytestmark = pytest.mark.xfail(run=True, reason="requires access to legend-metadata")


@pytest.fixture(scope="module")
def metadb():
    mdata = LegendMetadata()
    mdata.checkout("d0167ef")
    return mdata


def test_get_file(metadb):
    metadb["hardware/detectors/germanium/detectors/B00000A.json"]


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
        metadb["hardware"]["detectors"]["germanium"]["detectors"]["B00000A"]["det_name"]
        == "B00000A"
    )
    assert metadb.hardware.detectors.germanium.detectors.B00000A.det_name == "B00000A"


def test_chmap_remapping(metadb):
    assert (
        "daq"
        in metadb.hardware.configuration.channelmaps.on(datetime.now()).map("daq.fcid")[
            7
        ]
    )

    assert "daq" in metadb.channelmap().map("daq.fcid")[7]


def test_channelmap(metadb):
    assert "geometry" in metadb.channelmap(on=datetime.now()).V02162B
