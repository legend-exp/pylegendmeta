import pytest
from git import GitCommandError

from legendmeta import LegendMetadata

pytestmark = pytest.mark.xfail(run=True, reason="requires access to legend-metadata")


@pytest.fixture(scope="module")
def metadb():
    mdata = LegendMetadata()
    mdata.checkout("3866a0b")
    return mdata


def test_get_file(metadb):
    metadb["hardware/detectors/B00000A.json"]


def test_get_directory(metadb):
    metadb["hardware"]


def test_file_not_found(metadb):
    with pytest.raises(FileNotFoundError):
        metadb["non-existing-file.ext"]


def test_git_ref_not_found(metadb):
    with pytest.raises(GitCommandError):
        metadb.checkout("non-existent-ref")


def test_nested_get(metadb):
    assert metadb["hardware"]["detectors"]["B00000A"]["det_name"] == "B00000A"
