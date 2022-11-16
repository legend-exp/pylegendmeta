import pytest
from git import GitCommandError

from legendmeta import LegendMetadata

mdata = LegendMetadata()
mdata.checkout("3866a0b")


def test_get_file():
    mdata["hardware/detectors/B00000A.json"]


def test_get_directory():
    mdata["hardware"]


def test_file_not_found():
    with pytest.raises(FileNotFoundError):
        mdata["non-existing-file.ext"]


def test_git_ref_not_found():
    with pytest.raises(GitCommandError):
        mdata.checkout("non-existent-ref")


def test_nested_get():
    assert mdata["hardware"]["detectors"]["B00000A"]["det_name"] == "B00000A"
