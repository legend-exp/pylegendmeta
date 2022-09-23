import pytest
from git import GitCommandError

from legendmeta import LegendMetadata

mdata = LegendMetadata()
mdata.checkout("3866a0b")


def test_get_file():
    mdata.get_path("hardware/detectors/B00000A.json")


def test_get_directory():
    mdata.get_path("hardware")


def test_file_not_found():
    with pytest.raises(FileNotFoundError):
        mdata.get_path("non-existing-file.ext")


def test_git_ref_not_found():
    with pytest.raises(GitCommandError):
        mdata.checkout("non-existent-ref")


def test_getitem():
    assert mdata["hardware"]["detectors"]["B00000A"]["det_name"] == "B00000A"
