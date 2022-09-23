from pathlib import Path

import pytest

from legendmeta.jsondb import JsonDB

testdb = Path(__file__).parent / "testdb"


def test_access():
    jdb = JsonDB(testdb)
    assert isinstance(jdb["file1.json"], dict)
    assert isinstance(jdb["file2.json"], dict)
    assert isinstance(jdb["file1"], dict)
    assert isinstance(jdb["dir1"], JsonDB)
    assert isinstance(jdb["dir1"]["file3.json"], dict)
    assert isinstance(jdb["dir1"]["file3"], dict)
    assert isinstance(jdb["dir1/file3.json"], dict)
    assert isinstance(jdb["dir1"]["dir2"], JsonDB)
    assert isinstance(jdb["dir1"]["dir2"]["file4.json"], dict)
    assert isinstance(jdb["dir1/dir2/file4.json"], dict)
    assert jdb["file1.json"]["data"] is None

    with pytest.raises(ValueError):
        JsonDB("non-existent-db")
    with pytest.raises(ValueError):
        jdb["non-existent-file"]


def test_scan():
    jdb = JsonDB(testdb)
    jdb.scan()
