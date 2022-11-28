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
    with pytest.raises(FileNotFoundError):
        jdb["non-existent-file"]


def test_scan():
    jdb = JsonDB(testdb)
    jdb.scan()


def test_time_validity():
    jdb = JsonDB(testdb)
    assert isinstance(jdb["dir1"]["20220628T221955Z"], dict)

    assert jdb["dir1"]["20220628T221955Z"]["data"] == 1
    assert jdb["dir1"]["20220629T221955Z"]["data"] == 2
    # time point in between
    assert jdb["dir1"]["20220628T233500Z"]["data"] == 1
    # time point after
    assert jdb["dir1"]["20220630T233500Z"]["data"] == 2
    # time point before
    with pytest.raises(RuntimeError):
        assert jdb["dir1"]["20220627T233500Z"]["data"]
