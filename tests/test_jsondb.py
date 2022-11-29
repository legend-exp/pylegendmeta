from datetime import datetime, timezone
from pathlib import Path

import pytest

from legendmeta.jsondb import AttrsDict, JsonDB

testdb = Path(__file__).parent / "testdb"


def test_access():
    jdb = JsonDB(testdb)
    assert isinstance(jdb["file1.json"], AttrsDict)
    assert isinstance(jdb["file2.json"], AttrsDict)
    assert isinstance(jdb["file1"], AttrsDict)
    assert isinstance(jdb["dir1"], JsonDB)
    assert isinstance(jdb["dir1"]["file3.json"], AttrsDict)
    assert isinstance(jdb["dir1"]["file3"], AttrsDict)
    assert isinstance(jdb["dir1/file3.json"], AttrsDict)
    assert isinstance(jdb["dir1"]["dir2"], JsonDB)
    assert isinstance(jdb["dir1"]["dir2"]["file4.json"], AttrsDict)
    assert isinstance(jdb["dir1/dir2/file4.json"], AttrsDict)
    assert jdb["file1.json"]["data"] is None

    assert isinstance(jdb.file1, dict)
    assert isinstance(jdb.dir1, JsonDB)
    assert isinstance(jdb.dir1.file3, dict)
    assert jdb.file1.data is None
    assert jdb.dir1.file3.data == 1

    with pytest.raises(ValueError):
        JsonDB("non-existent-db")
    with pytest.raises(FileNotFoundError):
        jdb["non-existent-file"]
    with pytest.raises(FileNotFoundError):
        jdb.non_existent_file

    with pytest.raises(AttributeError):
        jdb.dir1.file3.non_existent_key


def test_scan():
    jdb = JsonDB(testdb)
    jdb.scan()


def test_time_validity():
    jdb = JsonDB(testdb)
    assert isinstance(jdb["dir1"].at("20220628T221955Z"), AttrsDict)

    assert jdb["dir1"].at("20220628T221955Z")["data"] == 1
    assert jdb.dir1.at("20220629T221955Z").data == 2
    # time point in between
    assert jdb["dir1"].at("20220628T233500Z")["data"] == 1
    # time point after
    assert jdb["dir1"].at("20220630T233500Z")["data"] == 2
    # time point before
    with pytest.raises(RuntimeError):
        jdb["dir1"].at("20220627T233500Z")["data"]

    # directory with no .jsonl
    with pytest.raises(RuntimeError):
        jdb["dir1"]["dir2"].at("20220627T233500Z")

    # invalid timestamp
    with pytest.raises(ValueError):
        jdb.dir1.at("20220627T2335002Z")

    # test usage of datetime object
    tstamp = datetime(2022, 6, 28, 23, 35, 00, tzinfo=timezone.utc)
    assert jdb.dir1.at(tstamp).data == 1
    assert jdb.dir1.at(tstamp, r"^file3.*", "all").data == 1
