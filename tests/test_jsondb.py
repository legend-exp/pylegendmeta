from datetime import datetime, timezone
from pathlib import Path

import pytest

from legendmeta.jsondb import AttrsDict, JsonDB, Props

testdb = Path(__file__).parent / "testdb"


def test_props():
    # test read_from
    test_dict = Props.read_from(str(Path(__file__).parent / "testdb/file2.json"))
    assert test_dict["data"] == 2

    # test subst_vars
    Props.subst_vars(test_dict, var_values={"_": str(Path(__file__).parent / "testdb")})
    assert test_dict["filepath"] == str(
        Path(__file__).parent / "testdb/dir1/file3.json"
    )

    test_dict2 = Props.read_from(str(Path(__file__).parent / "testdb/file3.json"))

    # test add_to
    Props.add_to(test_dict, test_dict2)
    assert test_dict["data"] == 3

    # test trim null
    Props.trim_null(test_dict)
    with pytest.raises(KeyError):
        test_dict["null_key"]

    test_dict = Props.read_from(
        [
            str(Path(__file__).parent / "testdb/file2.json"),
            str(Path(__file__).parent / "testdb/file3.json"),
        ],
        subst_pathvar=True,
        trim_null=True,
    )
    assert test_dict["data"] == 3
    assert test_dict["filepath"] == str(
        Path(__file__).parent / "testdb/dir1/file3.json"
    )
    with pytest.raises(KeyError):
        test_dict["null_key"]


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
    assert jdb["file1.json"]["data"] == 1
    assert isinstance(jdb["file1"]["group"], AttrsDict)

    assert isinstance(jdb.file1, AttrsDict)
    assert isinstance(jdb.file1.group, AttrsDict)
    assert isinstance(jdb.dir1, JsonDB)
    assert isinstance(jdb.dir1.file3, AttrsDict)
    assert jdb.file1.data == 1
    assert jdb.file2.data == 2
    assert jdb.dir1.file3.data == 1
    assert jdb.file1.group.data1 == 1

    assert isinstance(jdb.arrays, list)
    assert jdb.arrays[0] == 0
    assert isinstance(jdb.arrays[1], AttrsDict)
    assert jdb.arrays[1].data == 1
    assert isinstance(jdb.arrays[1].array, list)
    assert isinstance(jdb.arrays[1].array[1], AttrsDict)
    assert jdb.arrays[1].array[0] == 1
    assert jdb.arrays[1].array[1].data == 2

    assert jdb.file2.filepath == str(Path(__file__).parent / "testdb/dir1/file3.json")

    with pytest.raises(ValueError):
        JsonDB("non-existent-db")
    with pytest.raises(FileNotFoundError):
        jdb["non-existent-file"]
    with pytest.raises(FileNotFoundError):
        jdb.non_existent_file

    with pytest.raises(AttributeError):
        jdb.dir1.file3.non_existent_key


def test_keys():
    jdb = JsonDB(testdb)
    assert sorted(jdb.keys()) == ["arrays", "dir1", "dir2", "file1", "file2", "file3"]
    assert sorted(jdb.dir1.keys()) == ["dir2", "file3", "file5"]


def test_items():
    jdb = JsonDB(testdb)
    items = sorted(jdb.items())
    assert items[0][0] == "arrays"
    assert isinstance(items[0][1], list)
    assert items[1][0] == "dir1"
    assert isinstance(items[1][1], JsonDB)
    assert items[3][0] == "file1"
    assert isinstance(items[3][1], AttrsDict)


def test_scan():
    jdb = JsonDB(testdb)
    jdb.scan()

    assert sorted(jdb.__dict__.keys()) == [
        "_store",
        "arrays",
        "dir1",
        "dir2",
        "file1",
        "file2",
        "file3",
        "path",
    ]


def test_time_validity():
    jdb = JsonDB(testdb)
    assert isinstance(jdb["dir1"].on("20220628T221955Z"), AttrsDict)

    assert jdb["dir1"].on("20220628T221955Z")["data"] == 1
    assert jdb.dir1.on("20220629T221955Z").data == 2
    # time point in between
    assert jdb["dir1"].on("20220628T233500Z")["data"] == 1
    # time point after
    assert jdb["dir1"].on("20220630T233500Z")["data"] == 2
    # time point before
    with pytest.raises(RuntimeError):
        jdb["dir1"].on("20220627T233500Z")["data"]

    # directory with no .jsonl
    with pytest.raises(RuntimeError):
        jdb["dir1"]["dir2"].on("20220627T233500Z")

    # invalid timestamp
    with pytest.raises(ValueError):
        jdb.dir1.on("20220627T2335002Z")

    # test usage of datetime object
    tstamp = datetime(2022, 6, 28, 23, 35, 00, tzinfo=timezone.utc)
    assert jdb.dir1.on(tstamp).data == 1
    assert jdb.dir1.on(tstamp, r"^file3.*", "all").data == 1

    assert jdb.dir1.on(tstamp, system="phy").data == 1
    assert jdb.dir1.on(tstamp, system="cal").data == 1


def test_mapping():
    jdb = JsonDB(testdb)

    assert isinstance(jdb.map("label"), AttrsDict)
    assert jdb.map("label")[3].data == 2
    assert jdb.map("key.label")[3].data == 2
    assert isinstance(jdb.file1.group.map("label"), AttrsDict)
    assert jdb.file1.group.map("label")["a"].data == 1
    assert jdb.file1.group.map("label")["b"].data == 2

    with pytest.raises(RuntimeError):
        jdb.map("system", unique=True)

    assert jdb.map("system", unique=False)[2].map("label")[1].data == 3
    assert jdb.map("system", unique=False)[1].map("label")[2].data == 1

    with pytest.raises(ValueError):
        jdb.map("non-existent-label")


def test_modification():
    d = AttrsDict()
    d["a"] = 1
    assert d.a == 1

    d.b = 2
    assert d["b"] == 2


def test_merging():
    d = AttrsDict({"a": 1})
    d |= {"b": 2}
    assert d == {"a": 1, "b": 2}
    assert isinstance(d, AttrsDict)
    assert hasattr(d, "a")
    assert hasattr(d, "b")

    d2 = d | {"c": 3}
    assert isinstance(d2, AttrsDict)
    assert d2 == {"a": 1, "b": 2, "c": 3}
    assert hasattr(d2, "a")
    assert hasattr(d2, "b")
    assert hasattr(d2, "c")

    jdb = JsonDB(testdb)
    j = jdb.dir1 | jdb.dir2
    assert isinstance(j, AttrsDict)
    assert sorted(j.keys()) == ["dir2", "file3", "file5", "file7", "file8"]
    assert hasattr(j, "dir2")
    assert hasattr(j, "file8")

    with pytest.raises(TypeError):
        jdb |= jdb.dir1
