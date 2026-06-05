from __future__ import annotations

import pickle

import pytest
import sqlalchemy as sql
from dbetto import AttrsDict

from legendmeta import LegendMetadata, LegendSlowControlDB
from legendmeta.scdb_tables import HeadLdoSnap
from legendmeta.slowcontrol import DiodeSnap

pytestmark = [
    pytest.mark.xfail(
        run=True, reason="requires access to LEGEND slow control database"
    ),
    pytest.mark.needs_slowcontrol,
]


@pytest.fixture(scope="session")
def scdb():
    scdb = LegendSlowControlDB()
    scdb.connect()
    return scdb


def test_connection(scdb):
    pass


def test_select(scdb):
    session = scdb.make_session()
    query = sql.select(DiodeSnap).limit(10)
    result = session.execute(query).all()
    assert len(result) == 10
    session.close()


def test_select_head_ldo(scdb):
    session = scdb.make_session()
    query = sql.select(HeadLdoSnap).limit(10)
    result = session.execute(query).all()
    assert len(result) == 10
    session.close()


def test_str_table_pandas(scdb):
    data = scdb.dataframe("diode_snap_last")
    assert len(data) > 0


def test_str_select_pandas(scdb):
    data = scdb.dataframe("SELECT channel, vmon FROM diode_snap LIMIT 10")
    assert len(data) == 10


def test_select_pandas(scdb):
    data = scdb.dataframe(sql.select(DiodeSnap.channel, DiodeSnap.vmon).limit(10))
    assert len(data) == 10


def test_status(scdb):
    lmeta = LegendMetadata()
    chmap = lmeta.channelmap()
    channel = chmap.V02162B
    status = scdb.status(channel)
    assert isinstance(status, AttrsDict)
    assert "vmon" in status
    assert "vset" in status
    assert "cc4" in status
    for rail in ("vb", "vb1", "vb2", "vcc", "vcc1", "vee", "vee1", "vfet"):
        assert rail in status.cc4
        assert isinstance(status.cc4[rail], float)

    channel = chmap.S002
    status = scdb.status(channel)
    assert isinstance(status, AttrsDict)
    assert "vmon" in status
    assert "vset" in status

    # LDO rails are also available for aux channels with a buffer card
    channel = next(
        v
        for v in chmap.values()
        if getattr(v, "system", None) == "auxs"
        and "buffer_card" in v.get("electronics", {})
    )
    status = scdb.status(channel)
    assert isinstance(status, AttrsDict)
    assert "cc4" in status
    for rail in ("vb", "vb1", "vb2", "vcc", "vcc1", "vee", "vee1", "vfet"):
        assert rail in status.cc4
        assert isinstance(status.cc4[rail], float)


def test_pickle_legend_slowcontrol_db_roundtrip(scdb):
    payload = pickle.dumps(scdb)
    scdb2 = pickle.loads(payload)

    assert isinstance(scdb2, LegendSlowControlDB)
    assert scdb2.connection is None
    assert scdb2.session is None
