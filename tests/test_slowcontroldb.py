from __future__ import annotations

import pytest
import sqlalchemy as sql

from legendmeta import LegendMetadata, LegendSlowControlDB
from legendmeta.slowcontrol import DiodeSnap
from legendmeta.textdb import AttrsDict

pytestmark = pytest.mark.xfail(
    run=True, reason="requires access to LEGEND slow control database"
)


@pytest.fixture(scope="session")
def scdb():
    scdb = LegendSlowControlDB()
    scdb.connect()
    return scdb


def test_connection(scdb):  # noqa: ARG001
    pass


def test_select(scdb):
    session = scdb.make_session()
    query = sql.select(DiodeSnap).limit(10)
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

    channel = chmap.S002
    status = scdb.status(channel)
    assert isinstance(status, AttrsDict)
    assert "vmon" in status
    assert "vset" in status
