import pytest
import sqlalchemy as sql

from legendmeta import LegendSlowControlDB
from legendmeta.slowcontrol import DiodeSnap

pytestmark = pytest.mark.xfail(
    run=True, reason="requires access to LEGEND slow control database"
)


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


def test_str_table_pandas(scdb):
    df = scdb.dataframe("diode_snap_last")
    assert len(df) > 0


def test_str_select_pandas(scdb):
    df = scdb.dataframe("SELECT channel, vmon FROM diode_snap LIMIT 10")
    assert len(df) == 10


def test_select_pandas(scdb):
    df = scdb.dataframe(sql.select(DiodeSnap.channel, DiodeSnap.vmon).limit(10))
    assert len(df) == 10
