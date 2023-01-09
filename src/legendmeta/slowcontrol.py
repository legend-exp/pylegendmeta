from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime

import pandas
import sqlalchemy as db
from sqlalchemy.orm import DeclarativeBase, Mapped


class LegendSlowControlDB:
    """A class for interacting with the LEGEND Slow Control database.

    The LEGEND Slow Control system relies on a `PostgreSQL
    <https://www.postgresql.org/docs/current/index.html>`_ database living on
    ``legend-sc.lngs.infn.it``. The aim of the :class:`LegendSlowControlDB`
    class is to simplify access to the database from Python.
    """

    def __init__(self) -> None:
        self.connection: db.engine.base.Connection = None

    def connect(
        self, host: str = "localhost", port: int = 5432, password: str | None = None
    ) -> None:
        """Establish a connection to the database.

        Authentication is attempted with the read-only user ``scuser`` on a
        database named ``scdb``.

        Parameters
        ----------
        host
            database host. Can be a hostname (``localhost``,
            ``legend-sc.lngs.infn.it``, etc.) or an IP address.
        port
            port through which the database should be contacted.
        password
            password for user ``scuser`` of the ``scdb`` database. May be found
            on LEGEND's internal documentation (e.g. the Wiki web pages). If
            ``None``, uses the value of the ``$LEGEND_SCDB_PW`` shell variable.

        Examples
        --------
        If the Slow Control database connection is forwarded to a local machine
        (port 6942) (through e.g. an SSH tunnel), use:

        >>> scdb = LegendSlowControlDB()
        >>> scdb.connect("localhost", port=6942, password="···")

        Alternatively to giving the password to ``connect()``, it can be stored
        in the ``$LEGEND_SCDB_PW`` shell variable (in e.g. ``.bashrc``):

        .. code-block:: bash
           :caption: ``~/.bashrc``

           export LEGEND_SCDB_PW="···"

        Then:

        >>> scdb.connect("localhost", port=6942)
        """
        if password is None:
            password = os.getenv("LEGEND_SCDB_PW")

        if password is None:
            raise ValueError("must supply the database password")

        if self.connection is not None and not self.connection.closed:
            self.disconnect()

        self.connection = db.create_engine(
            f"postgresql://scuser:{password}@{host}:{port}/scdb"
        ).connect()

    def disconnect(self) -> None:
        """Disconnect from the database."""
        self.connection.close()

    def make_session(self) -> db.orm.Session:
        """Open and return a new  :class:`~sqlalchemy.orm.Session` object for executing database operations.

        Examples
        --------
        >>> import sqlalchemy as sql
        >>> from legendmeta.slowcontrol import DiodeSnap
        >>> session = scdb.make_session()
        >>> result = session.execute(sql.select(DiodeSnap.channel, DiodeSnap.imon).limit(3))
        >>> result.all()
        [(2, 0.0007), (1, 0.0001), (5, 5e-05)]

        See Also
        --------
        `SQLAlchemy documentation <https://www.sqlalchemy.org/>`_
        """
        return db.orm.Session(self.connection)

    def dataframe(self, expr: str | db.sql.Select) -> pandas.DataFrame:
        """Query the database and return a dataframe holding the result.

        Parameters
        ----------
        expr
            SQL table name, select SQL command text or SQLAlchemy selectable
            object.

        Examples
        --------
        SQL select syntax text or table name:

        >>> scdb.dataframe("SELECT channel, vmon FROM diode_snap LIMIT 3")
           channel    vmon
        0        2  2250.0
        1        1  3899.4
        2        5  1120.2

        >>> scdb.dataframe("diode_conf")
              confid  crate  slot  channel    vset  iset  rup  rdown  trip  vmax pwkill pwon                    tstamp
        0         15      0     0        0  4000.0   6.0   10      5  10.0  6000   KILL  Dis 2022-10-07 13:49:56+00:00
        1         15      0     0        1  4300.0   6.0   10      5  10.0  6000   KILL  Dis 2022-10-07 13:49:56+00:00
        2         15      0     0        2  4200.0   6.0   10      5  10.0  6000   KILL  Dis 2022-10-07 13:49:56+00:00
        ...

        :class:`sqlalchemy.sql.Select` object:

        >>> import sqlalchemy as sql
        >>> from legendmeta.slowcontrol import DiodeSnap
        >>> scdb.dataframe(sql.select(DiodeSnap.channel, DiodeSnap.vmon).limit(3))
           channel    vmon
        0        2  2250.0
        1        1  3899.4
        2        5  1120.2

        See Also
        --------
        pandas.read_sql
        """
        try:
            return pandas.read_sql(expr, self.connection)
        except db.exc.ObjectNotExecutableError:
            return pandas.read_sql(db.text(expr), self.connection)
        # TODO: automatically rollback if failed transaction
        # except db.exc.ProgrammingError as e:
        #     self.connection.rollback()
        #     raise e

    def status(self, channel: dict, at: str | datetime, system: str = "ged") -> dict:
        """Query information about a channel.

        >>> channel = lmeta.hardware.configuration.channelmaps.on(ts).B00089B
        >>> scdb.status(channel, at=ts)
        """
        raise NotImplementedError
        # df = self.dataframe(...tables...).sort_values(by=["tstamp"])
        # return df.loc(df.tstamp <= at).iloc(-1)


class Base(DeclarativeBase):
    pass


@dataclass
class DiodeSnap(Base):
    """Monitored parameters of HPGe detectors."""

    __tablename__ = "diode_snap"

    crate: Mapped[int]
    slot: Mapped[int]
    channel: Mapped[int]
    vmon: Mapped[float]
    imon: Mapped[float]
    status: Mapped[int]
    almask: Mapped[int]
    tstamp: Mapped[datetime] = db.orm.mapped_column(primary_key=True)


@dataclass
class DiodeConf(Base):
    """Configuration parameters of HPGe detectors."""

    __tablename__ = "diode_conf"

    confid: Mapped[int]
    crate: Mapped[int]
    slot: Mapped[int]
    channel: Mapped[int]
    vset: Mapped[float]
    iset: Mapped[float]
    rup: Mapped[int]
    rdown: Mapped[int]
    trip: Mapped[float]
    vmax: Mapped[int]
    pwkill: Mapped[str]
    pwon: Mapped[str]
    tstamp: Mapped[datetime] = db.orm.mapped_column(primary_key=True)


@dataclass
class SiPMSnap(Base):
    """Monitored parameters of SiPMs."""

    __tablename__ = "sipm_snap"

    board: Mapped[int]
    channel: Mapped[int]
    vmon: Mapped[float]
    imon: Mapped[float]
    status: Mapped[int]
    almask: Mapped[int]
    tstamp: Mapped[datetime] = db.orm.mapped_column(primary_key=True)


class SiPMConf(Base):
    """Configuration parameters of SiPMs."""

    __tablename__ = "sipm_conf"

    confid: Mapped[int]
    board: Mapped[int]
    channel: Mapped[int]
    vset: Mapped[float]
    iset: Mapped[float]
    tstamp: Mapped[datetime] = db.orm.mapped_column(primary_key=True)
