# Copyright (C) 2022 Luigi Pertoldi <gipert@pm.me>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from __future__ import annotations

import logging
import os
from datetime import datetime

import pandas as pd
import sqlalchemy as db

from .core import AttrsDict
from .scdb_tables import (
    DiodeConfMon,
    DiodeInfo,
    DiodeSnap,
    MuonConfMon,
    MuonInfo,
    MuonSnap,
    SiPMConfMon,
    SiPMInfo,
    SiPMSnap,
)

log = logging.getLogger(__name__)


class LegendSlowControlDB:
    """A class for interacting with the LEGEND Slow Control database.

    The LEGEND Slow Control system relies on a `PostgreSQL
    <https://www.postgresql.org/docs/current/index.html>`_ database living on
    ``legend-sc.lngs.infn.it``. The aim of the :class:`LegendSlowControlDB`
    class is to simplify access to the database from Python.
    """

    def __init__(self, connect=False) -> None:
        self.connection: db.engine.base.Connection = None
        self.session: db.orm.Session = None

        if connect:
            self.connect()

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
            msg = "must supply the database password"
            raise ValueError(msg)

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
        if self.session:
            log.warning(
                "A session seems to be already open and available "
                "at the .session attribute of this class object. "
                "You might want to use that one."
            )

        return db.orm.Session(self.connection)

    def get_tables(self) -> list[str]:
        """Get tables available in the database."""
        return db.inspect(self.connection.engine).get_table_names()

    def get_columns(self, table: str) -> list[str]:
        """Get columns available on `table` in the database."""
        return db.inspect(self.connection.engine).get_columns(table)

    def dataframe(self, expr: str | db.sql.Select) -> pd.DataFrame:
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

        >>> scdb.dataframe("diode_conf_mon")
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
            try:
                return pd.read_sql(expr, self.connection)
            except db.exc.ObjectNotExecutableError:
                return pd.read_sql(db.text(expr), self.connection)
        # try rolling back transaction if any exception occurs
        except Exception as exc:
            self.connection.rollback()
            raise exc

    def status(
        self, channel: dict, on: str | datetime | None = None, system: str | None = None
    ) -> dict:
        """Query status of a LEGEND DAQ channel.

        Collect all the relevant information about the status of a channel at a
        certain time from the Slow Control database, based on the channel type
        (i.e. germanium detector, SiPM or PMT).

        Parameters
        ----------
        channel
            this dictionary must contain information about the channel hardware
            configuration. Typically a LEGEND channel map entry (obtainable, for
            example, with :meth:`.core.LegendMetadata.channelmap`).
        on
            time at which the status is requested.
        system: "geds", "spms", "pmts", ...
            system the channel belong to. this information is used to ask the
            Slow Control database the right questions. If ``None`` will try to
            determine it from the available metadata.

        Examples
        --------
        >>> ts = datetime(...) # or LEGEND cycle timestamp
        >>> my_bege = lmeta.channelmap(on=ts).B00089B
        >>> scdb.status(my_bege, on=ts)
        {'vmon': 3399.9,
         'imon': 0.00015,
         'status': 1,
         'vset': 3400.0,
         'iset': 6.0,
         ...

        Warning
        -------
        This class method assumes a certain structure for legend-metadata.
        Might stop working if that structure is altered.
        """
        if not on:
            on = datetime.now()

        # sanity checks
        if isinstance(on, str):
            on = datetime.strptime(on, "%Y%m%dT%H%M%SZ")
        elif not isinstance(on, datetime):
            msg = "Bad input timestamp format"
            raise ValueError(msg)

        if not isinstance(channel, dict):
            msg = "Bad channel format: dict expected"
            raise ValueError(msg)

        if not system:
            system = channel.system

        # prepare environment to perform query
        if not self.session:
            self.session = self.make_session()

        output = AttrsDict()
        if system == "geds":
            for tbl in [DiodeInfo, DiodeSnap, DiodeConfMon]:
                stmt = (
                    db.select(tbl)
                    .where(tbl.slot == channel.voltage.card.id)
                    .where(tbl.channel == channel.voltage.channel)
                    .order_by(tbl.tstamp.desc())
                    .where(tbl.tstamp <= on)
                    .limit(1)
                )

                result = self.session.execute(stmt).first()

                if not result:
                    msg = f"Query on table '{tbl.__tablename__}' did not produce any result"
                    log.warning(msg)
                    continue

                output |= result[0].asdict()

        elif system == "spms":
            for tbl in [SiPMInfo, SiPMSnap, SiPMConfMon]:
                stmt = (
                    db.select(tbl)
                    .where(tbl.board == channel.electronics.card.id)
                    .where(tbl.channel == channel.electronics.channel)
                    .order_by(tbl.tstamp.desc())
                    .where(tbl.tstamp <= on)
                    .limit(1)
                )

                result = self.session.execute(stmt).first()

                if not result:
                    msg = f"Query on table '{tbl.__tablename__}' did not produce any result"
                    log.warning(msg)
                    continue

                output |= result[0].asdict()

        # NOTE: untested
        elif system == "pmts":
            for tbl in [MuonInfo, MuonSnap, MuonConfMon]:
                stmt = (
                    db.select(tbl)
                    .where(tbl.slot == channel.voltage.card.id)
                    .where(tbl.channel == channel.voltage.channel)
                    .order_by(tbl.tstamp.desc())
                    .where(tbl.tstamp <= on)
                    .limit(1)
                )

                result = self.session.execute(stmt).first()

                if not result:
                    msg = f"Query on table '{tbl.__tablename__}' did not produce any result"
                    log.warning(msg)
                    continue

                output |= result[0].asdict()
        else:
            msg = f"System '{system}' not (yet) supported"
            raise NotImplementedError(msg)

        if not output:
            msg = "Could not obtain any information about the channel"
            raise RuntimeError()

        return output
