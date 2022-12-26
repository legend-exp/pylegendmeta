from __future__ import annotations

import os

import pandas
from sqlalchemy import create_engine


class LegendSlowControlDB:
    """A class for interacting with the LEGEND Slow Control database.

    The LEGEND Slow Control system relies on a `PostgreSQL
    <https://www.postgresql.org/docs/current/index.html>`_ database living on
    ``legend-sc.lngs.infn.it``. The aim of the :class:`LegendSlowControlDB`
    class is to simplify access to the database from Python.
    """

    def __init__(self) -> None:
        self.connection = None

    def connect(
        self, host: str = "localhost", port: int = 5432, password: str = None
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
            on LEGEND's internal documentation (e.g. the Wiki web pages).
        """
        if password is None:
            password = os.getenv("LEGEND_SCDB_PW")

        if password is None:
            raise ValueError("must supply the database password")

        self.connection = create_engine(
            f"postgresql://scuser:{password}@{host}:{port}/scdb"
        ).connect()

    def disconnect(self) -> None:
        """Disconnect from the database."""
        self.connection.close()

    def get_dataframe(self, expr: str) -> pandas.DataFrame:
        """Query the database and return a dataframe holding the result.

        See Also
        --------
        pandas.read_sql
        """
        return pandas.read_sql(expr, self.connection)
