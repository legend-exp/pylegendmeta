Tutorial
========

After the ``pylegendmeta`` package is installed, let's import and instantiate
an object of the main class:

.. code::

   >>> from legendmeta import LegendMetadata
   >>> lmeta = LegendMetadata()

This will automatically clone the `legend-metadata
<https://github.com/legend-exp/legend-metadata>`_ GitHub repository in a
temporary (i.e. not preserved across system reboots) directory.

.. tip::

   It's possible to specify a custom location for the legend-metadata
   repository at runtime by pointing the ``$LEGEND_METADATA`` shell variable to
   it or, alternatively, as an argument to the :class:`~.core.LegendMetadata`
   constructor.

:class:`~.core.LegendMetadata` is a :class:`~.jsondb.JsonDB` object, which
implements an interface to a database of JSON files arbitrary scattered in a
filesystem. ``JsonDB`` does not assume any directory structure or file naming.

Access
------

Let's consider the following database:

.. code::

   legend-metadata
    ├── dir1
    │   └── file1.json
    ├── file2.json
    ├── file3.json
    └── validity.jsonl

With:

.. code-block::
   :linenos:
   :caption: ``dir1/file1.json``

   {
     "value": 1
   }

and similarly ``file2.json`` and ``file3.json``.

``JsonDB`` treats directories, files and JSON keys at the same semantic level.
Internally, the database is represented as a :class:`dict`, and can be
therefore accessed with the same syntax:

.. code:: python

   >>> lmeta["dir1"] # a dict
   >>> lmeta["file2.json"] # a dict
   >>> lmeta["dir1"]["file1.json"] # nested file
   >>> lmeta["dir1"]["file1"] # .json not strictly needed
   >>> lmeta["dir1/file1"] # can use a filesystem path
   >>> lmeta["dir1"]["file1"]["value"] # == 1

To allow you having to type a lot, a fancy attribute-style access mode is
available (try tab-completion in IPython!):

.. code:: python

   >>> lmeta.dir1
   >>> lmeta.dir1.file1
   >>> lmeta.dir1.file1.value

.. warning::

   The attribute-style access syntax cannot be used to query field names that
   cannot be parsed to valid Python variable names. For those, the classic
   dict-style access works.

Metadata validity
-----------------

Mappings of metadata to time periods, data taking systems etc. are specified
through JSONL files. If a ``.jsonl`` file is present in a directory, ``JsonDB``
exposes the :meth:`~.jsondb.JsonDB.on` interface to perform a query.

Let's assume the ``legend-metadata`` directory from the example above contains
the following file:

.. code-block::
   :linenos:
   :caption: ``validity.jsonl``

   {"valid_from": "20220628T000000Z", "select": "all", "apply": ["file2.json"]}
   {"valid_from": "20220629T000000Z", "select": "all", "apply": ["file3.json"]}

From code, it's possible to obtain the metadata valid for a certain time point:

.. code:: python

   >>> from datetime import datetime, timezone
   >>> lmeta.on(datetime(2022, 6, 28, 14, 35, 00, tzinfo=timezone.utc))
   {'value': 2}
   >>> lmeta.on("20220629T095300Z")
   {'value': 3}

For example, the following function call returns the current LEGEND hardware
channel map:

.. code:: python

   >>> lmeta.hardware.configuration.channelmaps.on(datetime.now())
   {'B00089B': {'detname': 'B00089B',
     'location': {'string': 10, 'position': 8},
     'daq': {'crate': 1,
      'card': {'id': 5, 'serialno': None, 'address': '0x350'},
      'channel': 2,
      'fc_channel': 102},
      ...

Remapping metadata
------------------

A second important method of ``JsonDB`` is :meth:`.JsonDB.map`, which allows to
query ``(key, value)`` dictionaries with an alternative unique key defined in
``value``. A typical application is querying parameters in a channel map
corresponding to a certain DAQ channel:

.. code:: python

   >>> chmap = lmeta.hardware.configuration.channelmaps.on(datetime.now())
   >>> chmap.map("daq.fc_channel")[7]
   {'detname': 'V05266A',
    'location': {'string': 1, 'position': 4},
    'daq': {'crate': 0,
     'card': {'id': 1, 'serialno': None, 'address': '0x410'},
     'channel': 3,
     ...

For further details, have a look at the documentation for :meth:`.AttrsDict.map`.

Slow Control interface
----------------------

A number of parameters related to the LEGEND hardware configuration and status
are recorded in the Slow Control database. The latter, PostgreSQL database
resides on the ``legend-sc.lngs.infn.it`` host, part of the LNGS network.

Connecting to the database from within the LEGEND LNGS environment does not
require any special configuration:

.. code:: python

   >>> from legendmeta import LegendSlowControlDB
   >>> scdb = LegendSlowControlDB()
   >>> scdb.connect(password="···")

.. note::

   The database password (for the ``scuser`` user) is confidential and may be
   found on the LEGEND internal wiki pages.

.. tip::

   Alternatively to giving the password to ``connect()``, it can be stored
   in the ``$LEGEND_SCDB_PW`` shell variable (in e.g. ``.bashrc``):

   .. code-block:: bash
      :caption: ``~/.bashrc``

      export LEGEND_SCDB_PW="···"

More :meth:`.LegendSlowControlDB.connect` keyword-arguments are available to
customize hostname and port through which the database can be contacted (in
case of e.g. custom port forwarding).

:meth:`.LegendSlowControlDB.dataframe` can be used to execute an SQL query and
return a :class:`pandas.DataFrame`. The following selects three rows from the
``slot``, ``channel`` and ``vmon`` columns in the ``diode_snap`` table:

.. code:: python

   >>> scdb.dataframe("SELECT slot, channel, vmon FROM diode_snap LIMIT 3")
      slot  channel    vmon
   0     3        6  4300.0
   1     9        2  2250.0
   2    10        3  3699.9

It's even possible to get an entire table as a dataframe:

.. code:: python

   >>> scdb.dataframe("diode_conf")
         confid  crate  slot  channel    vset  iset  rup  rdown  trip  vmax pwkill pwon                    tstamp
   0         15      0     0        0  4000.0   6.0   10      5  10.0  6000   KILL  Dis 2022-10-07 13:49:56+00:00
   1         15      0     0        1  4300.0   6.0   10      5  10.0  6000   KILL  Dis 2022-10-07 13:49:56+00:00
   2         15      0     0        2  4200.0   6.0   10      5  10.0  6000   KILL  Dis 2022-10-07 13:49:56+00:00
   ...

Executing queries natively through an `SQLAlchemy
<ihttps://www.sqlalchemy.org>`_ :class:`~sqlalchemy.orm.Session` is also
possible:

.. code:: python

   >>> import sqlalchemy as sql
   >>> from legendmeta.slowcontrol import DiodeSnap
   >>> session = scdb.make_session()
   >>> result = session.execute(sql.select(DiodeSnap.channel, DiodeSnap.imon).limit(3))
   >>> result.all()
   [(2, 0.0007), (1, 0.0001), (5, 5e-05)]
