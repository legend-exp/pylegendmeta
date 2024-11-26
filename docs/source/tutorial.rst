Tutorial
========

After the *pylegendmeta* package is installed, let's import and instantiate
an object of the main class:

>>> from legendmeta import LegendMetadata
>>> lmeta = LegendMetadata()

This will automatically clone the legend-metadata_ GitHub repository in a
temporary (i.e. not preserved across system reboots) directory.

.. tip::

   It's possible to specify a custom location for the legend-metadata_
   repository at runtime by pointing the ``$LEGEND_METADATA`` shell variable to
   it or, alternatively, as an argument to the :class:`~.core.LegendMetadata`
   constructor. Recommended if a custom legend-metadata_ is needed.

:class:`~.core.LegendMetadata` is a :class:`~.textdb.TextDB` object, which
implements an interface to a database of text files arbitrary scattered in a
filesystem. ``TextDB`` does not assume any directory structure or file naming.

.. note::

   Currently supported file formats are `JSON <https://json.org>`_ and `YAML
   <https://yaml.org>`_.

Access
------

Let's consider the following database:

.. code::

   legend-metadata
    ├── dir1
    │   └── file1.json
    ├── file2.json
    ├── file3.yaml
    └── validity.yaml

With:

.. code-block::
   :linenos:
   :caption: ``dir1/file1.json``

   {
     "value": 1
   }

and similarly ``file2.json`` and ``file3.yaml``.

``TextDB`` treats directories, files and JSON/YAML keys at the same semantic
level.  Internally, the database is represented as a :class:`dict`, and can be
therefore accessed with the same syntax:

>>> lmeta["dir1"] # a dict
>>> lmeta["file2.json"] # a dict
>>> lmeta["dir1"]["file1.json"] # nested file
>>> lmeta["dir1"]["file1"] # .json not strictly needed
>>> lmeta["dir1/file1"] # can use a filesystem path
>>> lmeta["dir1"]["file1"]["value"] # == 1

To allow you having to type a lot, a fancy attribute-style access mode is
available (try tab-completion in IPython!):

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
through YAML files (`specification
<https://legend-exp.github.io/legend-data-format-specs/dev/metadata>`_).
If a ``validity.yaml`` file is present in a directory, ``TextDB``
exposes the :meth:`~.textdb.TextDB.on` interface to perform a query.

Let's assume the ``legend-metadata`` directory from the example above contains
the following file:

.. code-block:: yaml
   :linenos:
   :caption: ``validity.yaml``

   - valid_from: 20230101T000000Z
      category: all
      apply:
         - file3.yaml

   - valid_from: 20230102T000000Z
      category: all
      mode: append
      apply:
         - file2.yaml

   - valid_from: 20230103T000000Z
      category: all
      mode: remove
      apply:
         - file2.yaml

   - valid_from: 20230104T000000Z
      category: all
      mode: reset
      apply:
         - file2.yaml

   - valid_from: 20230105T000000Z
      category: all
      mode: replace
      apply:
         - file2.yaml
         - file3.yaml

From code, it's possible to obtain the metadata valid for a certain time point:

>>> from datetime import datetime, timezone
>>> lmeta.on(datetime(2022, 6, 28, 14, 35, 00, tzinfo=timezone.utc))
{'value': 2}
>>> lmeta.on("20220629T095300Z")
{'value': 3}

For example, the following function call returns the current LEGEND hardware
channel map:

>>> lmeta.hardware.configuration.channelmaps.on(datetime.now())
{'V02160A': {'name': 'V02160A',
  'system': 'geds',
  'location': {'string': 1, 'position': 1},
  'daq': {'crate': 0,
   'card': {'id': 1, 'address': '0x410', 'serialno': None},
   'channel': 0,
   'rawid': 1104000},

.. tip::

   :meth:`.core.LegendMetadata.channelmap` offers a shortcut for the function
   call above and, in addition, augments the channel map with the information
   from the detector database. Check it out!

Remapping and grouping metadata
-------------------------------

A second important method of ``TextDB`` is :meth:`.TextDB.map`, which allows to
query ``(key, value)`` dictionaries with an alternative unique key defined in
``value``. A typical application is querying parameters in a channel map
corresponding to a certain DAQ channel:

>>> chmap = lmeta.hardware.configuration.channelmaps.on(datetime.now())
>>> chmap.map("daq.rawid")[1104003]
{'detname': 'V05266A',
 'system': 'geds',
 'location': {'string': 1, 'position': 4},
 'daq': {'crate': 0,
  'card': {'id': 1, 'serialno': None, 'address': '0x410'},
  'channel': 3,
 ...

If the requested key is not unique, an exception will be raised.
:meth:`.TextDB.map` can, however, handle non-unique keys too and return a
dictionary of matching entries instead, keyed by an arbitrary integer to allow
further :meth:`.TextDB.map` calls. The behavior is achieved by using
:meth:`.TextDB.group` or by setting the ``unique`` argument flag. A typical
application is retrieving all channels attached to the same CC4:

>>> chmap = lmeta.hardware.configuration.channelmaps.on(datetime.now())
>>> chmap.group("electronics.cc4.id")["C3"]
{0: {'name': 'V02160A',
  'system': 'geds',
  'location': {'string': 1, 'position': 1},
  'daq': {'crate': 0,
   'card': {'id': 1, 'address': '0x410', 'serialno': None},
   'channel': 0,

For further details, have a look at the documentation for :meth:`.AttrsDict.map`.

LEGEND channel maps
-------------------

The :meth:`.core.LegendMetadata.channelmap` method is a convenience method to
obtain channel-relevant metadata (hardware, analysis, etc.) in time:

>>> myicpc = lmeta.channelmap(datetime.now()).V00048B
>>> myicpc.production.mass_in_g  # static info from the detector database
1815.8
>>> myicpc.location.string  # hardware channel map info
8
>>> myicpc.analysis.usability  # analysis info
'on'

Since :meth:`~.core.LegendMetadata.channelmap` returns an :class:`~.AttrsDict`,
other useful operations like :meth:`~.AttrsDict.map` can be applied.

Slow Control interface
----------------------

A number of parameters related to the LEGEND hardware configuration and status
are recorded in the Slow Control database. The latter, PostgreSQL database
resides on the ``legend-sc.lngs.infn.it`` host, part of the LNGS network.

Connecting to the database from within the LEGEND LNGS environment does not
require any special configuration:

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

Two methods can be used to inspect the database:
:meth:`.LegendSlowControlDB.get_tables` and
:meth:`.LegendSlowControlDB.get_columns`:

>>> scdb.get_tables()
['muon_conf',
 'diode_info',
 'muon_conf_set',
 'diode_conf_list',
 'muon_info',
 'muon_conf_mon',
 ...
>>> scdb.get_columns("diode_info")
[{'name': 'crate',
  'type': INTEGER(),
  'nullable': False,
  'default': None,
  'autoincrement': False,
  'comment': None},
 {'name': 'slot',
 ...

:meth:`.LegendSlowControlDB.dataframe` can be used to execute an SQL query and
return a :class:`pandas.DataFrame`. The following selects three rows from the
``slot``, ``channel`` and ``vmon`` columns in the ``diode_snap`` table:

>>> scdb.dataframe("SELECT slot, channel, vmon FROM diode_snap LIMIT 3")
  slot  channel    vmon
0     3        6  4300.0
1     9        2  2250.0
2    10        3  3699.9

It's even possible to get an entire table as a dataframe:

>>> scdb.dataframe("diode_conf_mon")
     confid  crate  slot  channel    vset  iset  rup  rdown  trip  vmax pwkill pwon                    tstamp
0         15      0     0        0  4000.0   6.0   10      5  10.0  6000   KILL  Dis 2022-10-07 13:49:56+00:00
1         15      0     0        1  4300.0   6.0   10      5  10.0  6000   KILL  Dis 2022-10-07 13:49:56+00:00
2         15      0     0        2  4200.0   6.0   10      5  10.0  6000   KILL  Dis 2022-10-07 13:49:56+00:00
...

Executing queries natively through an `SQLAlchemy
<ihttps://www.sqlalchemy.org>`_ :class:`~sqlalchemy.orm.Session` is also
possible:

>>> import sqlalchemy as sql
>>> from legendmeta.slowcontrol import DiodeSnap
>>> session = scdb.make_session()
>>> result = session.execute(sql.select(DiodeSnap.channel, DiodeSnap.imon).limit(3))
>>> result.all()
[(2, 0.0007), (1, 0.0001), (5, 5e-05)]

Channel status [experimental]
`````````````````````````````

*pylegendmeta* offers a shortcut to retrieve the status of a channel from the
Slow Control via :meth:`.LegendSlowControlDB.status`.

>>> channel = lmeta.channelmap().V02162B
>>> scdb.status(channel)
{'group': 'String 7',
'label': 'V02162B',
'vmon': 4299.9,
'imon': 5e-05,
'status': 1,
'vset': 4300.0,
'iset': 6.0,
'rup': 5,
'rdown': 5,
'trip': 10.0,
'vmax': 6000,
'pwkill': 'KILL',
'pwon': 'Dis'}

.. _legend-metadata: https://github.com/legend-exp/legend-metadata
