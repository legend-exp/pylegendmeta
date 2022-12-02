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
The existence of a certain database entry is verified at runtime (lazy).

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

.. code::

   >>> lmeta["dir1"] # a dict
   >>> lmeta["file2.json"] # a dict
   >>> lmeta["dir1"]["file1.json"] # nested file
   >>> lmeta["dir1"]["file1"] # .json not strictly needed
   >>> lmeta["dir1/file1"] # can use a filesystem path
   >>> lmeta["dir1"]["file1"]["value"] # == 1

To allow you having to type a lot, a fancy attribute-style access mode is available:

.. code::

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
exposes the :meth:`~.jsondb.JsonDB.at` interface to perform a query.

Let's assume the ``legend-metadata`` directory from the example above contains
the following file:

.. code-block::
   :linenos:
   :caption: ``validity.jsonl``

   {"valid_from": "20220628T000000Z", "select": "all", "apply": ["file2.json"]}
   {"valid_from": "20220629T000000Z", "select": "all", "apply": ["file3.json"]}

From code, it's possible to obtain the metadata valid for a certain time point:

.. code::

   >>> from datetime import datetime, timezone
   >>> lmeta.at(datetime(2022, 6, 28, 14, 35, 00, tzinfo=timezone.utc))
   {'value': 2}
   >>> lmeta.at("20220629T095300Z")
   {'value': 3}
