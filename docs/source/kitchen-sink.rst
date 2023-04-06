Kitchen Sink
============

An unstructured collection of examples and frequently metadata-related
computations.

.. note::

    Global imports and definitions:

    .. code-block:: python

       from legendmeta import LegendMetadata
       from datetime import datetime
       import numpy

       lmeta = LegendMetadata()

How many kilograms of germanium are currently deployed in the LEGEND cryostat?
------------------------------------------------------------------------------

.. code-block:: python

   >>> mass = 0
   >>> for det, val in lmeta.channelmap(datetime.now()).items():
   >>>     if val.system == "geds":
   >>>         mass += val.production.mass_in_g
   >>> mass/1000 # in kg
   [REDACTED]

or, alternatively:

.. code-block:: python

   >>> # get only HPGe channels by mapping for "system"
   >>> geds = lmeta.channelmap(datetime.now()).map("system", unique=False).geds
   >>> # collect and sum up masses
   >>> masses = [v.production.mass_in_g for v in geds.values()]
   >>> numpy.cumsum(masses)[-1]
   [REDACTED]

   print(f"mass = {mass/1000} kg")

How many kilograms of Ge76 are currently deployed in the form of ICPC detectors and are usable for analysis?
------------------------------------------------------------------------------------------------------------

Calls to :meth:`.AttrsDict.map` can be chained together to build complex queries:

.. code-block:: python

   >>> # get HPGes, only ICPCs and only if their analysis status is ON
   >>> dets = lmeta.channelmap(datetime.now()) \
   >>>             .map("system", unique=False).geds \
   >>>             .map("type", unique=False).icpc \
   >>>             .map("analysis.usability", unique=False).on
   >>> # collect and sum up mass * enrichment (assuming that the enrichment fraction is also in mass)
   >>> data = [v.production.mass_in_g * v.production.enrichment for v in dets.values()]
   >>> numpy.cumsum(data)[-1]
   [REDACTED]

Which channel IDs correspond to detectors in string 1?
------------------------------------------------------

.. code-block:: python

   >>> ids = lmeta.channelmap() \
   >>>            .map("location.string", unique=False)[1] \
   >>>            .map("daq.rawid").keys()
   dict_keys([1104000, 1104001, 1104002, 1104003, 1104004, 1104005, 1105600, 1105602, 1105603])

.. tip::

    ``ids`` can be directly given to
    :meth:`pygama.flow.data_loader.DataLoader.set_datastreams` to load LEGEND
    data from the channel.
