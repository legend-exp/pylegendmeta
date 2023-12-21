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

>>> mass = 0
>>> for det, val in lmeta.channelmap(datetime.now()).items():
>>>     if val.system == "geds":
>>>         mass += val.production.mass_in_g
>>> mass/1000 # in kg
[REDACTED]

or, alternatively:

>>> # get only HPGe channels by mapping for "system"
>>> geds = lmeta.channelmap(datetime.now()).map("system", unique=False).geds
>>> # collect and sum up masses
>>> masses = [v.production.mass_in_g for v in geds.values()]
>>> numpy.cumsum(masses)[-1]
[REDACTED]


How many kilograms of Ge76 are currently deployed in the form of "ON" ICPC detectors?
-------------------------------------------------------------------------------------

Calls to :meth:`.AttrsDict.map` can be chained together to build complex queries:

>>> # get HPGes, only ICPCs and only if their analysis status is ON
>>> dets = (
...     lmeta.channelmap(datetime.now())
...     .map("system", unique=False).geds
...     .map("type", unique=False).icpc
...     .map("analysis.usability", unique=False).on
...)
>>> # collect and sum up mass * enrichment (assuming that the enrichment fraction is also in mass)
>>> data = [v.production.mass_in_g * v.production.enrichment for v in dets.values()]
>>> numpy.cumsum(data)[-1]
[REDACTED]


How many kilograms of germanium were not "OFF" on 23 Aug 2023?
--------------------------------------------------------------

>>> geds = (
...     lmeta.channelmap(datetime(2023, 8, 23))
...     .map("system", unique=False).geds
...     .map("analysis.usability", unique=False)
...)
>>> mass = 0
>>>
>>> for status in ("on", "ac", "no_psd"):
>>>     for info in geds[status].values():
>>>         mass += info.production.mass_in_g
>>> mass
[REDACTED]


Which channel IDs correspond to detectors in string 1?
------------------------------------------------------

>>> ids = (
...    lmeta.channelmap()
...    .map("location.string", unique=False)[1]
...    .map("daq.rawid")
...).keys()
dict_keys([1104000, 1104001, 1104002, 1104003, 1104004, 1104005, 1105600, 1105602, 1105603])

.. tip::

    ``ids`` can be directly given to
    :meth:`pygama.flow.data_loader.DataLoader.set_datastreams` to load LEGEND
    data from the channel.


When did physics run 3 of LEGEND-200 period 4 start?
----------------------------------------------------

>>> from legendmeta import to_datetime
>>> to_datetime(lmeta.dataprod.runinfo.p04.r003.phy.start_key)
datetime.datetime(2023, 5, 1, 20, 59, 51)


What is the current amount of exposure of HPGes usable for analysis?
--------------------------------------------------------------------------------------

.. code-block:: python
   :linenos:

   exposure = 0

   for period, runs in lmeta.dataprod.config.analysis_runs.items():
       for run in runs:
           if "phy" not in lmeta.dataprod.runinfo[period][run]:
               continue

           runinfo = lmeta.dataprod.runinfo[period][run].phy
           chmap = lmeta.channelmap(runinfo.start_key).map("system", unique=False).geds

           for _, gedet in chmap.items():
               if gedet.analysis.usability not in ("off", "ac"):
                   exposure += (
                       gedet.production.mass_in_g
                       / 1000
                       * runinfo.livetime_in_s
                       / 60
                       / 60
                       / 24
                       / 365
                   )

   print(exposure, "kg yr")

What is the exposure of each single HPGe usable for analysis over a selection of runs?
--------------------------------------------------------------------------------------

.. code-block:: python
   :linenos:

   runs = {
       "p03": ["r000", "r001", "r002", "r003", "r004", "r005"],
       "p04": ["r000", "r001", "r002", "r003"],
   }

   exposures = {}

   for period, v in runs.items():
       for run in v:
           runinfo = lmeta.dataprod.runinfo[period][run].phy
           chmap = lmeta.channelmap(runinfo.start_key)

           chmap = (
               chmap.map("system", unique=False)
               .geds.map("analysis.usability", unique=False)
               .on
           )

           for _, gedet in chmap.items():
               exposures.setdefault(gedet.name, 0)
               exposures[gedet.name] += (
                   gedet.production.mass_in_g
                   / 1000
                   * runinfo.livetime_in_s
                   / 60
                   / 60
                   / 24
                   / 365
               )

   print(exposures)
