Kitchen Sink
============

An unstructured collection of examples and frequently metadata-related
computations.

- **How many kilograms of germanium are currently deployed in the LEGEND cryostat?**

.. code-block:: python
   :linenos:

   from legendmeta import LegendMetadata
   from datetime import datetime

   lmeta = LegendMetadata()
   mass = 0

   for det, val in lmeta.channelmap(datetime.now()).items():
       if val.system == "geds":
           mass += val.production.mass_in_g / 1000

   print(f"mass = {mass} kg") # prints: mass = [REDACTED] kg
