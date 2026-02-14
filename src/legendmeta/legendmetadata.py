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
from datetime import datetime

from dbetto import AttrsDict
from packaging.version import Version

from .core import MetadataRepository

log = logging.getLogger(__name__)


class LegendMetadata(MetadataRepository):
    """LEGEND metadata.

    Class representing the LEGEND metadata repository with utilities for fast
    access.

    If no valid path to an existing legend-metadata directory is provided, will
    attempt to clone https://github.com/legend-exp/legend-metadata via SSH and
    git-checkout the latest stable tag (vM.m.p format).

    Parameters
    ----------
    path
        path to legend-metadata repository. If not existing, will attempt a
        git-clone through SSH. If ``None``, legend-metadata will be cloned
        in a temporary directory (see :func:`tempfile.gettempdir`).
    **kwargs
        further keyword arguments forwarded to :class:`TextDB.__init__`.
    """

    def __init__(self, path: str | None = None, **kwargs) -> None:
        super().__init__(
            path=path,
            repo_url="git@github.com:legend-exp/legend-metadata",
            env_var="LEGEND_METADATA",
            default_dir_name="legend-metadata-",
            **kwargs,
        )

    def channelmap(
        self, on: str | datetime | None = None, system: str = "all"
    ) -> AttrsDict:
        """Get a LEGEND channel map.

        Aliases ``legend-metadata.hardware.configuration.channelmaps.on()`` and
        tries to merge the returned channel map with the detector database
        `legend-metadata.hardware.detectors` and the analysis channel map
        `dataprod.config.on(...).analysis`.

        Parameters
        ----------
        on
            a :class:`~datetime.datetime` object or a string matching the
            pattern ``YYYYmmddTHHMMSSZ``.
        system: 'all', 'phy', 'cal', 'lar', ...
            query only a data taking "system".

        Warning
        -------
        This method assumes ``legend-exp/legend-metadata`` has a certain
        layout. Might stop working if changes are made to the structure of the
        repository.

        Examples
        --------
        >>> from legendmeta import LegendMetadata
        >>> from datetime import datetime
        >>> channel = lmeta.channelmap(on=datetime.now()).V05267B
        >>> channel.geometry.mass_in_g
        2362.0
        >>> channel.analysis.usability
        'on'

        See Also
        --------
        .textdb.TextDB.on
        """
        if on is None:
            on = datetime.now()

        chmap = self.hardware.configuration.channelmaps.on(
            on, pattern=None, system=system
        )

        # get analysis metadata
        if self.__closest_tag__ < Version("v0.5.9") or self.__version__ == "v0.5.9":
            anamap = self.dataprod.config.on(on, pattern=None, system=system).analysis
        else:
            anamap = self.datasets.statuses.on(on, pattern=None, system=system)

        # get full detector db
        detdb = self.hardware.detectors

        for det in chmap:
            # find channel info in detector database and merge it into
            # channelmap item, if possible
            try:
                if chmap[det]["system"] == "geds":
                    chmap[det] |= detdb.germanium.diodes[det]
                else:
                    chmap[det] |= detdb.lar.sipms[det]
            except (KeyError, FileNotFoundError):
                msg = f"Could not find detector '{det}' in hardware.detectors database"
                log.debug(msg)

            # find channel info in analysis database and add it into channelmap
            # item under "analysis", if possible
            if det in anamap:
                chmap[det]["analysis"] = anamap[det]
            else:
                msg = f"Could not find detector '{det}' in dataprod.config database"
                log.debug(msg)

        return chmap
