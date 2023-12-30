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
from getpass import getuser
from pathlib import Path
from tempfile import gettempdir

from git import GitCommandError, InvalidGitRepositoryError, Repo

from .textdb import AttrsDict, TextDB

log = logging.getLogger(__name__)


class LegendMetadata(TextDB):
    """LEGEND metadata.

    Class representing the LEGEND metadata repository with utilities for fast
    access.

    Parameters
    ----------
    path
        path to legend-metadata repository. If not existing, will attempt a
        git-clone through SSH. If ``None``, legend-metadata will be cloned
        in a temporary directory (see :func:`tempfile.gettempdir`).
    **kwargs
        further keyword arguments forwarded to :math:`TextDB.__init__`.
    """

    def __init__(self, path: str | None = None, **kwargs) -> None:
        self._default_git_ref = "main"

        if isinstance(path, str):
            self._repo_path = path
        else:
            self._repo_path = os.getenv(
                "LEGEND_METADATA",
                str(Path(gettempdir()) / ("legend-metadata-" + getuser())),
            )

        self._repo: Repo = self._init_metadata_repo()

        super().__init__(self._repo_path, **kwargs)

    def _init_metadata_repo(self):
        """Clone legend-metadata, if not existing, and checkout default Git ref."""
        if not Path(self._repo_path).exists():
            Path(self._repo_path).mkdir()

        repo = None
        try:
            repo = Repo(self._repo_path)
        except InvalidGitRepositoryError:
            msg = f"Cloning git@github.com:legend-exp/legend-metadata in {self._repo_path}..."
            log.info(msg)

            repo = Repo.clone_from(
                "git@github.com:legend-exp/legend-metadata",
                self._repo_path,
                multi_options=["--recurse-submodules"],
            )
            repo.git.checkout(self._default_git_ref)

        return repo

    def checkout(self, git_ref: str) -> None:
        """Select a legend-metadata version."""
        try:
            self._repo.git.checkout(git_ref)
            self._repo.git.submodule("update", "--init")
        except GitCommandError:
            self._repo.remote().pull()
            self._repo.git.checkout(git_ref)
            self._repo.git.submodule("update", "--init")

    def reset(self) -> None:
        """Checkout legend-metadata to the default Git ref."""
        self._repo.git.checkout(self._default_git_ref)

    def channelmap(self, on: str | datetime | None = None) -> AttrsDict:
        """Get a LEGEND channel map.

        Aliases ``legend-metadata.hardware.configuration.channelmaps.on()`` and
        tries to merge the returned channel map with the detector database
        `legend-metadata.hardware.detectors` and the analysis channel map
        `dataprod.config.on(...).analysis`.

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
            on, pattern=None, system="all"
        )

        # get analysis metadata
        anamap = self.dataprod.config.on(on, pattern=None, system="all").analysis

        # get full detector db
        detdb = self.hardware.detectors
        fulldb = detdb.germanium.diodes | detdb.lar.sipms

        for det in chmap:
            # find channel info in detector database and merge it into
            # channelmap item, if possible
            if det in fulldb:
                chmap[det] |= fulldb[det]
            else:
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
