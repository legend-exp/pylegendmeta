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
import re
from datetime import datetime
from getpass import getuser
from pathlib import Path
from tempfile import gettempdir

from dbetto import AttrsDict, TextDB
from git import GitCommandError, InvalidGitRepositoryError, Repo
from packaging.version import Version

log = logging.getLogger(__name__)


class LegendMetadata(TextDB):
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
        further keyword arguments forwarded to :math:`TextDB.__init__`.
    """

    def __init__(self, path: str | None = None, **kwargs) -> None:
        if isinstance(path, (str, Path)):
            self.__repo_path__ = path
        else:
            self.__repo_path__ = os.getenv("LEGEND_METADATA", "")

        if self.__repo_path__ == "":
            self.__repo_path__ = str(
                Path(gettempdir()) / ("legend-metadata-" + getuser())
            )

        # self.__repo__: Repo =
        self._init_metadata_repo()

        super().__init__(self.__repo_path__, **kwargs)

    def _init_metadata_repo(self) -> None:
        """Clone legend-metadata, if not existing, and checkout latest stable tag."""
        exp_path = os.path.expandvars(self.__repo_path__)
        while self.__repo_path__ != exp_path:
            self.__repo_path__ = exp_path
            exp_path = os.path.expandvars(self.__repo_path__)

        if not Path(self.__repo_path__).exists():
            msg = f"mkdir {self.__repo_path__}"
            log.debug(msg)
            Path(self.__repo_path__).mkdir()

        try:
            msg = f"trying to load Git repo in {self.__repo_path__}"
            log.debug(msg)
            self.__repo__ = Repo(self.__repo_path__)

        except InvalidGitRepositoryError:
            msg = f"Cloning git@github.com:legend-exp/legend-metadata in {self.__repo_path__}..."
            # set logging level as warning (default logging level), so it's
            # always printed and the user knows why it takes so long to initialize
            log.warning(msg)

            self.__repo__ = Repo.clone_from(
                "git@github.com:legend-exp/legend-metadata",
                self.__repo_path__,
                multi_options=["--recurse-submodules"],
            )

            # checkout legend-metadata at its latest stable tag
            if self.latest_stable_tag is not None:
                msg = (
                    f"Checking out the latest stable tag ({self.latest_stable_tag})..."
                )
                log.warning(msg)

                self.checkout(self.latest_stable_tag, rescan=False)
            else:
                msg = "No stable tags found, checking out the default branch"
                log.warning(msg)

    @property
    def latest_stable_tag(self) -> Version | None:
        """Latest stable legend-metadata tag (i.e. strictly numeric vM.m.p)"""
        tag_list = [tag.name for tag in self.__repo__.tags]

        version_regex = re.compile(r"^v\d+\.\d+\.\d+$")
        version_tags = [t for t in tag_list if version_regex.match(t)]

        if not version_tags:
            log.warning(
                "No valid version tags (vM.m.p) found in this repository, "
                "defaulting to the current Git ref."
            )
            return None

        # drop the leading 'v'
        version_tags.sort(key=lambda t: Version(t[1:]))

        return version_tags[-1]

    def checkout(self, git_ref: str | Version, rescan: bool = True) -> None:
        """Select a legend-metadata version."""
        if isinstance(git_ref, Version):
            git_ref = "v" + str(git_ref)

        try:
            self.__repo__.git.checkout(git_ref)
            self.__repo__.git.submodule("update", "--init")
        except GitCommandError:
            self.__repo__.remote().pull()
            self.__repo__.git.checkout(git_ref)
            self.__repo__.git.submodule("update", "--init")

        # now reset this TextDB instance
        super().reset(rescan=rescan)

    @property
    def __version__(self) -> str:
        """legend-metadata version.

        Calculated with ``git describe``, looking for the closest tag with a
        name based on semantic versioning.
        """
        return self.__repo__.git.describe(
            "--tags", "--always", "--match=v[0-9]*[0-9]*[0-9]*"
        )

    @property
    def __closest_tag__(self) -> Version:
        """legend-metadata Git tag closest to the current commit.

        Calculated with ``git describe``, looking for the closest tag with a
        name based on semantic versioning.
        """
        return Version(
            self.__repo__.git.describe(
                "--tags", "--always", "--match=v[0-9]*[0-9]*[0-9]*", "--abbrev=0"
            )
        )

    def show_metadata_version(self) -> None:
        """Logs version info for legend-metadata repository and all its submodules."""

        print(f"{self.__repo__.working_dir}: {self.__version__}")  # noqa: T201

        submods = self.__repo__.submodules
        for i, s in enumerate(submods):
            char = "└──" if i == len(submods) - 1 else "├──"
            version = s.module().git.describe("--tags", "--always")
            print(f"{char} {s.name}: {version}")  # noqa: T201

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
