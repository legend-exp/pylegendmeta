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

from .core import MetadataRepository


class HadesMetadata(MetadataRepository):
    """HADES metadata.

    Class representing the HADES metadata repository with utilities for fast
    access.

    If no valid path to an existing hades-metadata directory is provided, will
    attempt to clone https://github.com/legend-exp/hades-metadata via SSH and
    git-checkout the latest stable tag (vM.m.p format).

    Parameters
    ----------
    path
        path to hades-metadata repository. If not existing, will attempt a
        git-clone through SSH. If ``None``, hades-metadata will be cloned
        in a temporary directory (see :func:`tempfile.gettempdir`).
    **kwargs
        further keyword arguments forwarded to :class:`TextDB.__init__`.
    """

    def __init__(self, path: str | None = None, **kwargs) -> None:
        super().__init__(
            path=path,
            repo_url="git@github.com:legend-exp/hades-metadata",
            env_var="HADES_METADATA",
            default_dir_name="hades-metadata-",
            **kwargs,
        )
