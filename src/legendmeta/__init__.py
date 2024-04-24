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

"""A package to access `legend-metadata <https://github.com/legend-exp/legend-metadata>`_ in Python."""

from __future__ import annotations

from ._version import version as __version__
from .catalog import to_datetime
from .core import LegendMetadata
from .slowcontrol import LegendSlowControlDB
from .textdb import AttrsDict, JsonDB, TextDB

__all__ = [
    "__version__",
    "LegendMetadata",
    "LegendSlowControlDB",
    "JsonDB",
    "TextDB",
    "AttrsDict",
    "to_datetime",
]
