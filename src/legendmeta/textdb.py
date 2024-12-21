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

import dbetto


class AttrsDict(dbetto.AttrsDict):
    def __init__(self, *args, **kwargs):
        import warnings

        warnings.warn(
            "The AttrsDict class has moved to the dbetto package (https://github.com/gipert/dbetto). "
            "Please update your code, as AttrsDB will be removed from this package in the future.",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)


class JsonDB(dbetto.TextDB):
    def __init__(self, *args, **kwargs):
        import warnings

        warnings.warn(
            "The JsonDB class has been renamed to TextDB. "
            "Please update your code, as JsonDB will be removed in a future release.",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)


class TextDB(dbetto.TextDB):
    def __init__(self, *args, **kwargs):
        import warnings

        warnings.warn(
            "The TextDB class has moved to the dbetto package (https://github.com/gipert/dbetto). "
            "Please update your code, as TextDB will be removed from this package in the future.",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)
