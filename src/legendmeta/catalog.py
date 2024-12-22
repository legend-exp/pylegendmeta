from __future__ import annotations

import warnings

from dbetto.catalog import *  # noqa: F403


def to_datetime(value):
    return str_to_datetime(value)  # noqa: F405


warnings.warn(
    "The catalog module has moved renamed to the dbetto package (https://github.com/gipert/dbetto). "
    "Please update your code, as this module will be removed in a future release.",
    DeprecationWarning,
    stacklevel=2,
)
