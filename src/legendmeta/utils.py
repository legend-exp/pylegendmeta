from __future__ import annotations

import re
from dataclasses import dataclass

from dbetto import str_to_datetime

_RUN_RANGE_PATTERN = re.compile(r"^r\d{3}\.\.r\d{3}$")
_RUN_PATTERN = re.compile(r"^r\d{3}$")

_FILE_KEY_PATTERN = re.compile(r"^[^-]+-[^-]+-[^-]+-[^-]+-\d{8}T\d{6}Z$")


@dataclass(frozen=True, init=False)
class FileKey:
    """A parsed LEGEND file key of the form ``experiment-period-run-category-timestamp``."""

    experiment: str
    period: str
    run: str
    category: str
    timestamp: str

    def __init__(self, key: str) -> None:
        if not _FILE_KEY_PATTERN.match(key):
            msg = f"invalid file key format: {key!r}"
            raise ValueError(msg)
        experiment, period, run, category, timestamp = key.split("-", 4)
        try:
            str_to_datetime(timestamp)
        except ValueError as e:
            msg = f"invalid file key format: {key!r}"
            raise ValueError(msg) from e
        object.__setattr__(self, "experiment", experiment)
        object.__setattr__(self, "period", period)
        object.__setattr__(self, "run", run)
        object.__setattr__(self, "category", category)
        object.__setattr__(self, "timestamp", timestamp)

    def __str__(self) -> str:
        return f"{self.experiment}-{self.period}-{self.run}-{self.category}-{self.timestamp}"

    @property
    def datetime(self):
        """The timestamp as a :class:`datetime.datetime` object."""
        return str_to_datetime(self.timestamp)


def expand_runs(spec: object) -> list[str]:
    """Expand a run spec (string or list) to a flat list of individual run strings."""
    if isinstance(spec, str):
        m = _RUN_RANGE_PATTERN.match(spec)
        if m:
            start = int(spec[1:4])
            end = int(spec[7:10])
            return [f"r{i:03d}" for i in range(start, end + 1)]
        if _RUN_PATTERN.match(spec):
            return [spec]
        return []
    if isinstance(spec, list):
        result = []
        for item in spec:
            result.extend(expand_runs(str(item)))
        return result
    return []
