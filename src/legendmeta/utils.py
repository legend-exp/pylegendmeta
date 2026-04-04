from __future__ import annotations

import re

_RUN_RANGE_PATTERN = re.compile(r"^r\d{3}\.\.r\d{3}$")
_RUN_PATTERN = re.compile(r"^r\d{3}$")


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
