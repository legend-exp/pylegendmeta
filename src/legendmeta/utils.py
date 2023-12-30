from __future__ import annotations

import json
from pathlib import Path

import yaml

__file_extensions__ = {"json": [".json"], "yaml": [".yaml", ".yml"]}


def load_dict(fname: str, ftype: str | None = None) -> dict:
    """Load a text file as a Python dict."""
    fname = Path(fname)

    # determine file type from extension
    if ftype is None:
        for _ftype, exts in __file_extensions__.items():
            if fname.suffix in exts:
                ftype = _ftype

    with fname.open() as f:
        if ftype == "json":
            return json.load(f)
        if ftype == "yaml":
            return yaml.safe_load(f)

        msg = f"unsupported file format {ftype}"
        raise NotImplementedError(msg)
