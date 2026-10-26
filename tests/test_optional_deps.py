from __future__ import annotations

import subprocess
import sys


def _run_without_polars(code):
    # sys.modules["polars"] = None makes any later "import polars" fail
    return subprocess.run(
        [sys.executable, "-c", f"import sys; sys.modules['polars'] = None; {code}"],
        capture_output=True,
        text=True,
        check=False,
    )


def test_import_without_polars():
    assert _run_without_polars("import legendmeta").returncode == 0

    res = _run_without_polars("import legendmeta.tables")
    assert res.returncode != 0
    assert "pip install 'pylegendmeta[tables]'" in res.stderr


def test_import_without_matplotlib():
    code = "import sys; sys.modules['matplotlib'] = None; import legendmeta.vis"
    res = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=False
    )
    assert res.returncode != 0
    assert "pip install 'pylegendmeta[vis]'" in res.stderr
