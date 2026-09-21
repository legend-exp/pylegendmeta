"""Tests for the merge-cal-groupings derivation."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from legendmeta import police


def _merge(cal, aspects):
    return police._merge_cal_groupings_data(cal, aspects)


def test_boundary_union_splits_the_merged_partition():
    """A boundary in EITHER aspect splits the merged partition."""
    psd = {"default": {"calgroup010a": {"p19": "r000..r005"}}}
    esc = {
        "default": {
            "calgroup010a": {"p19": "r000..r002"},
            "calgroup010b": {"p19": "r003..r005"},
        }
    }
    out = _merge({}, {"psd": psd, "escale": esc})
    assert out["default"] == {
        "calgroup010a": {"p19": "r000..r002"},
        "calgroup010b": {"p19": "r003..r005"},
    }


def test_exclusion_union_drops_the_run_from_the_merge():
    """A run excluded from either aspect is excluded from the merge."""
    psd = {"default": {"calgroup010a": {"p19": ["r000", "r001", "r003"]}}}
    esc = {"default": {"calgroup010a": {"p19": "r000..r003"}}}
    out = _merge({}, {"psd": psd, "escale": esc})
    assert out["default"]["calgroup010a"]["p19"] == ["r000", "r001", "r003"]


def test_holes_do_not_split_the_partition():
    """Runs sharing the same aspect-group pair stay one partition across a
    hole — the human hole-list idiom."""
    psd = {"default": {"calgroup010a": {"p19": ["r000..r003", "r006..r008"]}}}
    esc = {"default": {"calgroup010a": {"p19": ["r000..r003", "r006..r008"]}}}
    out = _merge({}, {"psd": psd, "escale": esc})
    assert out["default"] == {"calgroup010a": {"p19": ["r000..r003", "r006..r008"]}}


def test_per_detector_split_and_letter_assignment():
    psd = {
        "default": {"calgroup010a": {"p19": "r000..r005"}},
        "V90001A": {
            "calgroup010a": {"p19": "r000..r002"},
            "calgroup010b": {"p19": "r003..r005"},
        },
    }
    esc = {"default": {"calgroup010a": {"p19": "r000..r005"}}}
    out = _merge({}, {"psd": psd, "escale": esc})
    assert out["default"] == {"calgroup010a": {"p19": "r000..r005"}}
    assert out["V90001A"] == {
        "calgroup010a": {"p19": "r000..r002"},
        "calgroup010b": {"p19": "r003..r005"},
    }


def test_identical_override_is_dropped():
    psd = {
        "default": {"calgroup010a": {"p19": "r000..r005"}},
        "V90001A": {"calgroup010a": {"p19": "r000..r005"}},
    }
    esc = {"default": {"calgroup010a": {"p19": "r000..r005"}}}
    out = _merge({}, {"psd": psd, "escale": esc})
    assert "V90001A" not in out


def test_empty_detector_map_masks_the_default():
    """Aspect files with NO common runs for a detector must mask the derived
    default, never inherit it (found live: V06649M p16, psd r002..r006 vs
    escale r000 — empty intersection)."""
    psd = {
        "default": {"calgroup010a": {"p19": "r000..r005"}},
        "V90001A": {"calgroup010a": {"p19": ["r000"]}},
    }
    esc = {
        "default": {"calgroup010a": {"p19": "r000..r005"}},
        "V90001A": {"calgroup010a": {"p19": ["r003"]}},
    }
    out = _merge({}, {"psd": psd, "escale": esc})
    assert out["V90001A"] == {"calgroup010a": {"p19": []}}


def test_non_derived_periods_pass_through():
    cal = {
        "default": {"calgroup003a": {"p07": "r000..r004"}},
        "V90001A": {"calgroup003a": {"p07": ["r001", "r002"]}},
    }
    psd = {"default": {"calgroup010a": {"p19": "r000..r002"}}}
    esc = {"default": {"calgroup010a": {"p19": "r000..r002"}}}
    out = _merge(cal, {"psd": psd, "escale": esc})
    assert out["default"]["calgroup003a"] == {"p07": "r000..r004"}
    assert out["V90001A"] == {"calgroup003a": {"p07": ["r001", "r002"]}}
    assert out["default"]["calgroup010a"] == {"p19": "r000..r002"}


def test_derived_period_replaces_stale_main_content():
    cal = {"default": {"calgroup010a": {"p19": "r000..r009"}}}
    psd = {"default": {"calgroup010a": {"p19": "r000..r002"}}}
    esc = {"default": {"calgroup010a": {"p19": "r000..r002"}}}
    out = _merge(cal, {"psd": psd, "escale": esc})
    assert out["default"]["calgroup010a"] == {"p19": "r000..r002"}


def _write_root(tmp_path: Path, cal, psd, esc):
    (tmp_path / "groupings").mkdir()
    (tmp_path / "cal_groupings.yaml").write_text(yaml.safe_dump(cal, sort_keys=False))
    (tmp_path / "groupings" / "psd_cal_groupings.yaml").write_text(
        yaml.safe_dump(psd, sort_keys=False)
    )
    (tmp_path / "groupings" / "escale_cal_groupings.yaml").write_text(
        yaml.safe_dump(esc, sort_keys=False)
    )


def test_cli_autofix_contract(tmp_path, monkeypatch, capsys):
    """The hook rewrites cal_groupings.yaml and exits 1 on change; a second
    run is a clean no-op; the output passes the validator."""
    psd = {"default": {"calgroup010a": {"p19": "r000..r005"}}}
    esc = {
        "default": {
            "calgroup010a": {"p19": "r000..r002"},
            "calgroup010b": {"p19": "r003..r005"},
        }
    }
    _write_root(tmp_path, {}, psd, esc)
    argv = [
        "merge-cal-groupings",
        str(tmp_path / "groupings" / "psd_cal_groupings.yaml"),
    ]
    monkeypatch.setattr("sys.argv", argv)
    with pytest.raises(SystemExit) as e:
        police.merge_cal_groupings()
    assert e.value.code == 1
    merged = yaml.safe_load((tmp_path / "cal_groupings.yaml").read_text())
    assert merged["default"] == {
        "calgroup010a": {"p19": "r000..r002"},
        "calgroup010b": {"p19": "r003..r005"},
    }
    assert police._validate_groupings_file(
        str(tmp_path / "cal_groupings.yaml"), "calgroup", verbose=False
    )
    # second run: no change, returns without SystemExit
    monkeypatch.setattr("sys.argv", argv)
    police.merge_cal_groupings()
    capsys.readouterr()
