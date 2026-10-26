"""Tests for the merge-cal-groupings derivation."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from legendmeta import police
from legendmeta.utils import expand_runs


def _merge(cal, aspects, overridden=frozenset()):
    return police._merge_cal_groupings_data(cal, aspects, overridden)


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


def test_empty_intersection_falls_back_to_escale():
    """Aspects that disagree with NO common run side with escale alone —
    the merged file's primary consumer is the energy partition calibration,
    and the human-curated cal_groupings made the same call (live cases:
    V06649M p16, V01240A p16+p18, V11925A p16)."""
    psd = {
        "default": {"calgroup010a": {"p19": "r000..r005"}},
        "V90001A": {"calgroup010a": {"p19": ["r000"]}},
    }
    esc = {
        "default": {"calgroup010a": {"p19": "r000..r005"}},
        "V90001A": {"calgroup010a": {"p19": ["r003"]}},
    }
    out = _merge({}, {"psd": psd, "escale": esc})
    assert out["V90001A"]["calgroup010a"]["p19"] == ["r003"]


def test_empty_intersection_without_escale_masks():
    """No escale runs to fall back to (psd-only coverage): the detector is
    masked from the derived default — an energy partition cannot be built
    from runs escale excluded."""
    psd = {
        "default": {"calgroup010a": {"p19": "r000..r005"}},
        "V90001A": {"calgroup010a": {"p19": ["r000"]}},
    }
    esc = {
        "default": {"calgroup010a": {"p19": "r000..r005"}},
        "V90001A": {"calgroup010a": {"p19": []}},
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


def _consumer_view(doc, det):
    """``{(period, run): group}`` as legend-dataflow ``CalGrouping`` sees it."""
    groups = dict(doc["default"])
    groups.update(doc.get(det, {}))
    return {
        (p, r): g
        for g, periods in groups.items()
        for p, runs in periods.items()
        for r in expand_runs(runs)
    }


MULTI = {"calgroup011a": {"p20": "r000..r002", "p21": "r000..r003"}}


def test_multi_period_override_keeps_all_periods():
    """A detector differing in one period of a multi-period partition must
    carry the partition's other periods too (the consumer replaces the whole
    partition) — live case V00074A p20-22."""
    psd = {
        "default": MULTI,
        "V90001A": {"calgroup011a": {"p20": ["r001"], "p21": "r000..r003"}},
    }
    esc = {"default": MULTI}
    out = _merge({}, {"psd": psd, "escale": esc})
    assert out["V90001A"] == {"calgroup011a": {"p20": ["r001"], "p21": "r000..r003"}}
    assert _consumer_view(out, "V90001A") == _consumer_view(psd, "V90001A")


def test_letters_are_assigned_across_periods():
    """Separate partitions in consecutive periods keep separate letters."""
    psd = {"default": MULTI}
    esc = {
        "default": MULTI,
        "V90001A": {
            "calgroup011a": {"p20": "r000..r002"},
            "calgroup011b": {"p21": "r000..r003"},
        },
    }
    out = _merge({}, {"psd": psd, "escale": esc})
    assert out["V90001A"] == {
        "calgroup011a": {"p20": "r000..r002"},
        "calgroup011b": {"p21": "r000..r003"},
    }


def test_fallback_is_decided_per_partition(capsys):
    """psd keeps one run of a multi-period partition: the aspects overlap, so
    there is no escale fallback in the periods psd leaves out."""
    psd = {"default": MULTI, "V90001A": {"calgroup011a": {"p20": ["r000"]}}}
    esc = {"default": MULTI}
    out = _merge({}, {"psd": psd, "escale": esc})
    assert out["V90001A"] == {"calgroup011a": {"p20": ["r000"]}}
    assert "WARNING" not in capsys.readouterr().out


def test_consumer_view_is_the_aspect_intersection():
    psd = {
        "default": MULTI,
        "V90001A": {
            "calgroup011a": {"p20": "r000..r002"},
            "calgroup011b": {"p21": "r000..r003"},
        },
        "V90002A": {"calgroup011a": {"p20": ["r000", "r002"], "p21": "r000..r003"}},
        "V90003A": {"calgroup011a": {"p20": ["r001"], "p21": "r000..r003"}},
    }
    esc = {
        "default": MULTI,
        "V90001A": {"calgroup011a": {"p20": "r001..r002", "p21": "r000..r002"}},
        "V90002A": {
            "calgroup011a": {"p20": "r000..r002", "p21": ["r000", "r001"]},
            "calgroup011b": {"p21": ["r002", "r003"]},
        },
    }
    out = _merge({}, {"psd": psd, "escale": esc})
    for det in ("V90001A", "V90002A", "V90003A"):
        a, b = _consumer_view(psd, det), _consumer_view(esc, det)
        assert set(_consumer_view(out, det)) == set(a) & set(b)


def _runinfo(spec):
    """``{"p20": ("cp", "cp")}`` -> runinfo with cal/phy keys per run."""
    return {
        p: {
            f"r{i:03d}": {t: {} for t in ("cal", "phy") if t[0] in kinds}
            for i, kinds in enumerate(runs)
        }
        for p, runs in spec.items()
    }


def _phy(cal, runinfo, periods=("p20", "p21"), phy=None, replaced=None):
    return police._merge_phy_groupings_data(
        phy or {}, cal, runinfo, set(periods), replaced
    )


RUNINFO = _runinfo({"p20": ("cp",) * 5, "p21": ("cp",) * 4})
RUNINFO_MULTI = _runinfo({"p20": ("cp",) * 3, "p21": ("cp",) * 4})  # fits MULTI


def test_phy_needs_the_next_cal_run():
    """A phy run is calibrated by the cal run before and after it."""
    cal = {"default": {"calgroup011a": {"p20": "r000..r003"}}}
    out = _phy(cal, RUNINFO)
    assert out["default"] == {"phygroup011a": {"p20": "r000..r002"}}


def test_phy_next_cal_run_may_open_the_next_period():
    out = _phy({"default": MULTI}, RUNINFO_MULTI)
    assert out["default"] == {
        "phygroup011a": {"p20": "r000..r002", "p21": "r000..r002"}
    }


def test_phy_hole_and_single_run_partition():
    cal = {
        "default": {"calgroup011a": {"p20": "r000..r004"}},
        "V90001A": {"calgroup011a": {"p20": ["r000", "r001", "r003", "r004"]}},
        "V90002A": {"calgroup011a": {"p20": ["r000"]}},
    }
    out = _phy(cal, RUNINFO)
    assert out["V90001A"] == {"phygroup011a": {"p20": ["r000", "r003"]}}
    assert out["V90002A"] == {"phygroup011a": {"p20": []}}


def test_phy_letters_follow_cal():
    cal = {
        "default": {"calgroup011a": {"p20": "r000..r004"}},
        "V90001A": {
            "calgroup011a": {"p20": ["r000"]},
            "calgroup011b": {"p20": "r001..r004"},
        },
    }
    out = _phy(cal, RUNINFO)
    assert out["V90001A"] == {
        "phygroup011a": {"p20": []},
        "phygroup011b": {"p20": "r001..r003"},
    }


def test_phy_skips_runs_without_phy_data():
    runinfo = _runinfo({"p20": ("cp", "c", "cp", "cp")})
    cal = {"default": {"calgroup011a": {"p20": "r000..r003"}}}
    out = _phy(cal, runinfo)
    assert out["default"] == {"phygroup011a": {"p20": ["r000", "r002"]}}


def test_phy_non_derived_periods_pass_through():
    phy = {
        "default": {"phygroup003a": {"p07": "r002..r007"}},
        "V90001A": {"phygroup003a": {"p07": ["r003"]}},
    }
    cal = {"default": {"calgroup011a": {"p20": "r000..r004"}}}
    out = _phy(cal, RUNINFO, phy=phy)
    assert out["default"]["phygroup003a"] == {"p07": "r002..r007"}
    assert out["V90001A"] == {"phygroup003a": {"p07": ["r003"]}}
    assert out["default"]["phygroup011a"] == {"p20": "r000..r003"}


def test_phy_cli_autofix_contract(tmp_path, monkeypatch, capsys):
    """The phy hook rewrites phy_groupings.yaml from cal_groupings.yaml and
    exits 1 on change; a second run is a clean no-op; the output passes the
    validator."""
    psd = esc = {"default": MULTI}
    _write_root(tmp_path, {}, psd, esc)
    (tmp_path / "runinfo.yaml").write_text(yaml.safe_dump(RUNINFO_MULTI))
    cal_file = str(tmp_path / "cal_groupings.yaml")
    for hook in (police.merge_cal_groupings, police.merge_phy_groupings):
        monkeypatch.setattr("sys.argv", ["hook", cal_file])
        with pytest.raises(SystemExit) as e:
            hook()
        assert e.value.code == 1
    merged = yaml.safe_load((tmp_path / "phy_groupings.yaml").read_text())
    assert merged["default"] == {
        "phygroup011a": {"p20": "r000..r002", "p21": "r000..r002"}
    }
    assert police._validate_groupings_file(
        str(tmp_path / "phy_groupings.yaml"), "phygroup", verbose=False
    )
    monkeypatch.setattr("sys.argv", ["merge-phy-groupings", cal_file])
    police.merge_phy_groupings()
    capsys.readouterr()


def test_overridden_cal_runs_are_left_out():
    """Cal runs replaced in run_override must not appear in cal_groupings
    (validate-cal-groupings rule) — live case p19 r000, r004."""
    psd = esc = {"default": {"calgroup010a": {"p19": "r000..r005"}}}
    out = _merge({}, {"psd": psd, "escale": esc}, {("p19", "r000"), ("p19", "r004")})
    assert out["default"]["calgroup010a"]["p19"] == ["r001..r003", "r005"]


def test_override_replacements():
    runinfo = {
        "p19": {
            f"r00{i}": {"cal": {"start_key": f"2025121{i}T000000Z"}} for i in range(4)
        }
    }
    entries = [
        {"valid_from": "20251210T000000Z", "apply": ["l200-p19-r001-cal-x"]},
        {"valid_from": "20251211T000000Z", "mode": "reset", "apply": []},
        {"valid_from": "20251213T000000Z", "apply": ["l200-p19-r002-cal-x"]},
    ]
    assert police._override_replacements(entries, runinfo) == {
        ("p19", "r000"): ("p19", "r001"),
        ("p19", "r003"): ("p19", "r002"),
    }


def test_phy_with_replaced_cal_runs():
    """A phy run whose cal is replaced is calibrated by the replacement; a
    replaced cal run never closes a bracket (live cases: p19 r000/r004 cal
    replaced by the next run; p16 r007 cal replaced by r006)."""
    replaced = {("p20", "r000"): ("p20", "r001"), ("p20", "r003"): ("p20", "r004")}
    cal = {"default": {"calgroup011a": {"p20": ["r001", "r002", "r004"]}}}
    out = _phy(cal, RUNINFO, replaced=replaced)
    assert out["default"] == {"phygroup011a": {"p20": "r000..r003"}}

    replaced = {("p20", "r004"): ("p20", "r003")}
    cal = {
        "default": {
            "calgroup011a": {"p20": "r000..r003"},
            "calgroup012a": {"p21": "r000..r003"},
        }
    }
    out = _phy(cal, RUNINFO, replaced=replaced)
    assert out["default"]["phygroup011a"] == {"p20": "r000..r002"}


def test_phy_one_run_cal_partition_gets_no_phy_runs():
    """A one-run cal partition is the "not partitioned" placeholder, even
    when a replaced cal run points into it (live case p19: r000 cal replaced
    by r001, psd-null detectors keep only r001)."""
    replaced = {("p20", "r000"): ("p20", "r001")}
    cal = {
        "default": {"calgroup011a": {"p20": "r001..r004"}},
        "V90001A": {"calgroup011a": {"p20": ["r001"]}},
    }
    out = _phy(cal, RUNINFO, replaced=replaced)
    assert out["default"]["phygroup011a"] == {"p20": "r000..r003"}
    assert out["V90001A"] == {"phygroup011a": {"p20": []}}
