from __future__ import annotations

import sys
import textwrap
from copy import deepcopy
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from legendmeta import police


def test_len_nested():
    assert police.len_nested({}) == 0
    assert police.len_nested({"a": 1}) == 1
    assert police.len_nested({"a": 1, "b": 2}) == 2
    assert police.len_nested({"a": 1, "b": {"c": 2}}) == 3
    assert police.len_nested({"a": 1, "b": {"c": 2, "d": "boh"}}) == 4


def test_validate_keys():
    template = {"a": 1, "b": 2, "c": {"d": "sick!", "e": 420}}

    assert police.validate_keys_recursive(template, template)
    assert police.validate_keys_recursive({"a": 3}, template)
    assert police.validate_keys_recursive({"a": 3, "c": {"d": -1}}, template)

    assert not police.validate_keys_recursive({}, template)
    assert not police.validate_keys_recursive({"z": 1}, template)
    assert not police.validate_keys_recursive({"z": 1, "h": {"b": 2}}, template)
    assert not police.validate_keys_recursive({"c": {"b": 2}}, template)


def test_validate_dict_schema():
    template = {"a": 0, "b": 0, "c": {"d": r"^\d*$", "e": 4.2}}

    case = {"a": 3, "b": 65, "c": {"d": "123", "e": 6.9}}

    assert police.validate_dict_schema(case, template)

    case1 = deepcopy(case)
    case1["c"]["d"] = "sick!"
    assert not police.validate_dict_schema(case1, template)

    case1 = deepcopy(case)
    case1["a"] = "sick!"
    assert not police.validate_dict_schema(case1, template)

    case1 = deepcopy(case)
    case1["a"] = 3.3
    assert not police.validate_dict_schema(case1, template)

    case1 = deepcopy(case)
    case1["z"] = 1
    assert not police.validate_dict_schema(case1, template)
    assert police.validate_dict_schema(case1, template, greedy=False)

    case1 = deepcopy(case)
    case1["c"]["z"] = 1
    assert not police.validate_dict_schema(case1, template)


# ---------------------------------------------------------------------------
# _get_overridden_runs
# ---------------------------------------------------------------------------

# run_override windows: apply is non-empty between two timestamps.
# Any run whose cal.start_key falls in [window_start, window_end) is overridden —
# the window can cover multiple calibrations, not just the one at the boundary.
_RUNINFO = {
    "p09": {
        # r004's cal starts at window open — overridden
        "r004": {
            "cal": {"start_key": "20240205T154037Z"},
            "phy": {"start_key": "x", "livetime_in_s": 1},
        },
        # r004b has a cal strictly inside the window — also overridden
        "r004b": {
            "cal": {"start_key": "20240208T000000Z"},
            "phy": {"start_key": "x", "livetime_in_s": 1},
        },
        # r005's cal is exactly at window close (the reset) — NOT overridden
        "r005": {
            "cal": {"start_key": "20240212T150814Z"},
            "phy": {"start_key": "x", "livetime_in_s": 1},
        },
        "r006": {
            "cal": {"start_key": "20240218T000000Z"},
            "phy": {"start_key": "x", "livetime_in_s": 1},
        },
    },
    "p19": {
        # r000's cal is overridden
        "r000": {
            "cal": {"start_key": "20251211T194309Z"},
            "phy": {"start_key": "x", "livetime_in_s": 1},
        },
        "r001": {
            "cal": {"start_key": "20251218T191855Z"},
            "phy": {"start_key": "x", "livetime_in_s": 1},
        },
        # r004's cal is overridden
        "r004": {
            "cal": {"start_key": "20260109T171806Z"},
            "phy": {"start_key": "x", "livetime_in_s": 1},
        },
        "r005": {
            "cal": {"start_key": "20260116T191157Z"},
            "phy": {"start_key": "x", "livetime_in_s": 1},
        },
    },
    "p03": {
        "r000": {"phy": {"start_key": "x", "livetime_in_s": 1}},  # no cal key
    },
}

_RUN_OVERRIDE_ENTRIES = [
    {"valid_from": "20240205T154037Z", "apply": ["l200-p09-r005-cal-20240212T150814Z"]},
    {"valid_from": "20240212T150814Z", "mode": "reset", "apply": []},
    {"valid_from": "20251211T194309Z", "apply": ["l200-p19-r001-cal-20251218T191855Z"]},
    {"valid_from": "20251218T191855Z", "mode": "reset", "apply": []},
    {"valid_from": "20260109T171806Z", "apply": ["l200-p19-r005-cal-20260116T191157Z"]},
    {"valid_from": "20260116T191157Z", "mode": "reset", "apply": []},
]


def test_get_overridden_runs_identifies_overridden():
    overridden = police._get_overridden_runs(_RUN_OVERRIDE_ENTRIES, _RUNINFO)
    # at window boundary
    assert ("p09", "r004") in overridden
    assert ("p19", "r000") in overridden
    assert ("p19", "r004") in overridden
    # strictly inside a window (not at the boundary) — also overridden
    assert ("p09", "r004b") in overridden


def test_get_overridden_runs_excludes_non_overridden():
    overridden = police._get_overridden_runs(_RUN_OVERRIDE_ENTRIES, _RUNINFO)
    # cal at exactly the reset timestamp (window close) is NOT overridden
    assert ("p09", "r005") not in overridden
    assert ("p09", "r006") not in overridden
    assert ("p19", "r001") not in overridden
    assert ("p19", "r005") not in overridden
    # run with no cal key is never overridden
    assert ("p03", "r000") not in overridden


def test_get_overridden_runs_empty_override():
    overridden = police._get_overridden_runs([], _RUNINFO)
    assert overridden == set()


def test_get_overridden_runs_all_reset_entries():
    entries = [{"valid_from": "20240212T150814Z", "mode": "reset", "apply": []}]
    overridden = police._get_overridden_runs(entries, _RUNINFO)
    assert overridden == set()


# ---------------------------------------------------------------------------
# _validate_groupings_file — sort and override checks
# ---------------------------------------------------------------------------

_GOOD_CAL_GROUPINGS = textwrap.dedent("""\
    default:
      calgroup001a:
        p03: r000..r005
      calgroup002a:
        p06: r000..r005
    B00002A:
      calgroup001a:
        p03: r000..r003
    V08682A:
      calgroup002a:
        p06: r001..r005
""")


def _write(tmp_path: Path, name: str, content: str) -> str:
    p = tmp_path / name
    p.write_text(content)
    return str(p)


def test_groupings_valid(tmp_path):
    f = _write(tmp_path, "cal_groupings.yaml", _GOOD_CAL_GROUPINGS)
    assert police._validate_groupings_file(f, "calgroup", verbose=False)


def test_groupings_default_not_first(tmp_path):
    bad = textwrap.dedent("""\
        B00002A:
          calgroup001a:
            p03: r000..r003
        default:
          calgroup001a:
            p03: r000..r005
    """)
    f = _write(tmp_path, "cal_groupings.yaml", bad)
    assert not police._validate_groupings_file(f, "calgroup", verbose=False)


def test_groupings_top_keys_unsorted(tmp_path):
    bad = textwrap.dedent("""\
        default:
          calgroup001a:
            p03: r000..r005
        V08682A:
          calgroup001a:
            p03: r001..r003
        B00002A:
          calgroup001a:
            p03: r000..r002
    """)
    f = _write(tmp_path, "cal_groupings.yaml", bad)
    assert not police._validate_groupings_file(f, "calgroup", verbose=False)


def test_groupings_groups_unsorted(tmp_path):
    bad = textwrap.dedent("""\
        default:
          calgroup002a:
            p06: r000..r005
          calgroup001a:
            p03: r000..r005
    """)
    f = _write(tmp_path, "cal_groupings.yaml", bad)
    assert not police._validate_groupings_file(f, "calgroup", verbose=False)


def test_groupings_periods_unsorted(tmp_path):
    bad = textwrap.dedent("""\
        default:
          calgroup001a:
            p06: r000..r005
            p03: r000..r004
    """)
    f = _write(tmp_path, "cal_groupings.yaml", bad)
    assert not police._validate_groupings_file(f, "calgroup", verbose=False)


def test_groupings_run_list_unsorted(tmp_path):
    bad = textwrap.dedent("""\
        default:
          calgroup001a:
            p09:
              - r003
              - r001
              - r005
    """)
    f = _write(tmp_path, "cal_groupings.yaml", bad)
    assert not police._validate_groupings_file(f, "calgroup", verbose=False)


def test_groupings_run_list_sorted(tmp_path):
    good = textwrap.dedent("""\
        default:
          calgroup001a:
            p09:
              - r001
              - r003
              - r005
    """)
    f = _write(tmp_path, "cal_groupings.yaml", good)
    assert police._validate_groupings_file(f, "calgroup", verbose=False)


def test_groupings_overridden_run_flagged(tmp_path):
    f = _write(tmp_path, "cal_groupings.yaml", _GOOD_CAL_GROUPINGS)
    overridden = {("p03", "r002")}
    assert not police._check_cal_override_runs(f, overridden, verbose=False)


def test_groupings_overridden_run_not_present_passes(tmp_path):
    f = _write(tmp_path, "cal_groupings.yaml", _GOOD_CAL_GROUPINGS)
    # p09/r005 is overridden but p09 doesn't appear in the file at all
    overridden = {("p09", "r005")}
    assert police._check_cal_override_runs(f, overridden, verbose=False)


# ---------------------------------------------------------------------------
# _check_chmap_key_name — channel map key vs name field
# ---------------------------------------------------------------------------


def test_chmap_key_name_absent_passes():
    """Entry without a 'name' field is always valid."""
    assert police._check_chmap_key_name("V01234A", {"system": "geds"})


def test_chmap_key_name_match_passes():
    """Entry whose 'name' matches the key is valid."""
    assert police._check_chmap_key_name(
        "V01234A", {"name": "V01234A", "system": "geds"}
    )


def test_chmap_key_name_mismatch_fails():
    """Entry whose 'name' differs from the key is invalid."""
    assert not police._check_chmap_key_name(
        "V01234A", {"name": "V99999Z", "system": "geds"}, verbose=False
    )


# ---------------------------------------------------------------------------
# validate_statuses — status key membership in channel map
# ---------------------------------------------------------------------------


def _write_status_files(tmp_path: Path, status_content: str) -> Path:
    """Create a minimal status validity file and data file in ``tmp_path``."""
    validity = textwrap.dedent("""\
        - valid_from: "20230101T000000Z"
          apply:
            - status.yaml
    """)
    (tmp_path / "validity.yaml").write_text(validity)
    (tmp_path / "status.yaml").write_text(status_content)
    return tmp_path / "validity.yaml"


def test_validate_statuses_key_not_in_chmap(tmp_path, monkeypatch):
    """validate_statuses fails when a status key is absent from the channel map."""
    # "ch_missing" is not a GE/SiPM channel, so only the chmap check fires.
    validity_file = _write_status_files(
        tmp_path,
        textwrap.dedent("""\
            ch_missing:
              flag: true
        """),
    )

    mock_meta = MagicMock()
    mock_meta.hardware.configuration.channelmaps.on.return_value = {}

    monkeypatch.setattr(sys, "argv", ["validate-statuses", str(validity_file)])
    with (
        patch("legendmeta.police.LegendMetadata", return_value=mock_meta),
        pytest.raises(SystemExit),
    ):
        police.validate_statuses()


def test_validate_statuses_key_in_chmap(tmp_path, monkeypatch):
    """validate_statuses passes when every status key is present in the channel map."""
    validity_file = _write_status_files(
        tmp_path,
        textwrap.dedent("""\
            ch_present:
              flag: true
        """),
    )

    mock_meta = MagicMock()
    mock_meta.hardware.configuration.channelmaps.on.return_value = {"ch_present": {}}

    monkeypatch.setattr(sys, "argv", ["validate-statuses", str(validity_file)])
    with patch("legendmeta.police.LegendMetadata", return_value=mock_meta):
        police.validate_statuses()  # should not raise


def test_validate_statuses_chmap_unavailable_skips_check(tmp_path, monkeypatch):
    """When the channel map cannot be loaded the membership check is silently skipped."""
    validity_file = _write_status_files(
        tmp_path,
        textwrap.dedent("""\
            ch_anything:
              flag: true
        """),
    )

    mock_meta = MagicMock()
    mock_meta.hardware.configuration.channelmaps.on.side_effect = Exception("no repo")

    monkeypatch.setattr(sys, "argv", ["validate-statuses", str(validity_file)])
    with patch("legendmeta.police.LegendMetadata", return_value=mock_meta):
        police.validate_statuses()  # chmap unavailable → no error


# _needs_reorder
# ---------------------------------------------------------------------------


def test_needs_reorder_same_order():
    assert not police._needs_reorder({"a": 1, "b": 2}, {"a": 1, "b": 2})


def test_needs_reorder_different_order():
    assert police._needs_reorder({"b": 2, "a": 1}, {"a": 1, "b": 2})


def test_needs_reorder_nested_same():
    a = {"x": {"b": 2, "a": 1}}
    b = {"x": {"b": 2, "a": 1}}
    assert not police._needs_reorder(a, b)


def test_needs_reorder_nested_different():
    a = {"x": {"b": 2, "a": 1}}
    b = {"x": {"a": 1, "b": 2}}
    assert police._needs_reorder(a, b)


def test_needs_reorder_non_dict_values_ignored():
    # Only key order matters; non-dict values are not compared
    assert not police._needs_reorder({"a": [3, 1, 2]}, {"a": [1, 2, 3]})


# ---------------------------------------------------------------------------
# _sort_status_entry
# ---------------------------------------------------------------------------

_FULL_ENTRY_SORTED = {
    "reason": "",
    "usability": "on",
    "processable": True,
    "is_blinded": True,
    "psd": {
        "is_bb_like": "low_aoe & high_aoe",
        "status": {"low_aoe": "valid", "high_aoe": "valid"},
    },
}


def test_sort_status_entry_already_sorted():
    result = police._sort_status_entry(_FULL_ENTRY_SORTED)
    assert list(result.keys()) == list(_FULL_ENTRY_SORTED.keys())
    assert list(result["psd"].keys()) == list(_FULL_ENTRY_SORTED["psd"].keys())


def test_sort_status_entry_reorders_keys():
    scrambled = {
        "processable": True,
        "is_blinded": True,
        "usability": "on",
        "reason": "",
        "psd": {
            "status": {"low_aoe": "valid"},
            "is_bb_like": "low_aoe",
        },
    }
    result = police._sort_status_entry(scrambled)
    assert list(result.keys()) == [
        "reason",
        "usability",
        "processable",
        "is_blinded",
        "psd",
    ]
    assert list(result["psd"].keys()) == ["is_bb_like", "status"]


def test_sort_status_entry_missing_keys():
    # Entries without psd/is_blinded/reason (e.g. early SiPM entries)
    entry = {"processable": True, "usability": "on"}
    result = police._sort_status_entry(entry)
    assert list(result.keys()) == ["usability", "processable"]


def test_sort_status_entry_extra_keys_appended():
    entry = {"extra": 42, "usability": "on", "processable": True}
    result = police._sort_status_entry(entry)
    # canonical keys come first, then unknown extras
    assert list(result.keys()) == ["usability", "processable", "extra"]


def test_sort_status_entry_psd_extra_keys_appended():
    entry = {
        "usability": "on",
        "processable": True,
        "psd": {
            "status": {"low_aoe": "valid"},
            "unknown_psd_key": True,
            "is_bb_like": "low_aoe",
        },
    }
    result = police._sort_status_entry(entry)
    assert list(result["psd"].keys()) == ["is_bb_like", "status", "unknown_psd_key"]


# ---------------------------------------------------------------------------
# _sort_groupings_data
# ---------------------------------------------------------------------------


def test_sort_groupings_data_default_first():
    data = {
        "Z_detector": {"calgroup001a": {"p03": "r000..r001"}},
        "default": {"calgroup001a": {"p03": "r000..r005"}},
        "A_detector": {"calgroup001a": {"p03": "r000..r002"}},
    }
    result = police._sort_groupings_data(data)
    keys = list(result.keys())
    assert keys[0] == "default"
    assert keys[1:] == sorted(["A_detector", "Z_detector"])


def test_sort_groupings_data_groups_sorted():
    data = {
        "default": {
            "calgroup002a": {"p06": "r000..r005"},
            "calgroup001a": {"p03": "r000..r005"},
        }
    }
    result = police._sort_groupings_data(data)
    assert list(result["default"].keys()) == ["calgroup001a", "calgroup002a"]


def test_sort_groupings_data_periods_sorted():
    data = {
        "default": {
            "calgroup001a": {"p06": "r000..r005", "p03": "r000..r004"},
        }
    }
    result = police._sort_groupings_data(data)
    assert list(result["default"]["calgroup001a"].keys()) == ["p03", "p06"]


def test_sort_groupings_data_run_list_sorted():
    data = {
        "default": {
            "calgroup001a": {"p09": ["r003", "r001", "r005"]},
        }
    }
    result = police._sort_groupings_data(data)
    assert result["default"]["calgroup001a"]["p09"] == ["r001", "r003", "r005"]


def test_sort_groupings_data_run_range_unchanged():
    data = {"default": {"calgroup001a": {"p03": "r000..r005"}}}
    result = police._sort_groupings_data(data)
    assert result["default"]["calgroup001a"]["p03"] == "r000..r005"


# ---------------------------------------------------------------------------
# _fix_groupings_file
# ---------------------------------------------------------------------------

_SORTED_GROUPINGS = textwrap.dedent("""\
    default:
      calgroup001a:
        p03: r000..r005
      calgroup002a:
        p06: r000..r005
    B00002A:
      calgroup001a:
        p03: r000..r003
    V08682A:
      calgroup002a:
        p06: r001..r005
""")

_UNSORTED_GROUPINGS = textwrap.dedent("""\
    default:
      calgroup002a:
        p06: r000..r005
      calgroup001a:
        p03: r000..r005
    V08682A:
      calgroup002a:
        p06: r001..r005
    B00002A:
      calgroup001a:
        p03: r000..r003
""")


def test_fix_groupings_file_already_sorted(tmp_path):
    f = _write(tmp_path, "cal_groupings.yaml", _SORTED_GROUPINGS)
    assert not police._fix_groupings_file(f)
    assert Path(f).read_text() == _SORTED_GROUPINGS


def test_fix_groupings_file_unsorted_returns_true(tmp_path):
    f = _write(tmp_path, "cal_groupings.yaml", _UNSORTED_GROUPINGS)
    assert police._fix_groupings_file(f)


def test_fix_groupings_file_result_passes_validation(tmp_path):
    f = _write(tmp_path, "cal_groupings.yaml", _UNSORTED_GROUPINGS)
    police._fix_groupings_file(f)
    assert police._validate_groupings_file(f, "calgroup", verbose=False)


def test_fix_groupings_file_data_preserved(tmp_path):
    f = _write(tmp_path, "cal_groupings.yaml", _UNSORTED_GROUPINGS)
    police._fix_groupings_file(f)
    fixed = yaml.safe_load(Path(f).read_text())
    original = yaml.safe_load(_UNSORTED_GROUPINGS)
    assert fixed == original


# ---------------------------------------------------------------------------
# _fix_status_files
# ---------------------------------------------------------------------------

_SORTED_STATUS = {
    "V02160A": {
        "reason": "",
        "usability": "on",
        "processable": True,
        "psd": {"is_bb_like": "low_aoe", "status": {"low_aoe": "valid"}},
    }
}

_UNSORTED_STATUS = {
    "V02160A": {
        "processable": True,
        "usability": "on",
        "reason": "",
        "psd": {"status": {"low_aoe": "valid"}, "is_bb_like": "low_aoe"},
    }
}


def _write_status_dir(tmp_path: Path, files: dict[str, dict]) -> Path:
    """Write a mock status directory with a validity.yaml and the given data files."""
    (tmp_path / "validity.yaml").write_text(
        "- valid_from: '20240101T000000Z'\n  apply: []\n"
    )
    for name, data in files.items():
        with (tmp_path / name).open("w") as fh:
            yaml.dump(data, fh, default_flow_style=False, sort_keys=False)
    return tmp_path / "validity.yaml"


def test_fix_status_files_already_sorted(tmp_path):
    validity = _write_status_dir(tmp_path, {"status.yaml": _SORTED_STATUS})
    assert not police._fix_status_files(str(validity))


def test_fix_status_files_unsorted_returns_true(tmp_path):
    validity = _write_status_dir(tmp_path, {"status.yaml": _UNSORTED_STATUS})
    assert police._fix_status_files(str(validity))


def test_fix_status_files_key_order_after_fix(tmp_path):
    validity = _write_status_dir(tmp_path, {"status.yaml": _UNSORTED_STATUS})
    police._fix_status_files(str(validity))
    fixed = yaml.safe_load((tmp_path / "status.yaml").read_text())
    assert list(fixed["V02160A"].keys()) == [
        "reason",
        "usability",
        "processable",
        "psd",
    ]
    assert list(fixed["V02160A"]["psd"].keys()) == ["is_bb_like", "status"]


def test_fix_status_files_data_preserved(tmp_path):
    validity = _write_status_dir(tmp_path, {"status.yaml": _UNSORTED_STATUS})
    police._fix_status_files(str(validity))
    fixed = yaml.safe_load((tmp_path / "status.yaml").read_text())
    # values must be identical to original, only key order changes
    assert fixed == yaml.safe_load(yaml.dump(_UNSORTED_STATUS))


def test_fix_status_files_skips_validity_yaml(tmp_path):
    # validity.yaml itself must never be touched
    validity = _write_status_dir(tmp_path, {})
    original_validity = validity.read_text()
    police._fix_status_files(str(validity))
    assert validity.read_text() == original_validity


def test_fix_status_files_multiple_files(tmp_path):
    validity = _write_status_dir(
        tmp_path,
        {
            "a.yaml": _UNSORTED_STATUS,
            "b.yaml": _SORTED_STATUS,
        },
    )
    assert police._fix_status_files(str(validity))
    fixed_a = yaml.safe_load((tmp_path / "a.yaml").read_text())
    # a.yaml was fixed
    assert list(fixed_a["V02160A"].keys()) == [
        "reason",
        "usability",
        "processable",
        "psd",
    ]
    # b.yaml was already sorted — data unchanged
    fixed_b = yaml.safe_load((tmp_path / "b.yaml").read_text())
    assert fixed_b == _SORTED_STATUS


# ---------------------------------------------------------------------------
# _iter_string_values
# ---------------------------------------------------------------------------


def test_iter_string_values_flat_dict():
    assert set(police._iter_string_values({"a": "x", "b": "y"})) == {"x", "y"}


def test_iter_string_values_nested():
    obj = {"a": {"b": "deep"}, "c": ["list_item"]}
    assert set(police._iter_string_values(obj)) == {"deep", "list_item"}


def test_iter_string_values_non_string_leaves_ignored():
    assert list(police._iter_string_values({"a": 1, "b": None, "c": True})) == []


def test_iter_string_values_bare_string():
    assert list(police._iter_string_values("hello")) == ["hello"]


# ---------------------------------------------------------------------------
# _check_dataflow_config_paths
# ---------------------------------------------------------------------------


def _make_dataflow_dir(tmp_path: Path) -> Path:
    """Create a minimal dataflow config directory with some real files."""
    (tmp_path / "log").mkdir()
    (tmp_path / "log" / "basic_logging.yaml").write_text("")
    (tmp_path / "tier").mkdir()
    (tmp_path / "tier" / "hit").mkdir(parents=True)
    (tmp_path / "tier" / "present.yaml").write_text("")
    return tmp_path


def test_check_dataflow_config_paths_all_present(tmp_path):
    d = _make_dataflow_dir(tmp_path)
    state = {
        "options": {
            "logging": str(d / "log" / "basic_logging.yaml"),
            "tier": str(d / "tier" / "present.yaml"),
        }
    }
    assert police._check_dataflow_config_paths(
        state, d, "20220101T000000Z", "all", verbose=False
    )


def test_check_dataflow_config_paths_missing_file(tmp_path):
    d = _make_dataflow_dir(tmp_path)
    state = {"inputs": {"config": str(d / "tier" / "missing.yaml")}}
    assert not police._check_dataflow_config_paths(
        state, d, "20220101T000000Z", "all", verbose=False
    )


def test_check_dataflow_config_paths_wildcard_skipped(tmp_path):
    d = _make_dataflow_dir(tmp_path)
    # Path contains % — not a concrete path, must not be checked even if file doesn't exist
    state = {"inputs": {"config": str(d / "tier" / "l200-p%-r%-T%-config.yaml")}}
    assert police._check_dataflow_config_paths(
        state, d, "20220101T000000Z", "all", verbose=False
    )


def test_check_dataflow_config_paths_non_db_paths_ignored(tmp_path):
    d = _make_dataflow_dir(tmp_path)
    # Strings that don't start with the db directory are not path checks
    state = {"key": "just a label", "num": 42}
    assert police._check_dataflow_config_paths(
        state, d, "20220101T000000Z", "all", verbose=False
    )


def test_check_dataflow_config_paths_list_values(tmp_path):
    d = _make_dataflow_dir(tmp_path)
    state = {
        "inputs": [
            str(d / "log" / "basic_logging.yaml"),
            str(d / "tier" / "missing.yaml"),
        ]
    }
    assert not police._check_dataflow_config_paths(
        state, d, "20220101T000000Z", "all", verbose=False
    )


# ---------------------------------------------------------------------------
# _validate_hit_config_file
# ---------------------------------------------------------------------------

_GOOD_HIT = {
    "outputs": ["energy", "is_valid"],
    "operations": {
        "energy": {
            "description": "calibrated energy",
            "expression": "daqenergy * a",
            "parameters": {"a": 1.0},
            "lgdo_attrs": {"unit": "keV"},
        },
        "is_valid": {
            "expression": "energy > 0",
        },
    },
    "aggregations": {
        "quality_flag": {
            "bit0": "is_valid",
        }
    },
}


def _write_hit(tmp_path: Path, name: str, data: dict) -> str:
    p = tmp_path / name
    with p.open("w") as fh:
        yaml.dump(data, fh, default_flow_style=False, sort_keys=False)
    return str(p)


def test_validate_hit_config_valid(tmp_path):
    f = _write_hit(tmp_path, "hit.yaml", _GOOD_HIT)
    assert police._validate_hit_config_file(f, verbose=False)


def test_validate_hit_config_only_operations(tmp_path):
    # outputs and aggregations are optional
    data = {"operations": {"flag": {"expression": "x > 0"}}}
    f = _write_hit(tmp_path, "hit.yaml", data)
    assert police._validate_hit_config_file(f, verbose=False)


def test_validate_hit_config_unexpected_top_key(tmp_path):
    data = deepcopy(_GOOD_HIT)
    data["unknown"] = "bad"
    f = _write_hit(tmp_path, "hit.yaml", data)
    assert not police._validate_hit_config_file(f, verbose=False)


def test_validate_hit_config_top_key_wrong_order(tmp_path):
    # operations before outputs
    data = {
        "operations": {"flag": {"expression": "x > 0"}},
        "outputs": ["flag"],
    }
    f = _write_hit(tmp_path, "hit.yaml", data)
    assert not police._validate_hit_config_file(f, verbose=False)


def test_validate_hit_config_outputs_not_list(tmp_path):
    data = {"outputs": "not_a_list"}
    f = _write_hit(tmp_path, "hit.yaml", data)
    assert not police._validate_hit_config_file(f, verbose=False)


def test_validate_hit_config_outputs_non_string_items(tmp_path):
    data = {"outputs": ["ok", 42]}
    f = _write_hit(tmp_path, "hit.yaml", data)
    assert not police._validate_hit_config_file(f, verbose=False)


def test_validate_hit_config_operation_missing_expression(tmp_path):
    data = {"operations": {"op": {"description": "missing expr"}}}
    f = _write_hit(tmp_path, "hit.yaml", data)
    assert not police._validate_hit_config_file(f, verbose=False)


def test_validate_hit_config_operation_expression_not_string(tmp_path):
    data = {"operations": {"op": {"expression": 42}}}
    f = _write_hit(tmp_path, "hit.yaml", data)
    assert not police._validate_hit_config_file(f, verbose=False)


def test_validate_hit_config_operation_parameters_not_dict(tmp_path):
    data = {"operations": {"op": {"expression": "x", "parameters": "bad"}}}
    f = _write_hit(tmp_path, "hit.yaml", data)
    assert not police._validate_hit_config_file(f, verbose=False)


def test_validate_hit_config_operation_unexpected_key(tmp_path):
    data = {"operations": {"op": {"expression": "x", "extra": True}}}
    f = _write_hit(tmp_path, "hit.yaml", data)
    assert not police._validate_hit_config_file(f, verbose=False)


def test_validate_hit_config_operation_wrong_key_order(tmp_path):
    # parameters before expression
    data = {
        "operations": {
            "op": {"parameters": {"a": 1}, "expression": "x * a"},
        }
    }
    f = _write_hit(tmp_path, "hit.yaml", data)
    assert not police._validate_hit_config_file(f, verbose=False)


def test_validate_hit_config_aggregation_bad_key(tmp_path):
    data = {"aggregations": {"flag": {"notabit": "x"}}}
    f = _write_hit(tmp_path, "hit.yaml", data)
    assert not police._validate_hit_config_file(f, verbose=False)


def test_validate_hit_config_aggregation_non_string_value(tmp_path):
    data = {"aggregations": {"flag": {"bit0": 42}}}
    f = _write_hit(tmp_path, "hit.yaml", data)
    assert not police._validate_hit_config_file(f, verbose=False)


# ---------------------------------------------------------------------------
# _sort_hit_config
# ---------------------------------------------------------------------------


def test_sort_hit_config_top_level_order():
    data = {
        "aggregations": {"flag": {"bit0": "x"}},
        "outputs": ["x"],
        "operations": {"op": {"expression": "x"}},
    }
    result = police._sort_hit_config(data)
    assert list(result.keys()) == ["outputs", "operations", "aggregations"]


def test_sort_hit_config_operation_key_order():
    data = {
        "operations": {
            "op": {
                "parameters": {"a": 1},
                "lgdo_attrs": {"unit": "keV"},
                "expression": "x * a",
                "description": "test",
            }
        }
    }
    result = police._sort_hit_config(data)
    assert list(result["operations"]["op"].keys()) == [
        "description",
        "expression",
        "parameters",
        "lgdo_attrs",
    ]


def test_sort_hit_config_already_sorted_no_reorder():
    data = {
        "outputs": ["x"],
        "operations": {"op": {"expression": "x", "parameters": {"a": 1}}},
    }
    result = police._sort_hit_config(data)
    assert not police._needs_reorder(data, result)


# ---------------------------------------------------------------------------
# _fix_hit_config_file
# ---------------------------------------------------------------------------


def test_fix_hit_config_file_already_sorted(tmp_path):
    f = _write_hit(tmp_path, "hit.yaml", _GOOD_HIT)
    assert not police._fix_hit_config_file(f)


def test_fix_hit_config_file_unsorted_returns_true(tmp_path):
    unsorted = {
        "operations": {"op": {"parameters": {"a": 1}, "expression": "x * a"}},
        "outputs": ["op"],
    }
    f = _write_hit(tmp_path, "hit.yaml", unsorted)
    assert police._fix_hit_config_file(f)


def test_fix_hit_config_file_result_passes_validation(tmp_path):
    unsorted = {
        "operations": {"op": {"parameters": {"a": 1}, "expression": "x * a"}},
        "outputs": ["op"],
    }
    f = _write_hit(tmp_path, "hit.yaml", unsorted)
    police._fix_hit_config_file(f)
    assert police._validate_hit_config_file(f, verbose=False)


def test_fix_hit_config_file_data_preserved(tmp_path):
    unsorted = {
        "operations": {
            "op": {"lgdo_attrs": {"unit": "keV"}, "parameters": {"a": 1}, "expression": "x * a"}
        },
        "outputs": ["op"],
    }
    f = _write_hit(tmp_path, "hit.yaml", unsorted)
    police._fix_hit_config_file(f)
    fixed = yaml.safe_load(Path(f).read_text())
    assert fixed == yaml.safe_load(yaml.dump(unsorted))


# ---------------------------------------------------------------------------
# _validate_dsp_proc_chain_file
# ---------------------------------------------------------------------------

_GOOD_DSP = {
    "outputs": ["energy", "bl_mean"],
    "processors": {
        "bl_mean": {
            "description": "baseline mean",
            "module": "dspeed.processors",
            "function": "mean",
            "args": ["waveform", "bl_mean"],
            "unit": "ADC",
        },
        "energy": {
            "description": "trap energy",
            "module": "dspeed.processors",
            "function": "trap_norm",
            "prereqs": ["bl_mean"],
            "args": ["wf_blsub", "10*us", "3*us", "energy"],
            "init_args": ["len(wf_blsub)", "10*us/wf_blsub.period"],
            "kwargs": {"signature": "(n),()->()", "types": ["fi->f"]},
            "defaults": {"db.etrap.rise": "10*us"},
            "unit": "ADC",
        },
        "shorthand": "bl_mean * 2",
    },
}


def _write_dsp(tmp_path: Path, name: str, data: dict) -> str:
    p = tmp_path / name
    with p.open("w") as fh:
        yaml.dump(data, fh, default_flow_style=False, sort_keys=False)
    return str(p)


def test_validate_dsp_proc_chain_valid(tmp_path):
    f = _write_dsp(tmp_path, "proc_chain.yaml", _GOOD_DSP)
    assert police._validate_dsp_proc_chain_file(f, verbose=False)


def test_validate_dsp_proc_chain_only_processors(tmp_path):
    data = {"processors": {"bl": {"module": "numpy", "function": "mean", "args": ["wf", "bl"]}}}
    f = _write_dsp(tmp_path, "proc_chain.yaml", data)
    assert police._validate_dsp_proc_chain_file(f, verbose=False)


def test_validate_dsp_proc_chain_string_shorthand(tmp_path):
    data = {"processors": {"QDrift": "trapQftp*16"}}
    f = _write_dsp(tmp_path, "proc_chain.yaml", data)
    assert police._validate_dsp_proc_chain_file(f, verbose=False)


def test_validate_dsp_proc_chain_unexpected_top_key(tmp_path):
    data = deepcopy(_GOOD_DSP)
    data["extra"] = "bad"
    f = _write_dsp(tmp_path, "proc_chain.yaml", data)
    assert not police._validate_dsp_proc_chain_file(f, verbose=False)


def test_validate_dsp_proc_chain_wrong_top_order(tmp_path):
    data = {"processors": {"bl": {"module": "numpy", "function": "mean", "args": ["wf", "bl"]}}, "outputs": ["bl"]}
    f = _write_dsp(tmp_path, "proc_chain.yaml", data)
    assert not police._validate_dsp_proc_chain_file(f, verbose=False)


def test_validate_dsp_proc_chain_outputs_not_list(tmp_path):
    data = {"outputs": "energy"}
    f = _write_dsp(tmp_path, "proc_chain.yaml", data)
    assert not police._validate_dsp_proc_chain_file(f, verbose=False)


def test_validate_dsp_proc_chain_unexpected_processor_key(tmp_path):
    data = {"processors": {"bl": {"module": "numpy", "function": "mean", "args": ["wf", "bl"], "extra": True}}}
    f = _write_dsp(tmp_path, "proc_chain.yaml", data)
    assert not police._validate_dsp_proc_chain_file(f, verbose=False)


def test_validate_dsp_proc_chain_wrong_processor_key_order(tmp_path):
    data = {
        "processors": {
            "energy": {
                "args": ["wf", "energy"],
                "function": "trap_norm",
                "module": "dspeed.processors",
            }
        }
    }
    f = _write_dsp(tmp_path, "proc_chain.yaml", data)
    assert not police._validate_dsp_proc_chain_file(f, verbose=False)


def test_validate_dsp_proc_chain_prereqs_before_args(tmp_path):
    # prereqs after args should fail order check
    data = {
        "processors": {
            "energy": {
                "module": "dspeed.processors",
                "function": "trap_norm",
                "args": ["wf", "energy"],
                "prereqs": ["bl_mean"],
            }
        }
    }
    f = _write_dsp(tmp_path, "proc_chain.yaml", data)
    assert not police._validate_dsp_proc_chain_file(f, verbose=False)


# ---------------------------------------------------------------------------
# _sort_dsp_proc_chain
# ---------------------------------------------------------------------------


def test_sort_dsp_proc_chain_top_level_order():
    data = {
        "processors": {"bl": {"module": "numpy", "function": "mean", "args": ["wf", "bl"]}},
        "outputs": ["bl"],
    }
    result = police._sort_dsp_proc_chain(data)
    assert list(result.keys()) == ["outputs", "processors"]


def test_sort_dsp_proc_chain_processor_key_order():
    data = {
        "processors": {
            "energy": {
                "args": ["wf", "energy"],
                "unit": "ADC",
                "prereqs": ["bl"],
                "defaults": {"db.rise": "10*us"},
                "module": "dspeed.processors",
                "function": "trap_norm",
                "description": "trap energy",
            }
        }
    }
    result = police._sort_dsp_proc_chain(data)
    assert list(result["processors"]["energy"].keys()) == [
        "description", "module", "function", "prereqs", "args", "defaults", "unit",
    ]


def test_sort_dsp_proc_chain_string_shorthand_unchanged():
    data = {"processors": {"QDrift": "trapQftp*16"}}
    result = police._sort_dsp_proc_chain(data)
    assert result["processors"]["QDrift"] == "trapQftp*16"


# ---------------------------------------------------------------------------
# _fix_dsp_proc_chain_file
# ---------------------------------------------------------------------------


def test_fix_dsp_proc_chain_file_already_sorted(tmp_path):
    f = _write_dsp(tmp_path, "proc_chain.yaml", _GOOD_DSP)
    assert not police._fix_dsp_proc_chain_file(f)


def test_fix_dsp_proc_chain_file_unsorted_returns_true(tmp_path):
    unsorted = {
        "processors": {"bl": {"args": ["wf", "bl"], "function": "mean", "module": "numpy"}},
        "outputs": ["bl"],
    }
    f = _write_dsp(tmp_path, "proc_chain.yaml", unsorted)
    assert police._fix_dsp_proc_chain_file(f)


def test_fix_dsp_proc_chain_file_result_passes_validation(tmp_path):
    unsorted = {
        "processors": {"bl": {"args": ["wf", "bl"], "function": "mean", "module": "numpy"}},
        "outputs": ["bl"],
    }
    f = _write_dsp(tmp_path, "proc_chain.yaml", unsorted)
    police._fix_dsp_proc_chain_file(f)
    assert police._validate_dsp_proc_chain_file(f, verbose=False)


def test_fix_dsp_proc_chain_file_data_preserved(tmp_path):
    unsorted = {
        "processors": {
            "energy": {
                "args": ["wf", "energy"],
                "unit": "ADC",
                "function": "trap_norm",
                "module": "dspeed.processors",
            }
        },
        "outputs": ["energy"],
    }
    f = _write_dsp(tmp_path, "proc_chain.yaml", unsorted)
    police._fix_dsp_proc_chain_file(f)
    fixed = yaml.safe_load(Path(f).read_text())
    assert fixed == yaml.safe_load(yaml.dump(unsorted))
