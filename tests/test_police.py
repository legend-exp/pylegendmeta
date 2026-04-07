from __future__ import annotations

import sys
import textwrap
from copy import deepcopy
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

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
