"""Tests for the legendmeta.vis grid plots on a minimal legend-datasets tree."""

from __future__ import annotations

import pytest
import yaml

mpl = pytest.importorskip("matplotlib")
mpl.use("Agg")

from legendmeta.vis import cli, common, plot_partition_groupings

CAL = {"default": {"calgroup011a": {"p20": "r000..r003"}}}


@pytest.fixture
def datasets(tmp_path):
    """legend-datasets tree: p20 r000..r003, two HPGes and one SiPM."""
    runinfo = {
        "p20": {
            f"r00{i}": {
                "cal": {"start_key": f"2026060{i + 1}T000000Z"},
                "phy": {"start_key": f"2026060{i + 1}T010000Z"},
            }
            for i in range(4)
        }
    }
    status = {
        "V00001A": {"usability": "on", "psd": {"is_bb_like": "low_aoe"}},
        "V00002A": {"usability": "ac", "psd": {"is_bb_like": "low_aoe"}},
        "S001": {"usability": "on"},  # SiPM: no psd block, not a row
    }
    (tmp_path / "statuses").mkdir()
    (tmp_path / "statuses" / "l200-p20-r%-T%-all-config.yaml").write_text(
        yaml.safe_dump(status)
    )
    (tmp_path / "statuses" / "validity.yaml").write_text(
        yaml.safe_dump(
            [
                {
                    "valid_from": "20260101T000000Z",
                    "category": "all",
                    "apply": ["l200-p20-r%-T%-all-config.yaml"],
                }
            ]
        )
    )
    (tmp_path / "runinfo.yaml").write_text(yaml.safe_dump(runinfo))
    (tmp_path / "cal_groupings.yaml").write_text(yaml.safe_dump(CAL))
    return tmp_path


def test_datasets_path_accepts_a_metadata_tree(datasets, tmp_path_factory):
    meta = tmp_path_factory.mktemp("meta")
    (meta / "datasets").symlink_to(datasets)
    assert common._datasets_path(meta) == meta / "datasets"
    assert common._datasets_path(datasets) == datasets


def test_layout_rows_are_hpges_from_statuses(datasets):
    layout = common._build_layout("p20", "cal", datasets)
    assert layout["hpges"] == ["V00001A", "V00002A"]
    assert layout["group_labels"] == {"V": "ICPC"}
    assert layout["usab_map"][("p20", "r002", "V00002A")] == "ac"
    assert len(layout["sorted_cols"]) == 4


def test_detector_block_replaces_the_whole_partition():
    default = {"calgroup011a": {"p20": "r000..r001", "p21": "r000..r001"}}
    det = {"calgroup011a": {"p21": "r000..r001"}}
    got = common.merge_with_defaults(det, default, 0)
    assert sorted(got) == [("p21", "r000"), ("p21", "r001")]


def test_one_run_override_is_blank_only_for_display():
    det = {"calgroup011a": {"p20": ["r001"]}}
    assert common.merge_with_defaults(det, CAL["default"], 0) == {}
    assert common.merge_with_defaults(det, CAL["default"], 0, blank_single=False) == {
        ("p20", "r001"): "calgroup011a"
    }


def test_plot_outlines_changed_cells(datasets, tmp_path):
    old = {
        "default": {
            "calgroup011a": {"p20": ["r000", "r001"]},
            "calgroup011b": {"p20": ["r002", "r003"]},
        },
        "V00002A": {"calgroup011a": {"p20": ["r000"]}},
    }
    out = tmp_path / "cal.png"
    changed = plot_partition_groupings(
        ["p20"], "cal", "cal", str(out), datasets, compare=old
    )
    assert out.stat().st_size > 0
    assert changed == {
        ("V00001A", "p20", "r002"),
        ("V00001A", "p20", "r003"),
        ("V00002A", "p20", "r001"),
        ("V00002A", "p20", "r002"),
        ("V00002A", "p20", "r003"),
    }


def test_cli_plots_only_changed_periods(datasets, tmp_path, monkeypatch):
    base = tmp_path / "base.yaml"
    out, summary = tmp_path / "cal.png", tmp_path / "cal.md"
    argv = ["plot-groupings", "--datasets", str(datasets), "--compare", str(base)]
    argv += ["-o", str(out), "--summary", str(summary)]

    base.write_text(yaml.safe_dump(CAL))
    monkeypatch.setattr("sys.argv", argv)
    cli.plot_groupings()
    assert not out.exists()
    assert "no partition changes" in summary.read_text()

    base.write_text(yaml.safe_dump({"default": {}}))
    monkeypatch.setattr("sys.argv", argv)
    cli.plot_groupings()
    assert out.exists()
    assert "`V00001A`: p20 r000..r003" in summary.read_text()
