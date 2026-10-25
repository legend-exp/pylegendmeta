"""Tests for the legendmeta.vis grid plots on a minimal legend-datasets tree."""

from __future__ import annotations

import shutil

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
                    "category": ["all", "cal"],  # as in legend-datasets
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


def test_changed_spans_cover_the_whole_partition():
    """A split in p22 of a p20-p22 partition shows p20-p22; untouched
    partitions add nothing."""
    default = {
        "calgroup010a": {"p19": "r000..r003"},
        "calgroup011a": {"p20": "r000..r001", "p21": "r000..r003", "p22": "r000..r005"},
    }
    old = {"default": default}
    new = {
        "default": default,
        "V00001A": {
            "calgroup011a": {
                "p20": "r000..r001",
                "p21": "r000..r003",
                "p22": "r000..r002",
            },
            "calgroup011b": {"p22": "r003..r005"},
        },
    }
    dets = ["V00001A", "V00002A"]
    assert common.changed_spans(new, old, dets) == {"p20", "p21", "p22"}
    assert common.changed_spans(old, old, dets) == set()


def test_label_colours_are_fixed_and_distinct():
    labels = [f"{n}{x}" for n in range(1, 13) for x in "abc"]
    colours = {lbl: common.label_colour(lbl) for lbl in labels}
    assert common.label_colour("11a") == colours["11a"]  # same in every plot
    assert len({colours["11a"], colours["11b"], colours["11c"]}) == 3
    assert colours["10a"] != colours["11a"]
    assert not {common.AC_COLOR, common.OFF_COLOR} & set(colours.values())


@pytest.fixture
def changed(datasets, tmp_path_factory):
    """``datasets`` plus a usability change for all runs from r002, a
    cal-only PSD change from r003 and a reason-only edit."""
    new = tmp_path_factory.mktemp("new")
    shutil.copytree(datasets, new, dirs_exist_ok=True)
    edits = {
        "l200-p20-r002-T%-all-config.yaml": (
            "20260603T000000Z",
            ["all", "cal"],
            {"V00001A": {"usability": "ac", "reason": "gain shift"}},
        ),
        "l200-p20-r003-T%-cal-config.yaml": (
            "20260604T000000Z",
            ["cal"],
            {"V00002A": {"psd": {"status": {"lq": "present"}}}},
        ),
        "l200-p20-r001-T%-all-config.yaml": (
            "20260602T000000Z",
            ["all", "cal"],
            {"V00002A": {"reason": "only a reason"}},
        ),
    }
    validity = yaml.safe_load((new / "statuses" / "validity.yaml").read_text())
    for name, (ts, category, content) in edits.items():
        (new / "statuses" / name).write_text(yaml.safe_dump(content))
        validity.append(
            {"valid_from": ts, "category": category, "mode": "append", "apply": [name]}
        )
    validity.sort(key=lambda e: e["valid_from"])
    (new / "statuses" / "validity.yaml").write_text(yaml.safe_dump(validity))
    return new


def test_changed_statuses_by_run_type(datasets, changed):
    cal = common.changed_statuses(changed, datasets, "cal")
    phy = common.changed_statuses(changed, datasets, "phy")
    usab = {
        ("V00001A", "p20", "r002"): ("on", "ac"),
        ("V00001A", "p20", "r003"): ("on", "ac"),
    }
    assert cal["usability"] == usab
    assert phy["usability"] == usab
    assert set(cal["psd"]) == {("V00002A", "p20", "r003")}  # cal-only change
    assert phy["psd"] == {}


def test_cli_plots_only_what_changed(datasets, changed, tmp_path, monkeypatch):
    out = tmp_path / "out"
    argv = ["plot-statuses", "--datasets", str(changed), "--compare", str(datasets)]
    monkeypatch.setattr("sys.argv", [*argv, "--outdir", str(out)])
    cli.plot_statuses()
    assert sorted(f.name for f in out.iterdir()) == [
        "psd_cal.md",
        "psd_cal.png",
        "usability_cal.md",
        "usability_cal.png",
        "usability_phy.md",
        "usability_phy.png",
    ]
    assert (
        "`V00001A`: p20 r002..r003 (on → ac)" in (out / "usability_phy.md").read_text()
    )
    assert "(lq: — → present)" in (out / "psd_cal.md").read_text()
