from __future__ import annotations

import argparse
from pathlib import Path

from dbetto import Props

from .common import GROUPING_YAML_MAP, _datasets_path, changed_cells
from .partitions import _read_groupings, plot_partition_groupings

MAX_LISTED = 40  # detectors listed in the summary before truncating


def _runs_text(runs: list[str]) -> str:
    """``["r000", "r001", "r002", "r005"]`` -> ``"r000..r002, r005"``."""
    spans: list[list[int]] = []
    for n in sorted(int(r[1:]) for r in runs):
        if spans and n == spans[-1][1] + 1:
            spans[-1][1] = n
        else:
            spans.append([n, n])
    return ", ".join(
        f"r{lo:03d}" if lo == hi else f"r{lo:03d}..r{hi:03d}" for lo, hi in spans
    )


def _summary(grouping: str, changed: set) -> str:
    """Markdown list of the detectors whose partitions changed."""
    by_det: dict[str, dict[str, list[str]]] = {}
    for det, period, run in changed:
        by_det.setdefault(det, {}).setdefault(period, []).append(run)
    if not by_det:
        return f"**{grouping} groupings**: no partition changes\n"
    lines = [f"**{grouping} groupings**: {len(by_det)} detector(s) changed"]
    for det in sorted(by_det)[:MAX_LISTED]:
        periods = by_det[det]
        text = "; ".join(f"{p} {_runs_text(periods[p])}" for p in sorted(periods))
        lines.append(f"- `{det}`: {text}")
    if len(by_det) > MAX_LISTED:
        lines.append(f"- … and {len(by_det) - MAX_LISTED} more")
    return "\n".join(lines) + "\n"


def plot_groupings() -> None:
    """Plot the cal/phy groupings of a legend-datasets tree.

    Invoked in CLI. With ``--compare``, cells whose partition differs from
    that groupings file are outlined, and without ``--periods`` only the
    periods that changed are plotted (no figure if nothing changed).
    """
    parser = argparse.ArgumentParser(
        prog="plot-groupings",
        description="Plot the partition groupings of a legend-datasets tree",
    )
    parser.add_argument(
        "--datasets", help="legend-datasets (or legend-metadata) path ($LEGEND_METADATA)"
    )
    parser.add_argument("--grouping", default="cal", choices=sorted(GROUPING_YAML_MAP))
    parser.add_argument(
        "--datatype",
        choices=["cal", "phy"],
        help="run type of the columns (default: phy for phy groupings, else cal)",
    )
    parser.add_argument("--periods", nargs="+", help="periods to plot")
    parser.add_argument("--compare", help="groupings file to compare with")
    parser.add_argument("-o", "--output", required=True, help="figure file")
    parser.add_argument("--summary", help="write a Markdown summary of the changes")
    args = parser.parse_args()

    datasets = _datasets_path(args.datasets)
    datatype = args.datatype or ("phy" if args.grouping == "phy" else "cal")
    periods = args.periods
    if periods is None:
        if args.compare is None:
            parser.error("--periods is required without --compare")
        new = _read_groupings(args.grouping, datasets)
        old = Props.read_from(args.compare)
        dets = sorted(({*new, *old} - {"default"}) | {"__default__"})
        periods = sorted({c[1] for c in changed_cells(new, old, dets)})

    changed: set = set()
    if periods:
        changed = plot_partition_groupings(
            periods, args.grouping, datatype, args.output, datasets, args.compare
        )
    if args.summary:
        Path(args.summary).write_text(_summary(args.grouping, changed))
