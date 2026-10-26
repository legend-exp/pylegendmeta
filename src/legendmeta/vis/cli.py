from __future__ import annotations

import argparse
from pathlib import Path

from dbetto import Props

from .common import GROUPING_YAML_MAP, _datasets_path, changed_spans, changed_statuses
from .partitions import _read_groupings, vis_partition_groupings
from .psd import vis_hpge_psd_status
from .usability import vis_channel_usability

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
    that groupings file are outlined, and without ``--periods`` the plot
    covers every period of each changed partition (no figure if nothing
    changed).
    """
    parser = argparse.ArgumentParser(
        prog="plot-groupings",
        description="Plot the partition groupings of a legend-datasets tree",
    )
    parser.add_argument(
        "--datasets",
        help="legend-datasets (or legend-metadata) path ($LEGEND_METADATA)",
    )
    parser.add_argument("--grouping", default="cal", choices=sorted(GROUPING_YAML_MAP))
    parser.add_argument(
        "--datatype",
        help="data type of the columns, any in runinfo.yaml (default: phy for phy"
        " groupings, else cal)",
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
        periods = sorted(changed_spans(new, old, dets))

    changed: set = set()
    if periods:
        changed = vis_partition_groupings(
            periods, args.grouping, datatype, args.output, datasets, args.compare
        )
    if args.summary:
        Path(args.summary).write_text(_summary(args.grouping, changed))


def _describe(kind: str, old, new) -> str:
    """Short text for one status change, e.g. ``on → ac`` or ``lq: valid → present``."""
    if kind == "usability":
        return f"{old or '—'} → {new or '—'}"
    a, b = old or {}, new or {}
    parts = []
    if a.get("is_bb_like") != b.get("is_bb_like"):
        parts.append(
            f"cut: {a.get('is_bb_like') or '—'} → {b.get('is_bb_like') or '—'}"
        )
    sa, sb = a.get("status") or {}, b.get("status") or {}
    parts += [
        f"{f}: {sa.get(f) or '—'} → {sb.get(f) or '—'}"
        for f in sorted(sa.keys() | sb.keys())
        if sa.get(f) != sb.get(f)
    ]
    return ", ".join(parts)


def _status_summary(kind: str, datatype: str, changes: dict) -> str:
    """Markdown list of status changes, one line per detector and change."""
    groups: dict[tuple[str, str], dict[str, list[str]]] = {}
    for (det, period, run), (old, new) in changes.items():
        what = _describe(kind, old, new)
        groups.setdefault((det, what), {}).setdefault(period, []).append(run)
    title = "usability" if kind == "usability" else "PSD status"
    n_dets = len({det for det, _ in groups})
    lines = [f"**{title} ({datatype} runs)**: {n_dets} detector(s) changed"]
    for det, what in sorted(groups)[:MAX_LISTED]:
        periods = groups[(det, what)]
        runs = "; ".join(f"{p} {_runs_text(periods[p])}" for p in sorted(periods))
        lines.append(f"- `{det}`: {runs} ({what})")
    if len(groups) > MAX_LISTED:
        lines.append(f"- … and {len(groups) - MAX_LISTED} more")
    return "\n".join(lines) + "\n"


def plot_statuses() -> None:
    """Plot detector usability and PSD status of a legend-datasets tree.

    Invoked in CLI. With ``--compare`` (a base legend-datasets tree), writes
    ``<kind>_<datatype>.png`` and a Markdown summary ``<kind>_<datatype>.md``
    (kind: usability, psd) only for what changed, each over the periods with
    changes; changed cells are outlined. Without it, ``--periods`` is
    required and every plot is written.
    """
    parser = argparse.ArgumentParser(
        prog="plot-statuses",
        description="Plot detector usability and PSD status of a legend-datasets tree",
    )
    parser.add_argument(
        "--datasets",
        help="legend-datasets (or legend-metadata) path ($LEGEND_METADATA)",
    )
    parser.add_argument("--compare", help="base legend-datasets tree to compare with")
    parser.add_argument("--periods", nargs="+", help="periods to plot")
    parser.add_argument(
        "--datatypes",
        nargs="+",
        default=["cal", "phy"],
        help="data types to compare, any in runinfo.yaml (e.g. cal phy ssc)",
    )
    parser.add_argument("--outdir", required=True, help="directory for the figures")
    args = parser.parse_args()
    if args.compare is None and args.periods is None:
        parser.error("--periods is required without --compare")

    datasets = _datasets_path(args.datasets)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    diffs = {}
    if args.compare is not None:
        diffs = {
            dt: changed_statuses(datasets, args.compare, dt) for dt in args.datatypes
        }

    for kind, plot in (
        ("usability", vis_channel_usability),
        ("psd", vis_hpge_psd_status),
    ):
        for dt in args.datatypes:
            changes = diffs[dt][kind] if diffs else {}
            periods = args.periods or sorted({p for _, p, _ in changes})
            if not periods:
                continue
            out = outdir / f"{kind}_{dt}"
            plot(
                periods,
                datatype=dt,
                output=str(out.with_suffix(".png")),
                datasets=datasets,
                compare=args.compare,
            )
            if changes:
                out.with_suffix(".md").write_text(_status_summary(kind, dt, changes))
