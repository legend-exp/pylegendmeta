from __future__ import annotations

import os
import re
from pathlib import Path

try:
    import matplotlib.patches as mpatches
    import matplotlib.pyplot as plt
    import numpy as np
    import openpyxl
    from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
    from openpyxl.utils import get_column_letter
except ImportError as e:
    msg = "legendmeta.vis needs matplotlib, install it with: pip install 'pylegendmeta[vis]'"
    raise ImportError(msg) from e
from dbetto import Props, TextDB

from legendmeta import LegendMetadata

#: partition hues (dark, light): tab20 without orange and red, which mean AC/OFF
PART_HUES = [
    ("1F77B4", "AEC7E8"),  # blue
    ("2CA02C", "98DF8A"),  # green
    ("9467BD", "C5B0D5"),  # purple
    ("8C564B", "C49C94"),  # brown
    ("E377C2", "F7B6D2"),  # pink
    ("7F7F7F", "C7C7C7"),  # grey
    ("BCBD22", "DBDB8D"),  # olive
    ("17BECF", "9EDAE5"),  # cyan
]
PERIOD_CMAP = "Pastel2"
STRING_CELL_SHADES = ["F2F2F2", "E4E4E4"]
EMPTY_COLOR = "FFFFFF"
ON_COLOR = "6BAE75"
AC_COLOR = "FFA500"
OFF_COLOR = "FF4444"

THICK = Side(style="medium", color="333333")
NORMAL = Side(style="thin", color="888888")
HAIR = Side(style="hair", color="CCCCCC")
FONT = "Arial"

GROUPING_YAML_MAP: dict[str, str] = {  # relative to legend-datasets
    "cal": "cal_groupings.yaml",
    "phy": "phy_groupings.yaml",
    "escale": "groupings/escale_cal_groupings.yaml",
    "psd": "groupings/psd_cal_groupings.yaml",
}
DET_TYPES = {"B": "BEGe", "C": "Coax", "P": "PPC", "V": "ICPC"}  # row groups
FIG_FORMATS = (".pdf", ".png", ".svg")
CHANGED_COLOR = "#000000"  # outline of cells that differ from ``compare``


def _metadata(metadata: str | Path | LegendMetadata | None) -> LegendMetadata:
    """A :class:`LegendMetadata` from a path, an instance or ``None``."""
    if isinstance(metadata, LegendMetadata):
        return metadata
    return LegendMetadata(None if metadata is None else str(metadata))


def _datasets_path(source=None) -> Path:
    """legend-datasets directory of a legend-datasets or legend-metadata tree.

    ``source`` is a path or a :class:`LegendMetadata` (default:
    ``$LEGEND_METADATA``).
    """
    if isinstance(source, LegendMetadata):
        source = source.__repo_path__
    if source is None:
        source = os.environ.get("LEGEND_METADATA")
        if not source:
            msg = "no legend-datasets path given and $LEGEND_METADATA is not set"
            raise ValueError(msg)
    path = Path(source)
    return path / "datasets" if (path / "datasets").is_dir() else path


def _datasets_file(datasets: Path, name: str) -> dict:
    """Read ``name`` from a legend-datasets directory."""
    return Props.read_from(str(datasets / name))


def expand_run_list(value: list | str) -> list[str]:
    """Expand a YAML run value to a flat list of run strings."""
    if isinstance(value, list):
        result = []
        for item in value:
            s = str(item)
            if ".." in s:
                start, end = s.split("..", 1)
                result.extend(
                    [f"r{n:03d}" for n in range(int(start[1:]), int(end[1:]) + 1)]
                )
            else:
                result.append(s)
        return result
    s = str(value)
    if ".." in s:
        start, end = s.split("..", 1)
        return [f"r{n:03d}" for n in range(int(start[1:]), int(end[1:]) + 1)]
    return [s]


def partition_num(part_name: str) -> int:
    """calgroup008b  →  8"""
    return int(part_name.rsplit("group", maxsplit=1)[-1][:-1])


def partition_label(part_name: str) -> str:
    """calgroup008b  →  '8b'  (short display label)"""
    temp = part_name.rsplit("group", maxsplit=1)[-1]
    return f"{int(temp[:-1])}{temp[-1]}"


def label_colour(label: str) -> str:
    """Fixed colour of a partition label, the same in every plot.

    The hue follows the partition number (``11`` of ``"11b"``), the shade the
    letter: a = light, b = dark, c = in between, then repeating.
    """
    dark, light = PART_HUES[int(label[:-1]) % len(PART_HUES)]
    mid = "".join(
        f"{(int(dark[i : i + 2], 16) + int(light[i : i + 2], 16)) // 2:02X}"
        for i in (0, 2, 4)
    )
    return (light, dark, mid)[(ord(label[-1]) - ord("a")) % 3]


def col_sort_key(period_run: tuple) -> tuple:
    """Sort key for (period, run) tuples e.g. ('p18', 'r003') → (18, 3)"""
    period, run = period_run
    return (int(period[1:]), int(run[1:]))


def hex_to_rgb01(h: str) -> tuple:
    """'FFA500' → (1.0, 0.647, 0.0)"""
    h = h.lstrip("#")
    return tuple(int(h[i : i + 2], 16) / 255 for i in (0, 2, 4))


def build_period_run_map(
    partition_dict: dict, min_part: int, skip_single: bool = False
) -> dict:
    """Flatten a default-block dict into {(period, run): partition_name}."""
    result = {}
    for part, period_runs in partition_dict.items():
        if partition_num(part) < min_part or not isinstance(period_runs, dict):
            continue
        for period, runs in period_runs.items():
            expanded = expand_run_list(runs)
            if not expanded or (skip_single and len(expanded) == 1):
                continue
            for run in expanded:
                result[(period, run)] = part
    return result


def merge_with_defaults(
    hpge_partitions: dict,
    default_partitions: dict,
    min_part: int,
    blank_single: bool = True,
) -> dict:
    """Merge a detector's partition overrides with the defaults.

    A detector block replaces the WHOLE partition it names, as in
    legend-dataflow ``CalGrouping``. With ``blank_single``, a one-run
    detector partition (the "not partitioned" placeholder) is left blank.
    Returns {(period, run): partition_name} for all valid runs.
    """
    merged = {**default_partitions, **hpge_partitions}
    result = {}
    for part, period_runs in merged.items():
        if partition_num(part) < min_part or not isinstance(period_runs, dict):
            continue
        keys = [
            (p, r) for p, runs in period_runs.items() for r in expand_run_list(runs)
        ]
        if blank_single and part in hpge_partitions and len(keys) == 1:
            continue
        for key in keys:
            result[key] = part
    return result


def _effective(groupings: dict, hpge: str) -> dict:
    """``{(period, run): partition}`` for one detector, no display blanking."""
    return merge_with_defaults(
        groupings.get(hpge) or {}, groupings.get("default") or {}, 0, blank_single=False
    )


def changed_cells(new: dict, old: dict, hpges: list[str]) -> set[tuple]:
    """``(hpge, period, run)`` whose partition differs between two groupings."""
    out = set()
    for hpge in hpges:
        a, b = _effective(new, hpge), _effective(old, hpge)
        out |= {(hpge, *k) for k in a.keys() | b.keys() if a.get(k) != b.get(k)}
    return out


def changed_spans(new: dict, old: dict, hpges: list[str]) -> set[str]:
    """Periods covered by every partition that changed, before or after.

    A run moving from ``calgroup011a`` (p20-p22) to ``calgroup011b`` (p22)
    gives p20, p21 and p22.
    """
    out = set()
    for hpge in hpges:
        a, b = _effective(new, hpge), _effective(old, hpge)
        diff = [k for k in a.keys() | b.keys() if a.get(k) != b.get(k)]
        parts = {m[k] for k in diff for m in (a, b) if k in m}
        out |= {p for m in (a, b) for (p, _), g in m.items() if g in parts}
    return out


def cmap_hex(cmap_name: str, n: int) -> list[str]:
    """Sample *n* evenly-spaced colours from a matplotlib colormap, returning hex strings."""
    cmap = plt.get_cmap(cmap_name)
    return [
        f"{int(r * 255):02X}{int(g * 255):02X}{int(b * 255):02X}"
        for r, g, b, _ in (cmap(i / max(n - 1, 1)) for i in range(n))
    ]


def xl_fill(h: str) -> PatternFill:
    return PatternFill("solid", fgColor=h)


def _build_run_layout(period: str, run: str, type: str, metadata=None) -> dict:
    """Load metadata for a single run.

    Returns a dict with keys: meta, str_pos, usab_map, psd_map, period, run.
    """
    meta = _metadata(metadata)
    runinfo = _datasets_file(_datasets_path(meta), "runinfo.yaml")
    timestamp = runinfo[period][run][type]["start_key"]

    run_chmap = meta.channelmap(timestamp)

    str_pos = {}
    usab_map = {}
    psd_map = {}
    for hpge, item in run_chmap.items():
        if item["system"] != "geds":
            continue
        str_pos[hpge] = {
            "string": item["location"]["string"],
            "position": item["location"]["position"],
        }
        analysis = item.get("analysis")
        if not analysis:
            continue
        usability = analysis.get("usability")
        if usability is not None:
            usab_map[hpge] = usability
        if "psd" in analysis:
            psd_map[hpge] = analysis["psd"]

    return {
        "meta": meta,
        "str_pos": str_pos,
        "usab_map": usab_map,
        "psd_map": psd_map,
        "period": period,
        "run": run,
    }


def _render_run(
    layout: dict, hpge_maps: dict, cell_colours, output: str | None
) -> None:
    """Render a single-run 2D spatial array (strings x positions).

    ``cell_colours(hpge, part_map) -> (fill_hex, label)``
    """
    str_pos = layout["str_pos"]

    # {string: {position: hpge}}
    grid: dict[int, dict[int, str]] = {}
    for hpge, loc in str_pos.items():
        grid.setdefault(loc["string"], {})[loc["position"]] = hpge

    strings = sorted(grid)
    max_pos = max((p for s in grid.values() for p in s), default=0)
    if not grid or not max_pos:
        return

    if output is not None and not output.endswith((".xlsx", *FIG_FORMATS)):
        msg = f"Unsupported output format: {output!r}. Use .xlsx, .pdf, .png or .svg."
        raise ValueError(msg)

    if output is not None and output.endswith(".xlsx"):
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Array"

        # header row: string numbers
        for col_i, string_num in enumerate(strings, 2):
            c = ws.cell(1, col_i, f"Str {string_num}")
            c.fill = xl_fill("2F4F7F")
            c.font = Font(bold=True, name=FONT, size=9, color="FFFFFF")
            c.alignment = Alignment(horizontal="center", vertical="center")

        # header column: position numbers
        for pos in range(1, max_pos + 1):
            c = ws.cell(pos + 1, 1, pos)
            c.font = Font(bold=True, name=FONT, size=9)
            c.alignment = Alignment(horizontal="center", vertical="center")

        for col_i, string_num in enumerate(strings, 2):
            for pos in range(1, max_pos + 1):
                hpge = grid[string_num].get(pos)
                row = pos + 1
                cell = ws.cell(row, col_i)
                if hpge:
                    fill_hex, lbl = cell_colours(hpge, hpge_maps.get(hpge, {}))
                    cell.fill = xl_fill(fill_hex)
                    cell.value = f"{hpge}\n{lbl}" if lbl else hpge
                    cell.font = Font(name=FONT, size=8)
                    cell.alignment = Alignment(
                        horizontal="center", vertical="center", wrap_text=True
                    )
                else:
                    cell.fill = xl_fill("EEEEEE")

        for col_i in range(1, len(strings) + 2):
            ws.column_dimensions[get_column_letter(col_i)].width = 11
        for row in range(1, max_pos + 2):
            ws.row_dimensions[row].height = 30

        wb.save(output)

    if output is None or output.endswith(FIG_FORMATS):
        n_str = len(strings)
        cell_w = 1.0
        fig, ax = plt.subplots(figsize=(n_str * cell_w + 0.5, max_pos * cell_w + 0.5))
        ax.set_xlim(-0.05, n_str + 0.05)
        ax.set_ylim(max_pos + 0.05, -0.05)
        ax.axis("off")

        for col_i, string_num in enumerate(strings):
            ax.text(
                col_i + 0.5,
                -0.3,
                f"Str {string_num}",
                ha="center",
                va="bottom",
                fontsize=7,
                fontweight="bold",
            )
            for pos in range(1, max_pos + 1):
                hpge = grid[string_num].get(pos)
                row_i = pos - 1
                if hpge:
                    fill_hex, lbl = cell_colours(hpge, hpge_maps.get(hpge, {}))
                    ax.add_patch(
                        mpatches.Rectangle(
                            (col_i, row_i),
                            1,
                            1,
                            facecolor=hex_to_rgb01(fill_hex),
                            edgecolor="#888888",
                            linewidth=0.5,
                        )
                    )
                    ax.text(
                        col_i + 0.5,
                        row_i + 0.35,
                        hpge,
                        ha="center",
                        va="center",
                        fontsize=4.5,
                        fontfamily="monospace",
                        fontweight="bold",
                    )
                    if lbl:
                        ax.text(
                            col_i + 0.5,
                            row_i + 0.65,
                            lbl,
                            ha="center",
                            va="center",
                            fontsize=5,
                            color="#333333",
                        )
                else:
                    ax.add_patch(
                        mpatches.Rectangle(
                            (col_i, row_i),
                            1,
                            1,
                            facecolor=hex_to_rgb01("EEEEEE"),
                            edgecolor="#CCCCCC",
                            linewidth=0.3,
                        )
                    )

        period = layout.get("period", "")
        run = layout.get("run", "")
        if period and run:
            ax.set_title(f"{period} {run}", fontsize=9, fontweight="bold")

        plt.tight_layout(pad=0.3)
        if output is None:
            plt.show()
        else:
            fig.savefig(output, bbox_inches="tight")
            plt.close(fig)


def _statuses_at(datasets, type: str, keys) -> dict:
    """``{(period, run): {hpge: status}}`` for the germanium detectors.

    Looked up as legend-dataflow does, with ``category=type`` (dbetto falls
    back to ``all`` for a category without entries, e.g. ``phy``).
    """
    datasets = _datasets_path(datasets)
    runinfo = _datasets_file(datasets, "runinfo.yaml")
    statuses = TextDB(str(datasets)).statuses
    out = {}
    for period, run in keys:
        info = runinfo.get(period, {}).get(run, {}).get(type)
        if info is None:
            continue
        found = statuses.on(info["start_key"], pattern=None, category=type)
        out[(period, run)] = {h: s for h, s in found.items() if "psd" in s}  # HPGes
    return out


def changed_statuses(new, old, type: str) -> dict[str, dict]:
    """Usability and PSD changes between two legend-datasets trees.

    Compares every ``type`` run of the new runinfo. Returns
    ``{"usability": {...}, "psd": {...}}``, each mapping
    ``(hpge, period, run)`` to ``(old value, new value)``.
    """
    runinfo = _datasets_file(_datasets_path(new), "runinfo.yaml")
    keys = [
        (period, run)
        for period, runs in runinfo.items()
        if isinstance(runs, dict)
        for run, info in runs.items()
        if isinstance(info, dict) and type in info
    ]
    a, b = _statuses_at(new, type, keys), _statuses_at(old, type, keys)
    out: dict[str, dict] = {"usability": {}, "psd": {}}
    for key in keys:
        na, nb = a.get(key, {}), b.get(key, {})
        for hpge in na.keys() | nb.keys():
            for kind, changes in out.items():
                x, y = nb.get(hpge, {}).get(kind), na.get(hpge, {}).get(kind)
                if x != y:
                    changes[(hpge, *key)] = (x, y)
    return out


def _layout_changes(layout: dict, compare, type: str, kind: str) -> set:
    """Grid cells whose ``kind`` status ('usability' or 'psd') differs in ``compare``."""
    new = layout["usab_map"] if kind == "usability" else layout["psd_map"]
    old = _statuses_at(compare, type, layout["sorted_cols"])
    return {
        (h, p, r)
        for p, r in layout["sorted_cols"]
        for h in layout["hpges"]
        if new.get((p, r, h)) != old.get((p, r), {}).get(h, {}).get(kind)
    }


def _build_layout(key: str | list[str], type: str, datasets=None) -> dict:
    """Compute the grid layout from legend-datasets alone.

    ``key`` is a period (``"p16"``), a list of periods or a runlists key
    (``"napoli26"``). Rows are the germanium detectors in the statuses (the
    entries with a ``psd`` block), grouped by detector type and sorted by
    name. Returns a dict with keys: datasets, str_pos, hpges, string_groups,
    strings, group_labels, string_shade_map, sorted_cols, periods,
    period_colour_map, period_groups, usab_map, psd_map.
    """
    datasets = _datasets_path(datasets)
    if isinstance(key, str) and re.match(r"^p\d+$", key):
        key = [key]
    runinfo = _datasets_file(datasets, "runinfo.yaml")

    period_max: dict[str, int] = {}
    if not isinstance(key, str):
        for period in key:
            runs_in_period = [
                r for r in runinfo.get(period, {}) if type in runinfo[period][r]
            ]
            if runs_in_period:
                period_max[period] = max(int(r[1:]) for r in runs_in_period)
    else:
        runlists = _datasets_file(datasets, "runlists.yaml")
        for period, runs_spec in runlists[key][type].items():
            runs = expand_run_list(runs_spec)
            if runs:
                period_max[period] = max(
                    period_max.get(period, 0), *(int(r[1:]) for r in runs)
                )

    sorted_cols = sorted(
        [(p, f"r{r:03d}") for p, mx in period_max.items() for r in range(mx + 1)],
        key=col_sort_key,
    )
    periods = sorted({p for p, _ in sorted_cols}, key=lambda p: int(p[1:]))
    period_colour_map = dict(
        zip(periods, cmap_hex(PERIOD_CMAP, max(len(periods), 1)), strict=False)
    )
    period_groups: dict[str, list] = {}
    for period, run in sorted_cols:
        period_groups.setdefault(period, []).append((period, run))

    usab_map = {}
    psd_map = {}
    for (period, run), found in _statuses_at(datasets, type, sorted_cols).items():
        for hpge, status in found.items():
            psd_map[(period, run, hpge)] = status["psd"]
            if "usability" in status:
                usab_map[(period, run, hpge)] = status["usability"]

    hpges = sorted({h for _, _, h in psd_map}, key=lambda d: (d[0], d))
    string_groups: dict[str, list] = {}
    for hpge in hpges:
        string_groups.setdefault(hpge[0], []).append(hpge)
    strings = sorted(string_groups)
    string_shade_map = {s: STRING_CELL_SHADES[i % 2] for i, s in enumerate(strings)}

    return {
        "datasets": datasets,
        "str_pos": dict.fromkeys(hpges),  # no positions without the hardware config
        "hpges": hpges,
        "string_groups": string_groups,
        "strings": strings,
        "group_labels": {g: DET_TYPES.get(g, g) for g in strings},
        "group_header": "Type",
        "string_shade_map": string_shade_map,
        "sorted_cols": sorted_cols,
        "periods": periods,
        "period_colour_map": period_colour_map,
        "period_groups": period_groups,
        "usab_map": usab_map,
        "psd_map": psd_map,
    }


def _render(
    layout: dict,
    hpge_maps: dict,
    cell_colours,
    output: str | None,
    highlight: set | None = None,
    title: str | None = None,
) -> None:
    """Render the grid to xlsx, a figure file, or an interactive plot.

    Cells ``(hpge, period, run)`` in ``highlight`` get a thick outline
    (figures only).
    """
    hpges = layout["hpges"]
    strings = layout["strings"]
    string_groups = layout["string_groups"]
    string_shade_map = layout["string_shade_map"]
    sorted_cols = layout["sorted_cols"]
    periods = layout["periods"]
    period_colour_map = layout["period_colour_map"]
    period_groups = layout["period_groups"]

    if output is not None and not output.endswith((".xlsx", *FIG_FORMATS)):
        msg = f"Unsupported output format: {output!r}. Use .xlsx, .pdf, .png or .svg."
        raise ValueError(msg)

    # ── xlsx ──────────────────────────────────────────────────────────────────

    if output is not None and output.endswith(".xlsx"):
        COL_OFFSET = 3
        GRP_ROW, HEADER_ROW, DATA_START = 1, 2, 3

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Groupings"

        ws.row_dimensions[GRP_ROW].height = 18
        col_cursor = COL_OFFSET
        for period in periods:
            runs = period_groups.get(period, [])
            if not runs:
                continue
            sc = col_cursor
            sl, el = get_column_letter(sc), get_column_letter(sc + len(runs) - 1)
            ws.merge_cells(f"{sl}{GRP_ROW}:{el}{GRP_ROW}")
            c = ws[f"{sl}{GRP_ROW}"]
            c.value = f"{period}  ({runs[0][1]} - {runs[-1][1]})"
            c.fill = xl_fill(period_colour_map[period])
            c.font = Font(bold=True, name=FONT, size=10)
            c.alignment = Alignment(horizontal="center", vertical="center")
            col_cursor += len(runs)

        ws.row_dimensions[HEADER_ROW].height = 70
        hdr_dark = xl_fill("2F4F7F")
        for col, label in enumerate(
            [layout.get("group_header", "String"), "Detector"]
            + [f"{p}_{r}" for p, r in sorted_cols],
            1,
        ):
            c = ws.cell(HEADER_ROW, col, label)
            c.fill = hdr_dark
            c.font = Font(bold=True, name=FONT, size=9, color="FFFFFF")
            c.alignment = (
                Alignment(horizontal="center", vertical="center", wrap_text=True)
                if col <= 2
                else Alignment(horizontal="center", vertical="bottom", text_rotation=90)
            )

        p_first_col = {}
        col_cursor = COL_OFFSET
        for period in periods:
            if period_groups.get(period):
                p_first_col[period] = col_cursor
                col_cursor += len(period_groups[period])
        last_col = COL_OFFSET + len(sorted_cols) - 1

        current_row = DATA_START
        for string_num in strings:
            string_hpges = string_groups[string_num]
            bg = string_shade_map[string_num]
            string_start = current_row

            for hpge_idx, hpge in enumerate(string_hpges):
                row = current_row
                is_first = hpge_idx == 0
                is_last = hpge_idx == len(string_hpges) - 1
                top = THICK if is_first else NORMAL
                bottom = THICK if is_last else None

                ca = ws.cell(row, 1)
                ca.fill = xl_fill(bg)
                ca.border = Border(
                    left=THICK,
                    right=NORMAL,
                    top=THICK if is_first else None,
                    bottom=THICK if is_last else None,
                )

                cb = ws.cell(row, 2, hpge)
                cb.fill = xl_fill(bg)
                cb.font = Font(bold=True, name=FONT, size=9)
                cb.alignment = Alignment(horizontal="center", vertical="center")
                cb.border = Border(left=NORMAL, right=THICK, top=top, bottom=bottom)

                for run_idx, (period, run) in enumerate(sorted_cols):
                    col = COL_OFFSET + run_idx
                    cell = ws.cell(row, col)
                    cell.border = Border(
                        left=THICK if col in p_first_col.values() else HAIR,
                        right=THICK if col == last_col else None,
                        top=top,
                        bottom=bottom,
                    )
                    fill_hex, lbl = cell_colours(hpge, period, run, hpge_maps[hpge])
                    cell.fill = xl_fill(fill_hex)
                    if lbl:
                        cell.value = lbl
                        cell.font = Font(name=FONT, size=8)
                        cell.alignment = Alignment(
                            horizontal="center", vertical="center"
                        )

                current_row += 1

            string_end = current_row - 1
            ws.merge_cells(f"A{string_start}:A{string_end}")
            c = ws.cell(string_start, 1)
            c.value = layout.get("group_labels", {}).get(
                string_num, f"String {string_num}"
            )
            c.font = Font(bold=True, name=FONT, size=10)
            c.fill = xl_fill(bg)
            c.alignment = Alignment(
                horizontal="center", vertical="center", wrap_text=True
            )

        ws.column_dimensions["A"].width = 7
        ws.column_dimensions["B"].width = 10
        for i in range(len(sorted_cols)):
            ws.column_dimensions[get_column_letter(COL_OFFSET + i)].width = 3.8
        for row in range(DATA_START, current_row):
            ws.row_dimensions[row].height = 15
        ws.freeze_panes = f"{get_column_letter(COL_OFFSET)}{DATA_START}"

        wb.save(output)

    # ── pdf / interactive ─────────────────────────────────────────────────────

    if output is None or output.endswith(FIG_FORMATS):
        n_det = len(hpges)
        n_cols = len(sorted_cols)

        label_grid = [[""] * n_cols for _ in range(n_det)]
        color_grid = np.ones((n_det, n_cols, 4))
        for row_i, det in enumerate(hpges):
            for col_i, (period, run) in enumerate(sorted_cols):
                fill_hex, lbl = cell_colours(det, period, run, hpge_maps[det])
                color_grid[row_i, col_i] = (*hex_to_rgb01(fill_hex), 1.0)
                label_grid[row_i][col_i] = lbl

        row_cursor = 0
        string_row_start = {}
        string_boundaries = []
        for string_num in strings:
            string_row_start[string_num] = row_cursor
            row_cursor += len(string_groups[string_num])
            string_boundaries.append(row_cursor)
        string_boundaries = string_boundaries[:-1]

        col_cursor = 0
        period_col_start = {}
        period_boundaries = []
        for period in periods:
            runs = period_groups.get(period, [])
            if not runs:
                continue
            period_col_start[period] = col_cursor
            col_cursor += len(runs)
            period_boundaries.append(col_cursor)
        period_boundaries = period_boundaries[:-1]

        STR_W, DET_W = 2.0, 3.5
        RUN_LBL_H, PER_BAR_H = 1.0, 0.8
        HDR_H = RUN_LBL_H + PER_BAR_H + 0.2
        x_str0, x_det0 = -(STR_W + DET_W), -DET_W
        cell_in = 0.28

        fig, ax = plt.subplots(
            figsize=(
                (STR_W + DET_W + n_cols) * cell_in + 0.2,
                (HDR_H + n_det) * cell_in + 0.3,
            )
        )
        ax.set_xlim(x_str0 - 0.05, n_cols + 0.05)
        ax.set_ylim(n_det, -HDR_H)
        ax.axis("off")

        for string_num in strings:
            start = string_row_start[string_num]
            size = len(string_groups[string_num])
            ax.add_patch(
                mpatches.Rectangle(
                    (x_str0, start),
                    n_cols - x_str0,
                    size,
                    facecolor=hex_to_rgb01(string_shade_map[string_num]),
                    edgecolor="none",
                    zorder=0,
                )
            )

        for row_i in range(n_det):
            for col_i in range(n_cols):
                ax.add_patch(
                    mpatches.Rectangle(
                        (col_i, row_i),
                        1,
                        1,
                        facecolor=color_grid[row_i, col_i],
                        edgecolor="#CCCCCC",
                        linewidth=0.3,
                        zorder=1,
                    )
                )
                lbl = label_grid[row_i][col_i]
                if lbl:
                    r, g, b, _ = color_grid[row_i, col_i]
                    dark = 0.299 * r + 0.587 * g + 0.114 * b < 0.5  # luminance
                    ax.text(
                        col_i + 0.5,
                        row_i + 0.5,
                        lbl,
                        ha="center",
                        va="center",
                        fontsize=5.5,
                        color="white" if dark else "#333333",
                        zorder=2,
                    )

        cols_index = {c: i for i, c in enumerate(sorted_cols)}
        for row_i, det in enumerate(hpges):
            for _, period, run in sorted(c for c in highlight or () if c[0] == det):
                if (period, run) not in cols_index:
                    continue
                ax.add_patch(
                    mpatches.Rectangle(
                        (cols_index[(period, run)] + 0.06, row_i + 0.06),
                        0.88,
                        0.88,
                        facecolor="none",
                        edgecolor=CHANGED_COLOR,
                        linewidth=1.4,
                        zorder=6,
                    )
                )

        for row_i, det in enumerate(hpges):
            ax.add_patch(
                mpatches.Rectangle(
                    (x_det0, row_i),
                    DET_W,
                    1,
                    facecolor="none",
                    edgecolor="#AAAAAA",
                    linewidth=0.3,
                    zorder=1,
                )
            )
            ax.text(
                x_det0 + DET_W / 2,
                row_i + 0.5,
                det,
                ha="center",
                va="center",
                fontsize=6,
                fontweight="bold",
                fontfamily="monospace",
                zorder=2,
            )

        for string_num in strings:
            start = string_row_start[string_num]
            size = len(string_groups[string_num])
            ax.add_patch(
                mpatches.Rectangle(
                    (x_str0, start),
                    STR_W,
                    size,
                    facecolor="none",
                    edgecolor="#555555",
                    linewidth=0.8,
                    zorder=3,
                )
            )
            ax.text(
                x_str0 + STR_W / 2,
                start + size / 2,
                layout.get("group_labels", {}).get(string_num, f"Str\n{string_num}"),
                ha="center",
                va="center",
                fontsize=6.5,
                fontweight="bold",
                zorder=4,
            )

        for sb in string_boundaries:
            ax.plot(
                [x_str0, n_cols], [sb, sb], color="#333333", linewidth=1.2, zorder=5
            )
        for pb in period_boundaries:
            ax.plot([pb, pb], [0, n_det], color="#333333", linewidth=1.2, zorder=5)

        bar_y0 = -(RUN_LBL_H + PER_BAR_H)
        bar_y_mid = bar_y0 + PER_BAR_H / 2
        for col_i, (_, run) in enumerate(sorted_cols):
            ax.text(
                col_i + 0.5,
                0,
                run,
                ha="center",
                va="bottom",
                fontsize=5.5,
                rotation=90,
                color="#222222",
                zorder=4,
            )

        for period in periods:
            runs = period_groups.get(period, [])
            if not runs:
                continue
            sc = period_col_start[period]
            ax.add_patch(
                mpatches.Rectangle(
                    (sc, bar_y0),
                    len(runs),
                    PER_BAR_H,
                    facecolor=hex_to_rgb01(period_colour_map[period]),
                    edgecolor="#555555",
                    linewidth=0.8,
                    zorder=3,
                )
            )
            ax.text(
                sc + len(runs) / 2,
                bar_y_mid,
                f"{period}  ({runs[0][1]}-{runs[-1][1]})" if len(runs) > 5 else period,
                ha="center",
                va="center",
                fontsize=7,
                fontweight="bold",
                zorder=4,
            )

        group_header = layout.get("group_header", "String")
        for x0, w, label in [
            (x_str0, STR_W, group_header),
            (x_det0, DET_W, "Detector"),
        ]:
            ax.add_patch(
                mpatches.Rectangle(
                    (x0, bar_y0),
                    w,
                    PER_BAR_H,
                    facecolor=hex_to_rgb01("2F4F7F"),
                    edgecolor="#333333",
                    linewidth=0.8,
                    zorder=3,
                )
            )
            ax.text(
                x0 + w / 2,
                bar_y_mid,
                label,
                ha="center",
                va="center",
                fontsize=7,
                fontweight="bold",
                color="white",
                zorder=4,
            )

        if title:
            ax.set_title(title, fontsize=10, fontweight="bold", loc="left")
        plt.tight_layout(pad=0.2)
        if output is None:
            plt.show()
        else:
            fig.savefig(output, bbox_inches="tight", dpi=150)
            plt.close(fig)
