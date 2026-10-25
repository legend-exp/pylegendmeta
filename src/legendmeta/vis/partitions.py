from __future__ import annotations

from pathlib import Path

from dbetto import Props

from .common import (
    AC_COLOR,
    EMPTY_COLOR,
    GROUPING_YAML_MAP,
    OFF_COLOR,
    _build_layout,
    _build_run_layout,
    _datasets_path,
    _render,
    _render_run,
    build_period_run_map,
    changed_cells,
    label_colour,
    merge_with_defaults,
    partition_label,
)


def _read_groupings(grouping: str, datasets: Path) -> dict:
    """The ``grouping`` file ('cal', 'phy', 'escale', 'psd') of legend-datasets."""
    if grouping not in GROUPING_YAML_MAP:
        msg = (
            f"Unknown grouping {grouping!r}. Valid options: {sorted(GROUPING_YAML_MAP)}"
        )
        raise ValueError(msg)
    return Props.read_from(str(datasets / GROUPING_YAML_MAP[grouping]))


def _load_groupings(groupings: dict, str_pos: dict) -> tuple[dict, dict]:
    """Build hpge_maps + label_colour_map from a groupings dict."""
    defaults = groupings["default"]
    default_map = build_period_run_map(defaults, min_part=0)
    hpge_maps = {
        hpge: merge_with_defaults(groupings[hpge], defaults, min_part=0)
        if hpge in groupings
        else default_map
        for hpge in str_pos
    }
    all_short_labels = sorted(
        {partition_label(part) for m in hpge_maps.values() for part in m.values()}
    )
    label_colour_map = {lbl: label_colour(lbl) for lbl in all_short_labels}
    return hpge_maps, label_colour_map


def plot_partition_groupings_run(
    period: str,
    run: str,
    grouping: str = "cal",
    datatype: str = "cal",
    output: str | None = None,
    metadata=None,
) -> None:
    """Plot partition groupings for a single run as a 2D array layout.

    Parameters
    ----------
    period
        Period string e.g. 'p16'.
    run
        Run string e.g. 'r003'.
    grouping
        Which grouping yaml: 'cal', 'phy', 'escale', or 'psd'.
    datatype
        'cal' or 'phy'.
    output
        Output file path (.pdf, .png, .svg or .xlsx). If None, shows the plot
        interactively.
    metadata
        legend-metadata path or :class:`LegendMetadata` (default: ``LEGEND_METADATA``).
    """
    layout = _build_run_layout(period, run, datatype, metadata)
    usab_map = layout["usab_map"]
    hpge_maps, label_colour_map = _load_groupings(
        _read_groupings(grouping, _datasets_path(layout["meta"])), layout["str_pos"]
    )

    def cell_colours(hpge: str, part_map: dict) -> tuple[str, str]:
        part = part_map.get((period, run))
        status = usab_map.get(hpge)
        lbl = partition_label(part) if part else ""
        base_hex = label_colour_map.get(lbl, "CCCCCC") if part else EMPTY_COLOR
        if status == "off":
            return OFF_COLOR, lbl
        if status == "ac":
            return AC_COLOR, lbl
        return base_hex, lbl

    _render_run(layout, hpge_maps, cell_colours, output)


def plot_partition_groupings(
    key: str | list[str],
    grouping: str = "cal",
    datatype: str = "cal",
    output: str | None = None,
    datasets=None,
    compare: str | dict | None = None,
) -> set[tuple[str, str, str]]:
    """Plot partition groupings with usability overlays.

    Parameters
    ----------
    key
        Runlist key (e.g. 'napoli26'), a period (e.g. 'p16') or a list of
        periods.
    grouping
        Which grouping yaml: 'cal', 'phy', 'escale', or 'psd'.
    datatype
        'cal' or 'phy'.
    output
        Output file path (.pdf, .png, .svg or .xlsx). If None, shows the plot
        interactively.
    datasets
        legend-datasets path, or a legend-metadata path or :class:`LegendMetadata`
        whose ``datasets/`` is used (default: ``$LEGEND_METADATA``).
    compare
        Groupings to compare with (file path or dict), e.g. the base branch's.
        Cells whose partition differs are outlined.

    Returns
    -------
    set
        ``(detector, period, run)`` whose partition differs from ``compare``.
    """
    layout = _build_layout(key, datatype, datasets)
    usab_map = layout["usab_map"]
    groupings = _read_groupings(grouping, layout["datasets"])
    hpge_maps, label_colour_map = _load_groupings(groupings, layout["str_pos"])
    changed = set()
    if compare is not None:
        old = (
            Props.read_from(str(compare)) if not isinstance(compare, dict) else compare
        )
        cols = set(layout["sorted_cols"])
        changed = {
            c for c in changed_cells(groupings, old, layout["hpges"]) if c[1:] in cols
        }

    def cell_colours(
        hpge: str, period: str, run: str, part_map: dict
    ) -> tuple[str, str]:
        part = part_map.get((period, run))
        status = usab_map.get((period, run, hpge))
        lbl = partition_label(part) if part else ""
        base_hex = label_colour_map.get(lbl, "CCCCCC") if part else EMPTY_COLOR
        if status == "off":
            return OFF_COLOR, lbl
        if status == "ac":
            return AC_COLOR, lbl
        return base_hex, lbl

    _render(
        layout,
        hpge_maps,
        cell_colours,
        output,
        highlight=changed,
        title=f"{grouping} groupings ({datatype} runs)",
    )
    return changed
