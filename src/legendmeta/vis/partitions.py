from __future__ import annotations

from dbetto import Props

from .common import (
    AC_COLOR,
    EMPTY_COLOR,
    GROUPING_YAML_MAP,
    OFF_COLOR,
    PART_CMAP,
    _build_layout,
    _render,
    build_period_run_map,
    cmap_hex,
    merge_with_defaults,
    partition_label,
)


def plot_partition_groupings(
    key: str,
    grouping: str,
    type: str = "cal",
    output: str | None = None,
) -> None:
    """Plot partition groupings with usability overlays.

    Parameters
    ----------
    key
        Runlist key (e.g. 'napoli26') or a period (e.g. 'p16').
    grouping
        Which grouping yaml: 'cal', 'phy', 'escale', or 'psd'.
    type
        'cal' or 'phy'.
    output
        Output file path (.pdf or .xlsx). If None, shows the plot interactively.
    """
    layout = _build_layout(key, type)
    usab_map = layout["usab_map"]
    str_pos = layout["str_pos"]

    groupings = Props.read_from(GROUPING_YAML_MAP[grouping])
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
    label_colour_map = {
        lbl: cmap_hex(PART_CMAP, max(len(all_short_labels), 1))[i]
        for i, lbl in enumerate(all_short_labels)
    }

    def cell_colours(hpge: str, period: str, run: str, part_map: dict) -> tuple[str, str]:
        part = part_map.get((period, run))
        status = usab_map.get((period, run, hpge))
        lbl = partition_label(part) if part else ""
        base_hex = label_colour_map.get(lbl, "CCCCCC") if part else EMPTY_COLOR
        if status == "off":
            return OFF_COLOR, lbl
        if status == "ac":
            return AC_COLOR, lbl
        return base_hex, lbl

    _render(layout, hpge_maps, cell_colours, output)
