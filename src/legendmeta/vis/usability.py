from __future__ import annotations

from .common import (
    AC_COLOR,
    EMPTY_COLOR,
    OFF_COLOR,
    ON_COLOR,
    _build_layout,
    _build_run_layout,
    _layout_changes,
    _render,
    _render_run,
)

_STATUS = {"off": OFF_COLOR, "ac": AC_COLOR, "on": ON_COLOR}


def plot_usability_run(
    period: str,
    run: str,
    datatype: str = "cal",
    output: str | None = None,
    metadata=None,
) -> None:
    """Plot detector usability for a single run as a 2D array layout.

    Parameters
    ----------
    period
        Period string e.g. 'p16'.
    run
        Run string e.g. 'r003'.
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
    hpge_maps = {hpge: {} for hpge in layout["str_pos"]}

    def cell_colours(hpge: str, _part_map: dict) -> tuple[str, str]:
        return _STATUS.get(usab_map.get(hpge), EMPTY_COLOR), ""

    _render_run(layout, hpge_maps, cell_colours, output)


def plot_usability(
    key: str | list[str],
    datatype: str = "cal",
    output: str | None = None,
    datasets=None,
    compare=None,
) -> set[tuple[str, str, str]]:
    """Plot detector usability (on/ac/off) per run.

    Parameters
    ----------
    key
        Runlist key (e.g. 'napoli26') or a period (e.g. 'p16').
    datatype
        'cal' or 'phy'.
    output
        Output file path (.pdf, .png, .svg or .xlsx). If None, shows the plot
        interactively.
    datasets
        legend-datasets path, or a legend-metadata path or :class:`LegendMetadata`
        whose ``datasets/`` is used (default: ``$LEGEND_METADATA``).
    compare
        Base legend-datasets tree to compare with: cells whose status differs
        are outlined.

    Returns
    -------
    set
        ``(detector, period, run)`` whose status differs from ``compare``.
    """
    layout = _build_layout(key, datatype, datasets)
    usab_map = layout["usab_map"]
    changed = set()
    if compare is not None:
        changed = _layout_changes(layout, compare, datatype, "usability")
    hpge_maps = {hpge: {} for hpge in layout["str_pos"]}

    def cell_colours(
        hpge: str, period: str, run: str, _part_map: dict
    ) -> tuple[str, str]:
        return _STATUS.get(usab_map.get((period, run, hpge)), EMPTY_COLOR), ""

    title = f"usability ({datatype} runs)"
    _render(layout, hpge_maps, cell_colours, output, highlight=changed, title=title)
    return changed
