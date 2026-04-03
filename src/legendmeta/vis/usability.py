from __future__ import annotations

from .common import (
    AC_COLOR, EMPTY_COLOR, OFF_COLOR, ON_COLOR,
    _build_layout, _build_run_layout, _render, _render_run,
)

_STATUS = {"off": OFF_COLOR, "ac": AC_COLOR, "on": ON_COLOR}


def plot_usability_run(
    period: str,
    run: str,
    type: str = "cal",
    output: str | None = None,
) -> None:
    """Plot detector usability for a single run as a 2D array layout.

    Parameters
    ----------
    period
        Period string e.g. 'p16'.
    run
        Run string e.g. 'r003'.
    type
        'cal' or 'phy'.
    output
        Output file path (.pdf or .xlsx). If None, shows the plot interactively.
    """
    layout = _build_run_layout(period, run, type)
    usab_map = layout["usab_map"]
    hpge_maps = {hpge: {} for hpge in layout["str_pos"]}

    def cell_colours(hpge: str, _part_map: dict) -> tuple[str, str]:
        return _STATUS.get(usab_map.get(hpge), EMPTY_COLOR), ""

    _render_run(layout, hpge_maps, cell_colours, output)


def plot_usability(
    key: str,
    type: str = "cal",
    output: str | None = None,
) -> None:
    """Plot detector usability (on/ac/off) per run.

    Parameters
    ----------
    key
        Runlist key (e.g. 'napoli26') or a period (e.g. 'p16').
    type
        'cal' or 'phy'.
    output
        Output file path (.pdf or .xlsx). If None, shows the plot interactively.
    """
    layout = _build_layout(key, type)
    usab_map = layout["usab_map"]
    hpge_maps = {hpge: {} for hpge in layout["str_pos"]}

    def cell_colours(hpge: str, period: str, run: str, _part_map: dict) -> tuple[str, str]:
        return _STATUS.get(usab_map.get((period, run, hpge)), EMPTY_COLOR), ""

    _render(layout, hpge_maps, cell_colours, output)
