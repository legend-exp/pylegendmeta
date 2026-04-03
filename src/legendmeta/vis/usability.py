from __future__ import annotations

from .common import AC_COLOR, EMPTY_COLOR, OFF_COLOR, ON_COLOR, _build_layout, _render


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
        status = usab_map.get((period, run, hpge))
        return {"off": (OFF_COLOR, ""), "ac": (AC_COLOR, ""), "on": (ON_COLOR, "")}.get(
            status, (EMPTY_COLOR, "")
        )

    _render(layout, hpge_maps, cell_colours, output)
