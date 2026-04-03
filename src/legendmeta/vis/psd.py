from __future__ import annotations

from .common import (
    AC_COLOR,
    EMPTY_COLOR,
    OFF_COLOR,
    ON_COLOR,
    _build_layout,
    _build_run_layout,
    _render,
    _render_run,
)

_STATUS_COLOR = {"valid": ON_COLOR, "present": AC_COLOR, "missing": OFF_COLOR}
_ABBREV = {
    "low_aoe": "AoE",
    "high_aoe": "AoE",
    "lq": "LQ",
    "ann": "ANN",
    "coax_rt": "RT",
}


def _bb_like_colour(psd: dict) -> tuple[str, str]:
    expr = psd.get("is_bb_like", "")
    if not expr or expr == "missing":
        return EMPTY_COLOR, ""
    fields = [f.strip() for f in expr.split("&")]
    statuses = {psd["status"].get(f, "missing") for f in fields}
    if "missing" in statuses:
        colour = OFF_COLOR
    elif "present" in statuses:
        colour = AC_COLOR
    else:
        colour = ON_COLOR
    parts = dict.fromkeys(
        _ABBREV.get(f, f) for f in fields
    )  # deduplicate, preserve order
    return colour, "+".join(parts)


def _psd_cell_colour(psd: dict, field: str | None) -> tuple[str, str]:
    if field is None:
        return _bb_like_colour(psd)
    status = psd.get("status", {}).get(field, "")
    return _STATUS_COLOR.get(status, EMPTY_COLOR), ""


def plot_psd_status_run(
    period: str,
    run: str,
    datatype: str = "cal",
    field: str | None = None,
    output: str | None = None,
) -> None:
    """Plot PSD status for a single run as a 2D array layout.

    Parameters
    ----------
    period
        Period string e.g. 'p16'.
    run
        Run string e.g. 'r003'.
    datatype
        'cal' or 'phy'.
    field
        PSD field to display: 'low_aoe', 'high_aoe', 'lq', 'ann', or 'coax_rt'.
        If None (default), evaluates the per-detector ``is_bb_like`` expression.
    output
        Output file path (.pdf or .xlsx). If None, shows the plot interactively.
    """
    layout = _build_run_layout(period, run, datatype)
    psd_map = layout["psd_map"]
    hpge_maps = {hpge: {} for hpge in layout["str_pos"]}

    def cell_colours(hpge: str, _part_map: dict) -> tuple[str, str]:
        psd = psd_map.get(hpge)
        return _psd_cell_colour(psd, field) if psd is not None else (EMPTY_COLOR, "")

    _render_run(layout, hpge_maps, cell_colours, output)


def plot_psd_status(
    key: str,
    datatype: str = "cal",
    field: str | None = None,
    output: str | None = None,
) -> None:
    """Plot PSD status per detector per run.

    Parameters
    ----------
    key
        Runlist key (e.g. 'napoli26') or a period (e.g. 'p16').
    datatype
        'cal' or 'phy'.
    field
        PSD field to display: 'low_aoe', 'high_aoe', 'lq', 'ann', or 'coax_rt'.
        If None (default), evaluates the per-detector ``is_bb_like`` expression
        (the defined combination of fields for the bb-like cut).
    output
        Output file path (.pdf or .xlsx). If None, shows the plot interactively.

    Cell colours
    ------------
    valid → green, present → orange, missing → red, no PSD data → white.
    For ``is_bb_like`` mode: all fields valid → green, any present but none
    missing → orange, any missing → red. Label shows the abbreviated combination.
    """
    layout = _build_layout(key, datatype)
    psd_map = layout["psd_map"]
    hpge_maps = {hpge: {} for hpge in layout["str_pos"]}

    def cell_colours(
        hpge: str, period: str, run: str, _part_map: dict
    ) -> tuple[str, str]:
        psd = psd_map.get((period, run, hpge))
        return _psd_cell_colour(psd, field) if psd is not None else (EMPTY_COLOR, "")

    _render(layout, hpge_maps, cell_colours, output)
