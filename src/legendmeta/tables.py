from __future__ import annotations

import warnings
from functools import cached_property

import polars as pl
from dbetto import AttrsDict, TextDB


def _resolve_runinfo(lmeta):
    """Get runinfo dict, handling metadata layout differences."""
    try:
        return lmeta.datasets.runinfo
    except (AttributeError, FileNotFoundError):
        return lmeta.dataprod.runinfo


def _resolve_statuses_on(lmeta):
    """Return a callable ts -> statuses dict, also handling metadata layout
    differences."""
    try:
        _ = lmeta.datasets.statuses
        return lmeta.datasets.statuses.on
    except (AttributeError, FileNotFoundError):
        return lambda ts: lmeta.dataprod.config.on(ts).analysis


def _textdb_to_df(db) -> pl.DataFrame:
    """Materialize a TextDB directory of records as a Polars DataFrame.

    Nested sub-directories are skipped (with warning) — their schema is
    unknown so it's unclear how to merge those with the top-level table.
    """
    file_items, dir_names = [], []
    for k, v in db.items():
        if isinstance(v, TextDB):
            dir_names.append(k)
        else:
            file_items.append(v)

    if dir_names:
        warnings.warn(
            f"_textdb_to_df: skipping {len(dir_names)} sub-directories: {dir_names}",
            stacklevel=2,
        )

    return pl.from_dicts(file_items, strict=False, infer_schema_length=None)


def _stringify_keys(obj):
    """Recursively coerce dict keys to strings.

    Required because Polars structs need string field names, but some metadata
    fields legitimately use integer keys — e.g. ``acs`` runs carry a
    ``sis_setups`` dict keyed by SIS number and then by source position.
    """
    if isinstance(obj, dict):
        return {str(k): _stringify_keys(v) for k, v in obj.items()}
    return obj


class Tables:
    """Polars DataFrame views over LEGEND metadata.

    Materializes and caches the dict-of-dict metadata structures into tabular
    form for SQL-like analysis. Time-dependent tables (``runinfo``,
    ``statuses``, ``channelmaps``) are concatenated across all known
    periods/runs.

    Examples
    --------
    >>> from legendmeta import LegendMetadata
    >>> from legendmeta.tables import Tables
    >>> lmeta = LegendMetadata()
    >>> tables = Tables(lmeta)
    >>> tables.runinfo
    """

    def __init__(self, lmeta):
        self._lmeta = lmeta

    @cached_property
    def runinfo(self) -> pl.DataFrame:
        _runinfo = _resolve_runinfo(self._lmeta)
        return pl.from_dicts(
            [
                {
                    "period": int(p.removeprefix("p")),
                    "run": int(r.removeprefix("r")),
                    "datatype": dt,
                    **_stringify_keys(info),  # acs runs, etc, have int-keyed sub-dicts
                }
                for p, runs in _runinfo.items()
                for r, datatypes in runs.items()
                for dt, info in datatypes.items()
            ],
            strict=False,
            infer_schema_length=None,
        )

    @cached_property
    def statuses(self) -> pl.DataFrame:
        """Per-(period, run, datatype, detector) analysis status DataFrame."""

        statuses_on = _resolve_statuses_on(self._lmeta)
        dfs = []
        for row in self.runinfo.iter_rows(named=True):
            st = statuses_on(row["start_key"])
            df = pl.from_dicts(
                [{"name": k, **_stringify_keys(v)} for k, v in st.items()],
                strict=False,
                infer_schema_length=None,
            )
            dfs.append(
                df.with_columns(
                    period=pl.lit(row["period"]),
                    run=pl.lit(row["run"]),
                    datatype=pl.lit(row["datatype"]),
                )
            )
        return pl.concat(dfs, how="diagonal_relaxed")

    @cached_property
    def channelmaps(self) -> AttrsDict:
        """Per-system channelmap DataFrames, accessible by attribute or key.

        Channel maps are NOT guaranteed constant within a period (e.g. in p18
        the SiPM DAQ mapping changed between the cal and phy runs of r000), so
        rows are keyed on ``(period, run, datatype)`` — one channel map per
        ``runinfo`` row, resolved at that row's ``start_key``, exactly like
        :attr:`statuses`.
        """
        by_system: dict[str, list[dict]] = {}
        for row in self.runinfo.iter_rows(named=True):
            chmap = self._lmeta.hardware.configuration.channelmaps.on(row["start_key"])
            for name, e in chmap.items():
                rawid = e.get("daq", {}).get("rawid")
                if rawid is None:
                    msg = (
                        f"missing rawid for {name} in period {row['period']} "
                        f"run {row['run']} ({row['datatype']}): daq={e.get('daq')!r}"
                    )
                    raise ValueError(msg)
                by_system.setdefault(e["system"], []).append({
                    **e,
                    "name": name,
                    "period": row["period"],
                    "run": row["run"],
                    "datatype": row["datatype"],
                    "rawid": rawid,
                })

        return AttrsDict(
            {
                sys: pl.from_dicts(entries, strict=False, infer_schema_length=None)
                for sys, entries in by_system.items()
            }
        )

    @cached_property
    def crystals(self) -> pl.DataFrame:
        return _textdb_to_df(self._lmeta.hardware.detectors.germanium.crystals)

    @cached_property
    def diodes(self) -> pl.DataFrame:
        return _textdb_to_df(self._lmeta.hardware.detectors.germanium.diodes)

    @cached_property
    def sipms(self) -> pl.DataFrame:
        return _textdb_to_df(self._lmeta.hardware.detectors.lar.sipms)

    @cached_property
    def fibers(self) -> pl.DataFrame:
        return _textdb_to_df(self._lmeta.hardware.detectors.lar.fibers)
