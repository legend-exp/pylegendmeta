from __future__ import annotations

import keyword
import os
import re
import string
import sys
from collections.abc import Collection, Mapping
from concurrent.futures import Executor, ProcessPoolExecutor
from copy import copy

if sys.version_info >= (3, 12):
    from itertools import batched, repeat
else:
    from itertools import repeat
from pathlib import Path

import awkward as ak
import numpy as np
import pandas as pd
from dbetto import Props, TextDB

import legendmeta

from . import MetadataRepository


def query_runs(
    runs: str | None = None,
    *,
    dataflow_config: Path | str | Mapping = "$REFPROD/dataflow-config.yaml",
    group_by: str | Collection[str] | None = None,
    sort_by: str | Collection[str] = "cycle",
    library: str = "ak",
    cycle_def: str | None = None,
    rundb_tier: str | None = None,
    ignored_cycles: str | Collection[str] | None = None,
):
    """
    Query runs and return a table containing one entry for each cycle and data
    extracted from cycle names. Optionally apply a boolean selection of runs to
    include using an expression ``runs``.

    Run DB is built by recursively cycling through directories in one of the
    data tiers (using a list of excluded files from metadata). The fields are
    parsed from the hyphen-separated elements of cycle names (as defined by
    the `cycle-def` arg below).

    Parameters
    ----------
    runs
        boolean python expression for selecting runs, using column names defined
        in ``cycle_def`` as variables.

        Examples:

        - select calibration data from periods 6, 7 and 8 (assuming l200-style cycle names)::

            "period>='p06' and period<='p08' and datatype=='cal'"

        - select runs for detectors V01234A and V06789B from Th calibration data
          (using Hades data cycle name ``experiment-det-datatype-run-starttime``)::

            "det in ["V01234A", "V06789B"] and datatype=='th_HS2_lat_psa'``

    dataflow_config
        config file of reference production. If not provided, use the environment
        variable ``$REFPROD`` as a directory, and find file ``dataflow-config.yaml``

    group_by
        if ``None`` (default) return a flat array with all cycles. If one or more fields
        are provided, group entries by these fields (using :meth:ak.run_lengths, so group
        consecutive equal values; this is done after sorting, so be careful if sorting
        changes order!) Fields that vary within groups will be un-flattened into 2-D ragged
        arrays. Note that ``runs`` query cannot act collectively on grouped cycles.

    sort_by
        field by which to sort table, or list of fields in order by priority

    library
        format of returned table. Can be ``ak`` (default), ``pd`` or ``np``

    cycle_def
        hyphen-separated names of fields in cycle names; names will be used for columns.
        By default get from dataflow-config.

        Examples:
        - ``experiment-period-run-datatype-cycle`` for a L200 cycle, e.g. ``l200-p03-r001-cal-19720101T000000Z``
        - ``experiment-chan-datatype-run-starttime`` for a Hades cycle, e.g. ``char_data-V05268A-th_HS2_lat_psa-r001-20201008T122118Z``

    rundb_tier
        name of tier (e.g. ``raw``) used to walk through directories to populate run DB.
        By default get from dataflow-config, or set to ``raw``

    ignored_cycles
        path(s) in metadata to list(s) of ignored cycles. By default get from dataflow-config,
        or else do not skip any cycles.
    """
    if isinstance(dataflow_config, (Path, str)):
        df_config = Props.read_from(
            os.path.expandvars(dataflow_config), subst_pathvar=True
        )
    elif isinstance(dataflow_config, Mapping):
        df_config = dataflow_config
    else:
        msg = "dataflow_config must be a str, Path, or Mapping"
        raise ValueError(msg)
    df_paths = df_config.get("paths")
    query_config = df_config.get("query", {})

    if cycle_def is None:
        if "cycle_def" not in query_config:
            msg = "cycle_def must be provided either as kwarg or in dataflow_config"
            raise ValueError(msg)
        cycle_def = query_config["cycle_def"]

    if rundb_tier is None:
        rundb_tier = query_config.get("rundb_tier", "raw")

    if ignored_cycles is None:
        ignored_cycles = query_config.get("ignored_cycles", None)

    cwd = Path.cwd()

    try:
        os.chdir(df_paths[f"tier_{rundb_tier}"])

        # Get list of removed cycles if it exists
        if ignored_cycles is not None:
            if isinstance(ignored_cycles, str):
                ignored_cycles = [ignored_cycles]
            meta = TextDB(df_paths["metadata"], lazy=True)
            removed = set()
            for iclist in ignored_cycles:
                removed |= set(_get_recursive(meta, iclist))
        else:
            removed = {}

        # parser to identify data files
        parse_cycle = re.compile(f"(.*)-tier_{rundb_tier}\\.lh5")
        col_names = cycle_def.split("-")
        records = []

        for relpath, _, files in os.walk("."):
            # parse file names for data
            for f in sorted(files):
                record = dict.fromkeys(col_names)
                record["relpath"] = relpath[2:]  # get rid of ./

                match = parse_cycle.search(f)
                if not match:
                    continue
                cycle_name = match.group(1)
                if cycle_name in removed:
                    continue

                cycle = cycle_name
                for k, v in zip(col_names, cycle_name.split("-"), strict=True):
                    record[k] = v
                record["cycle"] = cycle

                select_run = eval(runs, {}, record) if runs else True
                if bool(select_run):
                    records.append(record)

        # Format and return results
        records.sort(
            key=lambda rec: rec[sort_by]
            if isinstance(sort_by, str)
            else [rec[sb] for sb in sort_by]
        )
        result = ak.Array(records)

        if group_by is not None:
            if isinstance(group_by, str):
                lengths = [np.cumsum(ak.run_lengths(result[group_by]))]
            else:
                lengths = [np.cumsum(ak.run_lengths(result[f])) for f in group_by]
            lengths = np.unique(np.concatenate([0, *lengths]))
            result = ak.unflatten(result, lengths[1:] - lengths[:-1])
            result = ak.Array(
                {
                    f: ak.firsts(result[f])
                    if ak.all(ak.all(result[f] == ak.firsts(result[f]), axis=1), axis=0)
                    else result[f]
                    for f in result.fields
                }
            )

        if library == "ak":
            return result
        if library == "pd":
            return ak.to_dataframe(result)
        if library == "np":
            return ak.to_numpy(result)
        msg = "library must be 'ak', 'pd' or 'np'"
        raise ValueError(msg)

    finally:
        os.chdir(cwd)


def query_meta(
    fields: Collection[str],
    runs: str | ak.Array | Mapping[np.ndarray] | pd.DataFrame,
    channels: str,
    *,
    dataflow_config: Path | str | Mapping = "$REFPROD/dataflow-config.yaml",
    group_chans: bool = False,
    tiers: Collection[str] | None = None,
    metadata: str | type | MetadataRepository = None,
    chan_db: str | None = None,
    meta_dbs: Mapping | None = None,
    return_query_vals: bool = False,
    return_alias_map: bool = False,
    processes: int | None = None,
    executor: Executor | None = None,
    library: str = "ak",
    **query_run_kwargs,
):
    """
    Query the metadata and pars data, returning a table containing one entry
    for each run/channel with the requested data fields. Can also provide
    boolean expression to select cycles based on data from runs table,
    and a boolean expression to select based on information about runs and
    channels found in metadata and parameter databases.

    Values from databases are referenced using:

        [alias]@db_name.par_path

    where:

    - ``alias``: optional alias to use as column name in returned table. If
      not provided, column name will be ``db_name_par_path``, replacing
      periods with underscores
    - ``@db_name``: name of data source. Data sources are found on disk using
      information in the dataflow config file (see ``dataflow_config``):

        - ``@chan``: channel database from ``metadata.channel_map()``
        - ``@par[_tier]``: parameter database from specified tier.
        - Additional data sources defined using the ``meta_dbs`` arg or
          ``meta_dbs`` entry in ``dataflow_config``

    - ``par_path``: path in database to par, using periods to separate fields

    Examples:

    - ``@chan.name``: name of channel; will be aliased to ``chan_name``
    - ``rid@chan.daq.raw_id``: DAQ id of channel; aliased to ``rid``
    - ``lt@run.livetime_in_s``: livetime from runinfo; aliased to ``lt``
    -  ``aoe_lo@par_hit.pars.operations.AoE_Low_Cut.parameters.a``: cut value
        for low A/E cut from hit tier

    Parameters
    ----------
    fields
        list of fields to include in the table. See above for description of
        syntax for naming data sources from metadata and parameter databases.

        Example::

            ["@chan.daq.rawid", "@run.livetime", "aoe_low_cut@par_hit.pars.operations.AoE_Low_Cut.parameters.a"]

    runs
        boolean python expression for selecting runs, using column names defined
        in ``cycle_def`` as variables. See :meth:`query_runs`

        Examples:

        - select calibration data from periods 6, 7 and 8 (assuming l200-style cycle names)::

            "period>='p06' and period<='p08' and datatype=='cal'"

        - select runs for detectors V01234A and V06789B from Th calibration data
          (using Hades data cycle name ``experiment-det-datatype-run-starttime``)::

            "det in ["V01234A", "V06789B"] and datatype=='th_HS2_lat_psa'``

    channels
        expression used to select channels for each run. Expression can
        access values from channel, metadata, and parameter databases (with
        the channel database ``@chan`` likely being the most useful)

        Examples:

        - select all ICPC channels for each run that are marked as usable::

            "@chan.type=='icpc' and @chan.analysis.usability=='on'"

        - select SiPM channel 10 and will only include runs where it is can be processed::

            "@chan.name=='S010' and @chan.analysis.processible"

        Note: if a parameter does not exist for a channel, it will evaluate to ``None``.
        If this causes an error to be thrown, this expression will evaluate to ``False``,
        excluding the channel. If an parameter always evaluates to None, it will raise
        an Exception.

    dataflow_config
        config file of reference production. If not provided, use the environment
        variable ``$REFPROD`` as a directory, and find file ``dataflow-config.yaml``

    tiers
        search only provided tiers for pars. If ``None`` search all found tiers.
        By default, get from dataflow-config.

        Examples: ``["raw", "dsp", "hit"]`` or ``["raw", "psp", "pht", "evt"]``

    metadata
        class or name of class to use to construct metadata

    chan_db
        format string for path in metadata to list of channels for a given cycle.
        Format string may reference values from the run DB. By default, get from
        dataflow-config or call :meth:`LegendMetadata.channelmap(starttime)`.

    meta_dbs
        mapping from database name (i.e. the thing after ``@``) to mapping of configuration
        parameters. Parameters are as follows:

            - path: path to root of database relative to root of metadata
            - cycle_entry [optional]: sub-path to entry for a given cycle. May be a format
              string with references to run DB fields. If not provided
              use :meth:`dbetto.TextDB.on(starttime)` to find the cycle entry
            - channel_entry [optional]: sub-sub-path to entry for a given channel. May be
              a format string with references to run DB and channel DB fields. If not
              provided and group_cycle is ``False``, use ``@chan.name``

        Examples::

            meta_dbs = {
                "runinfo": {
                    "path": "path/to/runinfo",
                    "cycle_entry": "{period}/{run}/{datatype}",
                },
                "chaninfo": {
                    "path": "path/to/chaninfo",
                    "cycle_entry": None, # use chaninfo.on(starttime)
                    "channel_entry": "@chan.name"
                }
                ...
            }

    group_chans
        if ``True``, return one entry for each run or group of runs, with channel data
        nested as ragged arrays. Else, return one entry for each channel/run.

    return_query_vals
        if ``True``, return values found in query as columns; else only return those in ``fields``

    return_alias_map
        if ``True``, return the pair ``(table, alias_map)`` where table is the
        normal output of this function and alias_map is a mapping from alias
        names to database paths

    processes:
        number of processes. If ``None``, use number equal to threads available
        to ``executor`` (if provided), or else do not parallelize

    executor:
        :class:`concurrent.futures.Executor` object for managing parallelism.
        If ``None``, create a :class:`concurrent.futures.`ProcessPoolExecutor`
        with number of processes equal to ``processes``.

    library
        format of returned table. Can be ``ak`` (default), ``pd`` or ``np``

    query_run_kwargs
        see :meth:`query_run`
    """
    if isinstance(dataflow_config, (Path, str)):
        df_config = Props.read_from(
            os.path.expandvars(dataflow_config), subst_pathvar=True
        )
    elif isinstance(dataflow_config, Mapping):
        df_config = dataflow_config
    else:
        msg = "dataflow_config must be a str, Path, or Mapping"
        raise ValueError(msg)
    df_paths = df_config["paths"]
    query_config = df_config.get("query", {})

    # Query (or convert) run_records
    if runs is None or isinstance(runs, str):
        run_records = query_runs(
            runs,
            dataflow_config=df_config,
            **query_run_kwargs,
        )
    else:
        run_records = ak.Array(runs)
    if len(run_records) == 0:
        msg = "no run records were found"
        raise ValueError(msg)

    # setup metadata object
    if metadata is None:
        if "metadata" not in query_config:
            msg = "metadata must be provided either as kwarg or in dataflow_config"
        metadata = query_config["metadata"]
    if isinstance(metadata, str):
        meta = getattr(legendmeta, metadata)(df_paths["metadata"])
    elif isinstance(metadata, type):
        meta = metadata()
    elif isinstance(metadata, MetadataRepository):
        meta = metadata

    if not isinstance(meta, MetadataRepository):
        msg = "metadata must be a MetadataRepository derived class"

    # if using a string for chan_db, get it set up
    if chan_db is None:
        chan_db = query_config.get("chan_db", None)
    if chan_db is not None and not all(
        v in run_records.fields for v in _format_vars(chan_db)
    ):
        msg = "chan_db must only reference values from run_db"
        raise ValueError(msg)

    # get list of fields needed and build mapping to column names
    col_name_map = {}
    col_list = set()
    chan_vars = parse_query_paths(channels)
    field_vars = [parse_query_paths(v, fullmatch=True) for v in fields]

    # capture alias@path.to.val into two variables
    for _, alias, path in chan_vars + field_vars:
        # map from path to alias
        if col_name_map.get(path) is None:
            # alias must be unique
            if alias is not None and any(
                path != p and alias == a for p, a in col_name_map.items()
            ):
                msg = f"alias {alias} already assigned"
                raise ValueError(msg)
            col_name_map[path] = alias

        # path can only be aliased to a single name
        elif path in col_name_map and alias == col_name_map[path]:
            msg = f"{path} assigned multiple alias names"
            raise ValueError(msg)

    # Find all the un-aliased paths and assign them an alias
    for path, alias in col_name_map.items():
        if alias is None:
            new_alias = path.replace(".", "_").replace("@", "")
            col_name_map[path] = new_alias

    # add aliases to col_list
    for _, _, path in field_vars:
        col_list.add(col_name_map[path])

    for field, _, path in chan_vars:
        alias = col_name_map[path]
        channels = channels.replace(field, alias)
        if return_query_vals:
            col_list.add(alias)

    if return_query_vals:
        for f in run_records.fields:
            col_list.add(f)

    if meta_dbs is None:
        meta_dbs = query_config.get("meta_dbs", {})

    if tiers is None:
        tiers = query_config.get("tiers", [])

    # Build list of dbs to read from and perform checks on meta_var configuration
    db_list = {}
    for key, info in meta_dbs.items():
        if not any(v.split(".")[0] == f"@{key}" for v in col_name_map):
            continue

        path = info["path"]
        try:
            info["db"] = _get_recursive(meta, path)
        except (KeyError, AttributeError) as e:
            msg = f"{path} not found in metadata"
            raise ValueError(msg) from e

        if info.get("cycle_entry"):
            if not all(
                v in run_records.fields for v in _format_vars(info["cycle_entry"])
            ):
                msg = f"cycle_entry {info['cycle_entry']} for {key} references values not found in run DB"
                raise ValueError(msg)
        elif "validity" not in info["db"]:
            msg = f"path {path} for {key} in metadata does not have validity file"
            raise ValueError(msg)

        if info.get("channel_entry") and not all(
            v in run_records.fields or v[:4] == "@chan"
            for v in _format_vars(info["channel_entry"])
        ):
            msg = f"channel_entry {info['channel_entry']} for {key} references values not found in run DB or channel DB"
            raise ValueError(msg)

        db_list[f"@{key}"] = info | {"db": _get_recursive(meta, path)}

    # get the paths and groups corresponding to our query
    par_db_config = query_config.get("par_db", {"channel_entry": "{@chan.name}"})
    if "channel_entry" in par_db_config and not all(
        v in run_records.fields or v[:5] == "@chan"
        for v in _format_vars(par_db_config["channel_entry"])
    ):
        msg = f"channel_entry {par_db_config['channel_entry']} for par_db references values not found in run DB or channel DB"
        raise ValueError(msg)

    for key, path in df_paths.items():
        if not any(v.split(".")[0] == f"@{key}" for v in col_name_map):
            continue

        if key[:4] != "par_":
            continue
        if tiers is not None and key[4:] not in tiers:
            continue

        db = TextDB(path, lazy=True)
        # if not par_db_config.get("cycle_entry") and not "validity" in db:
        #    continue

        db_list[f"@{key}"] = par_db_config | {"db": db}

    # Check that all parameters we try to read have a valid source
    for path in col_name_map:
        if path in run_records.fields:
            continue
        db_name = path.split(".")[0]
        if db_name != "@chan" and db_name not in db_list:
            msg = f"Could not find meta database for {path}. Available dbs:\n"
            msg += "@chan " + " ".join(db_list.keys())
            raise ValueError(msg)

    # Now run the query...
    if processes is None and executor is None:
        records, eval_success, path_hits = _query_loop(
            run_records,
            col_list,
            channels,
            meta,
            chan_db,
            db_list,
            col_name_map,
            group_chans,
        )
    else:
        if processes is None:
            processes = executor._max_workers
        if executor is None:
            executor = ProcessPoolExecutor(processes)

        records = []
        eval_success = False
        path_hits = {}
        for rec, es, ph in executor.map(
            _query_loop,
            batched(run_records, int(np.ceil(len(run_records) / processes)))
            if sys.version_info >= (3, 12)
            else [
                run_records[
                    i * int(np.ceil(len(run_records) / processes)) : (i + 1)
                    * int(np.ceil(len(run_records) / processes))
                ]
                for i in range(processes)
            ],
            repeat(col_list, processes),
            repeat(channels, processes),
            repeat(meta, processes),
            repeat(chan_db, processes),
            repeat(db_list, processes),
            repeat(col_name_map, processes),
            repeat(group_chans, processes),
        ):
            records += rec
            eval_success |= es
            for path, hits in ph.items():
                path_hits[path] = path_hits.get(path, 0) + hits

    # if evaluating query was never successful...
    missing_params = [path for path, cts in path_hits.items() if cts == 0]
    if not eval_success or len(missing_params) > 0:
        msg = ""
        if not eval_success:
            msg = "Could not interpret channel query for any runs/channels:\n"
            msg += f"  {channels}\n"
        for path in missing_params:
            msg += f"{path} was not found for any run\n"
        raise ValueError(msg)

    # Format and return results
    result = ak.Array(records)
    if library == "ak":
        pass
    elif library == "pd":
        result = ak.to_dataframe(result)
    elif library == "np":
        if group_chans:
            msg = "library 'np' is not compatible with group_chans=True"
            raise ValueError(msg)

        # recursively walk through fields to produce nested dict of np arrays
        def ak_to_np(ak_tab):
            if len(ak_tab.fields) == 0:
                return ak.to_numpy(ak_tab)
            return {f: ak_to_np(ak_tab[f]) for f in ak_tab.fields}

        result = ak_to_np(result)
    else:
        msg = "library must be 'ak', 'pd' or 'np'"
        raise ValueError(msg)

    if return_alias_map:
        return (result, col_name_map)
    return result


def _query_loop(
    run_records: Collection,
    col_list: set,
    channels: str,
    meta: MetadataRepository,
    chan_db: str | None,
    db_list: dict[str, TextDB],
    col_name_map: dict,
    group_chans: bool,
):
    # Now loop through the runs, perform channel queries, and fetch fields
    records = []
    path_hits = dict.fromkeys(
        col_name_map, 0
    )  # count number of times a value is found to give more helpful errors
    eval_success = False  # track if the eval ever succeeds
    cycle_dbs = {}
    chanlist = None
    for run_record in run_records:
        if isinstance(run_record, ak.Record):
            run_record = run_record.tolist()  # noqa: PLW2901
        time = run_record["starttime"]
        while isinstance(time, list):
            time = time[0]

        if group_chans:
            record = copy(run_record)

        # Get pars DBs corresponding to current run
        for k, db_info in db_list.items():
            try:
                if "cycle_entry" not in db_info:
                    if cycle_dbs.get(k) is None or not cycle_dbs[k].is_valid(time):
                        cycle_dbs[k] = db_info["db"].on(time)
                else:
                    cycle_dbs[k] = _get_recursive(
                        db_info["db"], db_info["cycle_entry"].format(**run_record)
                    )
            except RuntimeError:
                # if there is no valid parameter database for this run...
                cycle_dbs[k] = None

        if not chan_db:
            if chanlist is None or not chanlist.is_valid(time):
                chanlist = meta.channelmap(on=time)
        else:
            chanlist = meta[chan_db.config(run_record)]

        # Get run DB entry corresponding to current run and get @run values
        for path, alias in col_name_map.items():
            # if it's in the run DB, it's already there
            if path in run_record:
                path_hits[path] += 1
                continue

            db_name = path.split(".")[0]
            if db_name == "@chan":
                continue
            db_info = db_list[db_name]

            if db_info.get("channel_entry"):
                continue

            try:
                run_record[alias] = eval(
                    path[1:], {}, {db_name[1:]: cycle_dbs[db_name]}
                )
                path_hits[path] += 1
            except (TypeError, KeyError, AttributeError):
                run_record[alias] = None

        ch_ct = 0
        for chan in chanlist.values():
            ch_record = copy(run_record)

            # Read values from database paths
            for path, alias in col_name_map.items():
                if path in run_record or alias in run_record:
                    continue

                db_name = path.split(".")[0]
                try:
                    if db_name == "@chan":
                        ch_db = chan
                    else:
                        ch_path = db_list[db_name]["channel_entry"].replace("@", "")
                        ch_db = _get_recursive(
                            cycle_dbs[db_name], ch_path.format(chan=chan, **run_record)
                        )

                    ch_record[alias] = eval(path[1:], {}, {db_name[1:]: ch_db})
                    path_hits[path] += 1
                except (TypeError, KeyError, AttributeError):
                    ch_record[alias] = None

            # Evaluate the channel expression on the found values
            try:
                keep_record = bool(eval(channels, {}, ch_record))
            except Exception:
                continue
            eval_success = True

            if keep_record:
                ch_ct += 1
                if group_chans:
                    for alias, param in ch_record.items():
                        if alias not in run_record:
                            record.setdefault(alias, []).append(param)
                else:
                    records.append(
                        {k: v for k, v in (ch_record).items() if k in col_list}
                    )

        if group_chans and ch_ct > 0:
            records.append({k: v for k, v in record.items() if k in col_list})

    return records, eval_success, path_hits


def _get_recursive(db: TextDB, path: str):
    """Helper to recursively access values from nested dict-likes"""
    fields = re.split("[,/]", path)
    ret = db
    for f in fields:
        ret = ret[f]
    return ret


def _format_vars(fstring: str):
    """Helper to get list of variables referenced in format string"""
    return [v[1] for v in string.Formatter().parse(fstring) if v[1]]


def parse_query_paths(expr: str, fullmatch: bool = False) -> tuple[str, str | None, str]:
    """
    Parse input string for variable names of the form::

        [alias][@ or :][par.path]

    and return a list of each matching pair, with the first element being the
    alias and the second element being the path. Aliases and names in paths must
    be legal python names (i.e. alphanumeric, doesn't start with a digit).
    If ``@`` is used to separate the alias and path, it is left in the path (to
    denote a metadata location); if ``:`` is used, it is omitted.
    Note that function names (i.e. a valid name followed by ``(``) are excluded.
    Values inside of ``[...]``, ``{...}``, ``"..."``, and ``'...'`` are also excluded.

    If fullmatch is ``True``, expect full string to match pattern and return single pair.
    """
    # Note: ast does not like @'s and :'s used in this way, so instead we parse with regex
    if not fullmatch:
        # remove substrings inside of brackets or quotes
        var_list = " ".join(
            re.split(r"(?:\{.*?\})|(?:\[.*?\])|(?:\".*?\")|(?:'.*?')", expr)
        )
        var_list = re.findall(r"[\w:@\.]+(?![\w:@\.(])", var_list)
    else:
        var_list = [expr]

    ret = []
    for var in var_list:
        # skip numerals
        try:
            float(var)
            if fullmatch:
                msg = f"'{var}' is not a valid variable"
                raise NameError(msg)
            continue
        except ValueError:
            pass

        # skip reserved keywords in python
        if keyword.iskeyword(var):
            if fullmatch:
                msg = f"'{var}' is an illegal name"
                raise NameError(msg)
            continue

        match = re.fullmatch(
            r"([a-zA-Z_]\w*)??:?(@?[a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*)", var
        )
        if match is None:
            msg = f"'{var}' could not be parsed"
            raise NameError(msg)

        if keyword.iskeyword(match.group(1)):
            msg = f"{match.group(1)} is an illegal name"
            raise NameError(msg)
        ret.append((match.group(0), match.group(1), match.group(2)))

    return ret if not fullmatch else ret[0]
