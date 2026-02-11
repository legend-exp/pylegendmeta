from __future__ import annotations

import keyword
import os
import re
from collections import OrderedDict
from collections.abc import Collection, Mapping
from copy import copy
from pathlib import Path

import awkward as ak
import numpy as np
import pandas as pd
from dbetto import Props, TextDB

from . import LegendMetadata


def query_runs(
    runs: str | None = None,
    *,
    dataflow_config: Path | str | Mapping = "$REFPROD/dataflow-config.yaml",
    cycle_def: str = "experiment-period-run-datatype-starttime",
    all_cycles: bool = False,
    sort_by: str | Collection[str] = "cycle",
    library: str = "ak",
):
    """
    Query runs by walking through directory tree. Return a table containing
    one entry for each run (directory), with a list of cycles and data extracted
    from cycle names defined by ``cycle_def``. Optionally select runs to
    include using an expression ``runs``.

    Parameters
    ----------
    runs
        python boolean expression for selecting runs, using column names defined
        in ``cycle_def`` as variables. If ``None`` return all runs found.

        Examples:
        - ``"period>='p06' and period<='p08' and datatype=='cal'"`` selects calibration data from periods 6, 7 and 8 (assuming default cycle names)
        - ``"det in ["V01234A", "V06789B"] and datatype=='th_HS2_lat_psa'`` selects runs for detectors V01234A and V06789B from Th calibration data (using Hades data cycle name ``experiment-det-datatype-run-starttime``)

    dataflow_config
        config file of reference production. If not provided, use the environment
        variable ``$REFPROD`` as a directory, and find file ``dataflow-config.yaml``

    cycle_def
        hyphen-separated names of fields in cycle names; names will be used for columns

        Examples:
        - (default) ``experiment-period-run-datatype-cycle`` for a L200 cycle, e.g. ``l200-p03-r001-cal-19720101T000000Z``
        - ``experiment-det-datatype-run-starttime`` for a Hades cycle, e.g. ``char_data-V05268A-th_HS2_lat_psa-r001-20201008T122118Z``

    all_cycles
        return entry for each cycle in each directory instead of first cycle alphabetically

    sort_by
        field by which to sort table, or list of fields in order by priority

    library
        format of returned table. Can be ``ak`` (default), ``pd`` or ``np``
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

    cwd = Path.cwd()

    try:
        os.chdir(df_paths["tier_raw"])

        # Get list of removed cycles if it exists
        try:
            removed = set(
                Props.read_from(
                    f"{df_paths['detector_status']}/ignored_daq_cycles.yaml"
                )["removed"]
            )
        except (FileNotFoundError, KeyError):
            removed = {}

        # parser to identify data files
        parse_cycle = re.compile("(.*)-tier_raw\\.lh5")
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

                if not all_cycles:
                    break

        # Format and return results
        records.sort(
            key=lambda rec: rec[sort_by]
            if isinstance(sort_by, str)
            else [rec[sb] for sb in sort_by]
        )
        result = ak.Array(records)
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
    tiers: Collection[str] | None = None,
    by_run: bool = False,
    return_query_vals: bool = False,
    cycle_def: str = "experiment-period-run-datatype-starttime",
    all_cycles: bool = False,
    return_alias_map: bool = False,
    library: str = "ak",
):
    """
    Query the metadata and pars data for a reference production. Return
    a table containing one entry for each run/channel with the requested
    data fields. Can also provide selections based on data from runs table,
    as well as information found in metadata and parameter databases.
    Values will be returned in a tabular format denoted by ``library``
    (default ``awkward.Array``) Values from databases are accessed using:

        [alias]@db_name.par_path

    where:

    - alias: optional alias to use as column name in returned table. If
      not provided, column name will be ``db_name_par_path``, replacing
      periods with underscores
    - @db_name: name of data source. Data sources are found on disk using
      information in the dataflow config file (see `dataflow_conifg`):

        - ``@det``: detector database from ``metadata.channel_map()``
        - ``@run``: run info database from ``metadata.datasets.runinfo``
        - ``@par[_tier]``: parameter database from specified tier. If no
            tier is provided, search all tiers in reverse order.

    - par_path: path in database to par, using periods to separate fields

    Examples:

    - ``@det.name``: name of detector; will be named ``det_name``
    - ``rid@det.daq.raw_id``: DAQ id of channel; will be named ``rid``
    - ``lt@run.livetime_in_s``: livetime from runinfo; named ``lt``
    -  ``aoe_lo@par_hit.pars.operations.AoE_Low_Cut.parameters.a``: cut value
        for low A/E cut from hit tier. Note if just ``par`` is used, it will
        be taken from ``pht`` tier instead.

    Parameters
    ----------
    fields
        list of fields to include in the table. See above for description of
        syntax for naming data sources from metadata and parameter databases.

        Example:

        - ``["@det.daq.rawid", "@run.livetime", "aoe_low_cut@par.pars.operations.AoE_Low_Cut.parameters.a"]``

    runs
        python boolean expression for selecting runs using :meth:query_runs, or
        table of runs similar to one returned by :meth:query_runs

        Examples:

        - ``"period>='p06' and period<='p08' and datatype=='cal'"`` selects calibration data from periods 6, 7 and 8 (assuming default cycle names)
        - ``"det in ["V01234A", "V06789B"] and datatype=='th_HS2_lat_psa'`` selects runs for detectors V01234A and V06789B from Th calibration data (using Hades data cycle name ``experiment-det-datatype-run-starttime``)

    channels
        expression used to select channels for each run. Expression can
        access values from all databases, as well as the run table.

        Examples:

        - ``"@det.system=='geds' and @det.type=='icpc' and @det.analysis.usability=='on'"``
          selects all ICPC detectors for each run that are marked as usable
        - ``"@det.name=='S010' and @det.analysis.processible"`` selects SiPM channel 10 and
          will only include runs where it is can be processed

        Note: if a parameter does not exist for a channel, it will evaluate to ``None``.
        If this causes an error to be thrown, this expression will evaluate to ``False``,
        excluding the channel. If an parameter always evaluates to False, it will raise
        an Exception.

    dataflow_config
        config file of reference production. If not provided, use the environment
        variable ``$REFPROD`` as a directory, and find file ``dataflow-config.yaml``

    tiers
        search only provided tiers for pars. If ``None`` search all found tiers.
        Examples: ``["dsp", "hit"]`` or ``["psp", "pht"]``

    by_run
        if ``True``, return nested array grouped by run, with inner variable length arrays of
        channel data

    return_query_vals
        if ``True``, return values found in query as columns; else only return those in ``fields``

    return_alias_map
        if ``True``, return the pair ``(table, alias_map)`` where table is the
        normal output of this function and alias_map is a mapping from alias
        names to database paths

    library
        format of returned table. Can be ``ak`` (default), ``pd`` or ``np``
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
    meta = LegendMetadata(df_paths["metadata"])

    try:
        runinfo = meta.datasets.runinfo
    except AttributeError:
        runinfo = None

    # get the paths and groups corresponding to our query
    par_dbs = OrderedDict(
        [
            (key, TextDB(path, lazy=True))
            for key, path in df_paths.items()
            if key[:4] == "par_"
            and Path(f"{path}/validity.yaml").exists()
            and (tiers is None or key[4:] in tiers)
        ]
    )

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
            print(path, alias, col_name_map[path])
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

    if isinstance(runs, str):
        run_records = query_runs(
            runs,
            dataflow_config=df_config,
            cycle_def=cycle_def,
            all_cycles=all_cycles,
        )
    else:
        run_records = ak.Array(runs)
    if len(run_records) == 0:
        msg = "no run records were found"
        raise ValueError(msg)

    # Now loop through the runs, perform channel queries, and fetch fields
    records = []
    path_hits = {}  # count number of times a value is found to give more helpful errors
    eval_success = False  # track if the eval ever succeeds
    for run_record in run_records:
        if isinstance(run_record, ak.Record):
            run_record = run_record.tolist()  # noqa: PLW2901
        time = run_record["starttime"]

        if by_run:
            record = copy(run_record)

        try:
            detlist = meta.channelmap(
                on=time,
            )
        except (AttributeError, KeyError, FileNotFoundError):
            detlist = [None]

        # Get run DB entry corresponding to current run and get @run values
        for path, alias in col_name_map.items():
            cts = path_hits.setdefault(path, 0)
            db = path.split(".")[0]
            if db == "@run":
                if runinfo is None:
                    msg = "runinfo database not found"
                    raise ValueError(msg)
                try:
                    run_record[alias] = eval(
                        f"run.{run_record['period']}.{run_record['run']}.{run_record['datatype']}{path[4:]}",
                        {},
                        {"run": runinfo},
                    )
                    cts += 1
                except AttributeError:
                    run_record[alias] = None
            elif path in run_record:
                cts += 1
            path_hits[path] = cts

        # Get pars DBs corresponding to current run
        run_par_dbs = {}
        for k, db in par_dbs.items():
            try:
                run_par_dbs[k] = db.on(time)
            except RuntimeError:
                # if there is no valid parameter database for this run...
                continue

        ch_ct = 0
        for det in detlist.values():
            ch_record = copy(run_record)

            # Read values from database paths
            for path, alias in col_name_map.items():
                cts = path_hits.setdefault(path, 0)
                db = path.split(".")[0]
                param = None
                if db == "@run" or path in run_record:
                    # these cases handled above
                    continue
                if db == "@det":
                    if det is None:
                        msg = "channelmap not found"
                        raise ValueError(msg)
                    try:
                        param = eval(path[1:], {}, {"det": det})
                        cts += 1
                    except (KeyError, AttributeError):
                        param = None
                elif "@par_" in db:
                    try:
                        name = db[1:]
                        par_db = run_par_dbs[name]
                        if det is not None:
                            par_db = par_db[det.name]

                        param = eval(path[1:], {}, {"par": par_db})
                        cts += 1
                    except (KeyError, AttributeError):
                        param = None
                elif db == "@par":
                    # search for the param in any of the tiers
                    # Return the first value found matching the path
                    for par_db in reversed(run_par_dbs.values()):
                        try:
                            if det is not None:
                                par_db = par_db[det.name]  # noqa: PLW2901

                            param = eval(path[1:], {}, {"par": par_db})
                            cts += 1
                        except (KeyError, AttributeError):
                            param = None
                            continue
                        break
                else:
                    msg = f"could not find metadata location {db}. Options are '@par', '@par_[tier]', '@det', '@run'"
                    raise ValueError(msg)
                ch_record[alias] = param
                path_hits[path] = cts

            # Evaluate the channel expression on the found values
            try:
                keep_record = bool(eval(channels, {}, ch_record))
            except Exception:
                continue
            eval_success = True
            ch_ct += 1

            if keep_record:
                if by_run:
                    for alias, param in ch_record.items():
                        if alias not in run_record:
                            record.setdefault(alias, []).append(param)
                else:
                    records.append(
                        {k: v for k, v in (ch_record).items() if k in col_list}
                    )

        if by_run and ch_ct > 0:
            records.append({k: v for k, v in record.items() if k in col_list})

    # if evaluating query was never successful...
    if not eval_success:
        msg = "Could not interpret channel query for any runs/channels:"
        msg += f"\n  {channels}"
        for path, cts in path_hits.items():
            if cts == 0:
                msg += f"\n{path} was not found for any run"
        raise ValueError(msg)

    # Format and return results
    result = ak.Array(records)
    if library == "ak":
        pass
    elif library == "pd":
        result = ak.to_dataframe(result)
    elif library == "np":
        if by_run:
            msg = "library 'np' is not compatible with by_run=True"
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


def parse_query_paths(expr: str, fullmatch: bool = False) -> Tuple[str, str]:
    """
    Parse input string for variable names of the form

        [alias][@ or :][par.path]

    and return a list of each matching pair, with the first element being the
    alias and the second element being the path. Aliases and names in paths must
    be legal python names (i.e. alphanumeric, doesn't start with a digit).
    If ``@`` is used to separate the alias and path, it is left in the path (to
    denote a metadata location); if ``:`` is used, it is omitted.
    Note that function names (i.e. a valid name followed by ``(``) are excluded.
    Values inside of ``[...]``, ``{...}``, ``"..."``, and `'...' are also excluded.

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
