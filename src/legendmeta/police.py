# Copyright (C) 2022 Luigi Pertoldi <gipert@pm.me>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from __future__ import annotations

import argparse
import re
import sys
from importlib import resources
from pathlib import Path

import yaml
from dbetto import Props, TextDB, utils
from dbetto.catalog import Catalog

from .legendmetadata import LegendMetadata
from .utils import _RUN_RANGE_PATTERN, expand_runs

templates = resources.files("legendmeta") / "templates"


def validate_legend_detector_db() -> bool:
    """Validate LEGEND detector database.

    Invoked in CLI.
    """
    parser = argparse.ArgumentParser(
        prog="validate-legend-detdb", description="Validate LEGEND detector database"
    )

    parser.add_argument("files", nargs="+", help="files")

    args = parser.parse_args()

    dict_temp = {}
    for typ in ("bege", "ppc", "coax", "icpc"):
        dict_temp[typ] = utils.load_dict(templates / f"{typ}-detector.yaml")

    for file in args.files:
        valid = True

        entry = utils.load_dict(file)

        if "type" not in entry:
            print(  # noqa: T201
                f"ERROR: '{file}' entry does not contain 'type' key"
            )
            valid *= False
            continue

        if entry["type"] not in dict_temp:
            print(  # noqa: T201
                f"WARNING: '{file}': no template for type '{entry['type']}' detector"
            )
            continue

        valid *= validate_dict_schema(
            entry,
            dict_temp[entry["type"]],
            greedy=False,
            typecheck=True,
            root_obj=file,
        )

        if not valid:
            sys.exit(1)


def validate_legend_channel_map() -> bool:
    """Validate list of LEGEND channel map files.

    Invoked in CLI.
    """
    parser = argparse.ArgumentParser(
        prog="validate-legend-chmaps", description="Validate LEGEND channel map files"
    )

    parser.add_argument("files", nargs="+", help="channel maps files")

    args = parser.parse_args()

    dict_temp = {}
    for typ in ("geds", "spms", "pmts", "auxs", "bsln", "puls"):
        dict_temp[typ] = utils.load_dict(templates / f"{typ}-channel.yaml")

    for d in {Path(f).parent for f in args.files}:
        db = TextDB(d)
        valid = True

        with Path(f"{d}/validity.yaml").open() as f:
            validity = yaml.safe_load(f)
            for line in validity():
                ts = line["valid_from"]
                sy = line["apply"]
                chmap = db.on(ts, system=sy)

                for k, v in chmap.items():
                    if "system" not in v:
                        print(  # noqa: T201
                            f"ERROR: '{k}' entry does not contain 'system' key"
                        )
                        valid *= False
                        continue

                    if v["system"] not in dict_temp:
                        print(  # noqa: T201
                            f"WARNING: '{k}': no template for system '{v['system']}' entry"
                        )
                        continue

                    if "name" in v and v["name"] != k:
                        print(  # noqa: T201
                            f"ERROR: '{k}': key does not match 'name' field '{v['name']}'"
                        )
                        valid *= False

                    valid *= validate_dict_schema(
                        v,
                        dict_temp[v["system"]],
                        greedy=False,
                        typecheck=False,
                        root_obj=k,
                    )

        if not valid:
            sys.exit(1)


def validate_dict_schema(
    adict: dict,
    template: dict,
    greedy: bool = True,
    typecheck=True,
    root_obj: str = "",
    verbose: bool = True,
) -> bool:
    """Validate the format of a dictionary based on a template.

    Prints error messages on the console. Returns false if dictionary is invalid.

    Parameters
    ----------
    adict
        dictionary to analyze.
    template
        template dictionary.
    greedy
        if false, do not fail if the analyzed dictionary contains extra keys.
    typecheck
        if true, perform type checking.
    root_obj
        key name (or path to) dictionary. Used for error printing.
    verbose
        if false, do not print error messages.
    """
    if not isinstance(adict, dict) or not isinstance(template, dict):
        msg = "input objects must be of type dict"
        raise ValueError(msg)

    valid: bool = True

    # make sure keys in template exist and are valid in adict
    for k, v in template.items():
        if k not in adict:
            if verbose:
                print(f"ERROR: '{root_obj}/{k}' key not found")  # noqa: T201
            valid = False
        elif isinstance(v, dict):
            if not isinstance(adict[k], dict):
                if verbose:
                    print(f"ERROR: '{root_obj}/{k}' must be a dictionary")  # noqa: T201
                valid = False
            else:
                valid *= validate_dict_schema(
                    adict[k],
                    v,
                    greedy=greedy,
                    typecheck=typecheck,
                    root_obj=f"{root_obj}/{k}",
                    verbose=verbose,
                )
        elif typecheck and not isinstance(adict[k], type(v)):
            # do not complain if float is requested but int is given
            if isinstance(v, float) and isinstance(adict[k], int):
                continue
            # make an exception for null (missing) fields
            if adict[k] is None:
                continue
            if verbose:
                print(  # noqa: T201
                    f"ERROR: value of '{root_obj}/{k}' must be {type(v)}"
                )
            valid = False
        elif isinstance(v, str) and v != "":
            if re.match(v, adict[k]) is None:
                if verbose:
                    print(  # noqa: T201
                        f"ERROR: key '{root_obj}/{k}' does not match template regex '{v}'"
                    )
                valid = False

    if greedy and len_nested(adict) != len_nested(template):
        if verbose:
            print("ERROR: the dictionary contains extra keys")  # noqa: T201
        valid = False

    if greedy:
        valid *= validate_keys_recursive(adict, template, verbose=verbose)

    return valid


def validate_keys_recursive(adict: dict, template: dict, verbose: bool = True) -> bool:
    """Return false if `adict` contains keys not in `template`."""
    valid = True

    # special case: adict is empty
    if len(adict) == 0 and len(template) != 0:
        valid = False

    # analyze adict
    for k, v in adict.items():
        if k not in template:
            if verbose:
                print(f"ERROR: '{k}' key not allowed")  # noqa: T201
            valid = False
        elif isinstance(v, dict):
            valid *= validate_keys_recursive(v, template[k], verbose=verbose)

    return valid


def len_nested(d: dict) -> int:
    """Recursively count keys in a dictionary."""
    count = 0
    for v in d.values():
        count += 1
        if isinstance(v, dict):
            count += len_nested(v)

    return count


def validate_validity():
    parser = argparse.ArgumentParser(
        prog="validate-validity", description="Validate LEGEND validity files"
    )
    parser.add_argument("files", nargs="+", help="validity files")
    args = parser.parse_args()

    valid = True
    for file in args.files:
        # check catalog builds
        Catalog.read_from(file)
        # check files in validity exist
        valid_dic = Props.read_from(str(file))
        for dic in valid_dic:
            for f in dic["apply"]:
                full_path = Path(file).parent / f
                if full_path.exists() is False:
                    print(  # noqa: T201
                        f" ERROR : no file {full_path}"
                    )
                    valid = False
    if not valid:
        sys.exit(1)


_VALID_USABILITY = {"on", "off", "ac"}
_VALID_PSD_STATUS = {"present", "valid", "missing"}
_GE_PREFIXES = ("V", "C", "B", "P")
_SIPM_PREFIXES = ("S",)
_RUN_PATTERN = re.compile(r"^r\d{3}$")


def validate_statuses() -> None:
    """Validate LEGEND status files.

    Invoked in CLI. Accepts validity files; derives the status directory from each.
    """
    parser = argparse.ArgumentParser(
        prog="validate-statuses", description="Validate LEGEND status files"
    )
    parser.add_argument("files", nargs="+", help="validity files")
    args = parser.parse_args()

    meta = LegendMetadata()

    valid = True
    for validity_file in args.files:
        d = Path(validity_file).parent
        db = TextDB(d)
        valid_dic = Props.read_from(str(validity_file))

        for dic in valid_dic:
            ts = dic["valid_from"]
            try:
                state = db.on(ts)
            except Exception as e:
                print(f"ERROR: could not load status at '{ts}': {e}")  # noqa: T201
                valid = False
                continue

            try:
                chmap = meta.channelmap(ts)
            except Exception as e:
                print(f"WARNING: could not load channel map at '{ts}': {e}")  # noqa: T201
                chmap = None

            for ch, entry in state.items():
                if not isinstance(entry, dict):
                    continue

                if chmap is not None and ch not in chmap:
                    print(f"ERROR: '{ch}' at '{ts}': key not found in channel map")  # noqa: T201
                    valid = False

                is_ge = ch.startswith(_GE_PREFIXES) and not ch.startswith("PMT")
                is_sipm = ch.startswith(_SIPM_PREFIXES)

                if not is_ge and not is_sipm:
                    continue

                # usability
                if "usability" not in entry:
                    print(f"ERROR: '{ch}' at '{ts}': missing 'usability' field")  # noqa: T201
                    valid = False
                elif entry["usability"] not in _VALID_USABILITY:
                    print(  # noqa: T201
                        f"ERROR: '{ch}' at '{ts}': usability '{entry['usability']}'"
                        f" not in {_VALID_USABILITY}"
                    )
                    valid = False

                # processable
                if "processable" not in entry:
                    print(f"ERROR: '{ch}' at '{ts}': missing 'processable' field")  # noqa: T201
                    valid = False
                elif not isinstance(entry["processable"], bool):
                    print(  # noqa: T201
                        f"ERROR: '{ch}' at '{ts}': 'processable' must be true or false"
                    )
                    valid = False

                if is_ge:
                    # reason
                    if "reason" not in entry:
                        print(f"ERROR: '{ch}' at '{ts}': missing 'reason' field")  # noqa: T201
                        valid = False
                    elif entry.get("usability") != "on" and not entry["reason"]:
                        print(  # noqa: T201
                            f"ERROR: '{ch}' at '{ts}': 'reason' must be non-empty"
                            f" when usability is '{entry.get('usability')}'"
                        )
                        valid = False

                    # psd/status
                    if "psd" not in entry:
                        print(f"ERROR: '{ch}' at '{ts}': missing 'psd' field")  # noqa: T201
                        valid = False
                    elif (
                        not isinstance(entry["psd"], dict)
                        or "status" not in entry["psd"]
                    ):
                        print(f"ERROR: '{ch}' at '{ts}': missing 'psd/status' dict")  # noqa: T201
                        valid = False
                    elif not isinstance(entry["psd"]["status"], dict):
                        print(  # noqa: T201
                            f"ERROR: '{ch}' at '{ts}': 'psd/status' must be a dict"
                        )
                        valid = False
                    else:
                        for k, v in entry["psd"]["status"].items():
                            if v not in _VALID_PSD_STATUS:
                                print(  # noqa: T201
                                    f"ERROR: '{ch}' at '{ts}': psd/status/{k}"
                                    f" value '{v}' not in {_VALID_PSD_STATUS}"
                                )
                                valid = False

    if not valid:
        sys.exit(1)


def _run_sort_key(spec: str) -> str:
    """Sort key for a run spec (individual or range): the starting run string."""
    return spec.split("..", maxsplit=1)[0]


def _validate_run_spec(runs: object, location: str, verbose: bool = True) -> bool:
    """Validate a run specification: string range or list of runs/ranges."""
    if isinstance(runs, str):
        if not _RUN_RANGE_PATTERN.match(runs):
            if verbose:
                print(  # noqa: T201
                    f"ERROR: '{location}': runs '{runs}' must be a range of the form r###..r###"
                )
            return False
        return True

    if isinstance(runs, list):
        valid = True
        for item in runs:
            s = str(item)
            if not (_RUN_PATTERN.match(s) or _RUN_RANGE_PATTERN.match(s)):
                if verbose:
                    print(  # noqa: T201
                        f"ERROR: '{location}': run item '{item}'"
                        " must be r### or r###..r###"
                    )
                valid = False
        return valid

    if verbose:
        print(f"ERROR: '{location}': runs must be a string or list, got {type(runs)}")  # noqa: T201
    return False


def _get_overridden_runs(
    run_override_entries: list, runinfo: dict
) -> set[tuple[str, str]]:
    """Return (period, run) pairs whose cal falls inside a run_override window.

    The run_override file defines windows of time during which certain
    replacement cal files are applied (non-empty ``apply`` list).  Any run
    whose ``cal.start_key`` falls within such a window (i.e. the timestamp is
    ≥ the window start and < the next reset timestamp) is considered overridden.
    A single window can cover multiple calibrations.

    Parameters
    ----------
    run_override_entries
        Parsed list of run_override entries (each a dict with 'valid_from' and 'apply').
    runinfo
        Parsed runinfo dict mapping period → run → info (with cal.start_key).
    """
    # Sort entries chronologically (timestamps are ISO strings that sort lexicographically)
    sorted_entries = sorted(run_override_entries, key=lambda e: str(e["valid_from"]))

    # Build (window_start, window_end) pairs where apply is non-empty.
    # window_end is the valid_from of the next entry (the reset); None means open-ended.
    windows: list[tuple[str, str | None]] = []
    for i, entry in enumerate(sorted_entries):
        if entry.get("apply"):
            start = str(entry["valid_from"])
            end = (
                str(sorted_entries[i + 1]["valid_from"])
                if i + 1 < len(sorted_entries)
                else None
            )
            windows.append((start, end))

    overridden: set[tuple[str, str]] = set()
    for period, runs in runinfo.items():
        for run, info in runs.items():
            if "cal" not in info:
                continue
            cal_ts = str(info["cal"]["start_key"])
            for start, end in windows:
                in_window = cal_ts >= start and (end is None or cal_ts < end)
                if in_window:
                    overridden.add((period, run))
                    break

    return overridden


def _check_cal_override_runs(
    file: str,
    overridden_runs: set[tuple[str, str]],
    verbose: bool = True,
) -> bool:
    """Check that no overridden run appears in a cal_groupings file.

    Parameters
    ----------
    file
        Path to the cal_groupings YAML file.
    overridden_runs
        Set of (period, run) pairs that must not appear in the file.
    verbose
        If False, suppress error output.
    """
    data = utils.load_dict(file)
    valid = True
    for name, groups in data.items():
        if not isinstance(groups, dict):
            continue
        for group, periods in groups.items():
            if not isinstance(periods, dict):
                continue
            for period, runs in periods.items():
                period_str = str(period)
                for run in expand_runs(runs):
                    if (period_str, run) in overridden_runs:
                        if verbose:
                            print(  # noqa: T201
                                f"ERROR: '{file}': '{name}/{group}/{period}/{run}'"
                                " is overridden in run_override and must not"
                                f" appear in '{file}'"
                            )
                        valid = False
    return valid


def _validate_groupings_file(
    file: str,
    group_prefix: str,
    verbose: bool = True,
) -> bool:
    """Shared validation logic for cal/phy groupings files.

    Parameters
    ----------
    file
        Path to the groupings YAML file.
    group_prefix
        Expected prefix for group names (e.g. 'calgroup' or 'phygroup').
    verbose
        If False, suppress error output.
    """
    group_re = re.compile(rf"^{re.escape(group_prefix)}\d{{3}}[a-z]$")
    period_re = re.compile(r"^p\d{2}$")

    data = utils.load_dict(file)

    if "default" not in data:
        if verbose:
            print(f"ERROR: '{file}': missing 'default' key")  # noqa: T201
        return False

    default = data["default"]
    valid = True

    # --- sort checks on top-level keys ---
    top_keys = list(data.keys())
    if top_keys[0] != "default":
        if verbose:
            print(  # noqa: T201
                f"ERROR: '{file}': 'default' must be the first key"
                f" (found '{top_keys[0]}')"
            )
        valid = False
    non_default_keys = top_keys[1:]
    if non_default_keys != sorted(non_default_keys):
        if verbose:
            print(  # noqa: T201
                f"ERROR: '{file}': top-level keys after 'default' must be"
                " lexicographically sorted"
            )
        valid = False

    for name, groups in data.items():
        if not isinstance(groups, dict):
            if verbose:
                print(f"ERROR: '{file}': top-level entry '{name}' is not a dict")  # noqa: T201
            valid = False
            continue

        # --- sort check on group names within each entry ---
        group_keys = list(groups.keys())
        if group_keys != sorted(group_keys):
            if verbose:
                print(  # noqa: T201
                    f"ERROR: '{file}': '{name}' group names must be"
                    " lexicographically sorted"
                )
            valid = False

        for group, periods in groups.items():
            # enforce group naming convention
            if not group_re.match(group):
                if verbose:
                    print(  # noqa: T201
                        f"ERROR: '{file}': '{name}/{group}'"
                        f" group name must match '{group_prefix}NNNx'"
                        f" (e.g. {group_prefix}001a)"
                    )
                valid = False

            if not isinstance(periods, dict):
                if verbose:
                    print(  # noqa: T201
                        f"ERROR: '{file}': '{name}/{group}' must be a dict of periods"
                    )
                valid = False
                continue

            # --- sort check on period names ---
            period_keys = [str(p) for p in periods]
            if period_keys != sorted(period_keys):
                if verbose:
                    print(  # noqa: T201
                        f"ERROR: '{file}': '{name}/{group}' period names must be"
                        " lexicographically sorted"
                    )
                valid = False

            for period, runs in periods.items():
                period_str = str(period)

                # enforce period naming convention
                if not period_re.match(period_str):
                    if verbose:
                        print(  # noqa: T201
                            f"ERROR: '{file}': '{name}/{group}/{period}'"
                            " period must match 'pNN' (e.g. p03)"
                        )
                    valid = False

                # validate run specification format
                valid &= _validate_run_spec(
                    runs, f"{file}/{name}/{group}/{period}", verbose=verbose
                )

                # sort check on list-type runs
                if isinstance(runs, list):
                    run_strs = [str(r) for r in runs]
                    if run_strs != sorted(run_strs, key=_run_sort_key):
                        if verbose:
                            print(  # noqa: T201
                                f"ERROR: '{file}': '{name}/{group}/{period}'"
                                " run list must be sorted"
                            )
                        valid = False

            # for non-default entries, the group dict must not exactly
            # replicate the corresponding default entry (redundant override)
            if name != "default" and group in default and periods == default[group]:
                if verbose:
                    print(  # noqa: T201
                        f"ERROR: '{file}': '{name}/{group}'"
                        " exactly matches the default entry — redundant override"
                    )
                valid = False

    return valid


def validate_cal_groupings() -> None:
    """Validate LEGEND calibration groupings files.

    Invoked in CLI. Expects run_override.yaml and runinfo.yaml to be in the
    same directory as each groupings file.
    """
    parser = argparse.ArgumentParser(
        prog="validate-cal-groupings",
        description="Validate LEGEND calibration groupings files",
    )
    parser.add_argument("files", nargs="+", help="cal_groupings files")
    args = parser.parse_args()

    valid = True
    for file in args.files:
        d = Path(file).parent
        run_override_path = d / "run_override.yaml"
        runinfo_path = d / "runinfo.yaml"

        overridden: set[tuple[str, str]] = set()
        if run_override_path.exists() and runinfo_path.exists():
            with run_override_path.open() as f:
                run_override_entries = yaml.safe_load(f) or []
            runinfo = utils.load_dict(str(runinfo_path))
            overridden = _get_overridden_runs(run_override_entries, runinfo)
        else:
            if not run_override_path.exists():
                print(  # noqa: T201
                    f"WARNING: '{run_override_path}' not found, skipping override check"
                )
            if not runinfo_path.exists():
                print(f"WARNING: '{runinfo_path}' not found, skipping override check")  # noqa: T201

        valid &= _validate_groupings_file(file, "calgroup")
        if overridden:
            valid &= _check_cal_override_runs(file, overridden)

    if not valid:
        sys.exit(1)


def validate_phy_groupings() -> None:
    """Validate LEGEND physics groupings files.

    Invoked in CLI.
    """
    parser = argparse.ArgumentParser(
        prog="validate-phy-groupings",
        description="Validate LEGEND physics groupings files",
    )
    parser.add_argument("files", nargs="+", help="phy_groupings files")
    args = parser.parse_args()

    valid = True
    for file in args.files:
        valid &= _validate_groupings_file(file, "phygroup")

    if not valid:
        sys.exit(1)
