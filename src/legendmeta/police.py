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
import json
import os
import re
import sys
from importlib import resources

from .jsondb import JsonDB

templates = resources.files("legendmeta") / "templates"


def validate_legend_channel_map() -> bool:
    """Validate list of LEGEND channel map files.

    Invoked in CLI.
    """
    parser = argparse.ArgumentParser(
        prog="validate-legend-chmaps", description="Validate LEGEND channel map files"
    )

    parser.add_argument("files", nargs="+", help="JSON channel maps files")

    args = parser.parse_args()

    dict_temp = {
        "geds": json.load(open(str(templates / "geds-channel.json"))),
        "spms": json.load(open(str(templates / "spms-channel.json"))),
    }

    for d in {os.path.dirname(f) for f in args.files}:
        db = JsonDB(d)
        valid = True

        with open(f"{d}/validity.jsonl") as f:
            for line in f.readlines():
                ts = json.loads(line)["valid_from"]
                sy = json.loads(line)["select"]
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
    adict: dict, template: dict, greedy: bool = True, typecheck=True, root_obj: str = ""
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
    """
    if not isinstance(adict, dict) or not isinstance(template, dict):
        raise ValueError("input objects must be of type dict")

    valid: bool = True

    # make sure keys in template exist and are valid in adict
    for k, v in template.items():
        if k not in adict:
            print(f"ERROR: '{root_obj}/{k}' key not found")  # noqa: T201
            valid = False
        elif isinstance(v, dict):
            if not isinstance(adict[k], dict):
                print(f"ERROR: '{root_obj}/{k}' must be a dictionary")  # noqa: T201
                valid = False
            else:
                valid *= validate_dict_schema(
                    adict[k],
                    v,
                    greedy=greedy,
                    typecheck=typecheck,
                    root_obj=f"{root_obj}/{k}",
                )
        else:
            if typecheck and not isinstance(adict[k], type(v)):
                print(  # noqa: T201
                    f"ERROR: value of '{root_obj}/{k}' must be {type(v)}"
                )  # noqa: T201
                valid = False
            elif isinstance(v, str) and v != "":
                if re.match(v, adict[k]) is None:
                    print(  # noqa: T201
                        f"ERROR: key '{root_obj}/{k}' does not match template regex '{v}'"
                    )
                    valid = False

    if greedy and len_nested(adict) != len_nested(template):
        print("ERROR: the dictionary contains extra keys")  # noqa: T201
        valid = False

    if greedy:
        valid *= validate_keys_recursive(adict, template)

    return valid


def validate_keys_recursive(adict: dict, template: dict) -> bool:
    """Return false if `adict` contains keys not in `template`."""
    valid = True

    # special case: adict is empty
    if len(adict) == 0 and len(template) != 0:
        valid = False

    # analyze adict
    for k, v in adict.items():
        if k not in template:
            print(f"ERROR: '{k}' key not allowed")  # noqa: T201
            valid = False
        elif isinstance(v, dict):
            valid *= validate_keys_recursive(v, template[k])

    return valid


def len_nested(d: dict) -> int:
    """Recursively count keys in a dictionary."""
    count = 0
    for v in d.values():
        count += 1
        if isinstance(v, dict):
            count += len_nested(v)

    return count
