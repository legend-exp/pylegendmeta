import argparse
import json
import re
import sys
from importlib import resources

templates = resources.files("legendmeta") / "templates"


def validate_legend_channel_map() -> bool:
    """Validate list of LEGEND channel map files.

    Called in CLI.
    """
    parser = argparse.ArgumentParser(
        prog="validate-legend-chmaps", description="Validate LEGEND channel map files"
    )

    parser.add_argument("file", nargs="+", help="JSON files containing channel maps")

    args = parser.parse_args()

    spm_temp = json.load(open(str(templates / "sipm-channel.json")))
    ged_temp = json.load(open(str(templates / "hpge-channel.json")))

    valid = True

    for file in args.file:
        with open(file) as f:
            chmap = json.load(f)
            for k, v in chmap.items():
                valid *= validate_dict_schema(
                    v,
                    spm_temp if k[0] == "S" else ged_temp,
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
