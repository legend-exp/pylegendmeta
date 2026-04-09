from __future__ import annotations

from pathlib import Path

import yaml

# Terminology used
# # grouping = {
#   'calgroup009a': {'p18': {'r000', 'r001', 'r002'}},
#   'calgroup009b': {'p18': {'r003', 'r004', 'r005'}},
# }
# partition: calgroup008a: p18: [r000..... r006]


def load_yaml(path: str) -> dict:
    with Path.open(path) as f:
        return yaml.safe_load(f)


def expand_run_list(value: list | str) -> list[str]:
    """Expand a YAML run value to a flat list of run strings."""
    if isinstance(value, list):
        result = []
        for item in value:
            s = str(item)
            if ".." in s:
                start, end = s.split("..")
                result.extend(
                    [f"r{n:03d}" for n in range(int(start[1:]), int(end[1:]) + 1)]
                )
            else:
                result.append(s)
        return result
    s = str(value)
    if ".." in s:
        start, end = s.split("..")
        return [f"r{n:03d}" for n in range(int(start[1:]), int(end[1:]) + 1)]
    return [s]


def compress_runs(runs: set[str] | list[str]) -> str | list[str]:
    """Compact a collection of run strings.
    Fully consecutive  → single range string  e.g. 'r000..r006'
    Non-consecutive    → flat list of individual runs  e.g. ['r000', 'r002', 'r003']
    Never mix ranges and singles inside a list."""
    nums = sorted(int(r[1:]) for r in runs)
    if nums == list(range(nums[0], nums[-1] + 1)):
        if nums[0] == nums[-1]:
            return f"r{nums[0]:03d}"
        return f"r{nums[0]:03d}..r{nums[-1]:03d}"
    return [f"r{n:03d}" for n in nums]


def partition_base(part_name: str) -> str:
    """calgroup008b  →  '008'  (zero-padded base number, for sub-partition grouping)"""
    return part_name.split("group")[-1][:-1]


def get_default_groupings(groupings: dict) -> dict:
    "default groupings should be defined in the .yaml"
    result = {}
    for part, period_dict in groupings.get("default", {}).items():
        if not isinstance(period_dict, dict):
            continue
        period_runs = {}
        for period, runs_val in period_dict.items():
            runs = expand_run_list(runs_val)  # convert r000..r002 to list of tuns
            if runs:
                period_runs[period] = set(runs)
        if period_runs:
            result[part] = period_runs  # e.g p16: [r000, r001...]
    return result


def get_full_groupings(groupings: dict, det: str) -> dict:
    """
    Similar to get_default_grouping
    Return {partition: {period: set_of_runs}} for det, falling back to defaults.
    """
    default = groupings.get("default", {})
    overrides = groupings.get(det, {})

    # different files may have different partitions so merge
    all_parts = set(default.keys()) | set(overrides.keys())

    result = {}
    for part in all_parts:
        # check if detector has an override
        source = overrides[part] if part in overrides else default.get(part, {})

        if not isinstance(source, dict):
            continue
        period_runs = {}
        for period, runs_val in source.items():
            runs = expand_run_list(runs_val)
            if runs:
                period_runs[period] = set(runs)
        if period_runs:
            result[part] = period_runs
    return result


def get_all_runs_in_grouping(grouping: dict, num: str) -> dict:
    """Union of all runs across every sub-partition sharing the same base number."""
    coverage = {}
    for part, period_runs in grouping.items():
        if partition_base(part) == num:  # select the partition of focus
            for period, runs in period_runs.items():
                if period not in coverage:
                    coverage[period] = set()
                coverage[period] |= runs
    return coverage


def intersect_groupings(group_a: dict, group_b: dict) -> dict:
    """logical AND of two {partition: {period: [set, of, runs]}}.
    When one file splits a partition (009a+009b) that the other keeps whole (009a),
    the split takes precedence.
    Single-run entries are treated separately as these are required for processing.
    So if there is a single run in a partition it is dropped, unless only on partition"""
    result = {}
    for part in set(group_a.keys()) | set(group_b.keys()):
        # Some partitions may not be in both files so grab the full run coverage of that period
        side_a = group_a.get(part) or get_all_runs_in_grouping(
            group_a, partition_base(part)
        )
        side_b = group_b.get(part) or get_all_runs_in_grouping(
            group_b, partition_base(part)
        )

        if not side_a or not side_b:
            # shouldn't really happen
            continue

        period_runs = {}
        for period in set(side_a.keys()) & set(side_b.keys()):
            runs_a = side_a[period]
            runs_b = side_b[period]

            intersection = runs_a & runs_b

            if intersection:
                period_runs[period] = intersection
            else:
                # no overlap — preserve a single-run id if exactly one side has one
                single_a = len(runs_a) == 1
                single_b = len(runs_b) == 1
                if single_a and single_b:
                    # if there is a solitary p16: r000 and p16 r002
                    # something is wrong -> has happened before
                    # print(
                    #    f"  WARNING: conflicting single-run ids for {part} {period}: "
                    #    f"escale={sorted(runs_a)} psd={sorted(runs_b)} — skipping"
                    # )
                    pass
                elif single_a:
                    period_runs[period] = runs_a
                elif single_b:
                    period_runs[period] = runs_b

        if period_runs:
            result[part] = period_runs
    return result


def grouping_to_yaml_format(assignment: dict) -> dict:
    """Convert {partition: {period: set_of_runs}} to a YAML-serialisable dict using range strings."""
    result = {}
    for part in sorted(assignment.keys()):
        result[part] = {
            period: compress_runs(runs)
            for period, runs in sorted(assignment[part].items())
        }
    return result


# ── load ──────────────────────────────────────────────────────────────────────

cal_groupings = load_yaml("cal_groupings.yaml")
escale_groupings = load_yaml("napoli_partitions_escale.yaml")
psd_groupings = load_yaml("napoli_partitions_psd.yaml")
default_partitions = set(cal_groupings.get("default", {}).keys())


def is_new_partition(part_name):
    return part_name not in default_partitions


cal_grouping_default = get_default_groupings(cal_groupings)
escale_default = get_default_groupings(escale_groupings)
psd_default = get_default_groupings(psd_groupings)

new_default = intersect_groupings(
    {part: items for part, items in escale_default.items() if is_new_partition(part)},
    {part: items for part, items in psd_default.items() if is_new_partition(part)},
)

combined_default = {}
for part, pd in cal_grouping_default.items():
    combined_default[part] = {
        period: compress_runs(runs) for period, runs in pd.items()
    }
combined_default.update(grouping_to_yaml_format(new_default))

all_hpges = (
    set(cal_groupings.keys()) | set(escale_groupings.keys()) | set(psd_groupings.keys())
) - {"default"}

combined_overrides = {}
for det in sorted(all_hpges):
    # old partitions — carry forward from og as-is
    old_parts = {}
    for part, period_dict in cal_groupings.get(det, {}).items():
        if not is_new_partition(part):
            old_parts[part] = period_dict  # keep original format

    esc_full = {
        part: item
        for part, item in get_full_groupings(escale_groupings, det).items()
        if is_new_partition(part)
    }
    psd_full = {
        part: item
        for part, item in get_full_groupings(psd_groupings, det).items()
        if is_new_partition(part)
    }
    new_combined = intersect_groupings(esc_full, psd_full)

    # filter per-partition: only keep partitions that differ from the combined default
    new_overrides = {
        part: runs
        for part, runs in new_combined.items()
        if new_default.get(part) != runs
    }

    if old_parts or new_overrides:
        entry = {}
        entry.update(old_parts)
        if new_overrides:
            entry.update(grouping_to_yaml_format(new_overrides))
        combined_overrides[det] = entry

out = {"default": combined_default}
out.update(combined_overrides)

out_path = "test_cal_groupings.yaml"
with Path.open(out_path, "w") as f:
    yaml.dump(out, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

# print(f"Written: {out_path}")
# print(f"  default partitions : {list(combined_default.keys())}")
# print(f"  detector overrides : {len(combined_overrides)}")
