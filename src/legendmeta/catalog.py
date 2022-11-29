#
# Copyright (C) 2015 Oliver Schulz <oschulz@mpp.mpg.de>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import bisect
import collections
import copy
import json
import os
import types
from collections import namedtuple
from datetime import datetime


def unix_time(value):
    if isinstance(value, str):
        return datetime.timestamp(datetime.strptime(value, "%Y%m%dT%H%M%SZ"))
    elif isinstance(value, datetime):
        return datetime.timestamp(value)
    else:
        raise ValueError(f"Can't convert type {type(value)} to unix time")


class PropsStream:
    @staticmethod
    def get(value):
        if isinstance(value, str):
            return PropsStream.read_from(value)
        elif isinstance(value, collections.abc.Sequence) or isinstance(
            value, types.GeneratorType
        ):
            return value
        else:
            raise ValueError(f"Can't get PropsStream from value of type {type(value)}")

    @staticmethod
    def read_from(file_name):
        with open(file_name) as file:
            for json_str in file:
                yield json.loads(json_str)


class Catalog(namedtuple("Catalog", ["entries"])):
    __slots__ = ()

    class Entry(namedtuple("Entry", ["valid_from", "file"])):
        __slots__ = ()

    @staticmethod
    def get(value):
        if isinstance(value, Catalog):
            return value
        if isinstance(value, str):
            return Catalog.read_from(value)
        else:
            raise ValueError(f"Can't get Catalog from value of type {type(value)}")

    @staticmethod
    def read_from(file_name):
        entries = {}

        for props in PropsStream.get(file_name):
            timestamp = props["valid_from"]
            if props.get("select") is None:
                system = "all"
            else:
                system = props["select"]
            file_key = props["apply"]
            if system not in entries:
                entries[system] = []
            entries[system].append(Catalog.Entry(unix_time(timestamp), file_key))

        for system in entries:
            entries[system] = sorted(
                entries[system], key=lambda entry: entry.valid_from
            )
        return Catalog(entries)

    def valid_for(self, timestamp, system="all", allow_none=False):
        if system in self.entries:
            valid_from = [entry.valid_from for entry in self.entries[system]]
            pos = bisect.bisect_right(valid_from, unix_time(timestamp))
            if pos > 0:
                return self.entries[system][pos - 1].file
            else:
                if allow_none:
                    return None
                else:
                    raise RuntimeError(
                        f"No valid entries found for timestamp: {timestamp}, system: {system}"
                    )
        else:
            if allow_none:
                return None
            else:
                raise RuntimeError(f"No entries found for system: {system}")

    @staticmethod
    def get_files(catalog_file, timestamp, category="all"):
        catalog = Catalog.read_from(catalog_file)
        return Catalog.valid_for(catalog, timestamp, category)


class Props:
    @staticmethod
    def read_from(sources, subst_pathvar=False, subst_env=False, trim_null=False):
        def read_impl(sources):
            if isinstance(sources, str):
                file_name = sources
                with open(file_name) as file:
                    result = json.load(file)
                    if subst_pathvar:
                        Props.subst_vars(
                            result,
                            var_values={"_": os.path.dirname(file_name)},
                            use_env=False,
                            ignore_missing=True,
                        )
                    return result

            elif isinstance(sources, list):
                result = {}
                for p in map(read_impl, sources):
                    Props.add_to(result, p)
                return result
            else:
                raise ValueError(
                    f"Can't run Props.read_from on sources-value of type {type(sources)}"
                )

        result = read_impl(sources)
        if subst_env:
            Props.subst_vars(result, var_values={}, use_env=True, ignore_missing=False)
        if trim_null:
            Props.trim_null(result)
        return result

    @staticmethod
    def write_to(file_name, obj, pretty=False):
        separators = None if pretty else (",", ":")
        indent = 2 if pretty else None
        with open(file_name, "w") as file:
            json.dump(obj, file, indent=indent, separators=separators)
            file.write("\n")

    @staticmethod
    def add_to(props_a, props_b):
        a = props_a
        b = props_b

        for key in b:
            if key in a:
                if isinstance(a[key], dict) and isinstance(b[key], dict):
                    Props.add_to(a[key], b[key])
                elif a[key] != b[key]:
                    a[key] = copy.copy(b[key])
            else:
                a[key] = copy.copy(b[key])

    @staticmethod
    def trim_null(props_a):
        a = props_a

        for key in a.keys():
            if isinstance(a[key], dict):
                Props.trim_null(a[key])
            elif a[key] is None:
                del a[key]
