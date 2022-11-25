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

import copy
import json
import os


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
