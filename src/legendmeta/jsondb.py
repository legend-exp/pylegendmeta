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

import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator

from legendmeta.catalog import Catalog, Props

log = logging.getLogger(__name__)


class AttrsDict(dict):
    """Access dictionary items as attributes.

    Examples
    --------
    >>> d = AttrsDict({"key1": {"key2": 1}})
    >>> d.key1.key2 # == 1
    >>> d1 = AttrsDict()
    >>> d1["a"] = 1
    >>> d1.a # == 1
    """

    def __init__(self, value: dict = None) -> None:
        if value is None:
            super().__init__()
        # can only be initialized with a dict
        elif isinstance(value, dict):
            for key in value:
                self.__setitem__(key, value[key])
        else:
            raise TypeError("expected dict")

        # attribute that holds cached remappings -- see map()
        super().__setattr__("_cached_remaps", {})

    def __setitem__(self, key: str | int | float, value: Any) -> Any:
        # convert dicts to AttrsDicts
        if not isinstance(value, AttrsDict):
            if isinstance(value, dict):
                value = AttrsDict(value)  # this should make it recursive
            # recurse lists
            elif isinstance(value, list):
                for i, el in enumerate(value):
                    if isinstance(el, dict):
                        value[i] = AttrsDict(el)  # this should make it recursive

        super().__setitem__(key, value)

        # if the key is a valid attribute name, create a new attribute
        if isinstance(key, str) and key.isidentifier():
            super().__setattr__(key, value)

        # reset special _cached_remaps private attribute -- see map()
        super().__setattr__("_cached_remaps", {})

    __setattr__ = __setitem__

    def __getattr__(self, name: str) -> Any:
        try:
            super().__getattr__(name)
        except AttributeError:
            raise AttributeError(f"dictionary does not contain a '{name}' key")

    def map(self, label: str, unique: bool = True) -> AttrsDict:
        """Remap dictionary according to an alternative unique label.

        Loop over keys in the first level and search for key named `label` in
        their values. If `label` is found and its value `newid` is unique,
        create a mapping between `newid` and the first-level dictionary `obj`.
        If `label` is of the form ``key.label``, ``label`` will be searched in
        a dictionary keyed by ``key``. If the label is unique a dictionary of
        dictionaries will be returned, if not unique and `unique` is false, a
        dictionary will be returned where each entry is a dictionary of
        dictionaries keyed by an arbitrary integer.

        Parameters
        ----------
        label
            name (key) at which the new label can be found. If nested in
            dictionaries, use ``.`` to separate levels, e.g.
            ``level1.level2.label``.
        unique
            bool specifying whether only unique keys are allowed. If true
            will raise an error if the specified key is not unique.

        Examples
        --------
        >>> d = AttrsDict({
        ...   "a": {
        ...     "id": 1,
        ...     "group": {
        ...       "id": 3,
        ...     },
        ...     "data": "x"
        ...   },
        ...   "b": {
        ...     "id": 2,
        ...     "group": {
        ...       "id": 4,
        ...     },
        ...     "data": "y"
        ...   },
        ... })
        >>> d.map("id")[1].data == "x"
        True
        >>> d.map("group.id")[4].data == "y"
        True

        Note
        ----
        No copy is performed, the returned dictionary is made of references to
        the original objects.

        Warning
        -------
        The result is cached internally for fast access after the first call.
        If the dictionary is modified, the cache gets cleared.
        """
        # if this is a second call, return the cached result
        if label in self._cached_remaps:
            return self._cached_remaps[label]

        splitk = label.split(".")
        newmap = AttrsDict()
        unique_tracker = True
        # loop over values in the first level
        for v in self.values():
            # find the (nested) label value
            newid = v
            try:
                for k in splitk:
                    newid = newid[k]
            # just skip if the label is not there
            except (KeyError, TypeError, FileNotFoundError):
                continue

            if not isinstance(newid, (int, float, str)):
                raise RuntimeError(f"'{label}' values are not all numbers or strings")
            # complain if a label with the same value was already found
            if newid in newmap:
                newkey = sorted(newmap[newid].keys())[-1] + 1
                newmap[newid].update({newkey: v})
                unique_tracker = False
            else:
                # add an item to the new dict with key equal to the value of the label
                newmap[newid] = {0: v}

        if unique is True and unique_tracker is False:
            raise RuntimeError(f"'{label}' values are not unique")

        if unique_tracker is True:
            newmap = AttrsDict({entry: newmap[entry][0] for entry in newmap})

        if not newmap:
            raise ValueError(f"could not find '{label}' anywhere in the dictionary")

        # cache it
        self._cached_remaps[label] = newmap
        return newmap

    # d |= other_d should still produce a valid AttrsDict
    def __ior__(self, other: dict | AttrsDict) -> AttrsDict:
        return AttrsDict(super().__ior__(other))

    # d1 | d2 should still produce a valid AttrsDict
    def __or__(self, other: dict | AttrsDict) -> AttrsDict:
        return AttrsDict(super().__or__(other))


class JsonDB:
    """Bare-bones JSON database.

    The database is represented on disk by a collection of JSON files
    arbitrarily scattered in a filesystem. Subdirectories are also
    :class:`.JsonDB` objects. In memory, the database is represented as an
    :class:`AttrsDict`.

    Note
    ----
    For large databases, a basic "lazy" mode is available. In this case, no
    global scan of the filesystem is performed at initialization time. Once a
    file is queried, it is also cached in the internal store for faster access.
    A call to :meth:`scan` is needed before iterating on the database files.

    Examples
    --------
    >>> from legendmeta.jsondb import JsonDB
    >>> jdb = JsonDB("path/to/dir")
    >>> jdb["file1.json"]  # is a dict
    >>> jdb["file1"]  # also works
    >>> jdb["dir1"]  # JsonDB instance
    >>> jdb["dir1"]["file1"]  # nested JSON file
    >>> jdb["dir1/file1"]  # also works
    >>> jdb.dir1.file # keys can be accessed as attributes
    """

    def __init__(self, path: str | Path, lazy: bool = False) -> None:
        """Construct a :class:`.JsonDB` object.

        Parameters
        ----------
        path
            path to the directory containing the database.
        lazy
            whether a database scan should be performed at initialization time.
        """
        self.path: Path = Path(path).expanduser().resolve()
        if not self.path.is_dir():
            raise ValueError("input path is not a valid directory")

        self._store = AttrsDict()

        if not lazy:
            self.scan()

    def scan(self, subdir: str = ".") -> None:
        """Populate the database by walking the filesystem.

        Parameters
        ----------
        subdir
            restrict scan to path relative to the database location.
        """
        for j in self.path.rglob(f"{subdir}/*.json"):
            try:
                self[j]
            except (json.JSONDecodeError, ValueError) as e:
                log.warning(f"could not scan file {j}")
                log.warning(f"reason: {e}")

    def keys(self) -> list[str]:
        return self._store.keys()

    def items(self) -> Iterator[(str, JsonDB | AttrsDict | list)]:
        return self._store.items()

    def on(
        self, timestamp: str | datetime, pattern: str = None, system: str = "all"
    ) -> AttrsDict | list:
        """Query database in `time[, file pattern, system]`.

        A (only one) valid ``validity.jsonl`` file must exist in the directory
        to specify a validity mapping. This functionality relies on the
        :mod:`.catalog` module.

        Parameters
        ----------
        timestamp
            a :class:`~datetime.datetime` object or a string matching the
            pattern ``YYYYmmddTHHMMSSZ``.
        pattern
            query by filename pattern.
        system: 'all', 'phy', 'cal', 'lar', ...
            query only a data taking "system".
        """
        jsonl = os.path.join(self.path, "validity.jsonl")
        if not os.path.isfile(jsonl):
            raise RuntimeError(f"no validity.jsonl file found in {self.path}")

        file_list = Catalog.get_files(jsonl, timestamp, system)
        # select only files matching pattern if specified
        if pattern is not None:
            c = re.compile(pattern)
            out_files = []
            for file in file_list:
                if c.match(file):
                    out_files.append(file)
            files = out_files
        else:
            files = file_list

        # read files in and combine as necessary
        db_ptr = self
        if isinstance(files, list):
            result = AttrsDict()
            for file in files:
                fp = self.path.rglob(file)
                fp = [i for i in fp][0]
                # TODO: what does this do exactly?
                Props.add_to(result, db_ptr[fp])
            db_ptr = result
        else:
            fp = self.path.rglob(file)
            fp = [i for i in fp][0]
            db_ptr = db_ptr[fp]
        Props.subst_vars(db_ptr, var_values={"_": self.path})
        return db_ptr

    def map(self, label: str, unique: bool = True) -> AttrsDict:
        """Remap dictionary according to a second unique `key`.

        See Also
        --------
        AttrsDict.map

        Warning
        -------
        If the database is lazy, call :meth:`.scan` in advance to populate it
        (or a subdirectory).
        """
        return self._store.map(label, unique=unique)

    def __getitem__(self, item: str | Path) -> JsonDB | AttrsDict | list:
        """Access files or directories in the database."""
        # resolve relative paths / links, but keep it relative to self.path
        item = Path(self.path / item).expanduser().resolve().relative_to(self.path)

        # now call this very function recursively to walk the directories to the file
        db_ptr = self
        for d in item.parts[0:-1]:
            db_ptr = db_ptr[d]

        # item_id should not contain any / at this point
        # store JSON file names without extension
        item_id = item.stem
        # skip if object is already in the store
        if item_id not in db_ptr._store:
            obj = db_ptr.path / item.name
            # if directory, construct another JsonDB object
            if obj.is_dir():
                db_ptr._store[item_id] = JsonDB(obj)
            else:
                # try to attach .json extension if file cannot be found
                if not obj.is_file():
                    obj = Path(str(obj) + ".json")

                # if it's a valid JSON file, construct an AttrsDict object
                if obj.is_file():
                    with obj.open() as f:
                        loaded = json.load(f)
                        if isinstance(loaded, dict):
                            loaded = AttrsDict(loaded)
                            Props.subst_vars(loaded, var_values={"_": self.path})
                        else:  # must be a list, check if there are dicts inside to convert
                            for i, el in enumerate(loaded):
                                if isinstance(el, dict):
                                    loaded[i] = AttrsDict(el)
                                    Props.subst_vars(
                                        loaded[i], var_values={"_": self.path}
                                    )

                        db_ptr._store[item_id] = loaded
                else:
                    raise FileNotFoundError(
                        f"{str(obj).replace('.json.json', '.json')} is not a valid file or directory"
                    )

            # set also an attribute, if possible
            if item_id.isidentifier():
                self.__setattr__(item_id, db_ptr._store[item_id])

        return db_ptr._store[item_id]

    def __getattr__(self, name: str) -> JsonDB | AttrsDict | list:
        try:
            return self[name]
        except KeyError:
            raise AttributeError(f"database does not contain '{name}'")

    # NOTE: self cannot stay a JsonDB, since the class is characterized by a
    # (unique) root directory. What would be the root directory of the merged
    # JsonDB?
    def __ior__(self, other: JsonDB) -> AttrsDict:
        raise TypeError("cannot merge JsonDB in-place")

    # NOTE: returning a JsonDB does not make much sense, see above
    def __or__(self, other: JsonDB) -> AttrsDict:
        if isinstance(other, JsonDB):
            return self._store | other._store
        else:
            return self._store | other

    def __len__(self) -> int:
        return len(self._store)

    def __iter__(self) -> Iterator:
        return iter(self._store)

    def __str__(self) -> str:
        return str(self._store)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}('{str(self.path)}')"
