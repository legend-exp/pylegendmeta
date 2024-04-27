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
import re
import sys
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from . import utils
from .catalog import Catalog, Props

log = logging.getLogger(__name__)


class AttrsDict(dict):
    """Access dictionary items as attributes.

    Examples
    --------
    >>> d = AttrsDict({"key1": {"key2": 1}})
    >>> d.key1.key2
    1
    >>> d1 = AttrsDict()
    >>> d1["a"] = 1
    >>> d1.a
    1
    """

    def __init__(self, value: dict | None = None) -> None:
        """Construct an :class:`.AttrsDict` object.

        Note
        ----
        The input dictionary is copied.

        Parameters
        ----------
        value
            a :class:`dict` object to initialize the instance with.
        """
        if value is None:
            super().__init__()
        # can only be initialized with a dict
        elif isinstance(value, dict):
            for key in value:
                self.__setitem__(key, value[key])
        else:
            msg = "expected dict"
            raise TypeError(msg)

        # attribute that holds cached remappings -- see map()
        super().__setattr__("__cached_remaps__", {})

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

        # reset special __cached_remaps__ private attribute -- see map()
        super().__setattr__("__cached_remaps__", {})

    __setattr__ = __setitem__

    def __getattr__(self, name: str) -> Any:
        try:
            super().__getattr__(name)
        except AttributeError as exc:
            msg = f"dictionary does not contain a '{name}' key"
            raise AttributeError(msg) from exc

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
        if label in self.__cached_remaps__:
            return self.__cached_remaps__[label]

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
                msg = f"'{label}' values are not all numbers or strings"
                raise RuntimeError(msg)

            if newid in newmap:
                newkey = sorted(newmap[newid].keys())[-1] + 1
                newmap[newid].update({newkey: v})
                unique_tracker = False
            else:
                # add an item to the new dict with key equal to the value of the label
                newmap[newid] = {0: v}

        if unique is True and unique_tracker is False:
            # complain if a label with the same value was already found
            msg = f"'{label}' values are not unique"
            raise RuntimeError(msg)

        if unique_tracker is True:
            newmap = AttrsDict({entry: newmap[entry][0] for entry in newmap})

        if not newmap:
            msg = f"could not find '{label}' anywhere in the dictionary"
            raise ValueError(msg)

        # cache it
        self.__cached_remaps__[label] = newmap
        return newmap

    def group(self, label: str) -> AttrsDict:
        """Group dictionary according to a `label`.

        This is equivalent to :meth:`.map` with `unique` set to ``False``.

        Parameters
        ----------
        label
            name (key) at which the new label can be found. If nested in
            dictionaries, use ``.`` to separate levels, e.g.
            ``level1.level2.label``.

        Examples
        --------
        >>> d = AttrsDict({
        ...   "a": {
        ...     "type": "A",
        ...     "data": 1
        ...   },
        ...   "b": {
        ...     "type": "A",
        ...     "data": 2
        ...   },
        ...   "c": {
        ...     "type": "B",
        ...     "data": 3
        ...   },
        ... })
        >>> d.group("type").keys()
        dict_keys(['A', 'B'])
        >>> d.group("type").A.values()
        dict_values([{'type': 'A', 'data': 1}, {'type': 'A', 'data': 2}])
        >>> d.group("type").B.values()
        dict_values([{'type': 'B', 'data': 3}])
        >>> d.group("type").A.map("data")[1]
        {'type': 'A', 'data': 1}

        See Also
        --------
        map
        """
        return self.map(label, unique=False)

    # d |= other_d should still produce a valid AttrsDict
    def __ior__(self, other: dict | AttrsDict) -> AttrsDict:
        return AttrsDict(super().__ior__(other))

    # d1 | d2 should still produce a valid AttrsDict
    def __or__(self, other: dict | AttrsDict) -> AttrsDict:
        return AttrsDict(super().__or__(other))


def JsonDB(*args, **kwargs):
    import warnings

    warnings.warn(
        "The JsonDB class has been renamed to TextDB. "
        "Please update your code, as JsonDB will be removed in a future release.",
        DeprecationWarning,
        stacklevel=2,
    )
    return TextDB(*args, **kwargs)


class TextDB:
    """A simple text file database.

    The database is represented on disk by a collection of text files
    arbitrarily scattered in a filesystem. Subdirectories are also
    :class:`.TextDB` objects. In memory, the database is represented as an
    :class:`AttrsDict`.

    Currently supported file formats are JSON and YAML.

    Tip
    ---
    For large databases, a basic "lazy" mode is available. In this case, no
    global scan of the filesystem is performed at initialization time. Once a
    file is queried, it is also cached in the internal store for faster access.
    Caution, this option is for advanced use (see warning message below).

    Warning
    -------
    A manual call to :meth:`scan` is needed before most class methods (e.g.
    iterating on the database files) can be properly used.

    Examples
    --------
    >>> from legendmeta.jsondb import TextDB
    >>> jdb = TextDB("path/to/dir")
    >>> jdb["file1.json"]  # is a dict
    >>> jdb["file1.yaml"]  # is a dict
    >>> jdb["file1"]  # also works
    >>> jdb["dir1"]  # TextDB instance
    >>> jdb["dir1"]["file1"]  # nested file
    >>> jdb["dir1/file1"]  # also works
    >>> jdb.dir1.file # keys can be accessed as attributes
    """

    def __init__(
        self, path: str | Path, lazy: str | bool = False, hidden: bool = False
    ) -> None:
        """Construct a :class:`.TextDB` object.

        Parameters
        ----------
        path
            path to the directory containing the database.
        lazy
            whether a database scan should be performed at initialization time.
            if ``auto``, be non-lazy only if working in a python interactive
            session.
        hidden
            ignore hidden (i.e. starting with ".") files of directories.
        """
        if isinstance(lazy, bool):
            self.__lazy__ = lazy
        elif lazy == "auto":
            self.__lazy__ = not hasattr(sys, "ps1")
        else:
            msg = f"unrecognized value {lazy=}"
            raise ValueError(msg)

        self.__hidden__ = hidden
        self.__path__ = Path(path).expanduser().resolve()

        if not self.__path__.is_dir():
            msg = "input path is not a valid directory"
            raise ValueError(msg)

        self.__store__ = AttrsDict()
        self.__ftypes__ = {"json", "yaml"}

        if not self.__lazy__:
            self.scan()

    @property
    def __extensions__(self) -> set:
        # determine list of supported file extensions
        return set().union(
            *[
                exts
                for ft, exts in utils.__file_extensions__.items()
                if ft in self.__ftypes__
            ]
        )

    def scan(self, recursive: bool = True, subdir: str = ".") -> None:
        """Populate the database by walking the filesystem.

        Parameters
        ----------
        recursive
            if ``True``, recurse subdirectories.
        subdir
            restrict scan to path relative to the database location.
        """
        # recursive search or not?
        _fcn = self.__path__.rglob if recursive else self.__path__.glob
        # build file list
        flist = (p for p in _fcn(f"{subdir}/*") if p.suffix in self.__extensions__)

        for j in flist:
            try:
                self[j.with_suffix("")]
            except (json.JSONDecodeError, yaml.YAMLError, ValueError) as e:
                msg = f"could not scan file {j}, reason: {e!r}"
                log.warning(msg)

    def keys(self) -> list[str]:
        return self.__store__.keys()

    def items(self) -> Iterator[(str, TextDB | AttrsDict | list)]:
        return self.__store__.items()

    def on(
        self, timestamp: str | datetime, pattern: str | None = None, system: str = "all"
    ) -> AttrsDict | list:
        """Query database in `time[, file pattern, system]`.

        A (only one) valid ``validity.jsonl`` file must exist in the directory
        to specify a validity mapping. This functionality relies on the
        :class:`.catalog.Catalog` class.

        The JSONL specification is documented at `this link
        <https://legend-exp.github.io/legend-data-format-specs/dev/metadata/#Specifying-metadata-validity-in-time-(and-system)>`_.

        The special ``$_`` string is expanded to the directory containing the
        text files.

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
        jsonl = self.__path__ / "validity.jsonl"
        if not jsonl.is_file():
            msg = f"no validity.jsonl file found in {self.__path__!s}"
            raise RuntimeError(msg)

        file_list = Catalog.get_files(str(jsonl), timestamp, system)
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
                fp = self.__path__.rglob(file)
                # TODO: what does this do exactly?
                Props.add_to(result, db_ptr[next(iter(fp))])
            db_ptr = result
        else:
            fp = self.__path__.rglob(file)
            db_ptr = db_ptr[next(iter(fp))]
        Props.subst_vars(db_ptr, var_values={"_": self.__path__})
        return db_ptr

    def map(self, label: str, unique: bool = True) -> AttrsDict:
        """Remap dictionary according to a second unique `label`.

        See Also
        --------
        AttrsDict.map

        Warning
        -------
        If the database is lazy, you must call :meth:`.scan` in advance to
        populate it, otherwise mappings cannot be created.
        """
        return self.__store__.map(label, unique=unique)

    def group(self, label: str) -> AttrsDict:
        """Group dictionary according to a second unique `label`.

        See Also
        --------
        AttrsDict.group

        Warning
        -------
        If the database is lazy, you must call :meth:`.scan` in advance to
        populate it, otherwise groupings cannot be created.
        """
        return self.__store__.group(label)

    def __getitem__(self, item: str | Path) -> TextDB | AttrsDict | list | None:
        """Access files or directories in the database."""
        # resolve relative paths / links, but keep it relative to self.__path__
        item = Path(item)

        if item.is_absolute() and item.is_relative_to(self.__path__):
            item = item.expanduser().resolve().relative_to(self.__path__)
        elif not item.is_absolute():
            item = (
                (self.__path__ / item).expanduser().resolve().relative_to(self.__path__)
            )
        else:
            msg = f"{item} lies outside the database root path {self.__path__!s}"
            raise ValueError(msg)

        ext_list = "[" + "|".join(self.__extensions__) + "]"
        msg = f"parsing directory or file{ext_list}: {item}"
        log.debug(msg)

        # now call this very function recursively to walk the directories to the file
        db_ptr = self
        for d in item.parts[:-1]:
            db_ptr = db_ptr[d]
            # check if we encountered hidden directory (must skip)
            if not self.__hidden__ and db_ptr is None:
                return None

        # item_id should not contain any / at this point
        # store file names without extension
        item_id = item.stem
        # skip if object is already in the store
        if item_id not in db_ptr.__store__:
            obj = db_ptr.__path__ / item.name

            # do not consider hidden files
            if not self.__hidden__ and obj.name.startswith("."):
                return None

            # if directory, construct another TextDB object
            if obj.is_dir():
                db_ptr.__store__[item_id] = TextDB(obj, lazy=self.__lazy__)

            else:
                # try to attach an extension if file cannot be found
                # but check if there are multiple files that only differ in extension (unsupported)
                found = True
                if not obj.is_file():
                    found = False
                    for ext in self.__extensions__:
                        if obj.with_suffix(ext).is_file():
                            if found:
                                msg = "the database cannot contain files that differ only in the extension"
                                raise RuntimeError(msg)

                            obj = obj.with_suffix(ext)
                            found = True

                if not found:
                    msg = f"{obj.with_stem(ext_list)} is not a valid file or directory"
                    raise FileNotFoundError(msg)

                # if it's a valid file, construct an AttrsDict object
                loaded = utils.load_dict(obj)
                if isinstance(loaded, dict):
                    loaded = AttrsDict(loaded)
                    Props.subst_vars(loaded, var_values={"_": self.__path__})
                else:  # must be a list, check if there are dicts inside to convert
                    for i, el in enumerate(loaded):
                        if isinstance(el, dict):
                            loaded[i] = AttrsDict(el)
                            Props.subst_vars(loaded[i], var_values={"_": self.__path__})

                db_ptr.__store__[item_id] = loaded

            # set also an attribute, if possible
            if item_id.isidentifier():
                db_ptr.__setattr__(item_id, db_ptr.__store__[item_id])

        return db_ptr.__store__[item_id]

    def __getattr__(self, name: str) -> TextDB | AttrsDict | list:
        try:
            return object.__getattribute__(self, name)
        except AttributeError:
            try:
                return self.__getitem__(name)
            except AttributeError as exc:
                msg = f"file database does not contain '{name}'"
                raise AttributeError(msg) from exc

    # NOTE: self cannot stay a TextDB, since the class is characterized by a
    # (unique) root directory. What would be the root directory of the merged
    # TextDB?
    def __ior__(self, other: TextDB) -> AttrsDict:
        msg = "cannot merge TextDB in-place"
        raise TypeError(msg)

    # NOTE: returning a TextDB does not make much sense, see above
    def __or__(self, other: TextDB) -> AttrsDict:
        if isinstance(other, TextDB):
            return self.__store__ | other.__store__

        return self.__store__ | other

    def __contains__(self, value: str) -> bool:
        return self.__store__.__contains__(value)

    def __len__(self) -> int:
        return len(self.__store__)

    def __iter__(self) -> Iterator:
        return iter(self.__store__)

    def __str__(self) -> str:
        return str(self.__store__)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}('{self.__path__!s}')"
