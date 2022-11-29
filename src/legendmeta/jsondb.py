from __future__ import annotations

import json
import logging
import os
import re
from glob import glob
from pathlib import Path
from typing import Any, Iterator

from legendmeta.catalog import Catalog, Props

log = logging.getLogger(__name__)


class AttrsDict(dict):
    """ """

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError as e:
            raise AttributeError(e)

    def __setattr__(self, name: str, value: Any):
        self[name] = value


class JsonDB:
    """Lazy JSON database.

    The database is represented on disk by a collection of JSON files
    arbitrarily scattered in a filesystem. Access to database items is lazy by
    default: no global scan of the filesystem is performed at the beginning
    (although it is possible to override this behavior by calling
    :meth:`scan`). Once a file is queried, it is also saved in and internal
    store for faster access.

    Note
    ----
    A call to :meth:`scan` is currently needed before iterating on the database
    files.

    Examples
    --------
    >>> from legendmeta.jsondb import JsonDB
    >>> jdb = JsonDB("path/to/dir")
    >>> jdb["file1.json"]  # is a dict
    >>> jdb["file1"]  # also works
    >>> jdb["dir1"]  # JsonDB instance
    >>> jdb["dir1"]["file1"]  # nested JSON file
    >>> jdb["dir1/file1"]  # also works
    """

    def __init__(self, path: str | Path) -> None:
        self.path: Path = Path(path).expanduser().resolve()
        if not self.path.is_dir():
            raise ValueError("input path is not a valid directory")

        self._store = AttrsDict()

    def scan(self) -> None:
        """Populate the database by walking the directory."""
        for j in self.path.rglob("*.json"):
            try:
                self[j]
            except (json.JSONDecodeError, ValueError):
                log.warning(f"could not scan file {j}")

    def at(self, timestamp: str, pattern: str = None, system: str = "all") -> AttrsDict:
        """Query database in `time[, file pattern, system]`.

        A (only one) valid ``.jsonl`` file must exist in the directory to
        specify a validity mapping. This functionality relies on the
        :mod:`.catalog` module.
        """
        # get the files from the jsonl
        files = glob(os.path.join(self.path, "*.jsonl"))
        if len(files) == 0:
            raise RuntimeError("no .jsonl file found")
        if len(files) > 1:
            raise RuntimeError("unsupported: multiple .jsonl files found")
        key_resolve = files[0]
        file_list = Catalog.get_files(key_resolve, timestamp, system)
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
                Props.add_to(result, db_ptr[fp])
            db_ptr = result
        else:
            fp = self.path.rglob(file)
            fp = [i for i in fp][0]
            db_ptr = db_ptr[fp]
        return db_ptr

    def __getitem__(self, item: str | Path) -> JsonDB | AttrsDict:
        """Access files or directories in the database."""
        # resolve relative paths / links, but keep it relative to self.path
        item = Path(self.path / item).expanduser().resolve().relative_to(self.path)

        # now call this very function recursively to walk the directories to the file
        db_ptr = self
        for d in item.parts[0:-1]:
            db_ptr = db_ptr[d]

        # store JSON file names without extension
        item_id = item.name.rstrip(".json")
        if item_id not in db_ptr._store:
            obj = db_ptr.path / item.name
            if obj.is_dir():
                db_ptr._store[item_id] = JsonDB(obj)
            else:
                # try to attach .json extension if file cannot be found
                if not obj.is_file():
                    obj = Path(str(obj) + ".json")

                if obj.is_file():
                    with obj.open() as f:
                        db_ptr._store[item_id] = AttrsDict(json.load(f))
                else:
                    raise FileNotFoundError(
                        f"{str(obj).replace('.json.json', '.json')} is not a valid file or directory"
                    )

        return db_ptr._store[item_id]

    def __getattr__(self, name: str) -> JsonDB | AttrsDict:
        try:
            return self[name]
        except KeyError as e:
            raise AttributeError(e)

    def __len__(self) -> int:
        return len(self._store)

    def __iter__(self) -> Iterator:
        return iter(self._store)

    def __str__(self) -> str:
        return str(self._store)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}('{str(self.path)}')"
