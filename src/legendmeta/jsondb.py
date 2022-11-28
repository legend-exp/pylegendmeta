from __future__ import annotations

import json
import logging
import os
import re
from pathlib import Path
from typing import Iterator

from legendmeta.catalog import Catalog
from legendmeta.props import Props

log = logging.getLogger(__name__)


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

    tstamp_form = re.compile(r"\d{8}T\d{6}Z")

    def __init__(self, path: str | Path) -> None:
        self.path: Path = Path(path).expanduser().resolve()
        if not self.path.is_dir():
            raise ValueError("input path is not a valid directory")

        self._store: dict = {}

    def scan(self) -> None:
        """Populate the database by walking the directory."""
        for j in self.path.rglob("*.json"):
            try:
                self[j]
            except (json.JSONDecodeError, ValueError):
                log.warning(f"could not scan file {j}")

    def _time_validity(
        self, timestamp: str, system="cal", pattern=None
    ) -> JsonDB | dict:
        key_resolve = os.path.join(self.path, "key_resolve.jsonl")
        file_list = Catalog.get_files(key_resolve, timestamp, system)
        # select only files matching pattern if specified
        if pattern is not None:
            c = re.compile(pattern)
            out_files = []
            for file in file_list:
                if c.match(file):
                    out_files.append(file)
            return out_files
        else:
            return file_list

    def __gettstamp__(self, d) -> dict:
        db_ptr = self
        # define defaults
        pattern = None
        system = "all"
        # check if system or file pattern is specified
        if len(d) > 16:
            if d.count(",") == 2:
                d, system, pattern = d.split(",")
            elif d.count(",") == 1:
                d, system = d.split(",")
        # get the files from the jsonl
        files = self._time_validity(d, system=system, pattern=pattern)

        # read files in and combine as necessary
        if isinstance(files, list):
            result = {}
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

    def __getitem__(self, item: str | Path) -> JsonDB | dict:
        """Access files or directories in the database."""
        # resolve relative paths / links, but keep it relative to self.path
        item = Path(self.path / item).expanduser().resolve().relative_to(self.path)

        # now call this very function recursively to walk the directories to the file
        db_ptr = self
        if isinstance(item.parts[0], str) and self.tstamp_form.match(
            item.parts[0][:16]
        ):
            return self.__gettstamp__(item.parts[0])
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
                        db_ptr._store[item_id] = json.load(f)
                else:
                    raise FileNotFoundError(
                        f"{str(obj).replace('.json.json', '.json')} is not a valid file or directory"
                    )

        return db_ptr._store[item_id]

    def __len__(self) -> int:
        return len(self._store)

    def __iter__(self) -> Iterator:
        return iter(self._store)

    def __str__(self) -> str:
        return str(self._store)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}('{str(self.path)}')"
