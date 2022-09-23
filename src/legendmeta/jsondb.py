from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Iterator

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

    def __getitem__(self, item: str | Path) -> JsonDB | dict:
        """Access files or directories in the database."""
        # resolve relative paths / links, but keep it relative to self.path
        item = Path(self.path / item).expanduser().resolve().relative_to(self.path)

        # now call this very function recursively to walk the directories to the file
        db_ptr = self
        for d in item.parts[0:-1]:
            db_ptr = db_ptr[d]

        # store JSON file names without extension
        item_id = item.name.removesuffix(".json")
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
                    raise ValueError(
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
