from __future__ import annotations

import logging
import os
from getpass import getuser
from tempfile import gettempdir

from git import GitCommandError, InvalidGitRepositoryError, Repo

from legendmeta.jsondb import JsonDB

log = logging.getLogger(__name__)


class LegendMetadata:
    """LEGEND metadata.

    Class representing the LEGEND metadata repository with utilities for fast
    access.

    Parameters
    ----------
    path
        path to legend-metadata repository. If not existing, will attempt a
        git-clone through SSH. If ``None``, legend-metadata will be cloned
        in a temporary directory (see :func:`gettempdir`).
    """

    def __init__(self, path: str = None) -> None:
        self._default_git_ref = "main"

        if isinstance(path, str):
            self._repo_path = path
        else:
            self._repo_path = os.getenv(
                "LEGEND_METADATA",
                os.path.join(gettempdir(), "legend-metadata-" + getuser()),
            )

        self._repo: Repo = self._init_metadata_repo()
        self._db: JsonDB = JsonDB(self._repo_path)

    def _init_metadata_repo(self):
        """Clone legend-metadata, if not existing, and checkout default Git ref."""
        if not os.path.exists(self._repo_path):
            os.mkdir(self._repo_path)

        repo = None
        try:
            repo = Repo(self._repo_path)
        except InvalidGitRepositoryError:
            log.info(
                f"Cloning git@github.com:legend-exp/legend-metadata in {self._repo_path}..."
            )
            repo = Repo.clone_from(
                "git@github.com:legend-exp/legend-metadata", self._repo_path
            )

        repo.git.checkout(self._default_git_ref)

        return repo

    def checkout(self, git_ref: str) -> None:
        """Select legend-metadata version."""
        try:
            self._repo.git.checkout(git_ref)
        except GitCommandError:
            self._repo.remote().pull()
            self._repo.git.checkout(git_ref)

    def reset(self) -> None:
        """Checkout legend-metadata to default Git ref."""
        self._repo.git.checkout(self._default_git_ref)

    def __getitem__(self, item: str) -> JsonDB | dict:
        """Get a JsonDB (if a directory) or dict (if a JSON file) from the metadata."""
        return self._db[item]
