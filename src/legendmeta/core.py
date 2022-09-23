import logging
import os
import os.path as path
from getpass import getuser
from tempfile import gettempdir

from git import GitCommandError, InvalidGitRepositoryError, Repo

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
            self._repo_path = path.join(gettempdir(), "legend-metadata-" + getuser())

        self._repo: Repo = self._init_testdata_repo()

    def _init_testdata_repo(self):
        """Clone legend-metadata, if not existing, and checkout default Git ref."""
        if not path.exists(self._repo_path):
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

    def get_path(self, filename: str) -> str:
        """Get an absolute path to a LEGEND metadata file.

        Parameters
        ----------
        filename : str
            path of the file relative to legend-metadata
        """
        full_path = path.abspath(path.join(self._repo_path, filename))

        if not path.exists(full_path):
            raise FileNotFoundError(
                f'Test file/directory "{filename}" not found in legend-metadata repository'
            )

        return full_path
