import logging
import os
import os.path as path
from getpass import getuser
from tempfile import gettempdir

from git import GitCommandError, InvalidGitRepositoryError, Repo

log = logging.getLogger(__name__)


class LegendMetadata:
    def __init__(self):
        self._default_git_ref = "main"
        self._repo_path = path.join(gettempdir(), "legend-metadata-" + getuser())
        self._repo: Repo = self._init_testdata_repo()

    def _init_testdata_repo(self):

        if not path.isdir(self._repo_path):
            os.mkdir(self._repo_path)

        repo = None
        try:
            repo = Repo(self._repo_path)
        except InvalidGitRepositoryError:
            log.info(
                f"Cloning https://github.com/legend-exp/legend-metadata in {self._repo_path}..."
            )
            repo = Repo.clone_from(
                "https://github.com/legend-exp/legend-metadata", self._repo_path
            )

        repo.git.checkout(self._default_git_ref)

        return repo

    def checkout(self, git_ref: str):
        try:
            self._repo.git.checkout(git_ref)
        except GitCommandError:
            self._repo.remote().pull()
            self._repo.git.checkout(git_ref)

    def reset(self):
        self._repo.git.checkout(self._default_git_ref)

    def get_path(self, filename: str):
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
