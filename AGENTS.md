# OpenAI Codex

## Testing requirements

Install all required dependencies for tests, linting and docs building by using
the `all` optional dependency group defined in `pyproject.toml`. With
python-pip, you would need to run `pip install -e '.[all]'`.

Make sure the `bin` directory where executables are installed by pip is added to
the `PATH` environment variable.

There are many dependencies, so it could take a while to install them. Please
wait, do not interrupt the install process.

Use Pytest to run all unit tests.

## Linting

To run linting, use pre-commit. Always make sure pre-commit checks pass before
committing.

## Submitting a Pull Request

Make sure to include a concise description of the changes and link the relevant
pull requests or issues.
