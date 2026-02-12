# Configuration file for the Sphinx documentation builder.
from __future__ import annotations

import sys
from importlib import metadata
from pathlib import Path

sys.path.insert(0, Path(__file__).parents[2].resolve().as_posix())

project = "pylegendmeta"
copyright = "The LEGEND Collaboration"
version = metadata.version("pylegendmeta")

extensions = [
    "sphinx.ext.githubpages",
    "sphinx.ext.autodoc",
    "sphinx.ext.mathjax",
    "sphinx.ext.napoleon",
    "sphinx.ext.intersphinx",
    "sphinx_copybutton",
    "sphinx_inline_tabs",
    "myst_parser",
]

source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}
master_doc = "index"
language = "python"

# Furo theme
html_theme = "furo"
html_theme_options = {
    "source_repository": "https://github.com/legend-exp/pylegendmeta",
    "source_branch": "main",
    "source_directory": "docs/source",
}
html_title = f"{project} {version}"

autodoc_default_options = {"ignore-module-all": True}

# sphinx-napoleon
# enforce consistent usage of NumPy-style docstrings
napoleon_numpy_docstring = True
napoleon_google_docstring = False
napoleon_use_ivar = True

# intersphinx
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "numpy": ("https://numpy.org/doc/stable", None),
    "pandas": ("https://pandas.pydata.org/docs", None),
    "matplotlib": ("https://matplotlib.org/stable", None),
    "sqlalchemy": ("https://docs.sqlalchemy.org", None),
    "pygama": ("https://pygama.readthedocs.io/en/stable", None),
    "dbetto": ("https://dbetto.readthedocs.io/en/stable", None),
}  # add new intersphinx mappings here

# sphinx-autodoc
# Include __init__() docstring in class docstring
autoclass_content = "both"
autodoc_typehints = "description"
autodoc_typehints_description_target = "documented_params"
autodoc_typehints_format = "short"
