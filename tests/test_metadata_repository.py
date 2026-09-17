from __future__ import annotations

import copy
import tempfile
from datetime import datetime
from pathlib import Path

import pytest
from git.exc import InvalidGitRepositoryError

from legendmeta import HadesMetadata, LegendMetadata, MetadataRepository


def test_legend_metadata_inherits_from_base():
    """Test that LegendMetadata inherits from MetadataRepository."""
    assert issubclass(LegendMetadata, MetadataRepository)


def test_hades_metadata_inherits_from_base():
    """Test that HadesMetadata inherits from MetadataRepository."""
    assert issubclass(HadesMetadata, MetadataRepository)


def test_legend_metadata_has_channelmap_method():
    """Test that LegendMetadata has the channelmap method."""
    assert hasattr(LegendMetadata, "channelmap")


def test_hades_metadata_does_not_have_channelmap_method():
    """Test that HadesMetadata does not have the channelmap method (it's legend-specific)."""
    # HadesMetadata should not have channelmap since it's specific to legend-metadata structure
    # Check that it doesn't have it or inherits it from a parent class
    assert "channelmap" not in HadesMetadata.__dict__


def test_metadata_repository_base_class_attributes():
    """Test that MetadataRepository base class has expected attributes."""
    # Check that base class has expected methods
    assert hasattr(MetadataRepository, "checkout")
    assert hasattr(MetadataRepository, "__version__")
    assert hasattr(MetadataRepository, "latest_stable_tag")
    assert hasattr(MetadataRepository, "_except_if_not_git_repo")


def test_legend_metadata_initialization():
    """Test that LegendMetadata can be initialized with a path."""
    dir1 = Path(tempfile.mkdtemp())
    (dir1 / "test.txt").touch()

    meta = LegendMetadata(dir1, lazy=True)
    assert isinstance(meta, LegendMetadata)
    assert isinstance(meta, MetadataRepository)


def test_hades_metadata_initialization():
    """Test that HadesMetadata can be initialized with a path."""
    dir1 = Path(tempfile.mkdtemp())
    (dir1 / "test.txt").touch()

    meta = HadesMetadata(dir1, lazy=True)
    assert isinstance(meta, HadesMetadata)
    assert isinstance(meta, MetadataRepository)


def test_copy_legend_metadata():
    """Test that LegendMetadata can be copied."""
    meta = LegendMetadata(path="tests/testdb", lazy=True)

    shallow = copy.copy(meta)
    assert isinstance(shallow, LegendMetadata)
    assert shallow is not meta

    deep = copy.deepcopy(meta)
    assert isinstance(deep, LegendMetadata)
    assert deep is not meta


def test_copy_hades_metadata():
    """Test that HadesMetadata can be copied."""
    meta = HadesMetadata(path="tests/testdb", lazy=True)

    shallow = copy.copy(meta)
    assert isinstance(shallow, HadesMetadata)
    assert shallow is not meta

    deep = copy.deepcopy(meta)
    assert isinstance(deep, HadesMetadata)
    assert deep is not meta


def test_legend_metadata_show_metadata_version():
    """Test that LegendMetadata has show_metadata_version method."""
    assert hasattr(LegendMetadata, "show_metadata_version")
    assert callable(LegendMetadata.show_metadata_version)


def _write_metadata(path: Path) -> Path:
    """Write the smallest metadata a channel map query needs and return its path."""
    chmaps = path / "hardware/configuration/channelmaps"
    statuses = path / "datasets/statuses"
    for directory in (chmaps, statuses):
        directory.mkdir(parents=True)
        (directory / "validity.yaml").write_text(
            "- valid_from: 20230101T000000Z\n  apply:\n    - l200-p01-config.yaml\n"
        )

    (chmaps / "l200-p01-config.yaml").write_text(
        "V00001A:\n  name: V00001A\n  system: geds\n  daq:\n    rawid: 1104000\n"
    )
    (statuses / "l200-p01-config.yaml").write_text("V00001A:\n  usability: 'on'\n")

    diodes = path / "hardware/detectors/germanium/diodes"
    diodes.mkdir(parents=True)
    (diodes / "V00001A.yaml").write_text("name: V00001A\ntype: icpc\n")

    return path


def test_non_git_metadata_without_flag():
    """A plain directory has no version, and the error says how to read it anyway."""
    meta = LegendMetadata(_write_metadata(Path(tempfile.mkdtemp())), lazy=True)

    assert not meta.is_git_repo
    with pytest.raises(InvalidGitRepositoryError, match="METADATA_NO_GIT_REPO"):
        meta.channelmap(datetime(2023, 6, 1))


def test_non_git_metadata(monkeypatch):
    """METADATA_NO_GIT_REPO reads a plain directory, without the version features."""
    monkeypatch.setenv("METADATA_NO_GIT_REPO", "1")
    meta = LegendMetadata(_write_metadata(Path(tempfile.mkdtemp())), lazy=True)

    assert not meta.is_git_repo

    channel = meta.channelmap(datetime(2023, 6, 1)).V00001A
    assert channel.daq.rawid == 1104000
    assert channel.type == "icpc"
    assert channel.analysis.usability == "on"

    with pytest.raises(InvalidGitRepositoryError, match="METADATA_NO_GIT_REPO"):
        _ = meta.__version__
    with pytest.raises(InvalidGitRepositoryError, match="METADATA_NO_GIT_REPO"):
        _ = meta.latest_stable_tag
