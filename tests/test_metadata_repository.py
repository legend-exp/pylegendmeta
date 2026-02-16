from __future__ import annotations

import copy
import tempfile
from pathlib import Path

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
