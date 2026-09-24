from __future__ import annotations

import copy
import pickle
import tempfile
from datetime import datetime
from pathlib import Path

import pytest
from git.exc import InvalidGitRepositoryError

from legendmeta import (
    HadesMetadata,
    Legend1000Metadata,
    LegendMetadata,
    MetadataRepository,
)


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


def test_legend1000_metadata(tmp_path):
    (tmp_path / "test.yaml").write_text("a: 1\n")

    meta = Legend1000Metadata(tmp_path, lazy=True)
    assert isinstance(meta, MetadataRepository)
    assert meta.test.a == 1


@pytest.mark.parametrize("lazy", [True, False])
def test_legend1000_metadata_defaults(monkeypatch, tmp_path, lazy):
    monkeypatch.setenv("METADATA_NO_GIT_REPO", "1")
    path = _write_metadata(tmp_path)

    config = "l200-p01-config.yaml"
    with (path / "hardware/configuration/channelmaps" / config).open("a") as f:
        f.write("V99999Z:\n  name: V99999Z\n  system: geds\n  daq:\n    rawid: 1\n")
    with (path / "datasets/statuses" / config).open("a") as f:
        f.write("V99999Z:\n  usability: 'off'\n")

    germanium = path / "hardware/detectors/germanium"
    (germanium / "diodes/V99999Z.yaml").write_text(
        "name: V99999Z\ntype: bege\nproduction:\n  order: 99\n  crystal: '999'\n  slice: Z\n"
    )
    (germanium / "crystals").mkdir()
    (germanium / "crystals/V99999.yaml").write_text(
        "name: '999'\norder: '99'\nslices:\n  Z:\n    detector_offset_in_mm: 10\n"
    )

    meta = Legend1000Metadata(path, lazy=lazy)
    diodes = meta.hardware.detectors.germanium.diodes

    # records on disk are returned unchanged
    assert diodes.V00001A.type == "icpc"
    assert diodes.V99999Z.name == "V99999Z"

    diode = diodes.V12345A
    assert diode.name == "V12345A"
    assert diode.type == "bege"
    assert diode.production.order == 12
    assert diode.production.crystal == "345"
    assert diode.production.slice == "A"
    assert meta["hardware/detectors/germanium/diodes/V12345B"].production.slice == "B"
    assert "V12345A" not in diodes
    with pytest.raises(FileNotFoundError):
        _ = diodes.V1234

    crystal = meta.hardware.detectors.germanium.crystals.V12345
    assert crystal.name == "345"
    assert crystal.order == "12"
    assert crystal.slices.A.detector_offset_in_mm == 10

    statuses = meta.datasets.statuses.on("20230601T000000Z")
    assert statuses.V00001A.usability == "on"
    assert statuses.V12345A.usability == "off"

    chmap = meta.channelmap("20230601T000000Z")
    assert list(chmap) == ["V00001A", "V99999Z"]
    assert chmap.V00001A.daq.rawid == 1104000
    assert chmap.V00001A.type == "icpc"
    assert chmap.V00001A.analysis.usability == "on"

    channel = chmap.V12345A
    assert channel.name == "V12345A"
    assert channel.production.crystal == "345"
    assert channel.analysis.usability == "off"
    with pytest.raises(TypeError):
        channel.name = "V00000A"

    unpickled = pickle.loads(pickle.dumps(meta))
    assert unpickled.hardware.detectors.germanium.diodes.V12345A.name == "V12345A"

    meta = Legend1000Metadata(path, lazy=lazy, use_defaults=False)
    assert meta.hardware.detectors.germanium.diodes.V00001A.type == "icpc"
    with pytest.raises(FileNotFoundError):
        _ = meta.hardware.detectors.germanium.diodes.V12345A
    with pytest.raises(AttributeError):
        _ = meta.datasets.statuses.on("20230601T000000Z").V12345A
    chmap = meta.channelmap("20230601T000000Z")
    assert chmap.V00001A.analysis.usability == "on"
    with pytest.raises(KeyError):
        _ = chmap["V12345A"]


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


def test_non_git_metadata_needs_files(monkeypatch):
    """With the variable set there is nothing to clone, so an empty directory is an error."""
    monkeypatch.setenv("METADATA_NO_GIT_REPO", "1")

    with pytest.raises(FileNotFoundError, match="METADATA_NO_GIT_REPO"):
        LegendMetadata(Path(tempfile.mkdtemp()), lazy=True)


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
