from __future__ import annotations

import copy
import pickle
import tempfile
from datetime import datetime
from pathlib import Path
from textwrap import dedent

import pytest
import yaml
from git.exc import InvalidGitRepositoryError

from legendmeta import (
    HadesMetadata,
    Legend1000Metadata,
    LegendMetadata,
    MetadataRepository,
)


def write_l1000db(path: Path) -> dict:
    """Write the LEGEND-1000 test metadata under `path` and return its records."""
    db = yaml.safe_load((Path(__file__).parent / "l1000db.yaml").read_text())
    for name, records in db.items():
        file = path / name
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_text(yaml.dump(records, sort_keys=False))
    return db


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
    monkeypatch.setenv("LEGEND1000_METADATA", str(tmp_path))

    # the dummy records the defaults are built from, one raw ID block each
    db = write_l1000db(tmp_path)
    dummies = db["hardware/configuration/channelmaps/l1000-p01-config.yaml"]
    geds, spms, pmts = dummies["V99999Z"], dummies["S9999Z"], dummies["PMT9999"]

    meta = Legend1000Metadata(lazy=lazy)
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
    assert statuses.S0102B == statuses.S9999Z

    chmap = meta.channelmap("20230601T000000Z")
    assert list(chmap) == ["V00001A", "V99999Z", "S9999Z", "PMT9999"]
    assert chmap.V00001A.daq.rawid == dummies["V00001A"]["daq"]["rawid"]
    assert chmap.V00001A.type == "icpc"
    assert chmap.V00001A.analysis.usability == "on"

    channel = chmap.V12345A
    assert channel.name == "V12345A"
    assert channel.location.string == 123
    assert channel.location.position == 45
    assert channel.production.crystal == "345"
    assert channel.analysis.usability == "off"
    assert channel.daq.rawid == geds["daq"]["rawid"] + 123045
    with pytest.raises(TypeError):
        channel.name = "V00000A"

    channel = chmap.S0102B
    assert channel.name == "S0102B"
    assert channel.system == "spms"
    assert channel.location.barrel == 1
    assert channel.location.fiber == "S0102"
    assert channel.location.position == "bottom"
    assert channel.analysis.usability == "ac"
    assert channel.daq.rawid == spms["daq"]["rawid"] + 1021
    assert chmap.S0102T.location.position == "top"
    assert chmap.S0102T.daq.rawid == spms["daq"]["rawid"] + 1020
    assert "S0102B" not in chmap
    # only the T and B ends of a fiber module are SiPM arrays
    with pytest.raises(KeyError):
        _ = chmap["S0102X"]

    channel = chmap.PMT1135
    assert channel.name == "PMT1135"
    assert channel.system == "pmts"
    assert channel.location.name == "wall"
    assert channel.daq.rawid == pmts["daq"]["rawid"] + 11035
    assert channel.analysis.usability == "on"
    assert chmap.PMT0104.location.name == "floor"
    assert chmap.PMT0104.daq.rawid == pmts["daq"]["rawid"] + 1004
    # the position in the tank does not follow from the name
    assert chmap.PMT0104.location.x == pmts["location"]["x"]

    # a raw ID counts from the raw ID of the default record of its system
    assert chmap.V12345A.daq.rawid - chmap.V99999Z.daq.rawid == 123045
    assert chmap.S0102B.daq.rawid - chmap.S9999Z.daq.rawid == 1021
    assert chmap.PMT1135.daq.rawid - chmap.PMT9999.daq.rawid == 11035

    # the raw ID blocks of the three systems do not overlap
    rawids = [chmap[det].daq.rawid for det in ("V12345A", "S0102B", "PMT1135")]
    assert len(set(rawids)) == len(rawids)

    unpickled = pickle.loads(pickle.dumps(meta))
    assert unpickled.hardware.detectors.germanium.diodes.V12345A.name == "V12345A"
    assert unpickled.channelmap("20230601T000000Z").S0102B.location.fiber == "S0102"

    meta = Legend1000Metadata(lazy=lazy, use_defaults=False)
    assert meta.hardware.detectors.germanium.diodes.V00001A.type == "icpc"
    with pytest.raises(FileNotFoundError):
        _ = meta.hardware.detectors.germanium.diodes.V12345A
    with pytest.raises(AttributeError):
        _ = meta.datasets.statuses.on("20230601T000000Z").V12345A
    chmap = meta.channelmap("20230601T000000Z")
    assert chmap.V00001A.analysis.usability == "on"
    with pytest.raises(KeyError):
        _ = chmap["V12345A"]
    with pytest.raises(KeyError):
        _ = chmap["S0102B"]
    with pytest.raises(KeyError):
        _ = chmap["PMT1135"]


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
            dedent("""\
                - valid_from: 20230101T000000Z
                  apply:
                    - l200-p01-config.yaml
                """)
        )

    (chmaps / "l200-p01-config.yaml").write_text(
        dedent("""\
            V00001A:
              name: V00001A
              system: geds
              daq:
                rawid: 1104000
            """)
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
