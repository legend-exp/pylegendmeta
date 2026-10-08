from __future__ import annotations

import os
import tempfile

import pytest
from dbetto import TextDB

from legendmeta import Legend1000Metadata

pytestmark = [
    pytest.mark.xfail(run=True, reason="requires access to legend1000-metadata"),
    pytest.mark.needs_metadata,
]

tmpdir = tempfile.mkdtemp()


@pytest.fixture
def l1000meta():
    # in the CI, legend1000-metadata is cloned in advance
    return Legend1000Metadata(
        os.getenv("LEGEND1000_METADATA_TESTDIR", tmpdir), lazy=True
    )


def test_default_diode(l1000meta):
    diodes = l1000meta.hardware.detectors.germanium.diodes
    default = diodes.V99999Z
    assert default.name == "V99999Z"

    diode = diodes.V12345A
    assert diode.name == "V12345A"
    assert diode.production.order == 12
    assert diode.production.crystal == "345"
    assert diode.production.slice == "A"
    assert diode.geometry == default.geometry
    assert "V12345A" in diodes
    assert "V12345A" not in list(diodes)


def test_default_crystal(l1000meta):
    crystals = l1000meta.hardware.detectors.germanium.crystals
    default = crystals.V99999

    crystal = crystals.V12345
    assert crystal.name == "345"
    assert crystal.order == "12"
    assert crystal.slices.A == default.slices.Z


def test_default_channelmap(l1000meta):
    chmap = l1000meta.hardware.configuration.channelmaps.on("20400101T000000Z")
    assert chmap.V12345A.name == "V12345A"
    assert chmap.V12345A.system == "geds"
    assert chmap.V12345A.location.string == 123
    assert chmap.V12345A.location.position == 45
    assert chmap.V00102A.location.position == 2
    assert chmap.V12345A.daq.rawid == 1123045
    assert chmap.V00102A.daq.rawid == 1001002

    statuses = l1000meta.datasets.statuses.on("20400101T000000Z")
    assert statuses.V12345A == statuses.V99999Z


def test_default_sipm_channelmap(l1000meta):
    chmap = l1000meta.hardware.configuration.channelmaps.on("20400101T000000Z")
    assert chmap.S0102B.name == "S0102B"
    assert chmap.S0102B.system == "spms"
    assert chmap.S0102B.location.barrel == 1
    assert chmap.S0102B.location.fiber == "S0102"
    assert chmap.S0102B.location.position == "bottom"
    assert chmap.S1203T.location.position == "top"
    assert chmap.S1203T.location.barrel == 12
    assert chmap.S0102B.daq.rawid == 2001021
    assert chmap.S1203T.daq.rawid == 2012030

    statuses = l1000meta.datasets.statuses.on("20400101T000000Z")
    assert statuses.S0102B == statuses.S9999Z


def test_default_pmt_channelmap(l1000meta):
    chmap = l1000meta.hardware.configuration.channelmaps.on("20400101T000000Z")
    assert chmap.PMT1135.name == "PMT1135"
    assert chmap.PMT1135.system == "pmts"
    assert chmap.PMT1135.location.name == "wall"
    assert chmap.PMT1135.daq.rawid == 3011035
    assert chmap.PMT0104.location.name == "floor"
    assert chmap.PMT0104.daq.rawid == 3001004

    statuses = l1000meta.datasets.statuses.on("20400101T000000Z")
    assert statuses.PMT1135 == statuses.PMT9999


def test_channelmap(l1000meta):
    chmap = l1000meta.channelmap()
    assert "V99999Z" in chmap

    channel = chmap.V12345A
    assert channel.name == "V12345A"
    assert channel.production.crystal == "345"
    assert channel.analysis == chmap.V99999Z.analysis


def test_no_defaults():
    path = os.getenv("LEGEND1000_METADATA_TESTDIR", tmpdir)
    l1000meta = Legend1000Metadata(path, lazy=True, use_defaults=False)

    diodes = l1000meta.hardware.detectors.germanium.diodes
    assert isinstance(diodes, TextDB)
    assert diodes.V99999Z.name == "V99999Z"
    with pytest.raises(FileNotFoundError):
        _ = diodes.V12345A
    with pytest.raises(KeyError):
        _ = l1000meta.channelmap("20400101T000000Z")["V12345A"]


def test_write_hardware_detectors_germanium_folder(tmp_path):
    path = os.getenv("LEGEND1000_METADATA_TESTDIR", tmpdir)
    l1000meta = Legend1000Metadata(
        path, lazy=True, channels=["V12345A", "V99999Z", "S0102B"]
    )
    l1000meta.write_hardware_detectors_germanium_folder(tmp_path)

    germanium = TextDB(tmp_path / "hardware" / "detectors" / "germanium")
    assert sorted(germanium.diodes) == ["V12345A", "V99999Z"]
    assert sorted(germanium.crystals) == ["V12345", "V99999"]

    diodes = l1000meta.hardware.detectors.germanium.diodes
    crystals = l1000meta.hardware.detectors.germanium.crystals
    assert germanium.diodes.V12345A == diodes.V12345A.to_dict()
    assert germanium.crystals.V12345 == crystals.V12345.to_dict()

    with pytest.raises(ValueError, match="channels list"):
        Legend1000Metadata(path, lazy=True).write_hardware_detectors_germanium_folder(
            tmp_path
        )
