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
    assert "V12345A" not in diodes


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

    statuses = l1000meta.datasets.statuses.on("20400101T000000Z")
    assert statuses.V12345A == statuses.V99999Z


def test_channelmap(l1000meta):
    chmap = l1000meta.channelmap("20400101T000000Z")
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
