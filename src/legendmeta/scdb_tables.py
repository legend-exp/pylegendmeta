# Copyright (C) 2022 Luigi Pertoldi <gipert@pm.me>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

# ruff: noqa: RUF009

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import sqlalchemy as db
from sqlalchemy.orm import DeclarativeBase, Mapped

from .textdb import AttrsDict


class Base(DeclarativeBase):
    pass


@dataclass
class DiodeSnap(Base):
    """Monitored parameters of HPGe detectors."""

    __tablename__ = "diode_snap"

    crate: Mapped[int]
    slot: Mapped[int]
    channel: Mapped[int]
    vmon: Mapped[float]
    imon: Mapped[float]
    status: Mapped[int]
    almask: Mapped[int]
    tstamp: Mapped[datetime] = db.orm.mapped_column(primary_key=True)

    def asdict(self) -> AttrsDict:
        return AttrsDict({"vmon": self.vmon, "imon": self.imon, "status": self.status})


@dataclass
class DiodeConfMon(Base):
    """Configuration parameters of HPGe detectors."""

    __tablename__ = "diode_conf_mon"

    confid: Mapped[int]
    crate: Mapped[int]
    slot: Mapped[int]
    channel: Mapped[int]
    vset: Mapped[float]
    iset: Mapped[float]
    rup: Mapped[int]
    rdown: Mapped[int]
    trip: Mapped[float]
    vmax: Mapped[int]
    pwkill: Mapped[str]
    pwon: Mapped[str]
    tstamp: Mapped[datetime] = db.orm.mapped_column(primary_key=True)

    def asdict(self) -> AttrsDict:
        return AttrsDict(
            {
                "vset": self.vset,
                "iset": self.iset,
                "rup": self.rup,
                "rdown": self.rdown,
                "trip": self.trip,
                "vmax": self.vmax,
                "pwkill": self.pwkill,
                "pwon": self.pwon,
            }
        )


@dataclass
class DiodeInfo(Base):
    """Static information about HPGe detectors."""

    __tablename__ = "diode_info"

    crate: Mapped[int]
    slot: Mapped[int]
    channel: Mapped[int]
    group: Mapped[str]
    label: Mapped[str]
    status: Mapped[int]
    itol: Mapped[float]
    vtol: Mapped[float]
    iupd: Mapped[float]
    vupd: Mapped[float]
    tstamp: Mapped[datetime] = db.orm.mapped_column(primary_key=True)

    def asdict(self) -> AttrsDict:
        return AttrsDict({"group": self.group, "label": self.label})


@dataclass
class SiPMSnap(Base):
    """Monitored parameters of SiPMs from the LAr instrumentation."""

    __tablename__ = "sipm_snap"

    board: Mapped[int]
    channel: Mapped[int]
    vmon: Mapped[float]
    imon: Mapped[float]
    status: Mapped[int]
    almask: Mapped[int]
    tstamp: Mapped[datetime] = db.orm.mapped_column(primary_key=True)

    def asdict(self) -> AttrsDict:
        return AttrsDict({"vmon": self.vmon, "imon": self.imon, "status": self.status})


class SiPMConfMon(Base):
    """Configuration parameters of SiPMs from the LAr instrumentation."""

    __tablename__ = "sipm_conf_mon"

    confid: Mapped[int]
    board: Mapped[int]
    channel: Mapped[int]
    vset: Mapped[float]
    iset: Mapped[float]
    tstamp: Mapped[datetime] = db.orm.mapped_column(primary_key=True)

    def asdict(self) -> AttrsDict:
        return AttrsDict(
            {
                "vset": self.vset,
                "iset": self.iset,
            }
        )


@dataclass
class SiPMInfo(Base):
    """Static information about SiPMs from the LAr instrumentation."""

    __tablename__ = "sipm_info"

    board: Mapped[int]
    channel: Mapped[int]
    group: Mapped[str]
    label: Mapped[str]
    status: Mapped[int]
    itol: Mapped[float]
    vtol: Mapped[float]
    iupd: Mapped[float]
    vupd: Mapped[float]
    tstamp: Mapped[datetime] = db.orm.mapped_column(primary_key=True)

    def asdict(self) -> AttrsDict:
        return AttrsDict({"group": self.group, "label": self.label})


@dataclass
class MuonSnap(Base):
    """Monitored parameters of PMTs from the muon veto."""

    __tablename__ = "muon_snap"

    crate: Mapped[int]
    slot: Mapped[int]
    channel: Mapped[int]
    vmon: Mapped[float]
    imon: Mapped[float]
    status: Mapped[int]
    almask: Mapped[int]
    tstamp: Mapped[datetime] = db.orm.mapped_column(primary_key=True)

    def asdict(self) -> AttrsDict:
        return AttrsDict({"vmon": self.vmon, "imon": self.imon, "status": self.status})


@dataclass
class MuonConfMon(Base):
    """Configuration parameters of PMTs from the muon veto."""

    __tablename__ = "muon_conf_mon"

    confid: Mapped[int]
    crate: Mapped[int]
    slot: Mapped[int]
    channel: Mapped[int]
    vset: Mapped[float]
    iset: Mapped[float]
    rup: Mapped[int]
    rdown: Mapped[int]
    trip: Mapped[float]
    vmax: Mapped[int]
    pwkill: Mapped[str]
    pwon: Mapped[str]
    tstamp: Mapped[datetime] = db.orm.mapped_column(primary_key=True)

    def asdict(self) -> AttrsDict:
        return AttrsDict(
            {
                "vset": self.vset,
                "iset": self.iset,
                "rup": self.rup,
                "rdown": self.rdown,
                "trip": self.trip,
                "vmax": self.vmax,
                "pwkill": self.pwkill,
                "pwon": self.pwon,
            }
        )


@dataclass
class MuonInfo(Base):
    """Static information about PMTs from the muon veto."""

    __tablename__ = "muon_info"

    crate: Mapped[int]
    slot: Mapped[int]
    channel: Mapped[int]
    group: Mapped[str]
    label: Mapped[str]
    status: Mapped[int]
    itol: Mapped[float]
    vtol: Mapped[float]
    iupd: Mapped[float]
    vupd: Mapped[float]
    tstamp: Mapped[datetime] = db.orm.mapped_column(primary_key=True)

    def asdict(self) -> AttrsDict:
        return AttrsDict({"group": self.group, "label": self.label})
