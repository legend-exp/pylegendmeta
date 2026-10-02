# Copyright (C) 2026 Luigi Pertoldi <gipert@pm.me>
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

from __future__ import annotations

import logging
import re
from collections.abc import Callable, Iterable, Sequence
from copy import deepcopy
from datetime import datetime
from functools import partial
from pathlib import Path
from typing import NamedTuple

from dbetto import AttrsDict, TextDB

from .core import MetadataRepository

log = logging.getLogger(__name__)

HPGE_PATTERN = r"V\d{5}[A-Z]"
SPMS_PATTERN = r"S\d{4}[TB]"
PMTS_PATTERN = r"PMT\d{4}"


class DetectorDefault(NamedTuple):
    """A name pattern, the record that stands in for it, and how to adjust a copy."""

    pattern: str
    record: str
    adjust: Callable[[AttrsDict, str], None]


class Legend1000Metadata(MetadataRepository):
    """LEGEND-1000 metadata.

    Class representing the LEGEND-1000 metadata repository with utilities for
    fast access.

    If no valid path to an existing legend1000-metadata directory is provided,
    will attempt to clone https://github.com/legend-exp/legend1000-metadata via
    SSH and git-checkout the latest stable tag (vM.m.p format).

    In the design phase all detectors of a system are equal, so the metadata
    describes one default germanium detector (:attr:`default_detector`), one
    default SiPM array (:attr:`default_sipm`) and one default PMT
    (:attr:`default_pmt`). Asking for any other name (e.g. ``V12345A``) that
    has no record of its own returns a copy of the default record, with the
    name-dependent fields updated. This applies to:

    - ``hardware.detectors.germanium.diodes``: ``name`` and the
      ``production`` order, crystal and slice.
    - ``hardware.detectors.germanium.crystals`` (e.g. ``V12345``): ``name``
      and ``order``. Any slice letter returns the default slice.
    - the output of ``hardware.configuration.channelmaps.on()``: ``name``,
      ``location`` and ``daq.rawid``.
    - the output of ``datasets.statuses.on()``.

    A name provides information on the position in the experiment. The
    position also defines the raw ID of the default record:

    - ``V<string: 3><position: 2><slice>``, e.g. ``V12345A``: string 123,
      position 45. Offset ``string * 1000 + position``.
    - ``S<string: 2><module: 2><end>``, e.g. ``S0102B``: the bottom end of
      fiber module ``S0102``, on string 1. Offset
      ``string * 1000 + module * 10 + end``, where ``end`` is 0 for ``T`` and
      1 for ``B``.
    - ``PMT<row: 2><position: 2>``, e.g. ``PMT1135``: row 11, position 35.
      Offset ``row * 1000 + position``. Rows below 10 are on the floor of the
      water tank, the rows above it on the wall. The ``x``, ``y`` and ``z`` of
      a PMT do not follow from its name.

    The default records in the metadata decide where the raw ID block of each
    system starts. In legend1000-metadata the blocks start at 1000000, 2000000
    and 3000000 for the germanium, SiPM and PMT systems, respectively.

    These records are not added to the database: iterating over it only sees
    records that exist on disk. Testing membership with ``in`` agrees with a
    lookup: it is ``True`` for every name that a default record stands in for.
    Pass ``use_defaults=False`` to read only the records on disk.

    The channels on disk are the default records only. Pass ``channels`` to
    set the channels that :meth:`channelmap` returns, e.g. the detectors of a
    geometry.

    Parameters
    ----------
    path
        path to legend1000-metadata repository. If not existing, will attempt a
        git-clone through SSH. If ``None``, the path is read from the
        ``LEGEND1000_METADATA`` environment variable, or else legend1000-metadata
        is cloned in a temporary directory (see :func:`tempfile.gettempdir`).
    use_defaults
        if ``True``, missing detector records fall back to the default detector
        record, as described above.
    channels
        if not ``None``, the names of the channels in the output of
        :meth:`channelmap`, in place of the channels in the channel map on
        disk.
    **kwargs
        further keyword arguments forwarded to :class:`TextDB.__init__`.

    Examples
    --------
    >>> l1000meta = Legend1000Metadata()
    >>> l1000meta.hardware.detectors.germanium.diodes.V12345A.production.crystal
    '345'
    """

    default_detector = "V99999Z"
    default_sipm = "S9999Z"
    default_pmt = "PMT9999"

    def __init__(
        self,
        path: str | None = None,
        use_defaults: bool = True,
        channels: Iterable[str] | None = None,
        **kwargs,
    ) -> None:
        self.__use_defaults__ = use_defaults
        self.__channels__ = None if channels is None else tuple(channels)
        # the database store for which the default records were last set up
        self.__defaults_store__ = None
        super().__init__(
            path=path,
            repo_url="git@github.com:legend-exp/legend1000-metadata",
            env_var="LEGEND1000_METADATA",
            default_dir_name="legend1000-metadata-",
            **kwargs,
        )

    def __getitem__(self, item: str | Path) -> TextDB | AttrsDict | list | None:
        if not self.__use_defaults__:
            return super().__getitem__(item)

        # the store is replaced on reset() or checkout(), set up the defaults again
        if self.__defaults_store__ is not self.__store__:
            self.__defaults_store__ = self.__store__
            self._setup_defaults()

        try:
            return super().__getitem__(item)
        except FileNotFoundError:
            # TextDB walks paths like "a/b/V12345A" without calling the
            # __getitem__ of the folders, so the default records are missed
            item = Path(item)
            if len(item.parts) < 2:
                raise
            return self[item.parent][item.name]

    def _setup_defaults(self) -> None:
        """Replace the database folders listed in the class documentation with defaulting ones."""
        # the channel maps and the statuses hold the records of all three systems
        channel = (
            DetectorDefault(HPGE_PATTERN, self.default_detector, _adjust_hpge_channel),
            DetectorDefault(SPMS_PATTERN, self.default_sipm, _adjust_sipm_channel),
            DetectorDefault(PMTS_PATTERN, self.default_pmt, _adjust_pmt_channel),
        )
        for path, defaults in (
            (
                "hardware/detectors/germanium/diodes",
                (DetectorDefault(HPGE_PATTERN, self.default_detector, _adjust_diode),),
            ),
            (
                "hardware/detectors/germanium/crystals",
                (
                    DetectorDefault(
                        r"V\d{5}", self.default_detector[:-1], _adjust_crystal
                    ),
                ),
            ),
            ("hardware/configuration/channelmaps", channel),
            ("datasets/statuses", channel),
        ):
            if not (self.__path__ / path).is_dir():
                continue

            parent_path, name = path.rsplit("/", 1)
            parent = self[parent_path]
            db = DefaultTextDB(
                self.__path__ / path,
                defaults,
                lazy=self.__lazy__,
                hidden=self.__hidden__,
            )
            parent.__store__[name] = db
            setattr(parent, name, db)

    def channelmap(
        self, on: str | datetime = "20400101T000000Z", category: str = "all"
    ) -> AttrsDict:
        """Get a LEGEND-1000 channel map.

        Aliases ``legend1000-metadata.hardware.configuration.channelmaps.on()``
        and merges each channel with its entry in the detector database
        ``hardware.detectors`` and with its analysis status from
        ``datasets.statuses.on()``, stored under ``analysis``.

        If ``use_defaults`` is enabled, any detector name missing from the
        channel map returns the default channel (see the class documentation).
        If ``channels`` was given, the channel map holds these channels.

        Parameters
        ----------
        on
            a :class:`~datetime.datetime` object or a string matching the
            pattern ``YYYYmmddTHHMMSSZ``. The default is the start of the
            first LEGEND-1000 period in the metadata.
        category: 'all', 'phy', 'cal', ...
            query only a data taking category.

        Examples
        --------
        >>> from legendmeta import Legend1000Metadata
        >>> l1000meta = Legend1000Metadata()
        >>> channel = l1000meta.channelmap(on="20400101T000000Z").V00101Z
        >>> channel.daq.rawid
        1001001
        >>> l1000meta.channelmap().S0102B.location.position
        'bottom'
        >>> l1000meta.channelmap().S0102B.daq.rawid
        2001021

        See Also
        --------
        dbetto.TextDB.on
        """
        chmap = self.hardware.configuration.channelmaps.on(
            on, pattern=None, category=category
        )
        statuses = self.datasets.statuses.on(on, pattern=None, category=category)
        get_channel = partial(self._channel, chmap, statuses)
        channels = chmap if self.__channels__ is None else self.__channels__

        return DefaultAttrsDict(
            {det: get_channel(det) for det in channels},
            [
                (HPGE_PATTERN, get_channel),
                (SPMS_PATTERN, get_channel),
                (PMTS_PATTERN, get_channel),
            ],
            readonly=True,
        )

    def _channel(self, chmap: AttrsDict, statuses: AttrsDict, det: str) -> AttrsDict:
        """Merge the channel map entry of `det` with its detector and status records."""
        channel = deepcopy(chmap[det])
        detdb = self.hardware.detectors

        system = channel["system"]
        try:
            if system == "geds":
                channel |= detdb.germanium.diodes[det]
            elif system == "spms":
                channel |= detdb.lar.sipms[det]
            elif system == "pmts":
                pass  # PMTs have no detector database of their own
            else:
                msg = f"Channel '{det}' has an unknown system, '{system}'"
                log.debug(msg)
        except (KeyError, FileNotFoundError):
            msg = f"Could not find detector '{det}' in hardware.detectors database"
            log.debug(msg)

        try:
            channel["analysis"] = statuses[det]
        except KeyError:
            msg = f"Could not find detector '{det}' in datasets.statuses database"
            log.debug(msg)

        return channel


def _adjust_hpge_channel(record: AttrsDict, name: str) -> None:
    """Set the fields of a channel record, if present, to match detector `name`.

    Detector ``V12345A`` sits in string 123, at position 45, so its raw ID is
    the raw ID of the default record plus 123045.
    """
    string = int(name[1:4])
    position = int(name[4:6])

    if "name" in record:
        record["name"] = name
    if "location" in record:
        record.location["string"] = string
        record.location["position"] = position
    if "daq" in record:
        record.daq["rawid"] += string * 1000 + position


def _adjust_sipm_channel(record: AttrsDict, name: str) -> None:
    """Set the fields of a channel record, if present, to match array `name`.

    Array ``S0102B`` reads out the bottom end of fiber module ``S0102``, on
    string 1, so its raw ID is the raw ID of the default record plus 1021.
    """
    string = int(name[1:3])
    module = int(name[3:5])
    top = name[5] == "T"

    if "name" in record:
        record["name"] = name
    if "location" in record:
        record.location["barrel"] = string
        record.location["fiber"] = name[:5]
        record.location["position"] = "top" if top else "bottom"
        record.location["module"] = module
    if "daq" in record:
        end = 0 if top else 1
        record.daq["rawid"] += string * 1000 + module * 10 + end


def _adjust_pmt_channel(record: AttrsDict, name: str) -> None:
    """Set the fields of a channel record, if present, to match PMT `name`.

    PMT ``PMT1135`` sits in row 11, at position 35, so its raw ID is the raw
    ID of the default record plus 11035. The rows below 10 are on the floor of
    the water tank, the rows above it are on the wall.

    The position in the tank (``location`` ``x``, ``y``, ``z`` and
    ``direction``) does not follow from the name. Those fields keep the value
    of the default record.
    """
    row = int(name[3:5])
    position = int(name[5:7])

    if "name" in record:
        record["name"] = name
    if "location" in record:
        record.location["name"] = "floor" if row < 10 else "wall"
    if "daq" in record:
        record.daq["rawid"] += row * 1000 + position


def _adjust_diode(record: AttrsDict, name: str) -> None:
    """Set the name and production fields of a diode record to match detector `name`."""
    record["name"] = name
    record.production["order"] = int(name[1:3])
    record.production["crystal"] = name[3:6]
    record.production["slice"] = name[6]


def _adjust_crystal(record: AttrsDict, name: str) -> None:
    """Set the fields of a crystal record to match crystal `name` (e.g. ``V12345``)."""
    record["name"] = name[3:6]
    record["order"] = name[1:3]
    if "slices" in record:
        slices = record.slices
        default = next(iter(slices))
        record["slices"] = DefaultAttrsDict(
            slices, [(r"[A-Z]", partial(_default_record, slices, default, None))]
        )


def _default_record(
    records: AttrsDict,
    default: str,
    adjust: Callable[[AttrsDict, str], None] | None,
    name: str,
) -> AttrsDict:
    """Return a copy of the `default` record, adjusted to `name`."""
    try:
        record = deepcopy(records[default])
    except KeyError as exc:
        msg = f"'{name}' not found, and no default record '{default}' to fall back on"
        raise KeyError(msg) from exc

    if adjust is not None:
        adjust(record, name)
    return record


class DefaultAttrsDict(AttrsDict):
    """AttrsDict that builds a record for a missing key whose name matches a pattern.

    `factories` holds one ``(pattern, factory)`` pair per kind of name. The
    first pattern that matches wins, and the record is ``factory(key)``.
    """

    def __init__(
        self,
        value: dict,
        factories: Sequence[tuple[str, Callable[[str], AttrsDict]]],
        readonly: bool = False,
    ) -> None:
        dict.__setattr__(self, "__factories__", tuple(factories))
        super().__init__(value, readonly=readonly)

    def __missing__(self, key: str) -> AttrsDict:
        if isinstance(key, str):
            for pattern, factory in self.__factories__:
                if re.fullmatch(pattern, key):
                    record = factory(key)
                    if self.__readonly__:
                        record.__readonly__ = True
                    return record

        raise KeyError(key)

    def __contains__(self, key: object) -> bool:
        if super().__contains__(key):
            return True
        try:
            self.__missing__(key)
        except KeyError:
            return False
        return True

    def __getattr__(self, name: str) -> AttrsDict:
        if not name.startswith("__"):
            try:
                return self[name]
            except KeyError:
                pass
        return super().__getattr__(name)

    def __getstate__(self) -> dict:
        return super().__getstate__() | {"__factories__": self.__factories__}

    def __setstate__(self, state: dict) -> None:
        super().__setstate__(state)
        dict.__setattr__(self, "__factories__", state["__factories__"])


class DefaultTextDB(TextDB):
    """TextDB that returns an adjusted copy of a default record for missing names.

    `defaults` holds one :class:`DetectorDefault` per kind of name. The first pattern
    that matches wins. The output of :meth:`on` falls back in the same way.
    """

    def __init__(
        self,
        path: str | Path,
        defaults: Sequence[DetectorDefault],
        **kwargs,
    ) -> None:
        self.__defaults__ = tuple(defaults)
        super().__init__(path, **kwargs)

    def __getitem__(self, item: str | Path) -> TextDB | AttrsDict | list | None:
        try:
            return super().__getitem__(item)
        except FileNotFoundError:
            name = str(item)
            for default in self.__defaults__:
                if name != default.record and re.fullmatch(default.pattern, name):
                    record = deepcopy(super().__getitem__(default.record))
                    default.adjust(record, name)
                    return record
            raise

    def __contains__(self, item: str | Path) -> bool:
        try:
            self[item]
        except FileNotFoundError:
            return False
        return True

    def on(self, *args, **kwargs) -> AttrsDict | list:
        result = super().on(*args, **kwargs)
        return DefaultAttrsDict(
            result,
            [
                (d.pattern, partial(_default_record, result, d.record, d.adjust))
                for d in self.__defaults__
            ],
            readonly=True,
        )

    def __getstate__(self) -> dict:
        return super().__getstate__() | {"__defaults__": self.__defaults__}

    def __setstate__(self, state: dict) -> None:
        super().__setstate__(state)
        self.__defaults__ = state["__defaults__"]
