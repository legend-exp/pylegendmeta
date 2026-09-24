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
from collections.abc import Callable
from copy import deepcopy
from datetime import datetime
from functools import partial
from pathlib import Path

from dbetto import AttrsDict, TextDB

from .core import MetadataRepository

log = logging.getLogger(__name__)

# detector names are V<order: 2 digits><crystal: 3 digits><slice: 1 letter>
_DETECTOR = r"V\d{5}[A-Z]"


class Legend1000Metadata(MetadataRepository):
    """LEGEND-1000 metadata.

    Class representing the LEGEND-1000 metadata repository with utilities for
    fast access.

    If no valid path to an existing legend1000-metadata directory is provided,
    will attempt to clone https://github.com/legend-exp/legend1000-metadata via
    SSH and git-checkout the latest stable tag (vM.m.p format).

    In the design phase all germanium detectors are equal, so the metadata
    describes a single default detector (:attr:`default_detector`). Asking for
    any other detector name (e.g. ``V12345A``) that has no record of its own
    returns a copy of the default record, with the name-dependent fields
    updated. This applies to:

    - ``hardware.detectors.germanium.diodes``: ``name`` and the
      ``production`` order, crystal and slice.
    - ``hardware.detectors.germanium.crystals`` (e.g. ``V12345``): ``name``
      and ``order``. Any slice letter returns the default slice.
    - the output of ``hardware.configuration.channelmaps.on()``: ``name``.
    - the output of ``datasets.statuses.on()``.

    These records are not added to the database: iterating over it or testing
    membership with ``in`` only sees records that exist on disk. Pass
    ``use_defaults=False`` to read only the records on disk.

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
    **kwargs
        further keyword arguments forwarded to :class:`TextDB.__init__`.

    Examples
    --------
    >>> l1000meta = Legend1000Metadata()
    >>> l1000meta.hardware.detectors.germanium.diodes.V12345A.production.crystal
    '345'
    """

    default_detector = "V99999Z"
    """Name of the detector whose records stand in for missing ones."""

    def __init__(
        self,
        path: str | None = None,
        use_defaults: bool = True,
        **kwargs,
    ) -> None:
        self.__use_defaults__ = use_defaults
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
        det = self.default_detector
        for path, pattern, default, adjust in (
            ("hardware/detectors/germanium/diodes", _DETECTOR, det, _adjust_diode),
            (
                "hardware/detectors/germanium/crystals",
                r"V\d{5}",
                det[:-1],
                _adjust_crystal,
            ),
            ("hardware/configuration/channelmaps", _DETECTOR, det, _adjust_name),
            ("datasets/statuses", _DETECTOR, det, _adjust_name),
        ):
            parent_path, name = path.rsplit("/", 1)
            try:
                parent = self[parent_path]
            except FileNotFoundError:
                continue
            if not (parent.__path__ / name).is_dir():
                continue

            db = _DefaultTextDB(
                parent.__path__ / name,
                pattern,
                default,
                adjust,
                lazy=self.__lazy__,
                hidden=self.__hidden__,
            )
            parent.__store__[name] = db
            setattr(parent, name, db)

    def channelmap(
        self, on: str | datetime | None = None, category: str = "all"
    ) -> AttrsDict:
        """Get a LEGEND-1000 channel map.

        Aliases ``legend1000-metadata.hardware.configuration.channelmaps.on()``
        and merges each channel with its entry in the detector database
        ``hardware.detectors`` and with its analysis status from
        ``datasets.statuses.on()``, stored under ``analysis``.

        If ``use_defaults`` is enabled, any detector name missing from the
        channel map returns the default channel (see the class documentation).

        Parameters
        ----------
        on
            a :class:`~datetime.datetime` object or a string matching the
            pattern ``YYYYmmddTHHMMSSZ``. Defaults to now.
        category: 'all', 'phy', 'cal', ...
            query only a data taking category.

        Examples
        --------
        >>> from legendmeta import Legend1000Metadata
        >>> l1000meta = Legend1000Metadata()
        >>> channel = l1000meta.channelmap(on="20400101T000000Z").V00101Z
        >>> channel.daq.rawid
        1

        See Also
        --------
        dbetto.TextDB.on
        """
        if on is None:
            on = datetime.now()

        chmap = self.hardware.configuration.channelmaps.on(
            on, pattern=None, category=category
        )
        statuses = self.datasets.statuses.on(on, pattern=None, category=category)
        get_channel = partial(self._channel, chmap, statuses)

        if not self.__use_defaults__:
            return AttrsDict({det: get_channel(det) for det in chmap}, readonly=True)

        return _DefaultDict(
            {det: get_channel(det) for det in chmap},
            _DETECTOR,
            get_channel,
            readonly=True,
        )

    def _channel(self, chmap: AttrsDict, statuses: AttrsDict, det: str) -> AttrsDict:
        """Merge the channel map entry of `det` with its detector and status records."""
        channel = deepcopy(chmap[det])
        detdb = self.hardware.detectors

        try:
            if channel["system"] == "geds":
                channel |= detdb.germanium.diodes[det]
            else:
                channel |= detdb.lar.sipms[det]
        except (KeyError, FileNotFoundError):
            msg = f"Could not find detector '{det}' in hardware.detectors database"
            log.debug(msg)

        try:
            channel["analysis"] = statuses[det]
        except KeyError:
            msg = f"Could not find detector '{det}' in datasets.statuses database"
            log.debug(msg)

        return channel


def _adjust_name(record: AttrsDict, name: str) -> None:
    """Set the record name to `name`, if the record has one."""
    if "name" in record:
        record["name"] = name


def _adjust_diode(record: AttrsDict, name: str) -> None:
    """Set the name and production fields of a diode record to match detector `name`."""
    record["name"] = name
    prod = record.production
    prod["order"] = type(prod.order)(name[1:3])
    prod["crystal"] = name[3:6]
    prod["slice"] = name[6]


def _adjust_crystal(record: AttrsDict, name: str) -> None:
    """Set the fields of a crystal record to match crystal `name` (e.g. ``V12345``)."""
    record["name"] = name[3:6]
    record["order"] = type(record.order)(name[1:3])
    if "slices" in record:
        slices = record.slices
        default = next(iter(slices))
        record["slices"] = _DefaultDict(
            slices, r"[A-Z]", partial(_default_record, slices, default, None)
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


class _DefaultDict(AttrsDict):
    """AttrsDict that returns ``factory(key)`` for missing keys matching `pattern`."""

    def __init__(
        self,
        value: dict,
        pattern: str,
        factory: Callable[[str], AttrsDict],
        readonly: bool = False,
    ) -> None:
        dict.__setattr__(self, "__pattern__", pattern)
        dict.__setattr__(self, "__factory__", factory)
        super().__init__(value, readonly=readonly)

    def __missing__(self, key: str) -> AttrsDict:
        if not isinstance(key, str) or not re.fullmatch(self.__pattern__, key):
            raise KeyError(key)

        record = self.__factory__(key)
        if self.__readonly__:
            record.__readonly__ = True
        return record

    def __getattr__(self, name: str) -> AttrsDict:
        if not name.startswith("__"):
            try:
                return self[name]
            except KeyError:
                pass
        return super().__getattr__(name)

    def __getstate__(self) -> dict:
        return super().__getstate__() | {
            "__pattern__": self.__pattern__,
            "__factory__": self.__factory__,
        }

    def __setstate__(self, state: dict) -> None:
        super().__setstate__(state)
        dict.__setattr__(self, "__pattern__", state["__pattern__"])
        dict.__setattr__(self, "__factory__", state["__factory__"])


class _DefaultTextDB(TextDB):
    """TextDB that returns an adjusted copy of the `default` record for missing names matching `pattern`.

    The output of :meth:`on` falls back in the same way.
    """

    def __init__(
        self,
        path: str | Path,
        pattern: str,
        default: str,
        adjust: Callable[[AttrsDict, str], None],
        **kwargs,
    ) -> None:
        self.__pattern__ = pattern
        self.__default__ = default
        self.__adjust__ = adjust
        super().__init__(path, **kwargs)

    def __getitem__(self, item: str | Path) -> TextDB | AttrsDict | list | None:
        try:
            return super().__getitem__(item)
        except FileNotFoundError:
            name = str(item)
            if name == self.__default__ or not re.fullmatch(self.__pattern__, name):
                raise
            record = deepcopy(super().__getitem__(self.__default__))
            self.__adjust__(record, name)
            return record

    def on(self, *args, **kwargs) -> AttrsDict | list:
        result = super().on(*args, **kwargs)
        return _DefaultDict(
            result,
            self.__pattern__,
            partial(_default_record, result, self.__default__, self.__adjust__),
            readonly=True,
        )

    def __getstate__(self) -> dict:
        return super().__getstate__() | {
            "__pattern__": self.__pattern__,
            "__default__": self.__default__,
            "__adjust__": self.__adjust__,
        }

    def __setstate__(self, state: dict) -> None:
        super().__setstate__(state)
        self.__pattern__ = state["__pattern__"]
        self.__default__ = state["__default__"]
        self.__adjust__ = state["__adjust__"]
