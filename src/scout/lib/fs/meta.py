# src/scout/lib/fs/meta.py
"""Read filesystem and host identity for the meta table; Linux only.
Author: Marcus
Created: 2026-09-15
License: AGPL-3.0-or-later
"""

import os
import socket
from collections.abc import Callable
from pathlib import Path

_MOUNTS = Path("/proc/mounts")
_BY_UUID = Path("/dev/disk/by-uuid")
_BY_LABEL = Path("/dev/disk/by-label")
_SYS_BLOCK = Path("/sys/dev/block")


def best_mount(
    fields: list[str], target: Path, best: tuple[int, str, str] | None = None
) -> tuple[int, str, str] | None:
    """Return the better of best and this line's mount for target"""
    if len(fields) < 3:
        return best
    length, device, point, kind = len(fields[1]), fields[0], fields[1], fields[2]
    if Path(point) not in (target, *target.parents):
        return best
    if best is not None and best[0] >= length:
        return best
    return (length, device, kind)


def mount_of(target: Path, mounts: Path = _MOUNTS) -> tuple[str, str] | None:
    """Return (device, fs type) of the mount holding target, or None unreadable."""
    try:
        lines = mounts.read_text().splitlines()
    except OSError:
        return None
    best: tuple[int, str, str] | None = None
    for line in lines:
        best = best_mount(line.split(), target, best)
    return (best[1], best[2]) if best is not None else None


def fs_type(target: Path, mounts: Path = _MOUNTS) -> str | None:
    """Return the fs type of the mount holding target, or None unreadable."""
    mount = mount_of(target, mounts=mounts)
    return mount[1] if mount is not None else None


def by_dir_name(
    target: Path, mounts: Path = _MOUNTS, by_dir: Path | None = None
) -> str | None:
    """Return the name of the by_dir entry resolving to target's device."""
    if by_dir is None:
        return None
    if (mount := mount_of(target, mounts=mounts)) is None:
        return None
    mount_path = Path(mount[0]).resolve()
    try:
        for entry in by_dir.iterdir():
            if entry.resolve() == mount_path:
                return entry.name
    except OSError:
        return None
    return None


def fs_uuid(
    target: Path, mounts: Path = _MOUNTS, by_uuid: Path = _BY_UUID
) -> str | None:
    """Return the fs uuid of the target's device, or None when unknown."""
    return by_dir_name(target, mounts, by_dir=by_uuid)


def fs_label(
    target: Path, mounts: Path = _MOUNTS, by_label: Path = _BY_LABEL
) -> str | None:
    """Return the fs label of the target's device, or None when unknown."""
    return by_dir_name(target, mounts, by_dir=by_label)


def _stat_dev(path: Path) -> int:
    """Return the device number the path lives on."""
    return os.stat(path).st_dev


def fs_model(
    target: Path,
    sys_block: Path = _SYS_BLOCK,
    dev_of: Callable[[Path], int] = _stat_dev,
) -> str | None:
    """Return the hardware model of the target's device, or None unknown."""
    try:
        dev = dev_of(target)
        name = f"{os.major(dev)}:{os.minor(dev)}"
        return (sys_block / name / "device" / "model").read_text().strip()
    except OSError:
        return None


def hostname(get: Callable[[], str] = socket.gethostname) -> str | None:
    """Return the host's name, or None when unavailable."""
    try:
        return get()
    except OSError:
        return None


def read_all(target: Path) -> dict[str, str | None]:
    """Return every reader's answer for target, keyed by meta property."""
    return {
        "fs_type": fs_type(target),
        "fs_uuid": fs_uuid(target),
        "fs_label": fs_label(target),
        "fs_model": fs_model(target),
        "hostname": hostname(),
    }
