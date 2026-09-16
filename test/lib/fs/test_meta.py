# test/lib/fs/test_meta.py
"""Pin the fs detail readers against canned input; never the real system.
Author: Marcus
Created: 2026-09-15
License: AGPL-3.0-or-later
"""

from pathlib import Path

from scout.lib.fs import meta


class TestBestMount:
    """best_mount folds one mounts line into the best match so far."""

    def test_short_line_keeps_best(self) -> None:
        """A line with under three fields returns best unchanged."""
        mount, best = ["/dev/sda", "/"], (0, "/dev/x", "tmpfs")
        assert meta.best_mount(mount, Path("/x"), best=best) == best

    def test_non_ancestor_keeps_best(self) -> None:
        """A mountpoint not containing target returns best unchanged."""
        mount, best = ["dev/sda", "/home", "ext4", "0"], (0, "/dev/x", "tmpfs")
        assert meta.best_mount(mount, Path("/usr"), best=best) == best

    def test_deeper_match_replaces_best(self) -> None:
        """A containing mountpoint longer than best's replaces it."""
        mount = ["/dev/sdb1", "/home/bob", "btrfs", "rw", "0"]
        target = Path("/home/bob/music")
        best = (5, "/dev/sda1", "ext4")
        assert meta.best_mount(mount, target, best) == (9, "/dev/sdb1", "btrfs")

    def test_shallower_match_keeps_best(self) -> None:
        """A containing mountpoint shorter than best's is ignored."""
        mount = ["/dev/sdb1", "/home", "btrfs", "rw", "0"]
        target = Path("/home/bob")
        best = (9, "/dev/sdc1", "ext4")
        assert meta.best_mount(mount, target, best) == best


def mk_dev_mounts(tmp_path: Path) -> tuple[Path, Path]:
    """Write a fake device and a one-line mounts file naming it as root's."""
    (dev := tmp_path / "nvme0n1").write_bytes(b"I'm an NVME")
    (mounts := tmp_path / "mounts").write_text(f"{dev} / btrfs rw 0 0\n")
    return dev, mounts


def mk_by_dir(tmp_path: Path, name: str, dest: Path) -> Path:
    """Make a symlink dir holding one entry name pointing at dest."""
    (d := tmp_path / "by-dir").mkdir()
    (d / name).symlink_to(dest)
    return d


class TestMountOf:
    """mount_of returns the device and fs type of the mount holding target."""

    MOUNTS = (
        "/dev/root / ext4 rw,relatime 0 0\n"
        "/dev/sda1 /home ext4 rw 0 0\n"
        "/dev/sdb1 /home/marcus/media btrfs rw 0 0\n"
        "tmpfs /tmp tmpfs rw 0 0\n"
    )

    def test_longest_matching_mountpoint_wins(self, tmp_path: Path) -> None:
        """The mount whose mountpoint is the longest prefix of target wins;
        its device and fs type come back as a tuple."""
        (mounts := tmp_path / "mounts").write_text(self.MOUNTS)
        target = Path("/home/marcus/media/photos")
        assert meta.mount_of(target, mounts=mounts) == ("/dev/sdb1", "btrfs")

    def test_absent_mounts_is_none(self, tmp_path: Path) -> None:
        """A missing mounts file yields None, never a raise."""
        bad = tmp_path / "no-such-file"
        assert meta.mount_of(Path("/anywhere"), mounts=bad) is None

    def test_garbage_mounts_is_none(self, tmp_path: Path) -> None:
        """An unparseable mounts file yields None, never a raise."""
        (mounts := tmp_path / "mounts").write_text("garbage\nx y\n")
        assert meta.mount_of(Path("/anywhere"), mounts=mounts) is None


class TestFsType:
    """fs_type returns the fs type of the mount holding target."""

    def test_reads_type_of_found_mount(self, tmp_path: Path) -> None:
        """The type field of the matching mount comes back."""
        (mounts := tmp_path / "mounts").write_text("/dev/sda1 / ext4 rw 0 0\n")
        assert meta.fs_type(Path("/anywhere"), mounts=mounts) == "ext4"

    def test_no_mount_is_none(self, tmp_path: Path) -> None:
        """No matching mount yields None."""
        assert meta.fs_type(Path("/x"), mounts=tmp_path / "no-such-file") is None


class TestByDirName:
    """by_dir_name finds the entry in a symlink dir resolving to target's device."""

    def test_matching_entry_name_wins(self, tmp_path: Path) -> None:
        """The entry whose resolved path is the mount's device wins."""
        dev, mounts = mk_dev_mounts(tmp_path)
        (by_dir := tmp_path / "by-uuid").mkdir()
        (by_dir / (name := "abcd-1234")).symlink_to(dev)
        assert meta.by_dir_name(Path("/x"), mounts=mounts, by_dir=by_dir) == name

    def test_absent_dir_is_none(self, tmp_path: Path) -> None:
        """A missing dir yields None, never a raise."""
        _, mounts = mk_dev_mounts(tmp_path)
        bad = tmp_path / "no-such_dir"
        assert meta.by_dir_name(Path("/x"), mounts=mounts, by_dir=bad) is None

    def test_no_matching_entry_is_none(self, tmp_path: Path) -> None:
        """A dir with no entry resolving to the device yields None."""
        dev, mounts = mk_dev_mounts(tmp_path)
        by_dir = mk_by_dir(tmp_path, "ffff-0000", other := (tmp_path / "other-dev"))
        assert meta.by_dir_name(Path("/x"), mounts=mounts, by_dir=by_dir) is None
        assert dev != other


class TestFsUuid:
    """fs_uuid names the by-uuid symlink resolving to the target's device."""

    def test_absent_dir_is_none(self, tmp_path: Path) -> None:
        """A missing dir yields None, never a raise."""
        _, mounts = mk_dev_mounts(tmp_path)
        bad = tmp_path / "no-such-dir"
        assert meta.by_dir_name(Path("/x"), mounts=mounts, by_dir=bad) is None

    def test_no_matching_entry_is_none(self, tmp_path: Path) -> None:
        """A dir with no entry resolving to the device yields None."""
        _, mounts = mk_dev_mounts(tmp_path)
        by_dir = mk_by_dir(tmp_path, "ffff-0000", tmp_path / "other-dev")
        assert meta.by_dir_name(Path("/x"), mounts=mounts, by_dir=by_dir) is None


class TestFsLabel:
    """fs_label names the by-label symlink resolving to the target's device."""

    def test_reads_label_of_targets_device(self, tmp_path: Path) -> None:
        """The entry name in the by-label dir comes back as the label."""
        dev, mounts = mk_dev_mounts(tmp_path)
        by_label = mk_by_dir(tmp_path, (label := "testdisk"), dev)
        assert meta.fs_label(Path("/x"), mounts=mounts, by_label=by_label) == label


class TestFsModel:
    """fs_model reads the device model from sysfs for the target's device."""

    def _dev(self, _) -> int:
        return 2049

    def test_reads_model_from_sysfs(self, tmp_path: Path) -> None:
        """The model file under the device's sysfs dir comes back stripped."""
        dev_dir = tmp_path / "sys" / "8:1"
        (dev_dir / "device").mkdir(parents=True)
        (dev_dir / "device" / "model").write_text("TestVendor SSD\n")
        got = meta.fs_model(tmp_path, sys_block=tmp_path / "sys", dev_of=self._dev)
        assert got == "TestVendor SSD"

    def test_absent_sysfs_entry_is_none(self, tmp_path: Path) -> None:
        """A device with no sysfs model file yields None, never a raise."""
        (sys_blk := tmp_path / "sys").mkdir()
        assert meta.fs_model(tmp_path, sys_block=sys_blk, dev_of=self._dev) is None


class TestHostname:
    """hostname returns the injected reader's answer, or None on failure."""

    def _raise_boom(self):
        raise OSError

    def test_returns_canned_name(self) -> None:
        """The injected reader's value passes through."""
        assert meta.hostname(get=lambda: "testhost") == "testhost"

    def test_raising_reader_is_none(self) -> None:
        """An OSError from the reader yields None, never a raise."""
        assert meta.hostname(get=self._raise_boom) is None
