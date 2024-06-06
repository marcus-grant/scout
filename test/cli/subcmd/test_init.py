import os
import pytest
import subprocess
from unittest.mock import patch, Mock

from cli import main
from lib.handler.db_connector import (
    DBConnectorError,
    DBNotInDirError,
    DBFileOccupiedError,
    DBRootNotDirError,
    DBNoFsMetaTableError,
    DBTargetPropMissingError,
)


# TODO: Make subcommand print usage for this subcommand on error
# TODO: Docstrings
def run_scout_init(argv, **kwargs):
    argv = ["./scout", "init"] + argv
    kwargs["capture_output"] = True
    kwargs["text"] = True
    return subprocess.run(argv, **kwargs)


def run_main_init(argv):
    argv = ["./scout", "init"] + argv
    return main(argv)


class TestOpts:
    """Test Suite for Init Subcommand argparser."""

    # TODO: cli.main needs to be refactored better for testing.
    # Will add argv param to main() to allow for testing argparse
    # Will add return with exit code to main to test exits
    # Will add exit(main(sys.argv)) to __main__ check to allow for testing
    # Also the above will allow regular execution of cli as main script
    #
    @pytest.mark.parametrize("option", ["-h", "--help"])
    def testUsageFlag(self, option):
        """Test '-h' or '--help' triggers printing of usage and exits with 0."""
        result = run_scout_init([option])
        assert result.returncode == 0
        assert "usage" in result.stdout.lower()

    def testMainSameAsScript(self, capsys):
        """Tests calling main function same as running cli module as script."""
        with pytest.raises(SystemExit) as exc_info:
            run_main_init(["-h"])
        captured = capsys.readouterr()
        assert "usage" in captured.out.lower()
        assert exc_info.value.code == 0
        assert exc_info.type == SystemExit

    def testDescriptionFormat(self, capsys):
        """Test description of subcommand is formatted correctly.
        Focusing on section headings spacing, capitalization and content."""
        with pytest.raises(SystemExit):
            run_main_init(["-h"])
        captured = capsys.readouterr()
        assert " init " in captured.out
        assert "Usage: " in captured.out
        assert "\nPositional arguments:\n" in captured.out
        assert "\nOptions:\n" in captured.out

    def testUnrecognizedFlag(self, capsys):
        """Test unrecognized flag triggers printing of usage and exits with 2."""
        rc = run_main_init(["--unknown"])
        assert "Usage:" in capsys.readouterr().err
        assert rc == 2

    def testNoOpts(self, capsys):
        """Test no options triggers default pwd target and repo in pwd, exit 0."""
        cwd = os.getcwd()
        with patch("cli.subcmd.init.ScoutManager.init_db") as mock:
            rc = run_main_init([])
            assert rc == 0
            assert mock.called
            assert mock.call_args[0][0] == f"{cwd}/.scout.db"
            assert mock.call_args[0][1] == str(cwd)

    def testTargetOpt(self, capsys):
        """Test target option sets target correctly."""
        cwd = os.getcwd()
        target = f"{cwd}/test/foobar"
        mock_db = Mock()
        mock_db.path = cwd
        mock_db.root = target
        with patch(
            "cli.subcmd.init.ScoutManager.init_db", return_value=mock_db
        ) as mock:
            rc = run_main_init([target])
        assert rc == 0
        stdout = capsys.readouterr().out
        assert "init" in stdout.lower()
        assert target in stdout
        mock.assert_called_once_with(f"{target}/.scout.db", target)

    @pytest.mark.parametrize("option", ["-r", "--repo"])
    def testRepoOpt(self, capsys, option):
        """Test target option sets target correctly."""
        cwd = os.getcwd()
        repo = f"{cwd}/test/foobar"
        mock_db = Mock()
        mock_db.path = repo
        mock_db.root = cwd
        with patch(
            "cli.subcmd.init.ScoutManager.init_db", return_value=mock_db
        ) as mock:
            rc = run_main_init([option, repo])
        stdout = capsys.readouterr().out
        assert "init" in stdout.lower()
        assert cwd in stdout
        assert rc == 0
        mock.assert_called_once_with(repo, cwd)

    @pytest.mark.parametrize("option", ["-r", "--repo"])
    def testAllOpts(self, capsys, option):
        """Test repo option sets repo correctly."""
        target = "/test/target"
        repo = "/test/repo"
        # Mock object with path=repo and root=test
        mock_db = Mock()
        mock_db.path = repo
        mock_db.root = target
        with patch(
            "cli.subcmd.init.ScoutManager.init_db", return_value=mock_db
        ) as mock:
            rc = run_main_init([target, option, repo])
        assert rc == 0
        stdout = capsys.readouterr().out
        assert "init" in stdout.lower()
        assert target in stdout
        mock.assert_called_once_with(repo, target)


class TestHandleRaises:
    """Tests the raise conditions in handle_subcommand."""

    def testNotInDir(self, capsys):
        """Test DBNotInDirError when target doesn't have valid parent directory."""
        target = "/definitely/not/a/valid/path"
        rc = run_main_init([target])
        assert rc == 16
        captured = capsys.readouterr()
        assert "Error: " in captured.err
        assert "Usage: " in captured.err
        assert target in captured.err
