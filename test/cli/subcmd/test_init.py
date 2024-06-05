import pytest
import subprocess

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
    main(argv)


class TestMain:
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
        with pytest.raises(SystemExit) as exc_info:
            run_main_init(["-h"])

        captured = capsys.readouterr()
        assert " init " in captured.out
        assert "Usage: " in captured.out
        assert "\nPositional arguments:\n" in captured.out
        assert "\nOptions:\n" in captured.out

    # def testUnrecognizedFlag(self):
    #     """Test unrecognized flag triggers printing of usage and exits with 2."""
    #     result = run_scout_init(["--unknown"])
    #     assert result.returncode == 2
    #     assert "Usage:" in result.stderr

    # def testNoOpts(self):
    #     """Test no options triggers default pwd target and repo in pwd, exit 0."""
    #     result = run_scout_init([])
    #     assert result.returncode == 0
    #     assert "." in result.stdout
    #     assert "./.scout.db" in result.stdout

    # def testTargetOpt(self):
    #     """Test target option sets target correctly."""
    #     result = run_scout_init(["test"])
    #     assert result.returncode == 0
    #     assert "test" in result.stdout
    #     result = run_scout_init(["-r", "test"])
    #     assert result.returncode == 0
    #     assert "test" in result.stdout

    # def testRepoOpt(self):
    #     """Test repo option sets repo correctly."""
    #     result = run_scout_init(["-r", "test"])
    #     assert result.returncode == 0
    #     assert "test" in result.stdout


class TestHandleRaises:
    """Tests the raise conditions in handle_subcommand."""

    def testNotInDir(self):
        """Test DBNotInDirError when target doesn't have valid parent directory."""
        pass
        # with pytest.raises(DBNotInDirError):
