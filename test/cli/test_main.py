import pytest
import subprocess

from cli import VERSION, NAME, main


# TODO: Docstrings
def run_scout(argv, **kwargs):
    argv = ["./scout"] + argv
    kwargs["capture_output"] = True
    kwargs["text"] = True
    return subprocess.run(argv, **kwargs)


class TestMain:
    """Test Suite for main function of main module of CLI."""

    @pytest.mark.parametrize("option", ["-h", "--help"])
    def testUsageFlag(self, option):
        """Test '-h' or '--help' triggers printing of usage and exits with 0."""
        result = run_scout([option])
        assert result.returncode == 0
        assert "usage" in result.stdout.lower()

    def testDescriptionFormat(self):
        """Test description of the CLI is formatted correctly.
        Focusing on section headings spacing, capitalization and content."""
        result = run_scout(["-h"])
        assert "\nUsage: " in result.stdout
        assert "\nDescription:\n" in result.stdout
        assert "\nSubcommands:\n" in result.stdout
        assert NAME in result.stdout

    def testUnrecognizedFlag(self):
        """Test unrecognized flag triggers printing of usage and exits with 2."""
        result = run_scout(["--unknown"])
        assert result.returncode == 2
        assert "Usage:" in result.stderr

    def testNoOpts(self):
        """Test no options triggers printing of usage, exits with 2,
        and states non-implementation of TUI."""
        result = run_scout([])
        assert result.returncode == 2
        assert "\nUsage: " in result.stderr
        assert "not implement" in result.stderr.lower()
        assert "TUI" in result.stderr

    @pytest.mark.parametrize("option", ["-v", "--version"])
    def testVersionFlag(self, option):
        """Test '-v' or '--version' triggers printing of version and exits with 0."""
        result = run_scout([option])
        assert result.returncode == 0
        assert VERSION in result.stdout


class TestSameAsScript:
    """Test Suite for main function being same as when script is run as module run."""

    def testUsageFlag(self, capsys):
        """Test '-h' or '--help' triggers printing of usage and exits with 0."""
        with pytest.raises(SystemExit) as exc_info:
            main(["-h"])

        captured = capsys.readouterr()
        assert exc_info.type == SystemExit
        assert exc_info.value.code == 0
        assert "usage" in captured.out.lower()

    def testUnrecognizedFlag(self, capsys):
        """Test unrecognized flag triggers printing of usage and exits with 2."""
        result = main(["--unknown"])
        captured = capsys.readouterr()
        assert result == 2
        assert "Usage:" in captured.err

    def testNoOpts(self, capsys):
        """Test no options triggers printing of usage, exits with 2,
        and states non-implementation of TUI."""
        result = main([])
        captured = capsys.readouterr()
        assert result == 2
        assert "\nUsage: " in captured.err
        assert "not implement" in captured.err.lower()
        assert "TUI" in captured.err

    def testVersionFlag(self, capsys):
        """Test '-v' or '--version' triggers printing of version and exits with 0."""
        with pytest.raises(SystemExit) as exc_info:
            main(["-v"])
        captured = capsys.readouterr()
        assert exc_info.type == SystemExit
        assert exc_info.value.code == 0
        assert VERSION in captured.out
