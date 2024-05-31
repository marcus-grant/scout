import pytest
import subprocess


# TODO: Make subcommand print usage for this subcommand on error
# TODO: Docstrings
def run_scout_init(argv, **kwargs):
    argv = ["./scout", "init"] + argv
    kwargs["capture_output"] = True
    kwargs["text"] = True
    return subprocess.run(argv, **kwargs)


class TestMain:
    """Test Suite for Init Subcommand argparser."""

    @pytest.mark.parametrize("option", ["-h", "--help"])
    def testUsageFlag(self, option):
        """Test '-h' or '--help' triggers printing of usage and exits with 0."""
        result = run_scout_init([option])
        assert result.returncode == 0
        assert "usage" in result.stdout.lower()

    def testDescriptionFormat(self):
        """Test description of subcommand is formatted correctly.
        Focusing on section headings spacing, capitalization and content."""
        result = run_scout_init(["-h"])
        assert " init " in result.stdout
        assert "Usage: " in result.stdout
        assert "\nPositional arguments:\n" in result.stdout
        assert "\nOptions:\n" in result.stdout

    def testUnrecognizedFlag(self):
        """Test unrecognized flag triggers printing of usage and exits with 2."""
        result = run_scout_init(["--unknown"])
        assert result.returncode == 2
        assert "Usage:" in result.stderr

    def testNoOpts(self):
        """Test no options triggers default pwd target and repo in pwd, exit 0."""
        result = run_scout_init([])
        assert result.returncode == 0
        assert "." in result.stdout
        assert "./.scout.db" in result.stdout

    def testTargetOpt(self):
        """Test target option sets target correctly."""
        result = run_scout_init(["test"])
        assert result.returncode == 0
        assert "test" in result.stdout
        result = run_scout_init(["-r", "test"])
        assert result.returncode == 0
        assert "test" in result.stdout

    def testRepoOpt(self):
        """Test repo option sets repo correctly."""
        result = run_scout_init(["-r", "test"])
        assert result.returncode == 0
        assert "test" in result.stdout
