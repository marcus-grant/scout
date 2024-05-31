import pytest
import subprocess


def testrun(*args, **kwargs):
    return subprocess.run(
        ["./scout"] + list(*args), **kwargs, capture_output=True, text=True
    )


class TestMain:
    """Test Suite for main function of main module of CLI."""

    @pytest.mark.parametrize("option", ["-h", "--help"])
    def testUsageFlag(self, option):
        """Test '-h' or '--help' triggers printing of usage and exits with 0."""
        result = testrun([option])
        assert result.returncode == 0
        assert "Usage:" in result.stdout

