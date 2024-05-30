import pytest


class TestMain:
    """Test Suite for main function of main module of CLI."""

    def testUsage(self):
        """Test '-h' or '--help' triggers printing of usage and exits with 0."""
