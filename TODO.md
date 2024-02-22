# To-Do

## Introduction

Just a quick & dirty to-do list for the project.

## Code to Use

### File System Handling Tests

This comes from the old `tests/model/test_fs_dir.py` file.
Before we put all FS model classes in one file and
moved all handling of FS operations to its own module.

```py
@pytest.fixture()
def setup_fakefs():
    with Patcher() as patcher:
        # Create the fake FS directory paths
        dir_paths = [
            "/testroot/basic/small",  # Basic <1MiB test files
            "/testroot/basic/big",  # Basic >= 1MiB test files
            # "/testroot/random",  # Random generated files with known seed
            # "/testroot/random/small/prime",  # <1MiB prime byte-count
            # "/testroot/random/big/prime",  # >=1MiB prime byte-count
            "/testroot/special",  # (encoding, hidden, unsafe chars, etc)
            # "/testroot/special/utf8/name",  # UTF-8 high-codepoint file names
            # "/testroot/special/utf8/content",  # UTF-8 high-codepoint contents
        ]
        file_paths = [
            "/testroot/basic/small/empty.txt",
            "/testroot/basic/small/hello.txt",
        ]
        file_contents = [
            "",
            "Hello, World!\n",
        ]

        # Automatically create a common directory structure or files needed by all tests
        for path in dir_paths:
            patcher.fs.create_dir(path)  # type: ignore pyright cant find create_dir method
        for path, contents in zip(file_paths, file_contents):
            patcher.fs.create_file(path, contents=contents)  # type: ignore
        yield patcher  # Yielding patcher for tests that might want to use it directly


def test_mock_dirs(setup_fakefs):
    assert pathlib.Path("/testroot/basic/small").exists()
    assert pathlib.Path("/testroot/basic/big").exists()
    # assert pathlib.Path("/testroot/random/small/prime").exists()
    # assert pathlib.Path("/testroot/random/big/prime").exists()
    # assert pathlib.Path("/testroot/special/utf8/name").exists()


def test_mock_files(setup_fakefs):
    # Check file exists
    assert pathlib.Path("/testroot/basic/small/empty.txt").exists()
    assert pathlib.Path("/testroot/basic/small/hello.txt").exists()
    # Check contains expected content
    assert pathlib.Path("/testroot/basic/small/empty.txt").read_text() == ""
    assert (
        pathlib.Path("/testroot/basic/small/hello.txt").read_text() == "Hello, World!\n"
    )


@pytest.mark.parametrize(
    "path, name",
    [
        ("/testroot/basic/small", "small"),
    ],
)
def test_directory_from_path(path, name):
    d = Directory.from_path(path)
    assert d.path == path
    assert d.name == name

    # assert pathlib.Path("/testroot/basic/small").exists()

    # dir = Directory.from_path("/testroot/basic/small")

    # assert dir.name == "small"
    # assert dir.path == "/testroot/basic/small"
    # assert dir.name == pathlib.Path(dir.path).name
    # assert dir.path == pathlib.Path(dir.path).as_posix
```
