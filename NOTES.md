# Notes about Project

## Some Initial Notes

- Two SQL data models should be considered for directories to
  model their relationships.
  - Path Materialization Column:
    - This is where the directory table has a column showing the path.
    - It would look just like any POSIX path:  *e.g.* `/home/admin/Documents`.
    - This makes getting and storing the path easy.
    - However, it makes querying sub-paths more difficult as
     it requires recursion.
    - It also requires a potentially large column size to fit all paths.
  - Closure Table:
    - Stores the relationships between directories by
      storing every ancestor-descendant relationship.
    - If a directory **A** contains directory **B**,
      and **B** contains **C**, then the closure table will
      contain the following rows:
      - Columns: ancestor, descendant, depth
      - Rows:
        - A, A, 0
        - A, B, 1
        - A, C, 2
        - B, C, 1
    - Non recursive queries.
    - Probably smaller as each row is just a PK, 2 * FKs and a small integer.
    - Paths get materialized by checking for all rows `WHERE`
      a given descendant is the directory in question.
      - Can even `ORDER BY` depth to get the order of ancestors from root to dir

## Chippity Conversation

### 0. Overview

Setting up a Python project for your file tree indexer/meta data cataloguer,
"scout", involves a few steps. Let's break it down into manageable parts:

1. **Project Structure**: Create a structured project layout.
2. **`fs.py` Module**:
    Develop a module to handle file and directory operations and hashing.
3. **Pytest Environment**:
    Set up a testing environment using pytest,
    including mocking filesystem reads.

### 1. Project Structure

Your project structure might look like this:

```txt
scout/
│
├── lib/
│   └── fs.py
│
├── tests/
│   └── test_fs.py
│
├── .gitignore
├── requirements.txt
└── setup.py
```

- `lib/` will contain your core functionality (like `fs.py` for file system operations).
- `tests/` will hold your pytest test cases.
- `.gitignore` to specify untracked files to ignore.
- `requirements.txt` for listing your project dependencies.
- `setup.py` for project setup and metadata.

### 2. `fs.py` Module

The `fs.py` module will include functions for
handling file system operations and hashing.
Here’s a simple structure:

```python
import hashlib
import os

def hash_file(filepath, hash_func='sha256'):
    """
    Hashes a file using the specified hash function.
    """
    h = hashlib.new(hash_func)
    with open(filepath, 'rb') as file:
        for chunk in iter(lambda: file.read(4096), b""):
            h.update(chunk)
    return h.hexdigest()

def list_files(directory):
    """
    List all files in a given directory.
    """
    # Implementation here

# Add more file system related functions as needed
```

### 3. Pytest Environment

For testing, you'll need to set up `pytest` and
possibly use libraries like `pytest-mock` or `pyfakefs` for mocking filesystem reads.

- Install pytest and any additional packages (`pytest-mock`, `pyfakefs`):

  ```bash
  pip install pytest pytest-mock pyfakefs
  ```

- Add these to `requirements.txt` for consistency in dependencies.

- Create a `test_fs.py` in the `tests/` directory to write your test cases:

  ```python
  import pytest
  from pyfakefs.fake_filesystem_unittest import Patcher
  from scout.lib.fs import hash_file, list_files

  def test_hash_file():
      # Your test implementation for hash_file
      pass

  def test_list_files():
      # Your test implementation for list_files
      pass
  ```

- Use `pyfakefs` for mocking filesystem reads.
  It allows you to create a fake file system for testing.

- To run tests, execute:

  ```bash
  pytest
  ```

### Additional Notes

- Ensure you follow best practices in Python coding and structuring,
  including PEP8 guidelines.
- As you're using Ansible, remember to follow Ansible best practices as well,
  especially in terms of module naming and structure.

This setup provides a solid foundation for your `scout` project.
As the project grows,
you can continuously refactor and improve your code and testing environment.
