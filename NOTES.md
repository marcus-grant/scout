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
- It's REALLY friggin hard to determine what's better,
  materialized paths or closure tables for
  this project without some testing.
- Since materialized paths are easier to implement,
  that's what I'll start with.
- Below is a snippet of the table creation I used in an earlier commit.
  - That commit hash is: 3a2542431c93e6b30334fa5cc99a0dca07e20f2b
      
```python
    def _init_db(self):
        """Initialize db & create directory table if not there."""
        with sqlite3.connect(self.path_db) as conn:
            query = """ CREATE TABLE IF NOT EXISTS dir (
                            id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                            path TEXT NOT NULL UNIQUE
                        );"""
            conn.cursor().execute(query)
            query = """CREATE TABLE IF NOT EXISTS dir_ancestor (
                            dir_id INTEGER NOT NULL,
                            ancestor_id INTEGER NOT NULL,
                            depth INTEGER NOT NULL,
                            PRIMARY KEY (dir_id, ancestor_id),
                            FOREIGN KEY (dir_id) REFERENCES directory(id),
                            FOREIGN KEY (ancestor_id) REFERENCES directory(id)
                        );"""
            conn.cursor().execute(query)
            conn.commit()
            return conn


```

## Example Closure Queries

```sql
DROP TABLE IF EXISTS dir;
CREATE TABLE IF NOT EXISTS dir (
  id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  name TEXT
);
DROP TABLE IF EXISTS dir_ancestor;
CREATE TABLE IF NOT EXISTS dir_ancestor (
  dir_id INTEGER NOT NULL,
  ancestor_id INTEGER NOT NULL,
  depth INTEGER NOT NULL,
  PRIMARY KEY (dir_id, ancestor_id),
  FOREIGN KEY (dir_id) REFERENCES dir(id),
  FOREIGN KEY (ancestor_id) REFERENCES dir(id)
);

-- Dir Tree: (id)
-- a(1)/ ─┬─ b(2)/─── c(3)/
--        ├─ d(4)/
--        └─ e(5)/
-- f(6)/ ─┬─ g(7)/
--        └─ h(8)/
INSERT INTO dir (name) VALUES ('a');
INSERT INTO dir (name) VALUES ('b');
INSERT INTO dir (name) VALUES ('c');
INSERT INTO dir (name) VALUES ('d');
INSERT INTO dir (name) VALUES ('e');
INSERT INTO dir (name) VALUES ('f');
INSERT INTO dir (name) VALUES ('g');
INSERT INTO dir (name) VALUES ('h');
INSERT INTO dir_ancestor (dir_id, ancestor_id, depth) VALUES(1, 1, 0);
INSERT INTO dir_ancestor (dir_id, ancestor_id, depth) VALUES(2, 1, 1);
INSERT INTO dir_ancestor (dir_id, ancestor_id, depth) VALUES(2, 2, 0);
INSERT INTO dir_ancestor (dir_id, ancestor_id, depth) VALUES(3, 1, 2);
INSERT INTO dir_ancestor (dir_id, ancestor_id, depth) VALUES(3, 2, 1);
INSERT INTO dir_ancestor (dir_id, ancestor_id, depth) VALUES(3, 3, 0);
INSERT INTO dir_ancestor (dir_id, ancestor_id, depth) VALUES(4, 1, 1);
INSERT INTO dir_ancestor (dir_id, ancestor_id, depth) VALUES(4, 4, 0);
INSERT INTO dir_ancestor (dir_id, ancestor_id, depth) VALUES(5, 1, 1);
INSERT INTO dir_ancestor (dir_id, ancestor_id, depth) VALUES(5, 5, 0);
INSERT INTO dir_ancestor (dir_id, ancestor_id, depth) VALUES(6, 6, 0);
INSERT INTO dir_ancestor (dir_id, ancestor_id, depth) VALUES(7, 6, 1);
INSERT INTO dir_ancestor (dir_id, ancestor_id, depth) VALUES(7, 7, 0);
INSERT INTO dir_ancestor (dir_id, ancestor_id, depth) VALUES(8, 6, 1);
INSERT INTO dir_ancestor (dir_id, ancestor_id, depth) VALUES(8, 8, 0);

-- CTE to recursively build paths
-- Recursive CTE to build full directory paths
WITH RECURSIVE DirPath AS (
  SELECT
    d.id,
    d.name,
    CAST('/' AS TEXT) || d.name AS path  -- Start path with root directory name
  FROM dir d
  WHERE NOT EXISTS (
    -- Select directories that have no parent in dir_ancestor table
    SELECT 1
    FROM dir_ancestor da
    WHERE da.dir_id = d.id AND da.depth > 0
  )
  UNION ALL
  -- Recursively append parent directory names to build the full path
  SELECT
    d.id,
    d.name,
    dp.path || '/' || d.name
  FROM dir d
  JOIN dir_ancestor da ON d.id = da.dir_id
  JOIN DirPath dp ON da.ancestor_id = dp.id
)
SELECT * FROM DirPath
-- Optional: Order by path or id for better readability
ORDER BY path, id;



```

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
