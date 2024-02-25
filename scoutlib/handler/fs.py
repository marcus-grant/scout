import os
from typing import Optional

from scoutlib.model.fs import Directory, File

Dir = Directory


# Function to use Directory class to create a list of all directories of a path
def find_all_dirs(rootpath: str = os.sep) -> list[Directory]:
    """Finds all directories under the given start_path."""
    if not os.path.isdir(rootpath):
        return []
    dirs = [Directory.from_path(rootpath)]
    for rootpath, dirnames, _ in os.walk(rootpath):
        for dirname in dirnames:
            dirs.append(Dir.from_path(os.path.join(rootpath, dirname)))
    return dirs


def find_common_root(dirs: list[Directory]) -> Optional[Directory]:
    """Finds the deepest shared root directory from a list of directories."""
    if len(dirs) == 0:  # If there are no directories, None
        return None
    # Extract path strs from Directories
    paths = [d.path for d in dirs]
    # Get the path for common root
    matches = [d for d in dirs if d.path == os.path.commonpath(paths)]
    # DEBUG raise LookupError(f"matches:{matches}, paths: {paths}")
    try:
        root = matches[0]
    except IndexError:
        return None
    return root


def dirs_sorted_dfs(dirs: list[Directory]) -> list[Directory]:
    """
    Sorts a list of directories in depth-first order.
    Sorts primarily by lexical order from path,
    then by depth denoted by number of path separators.
    Only needs to look at split paths of each directory.
    """
    if len(dirs) == 0:  # If there are no directories, do nothing
        return []

    # Create a sort key function to help sort by depth and then name
    def sort_key(d: Directory):
        return (
            d.path,  # Secondary sort by lexical order from pathstring
            d.path.count(os.sep),  # Count path separators to get depth
        )

    return sorted(dirs, key=sort_key)
