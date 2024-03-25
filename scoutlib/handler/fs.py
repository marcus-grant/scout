import os
from typing import Optional

from scoutlib.model.dir import Dir


# Function to use Dir class to create a list of all directories of a path
def find_all_dirs(rootpath: str = os.sep) -> list[Dir]:
    """Finds all directories under the given start_path."""
    if not os.path.isdir(rootpath):
        return []
    dirs = [Dir.from_path(rootpath)]
    for rootpath, dirnames, _ in os.walk(rootpath):
        for dirname in dirnames:
            dirs.append(Dir.from_path(os.path.join(rootpath, dirname)))
    return dirs


def find_common_root(dirs: list[Dir]) -> Optional[Dir]:
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


def dirs_sorted_dfs(dirs: list[Dir]) -> list[Dir]:
    """
    Sorts a list of directories in depth-first order.
    Sorts primarily by lexical order from path,
    then by depth denoted by number of path separators.
    Only needs to look at split paths of each directory.
    """
    if len(dirs) == 0:  # If there are no directories, do nothing
        return []

    # Create a sort key function to help sort by depth and then name
    def sort_key(d: Dir):
        return (
            d.path,  # Secondary sort by lexical order from pathstring
            str(d.path).count(os.sep),  # Count path separators to get depth
        )

    return sorted(dirs, key=sort_key)
