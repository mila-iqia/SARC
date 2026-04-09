"""
Dispatch prometheus cache files into YYYY/MM/DD/ subdirectories.

Files are expected to be named like:
  <cluster>.<job_id>.<start_time>_to_<end_time>.<rest>.json

where <start_time> is formatted as %Y-%m-%dT%Hh%Mm%Ss.
The date (YYYY/MM/DD) is extracted from <start_time> and used as the subdirectory.
"""

import re
import sys
from pathlib import Path

from tqdm import tqdm

DATE_PATTERN = re.compile(r"\.(\d{4})-(\d{2})-(\d{2})T\d{2}h\d{2}m\d{2}s_to_")


def dispatch(folder: Path) -> None:
    files = [f for f in folder.iterdir() if f.is_file() and f.suffix == ".json"]
    if not files:
        print(f"No .json files found in {folder}")  # noqa: T201
        return

    moved = 0
    skipped = 0
    for f in tqdm(files, unit="file"):
        m = DATE_PATTERN.search(f.name)
        if not m:
            print(f"  SKIP (no date found): {f.name}")  # noqa: T201
            skipped += 1
            continue

        year, month, day = m.group(1), m.group(2), m.group(3)
        dest_dir = folder / year / month / day
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / f.name
        f.rename(dest)
        moved += 1

    print(f"Done: {moved} file(s) moved, {skipped} skipped.")  # noqa: T201


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <folder>")  # noqa: T201
        sys.exit(1)

    folder = Path(sys.argv[1])
    if not folder.is_dir():
        print(f"Error: {folder} is not a directory")  # noqa: T201
        sys.exit(1)

    dispatch(folder)
