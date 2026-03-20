#!/usr/bin/env python3
"""Convert old sacct cache files to the new Cache system format.

Old format: {cluster}.{start_time}.{end_time}.json files in sacct/ folder
New format: ZIP files (LZMA) organized as YYYY/MM/DD/HH:MM:SS in converted/jobs/
            Each day becomes one CacheEntry containing all files from that day.

Usage:
    python convert-old-sacct-cache.py [source_dir]

    source_dir defaults to /Users/brunocarrez/dev/sarc-cache-conversion
"""

from __future__ import annotations

import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from zipfile import ZIP_LZMA, ZipFile

from tqdm import tqdm

DEFAULT_BASE_DIR = Path("/Users/brunocarrez/dev/sarc-cache-conversion")

# Filename pattern: {cluster}.{YYYY-MM-DDTHH:MM}.{YYYY-MM-DDTHH:MM}.json
FILENAME_RE = re.compile(
    r"^(?P<cluster>[^.]+)"
    r"\.(?P<start>\d{4}-\d{2}-\d{2}T\d{2}:\d{2})"
    r"\.(?P<end>\d{4}-\d{2}-\d{2}T\d{2}:\d{2})"
    r"\.json$"
)
DATE_FORMAT = "%Y-%m-%dT%H:%M"


def parse_filename(filename: str) -> tuple[str, datetime, datetime] | None:
    m = FILENAME_RE.match(filename)
    if not m:
        return None
    cluster = m.group("cluster")
    start = datetime.strptime(m.group("start"), DATE_FORMAT).replace(
        tzinfo=timezone.utc
    )
    end = datetime.strptime(m.group("end"), DATE_FORMAT).replace(tzinfo=timezone.utc)
    return cluster, start, end


def dir_from_date(base: Path, dt: datetime) -> Path:
    return base / f"{dt.year:04}" / f"{dt.month:02}" / f"{dt.day:02}"


def main() -> None:
    base_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_BASE_DIR
    source_dir = base_dir / "sacct"
    output_dir = base_dir / "converted" / "jobs"

    if not source_dir.exists():
        print(f"Source directory not found: {source_dir}", file=sys.stderr)  # noqa: T201
        sys.exit(1)

    # Group files by start date: each window spans one day starting at e.g. 05:00
    by_day: dict[str, list[tuple[str, datetime, datetime, Path]]] = defaultdict(list)

    for path in sorted(source_dir.iterdir()):
        if not path.is_file():
            continue
        parsed = parse_filename(path.name)
        if parsed is None:
            print(f"Skipping unrecognized file: {path.name}", file=sys.stderr)  # noqa: T201
            continue
        cluster, start_dt, end_dt = parsed
        day_key = start_dt.strftime("%Y-%m-%d")
        by_day[day_key].append((cluster, start_dt, end_dt, path))

    output_dir.mkdir(parents=True, exist_ok=True)

    converted = 0
    skipped = 0

    with tqdm(sorted(by_day.items()), unit="day") as progress:
        for day_key, files in progress:
            progress.set_description(day_key)

            entry_time = files[0][1]
            out_dir = dir_from_date(output_dir, entry_time)
            out_dir.mkdir(parents=True, exist_ok=True)
            out_file = out_dir / entry_time.time().isoformat("seconds")

            if out_file.exists():
                skipped += 1
                continue

            with ZipFile(out_file, mode="x", compression=ZIP_LZMA) as zf:
                for cluster, start_dt, end_dt, path in sorted(files):
                    key = (
                        f"{cluster}"
                        f"_{start_dt.strftime(DATE_FORMAT)}"
                        f"_{end_dt.strftime(DATE_FORMAT)}"
                    )
                    zf.writestr(key, path.read_bytes())

            converted += 1

    print(f"\nDone: {converted} day(s) converted, {skipped} skipped → {output_dir}")  # noqa: T201


if __name__ == "__main__":
    main()
