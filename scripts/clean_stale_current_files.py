"""Clean up abandoned cache entries left in their `.current` write state.

`Cache.create_entry` writes to `<time>.current` and renames it on exit.
Nothing removes a file that misses that rename, so the
`temporary_cache_file` health check reports it forever. Only files
untouched for longer than any job's timeout are considered, so no run
can still be writing them.

Invalid zips are deleted: the writer was killed before close wrote the
zip index (e.g. a Cloud Run job past its timeout), or the close failed.
A killed fetch records no progress, so later runs fetch that data again.
After a failed close, an `--auto_interval` fetch still records its
intervals as fetched. That run's logs show the close error and the
intervals it fetched: refetch them with `sarc fetch jobs --intervals`.

Valid zips are kept: only their rename failed, and `fetch_jobs` may have
recorded their progress, so their data exists nowhere else. Restore one
by dropping its `.current` suffix, then re-parse with `--since` if
parsing already went past its date.

Dry run by default. Apply with:

    SARC_CONFIG=<file> uv run python scripts/clean_stale_current_files.py --no-dry-run
"""

import argparse
import sys
import time
import zipfile
from pathlib import Path

from sarc.config import config

DEFAULT_MAX_AGE_HOURS = 48.0


def stale_current_files(
    root: Path, max_age_seconds: float
) -> list[tuple[Path, float, int]]:
    now = time.time()
    stale = []
    for path in sorted(root.rglob("*.current")):
        try:
            stat = path.stat()
        except FileNotFoundError:
            # Renamed by a run that finished after the listing.
            continue
        age = now - stat.st_mtime
        if age > max_age_seconds:
            stale.append((path, age, stat.st_size))
    return stale


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Clean up abandoned cache entries left in their `.current` write state."
    )
    parser.add_argument(
        "--max-age-hours",
        type=float,
        default=DEFAULT_MAX_AGE_HOURS,
        help="Only delete .current files untouched for longer than this. "
        "Keep it above any job's timeout.",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=None,
        help="Cache root to clean. Default: the configured cache.",
    )
    parser.add_argument(
        "--dry-run", action=argparse.BooleanOptionalAction, default=True
    )
    args = parser.parse_args()

    if args.cache_dir is not None:
        cache_dir = args.cache_dir
    else:
        cache_dir = config.cache
        if cache_dir is None:
            print("no cache configured (config.cache is None)", file=sys.stderr)
            return 1
    if not cache_dir.is_dir():
        print(f"cache directory not found: {cache_dir}", file=sys.stderr)
        return 1

    stale = stale_current_files(cache_dir, args.max_age_hours * 3600)
    deleted = 0
    kept = 0
    for path, age, size in stale:
        details = f"{path} (age {age / 3600:.0f} h, {size} B)"
        if zipfile.is_zipfile(path):
            print(f"keeping complete entry {details}")
            kept += 1
            continue
        print(f"{'would delete' if args.dry_run else 'deleting'} {details}")
        if not args.dry_run:
            path.unlink(missing_ok=True)
        deleted += 1
    print(
        f"{'would delete' if args.dry_run else 'deleted'} {deleted} "
        f"stale .current file(s) in {cache_dir}"
    )
    if kept:
        print(
            f"kept {kept} complete entry(ies): drop their `.current` suffix "
            "to restore them"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
