"""Delete abandoned cache entries left in their `.current` write state.

`Cache.create_entry` writes each run's fetch to `<time>.current` and renames
it into a readable entry in its `finally`. A `.current` file only survives
when the writer was SIGKILLed before that `finally` ran — a Cloud Run run
past its timeout does exactly that, once per killed hour. Nothing removes
the survivors, so they pile up and the `temporary_cache_file` health check
reports every fossil of every past incident forever.

A `.current` is abandoned once no run can still be writing it: older than
any job's timeout plus margin — default 48 h.

Dry run by default. Apply with:

    SARC_CONFIG=<file> uv run python scripts/clean_stale_current_files.py --no-dry-run
"""

import argparse
import sys
import time
from pathlib import Path

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
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
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
        help="Cache root to clean. Default: the configured cache (SARC_CACHE_PATH).",
    )
    parser.add_argument(
        "--dry-run", action=argparse.BooleanOptionalAction, default=True
    )
    args = parser.parse_args()

    if args.cache_dir is not None:
        cache_dir = args.cache_dir
    else:
        from sarc.config import config

        cache_dir = config.cache
        if cache_dir is None:
            print("no cache configured (config.cache is None)", file=sys.stderr)
            return 1

    stale = stale_current_files(cache_dir, args.max_age_hours * 3600)
    for path, age, size in stale:
        print(
            f"{'would delete' if args.dry_run else 'deleting'} {path} "
            f"(age {age / 3600:.0f} h, {size} B)"
        )
        if not args.dry_run:
            path.unlink(missing_ok=True)
    print(
        f"{'would delete' if args.dry_run else 'deleted'} {len(stale)} "
        f"stale .current file(s) in {cache_dir}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
