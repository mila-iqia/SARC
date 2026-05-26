"""Extract `gpu_billing` entries from a SARC MongoDB into a JSON file.

The SQL branch dropped the MongoDB-backed sarc.config, so this script does
not import anything from `sarc`. It connects directly via pymongo using the
credentials given in a YAML config file (so the connection string does not
end up in shell history or `ps`).

Config YAML format:

    connection_string: mongodb://user:pass@host:port/?authSource=admin
    database_name: sarc-dev

Usage:

    uv run python fixes/gpu_billing_from_mongodb.py \\
        --config secrets/mongo-extract.yaml \\
        --output  fixes/gpu_billings.json
"""

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import yaml
from pymongo import MongoClient

_COLLECTION = "gpu_billing"


def _serialize(obj):
    if isinstance(obj, datetime):
        # Preserve UTC ISO format, e.g. "2024-01-01T00:00:00+00:00"
        return obj.replace(tzinfo=UTC).isoformat()
    raise TypeError(f"Type {type(obj).__name__} not JSON serialisable")


def _load_config(path: Path) -> tuple[str, str]:
    cfg = yaml.safe_load(path.read_text())
    if not isinstance(cfg, dict):
        sys.exit(f"Config {path} must be a YAML mapping.")
    try:
        return cfg["connection_string"], cfg["database_name"]
    except KeyError as e:
        sys.exit(f"Config {path} is missing key: {e.args[0]!r}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--config",
        "-c",
        required=True,
        type=Path,
        help="YAML with `connection_string` and `database_name`.",
    )
    parser.add_argument(
        "--output",
        "-o",
        required=True,
        type=Path,
        help="JSON output path (will be overwritten).",
    )
    args = parser.parse_args()

    connection_string, database_name = _load_config(args.config)

    client = MongoClient(connection_string)
    try:
        coll = client[database_name][_COLLECTION]
        cursor = coll.find({}, {"_id": 0}).sort([("cluster_name", 1), ("since", 1)])
        docs = list(cursor)
    finally:
        client.close()

    args.output.write_text(json.dumps(docs, indent=2, default=_serialize))
    print(f"Wrote {len(docs)} {_COLLECTION} entries to {args.output}")  # noqa: T201


if __name__ == "__main__":
    main()
