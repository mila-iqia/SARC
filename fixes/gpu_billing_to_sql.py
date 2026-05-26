"""Import gpu_billing entries from a JSON file into the SARC SQL database.

The JSON is expected to have been produced by
`fixes/gpu_billing_from_mongodb.py` and contains a list of objects shaped as:

    [
      {"cluster_name": "mila", "since": "2024-01-01T00:00:00+00:00",
       "gpu_to_billing": {"v100": 1.0, "a100": 4.0, ...}},
      ...
    ]

Uses the standard SARC config: reads `SARC_CONFIG` for the YAML and runs in
scraping mode (required because `config().db.session()` triggers the schema
upgrade path).

Usage:

    SARC_CONFIG=secrets/sarc-dev-local-sql.yaml SARC_MODE=scraping \\
        uv run python fixes/gpu_billing_to_sql.py \\
            --input secrets/metrics-results/gpu_billing.json
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

from sarc.config import config
from sarc.db.cluster import GPUBillingDB, SlurmClusterDB


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--input",
        "-i",
        required=True,
        type=Path,
        help="JSON file produced by gpu_billing_from_mongodb.py",
    )
    args = parser.parse_args()

    docs = json.loads(args.input.read_text())
    if not isinstance(docs, list):
        sys.exit(f"Expected a JSON array in {args.input}")

    inserted = 0
    unknown_clusters: dict[str, int] = {}

    with config().db.session() as sess:
        # Cache cluster_id lookups to avoid one query per entry.
        cluster_id_cache: dict[str, int | None] = {}

        for doc in docs:
            cluster_name = doc["cluster_name"]
            if cluster_name not in cluster_id_cache:
                cluster_id_cache[cluster_name] = SlurmClusterDB.id_by_name(
                    sess, cluster_name
                )
            cluster_id = cluster_id_cache[cluster_name]
            if cluster_id is None:
                unknown_clusters[cluster_name] = (
                    unknown_clusters.get(cluster_name, 0) + 1
                )
                continue

            # datetime.fromisoformat handles the "+00:00" UTC suffix written
            # by the extract script.
            since = datetime.fromisoformat(doc["since"])
            GPUBillingDB.get_or_create(
                sess,
                cluster_id=cluster_id,
                since=since,
                gpu_to_billing=doc["gpu_to_billing"],
            )
            inserted += 1
        sess.commit()

    skipped = sum(unknown_clusters.values())
    print(  # noqa: T201
        f"Processed {len(docs)} entries: {inserted} merged, {skipped} skipped."
    )
    if unknown_clusters:
        print(  # noqa: T201
            "Unknown clusters (not in SQL DB): "
            + ", ".join(f"{n} ({c})" for n, c in sorted(unknown_clusters.items()))
        )


if __name__ == "__main__":
    main()
