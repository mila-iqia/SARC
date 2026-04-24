import argparse
import subprocess
import sys
from pathlib import Path

from pymongo import MongoClient

SOURCE_REMOTE_HOST = "sarc-local"
SOURCE_REMOTE_PATH = "/home/sarc/SARC/sarc-cache/prometheus"
LOCAL_PATH = Path("~/dev/sarc-cache/prometheus").expanduser()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Sync prometheus cache and MongoDB databases from GCP."
    )
    parser.add_argument(
        "source_mongo", help="Source MongoDB connection string (mongodb://...)"
    )
    parser.add_argument(
        "target_mongo", help="Target MongoDB connection string (mongodb://...)"
    )
    args = parser.parse_args()
    for name, value in [
        ("source_mongo", args.source_mongo),
        ("target_mongo", args.target_mongo),
    ]:
        if not value.startswith(("mongodb://", "mongodb+srv://")):
            print(
                f"Error: {name} is not a valid MongoDB connection string: {value}",
                file=sys.stderr,
            )
            sys.exit(1)
    return args


def main() -> int:
    args = parse_args()

    source_client = MongoClient(args.source_mongo)
    source_db = source_client.get_default_database()
    source_clusters = {
        doc["cluster_name"]: doc["end_time_prometheus"]
        for doc in source_db["clusters"].find(
            {}, {"cluster_name": 1, "end_time_prometheus": 1, "_id": 0}
        )
    }

    print(f"Source clusters ({len(source_clusters)}):")
    for cluster_name, end_time in source_clusters.items():
        print(f"  {cluster_name}: {end_time}")

    LOCAL_PATH.mkdir(parents=True, exist_ok=True)
    cmd = [
        "rsync",
        "--archive",
        "--verbose",
        "--progress",
        f"{SOURCE_REMOTE_HOST}:{SOURCE_REMOTE_PATH}/",
        str(LOCAL_PATH) + "/",
    ]
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd)  # noqa: PLW1510
    if result.returncode != 0:
        return result.returncode

    for ds_store in LOCAL_PATH.rglob(".DS_Store"):
        ds_store.unlink()

    gcloud_cmd = [
        "gcloud",
        "storage",
        "rsync",
        "prometheus",
        "gs://sarc-cache/prometheus",
        "--recursive",
        "--exclude=.DS_Store",
    ]
    print(f"Running: {' '.join(gcloud_cmd)}")
    result = subprocess.run(gcloud_cmd, cwd=LOCAL_PATH.parent)  # noqa: PLW1510
    if result.returncode != 0:
        return result.returncode

    target_client = MongoClient(args.target_mongo)
    target_db = target_client.get_default_database()
    target_clusters = target_db["clusters"]
    for cluster_name, end_time in source_clusters.items():
        result = target_clusters.update_one(
            {"cluster_name": cluster_name}, {"$set": {"end_time_prometheus": end_time}}
        )
        if result.matched_count == 0:
            print(
                f"Warning: cluster '{cluster_name}' not found in target database, skipping.",
                file=sys.stderr,
            )

    return 0


if __name__ == "__main__":
    sys.exit(main())
