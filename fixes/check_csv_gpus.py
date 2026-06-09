import ast
import csv
import json
import logging
import sys
from collections import Counter

import sqlmodel
from tqdm import tqdm

from sarc.config import config
from sarc.db.support import GpuRguDB

logger = logging.getLogger(__name__)


def _parse_nodes(v: str) -> list[str]:
    if v in {"", "[]"}:
        return []
    # Stored as Python repr: ['cn-c031', 'cn-a005']. Slurm node names have no
    # quotes/special chars, so json.loads(...) after ' -> " is ~5x faster than
    # ast.literal_eval. Fall back if a name surprises us.
    try:
        parsed = json.loads(v.replace("'", '"'))
    except json.JSONDecodeError:
        parsed = ast.literal_eval(v)
    assert isinstance(parsed, list)
    return [str(x) for x in parsed]


def main():
    unknown_clusters = Counter()
    nb_cannot_harmonize = Counter()

    csv_path = sys.argv[1]
    cluster_cfgs = config.clusters
    with config.db.session() as sess:
        known_gpus: set[str] = set(sess.exec(sqlmodel.select(GpuRguDB.name)).all())
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in tqdm(reader, desc="job(s)"):
            cluster_name = row["cluster_name"]
            gpu_type = row["allocated.gpu_type"] or None
            if gpu_type is None or gpu_type in known_gpus:
                continue
            cluster_cfg = cluster_cfgs.get(cluster_name)
            if cluster_cfg is None:
                unknown_clusters[cluster_name] += 1
                continue
            nodes = _parse_nodes(row["nodes"])
            full_name = None
            if " : " in gpu_type:
                full_name, gpu_type = gpu_type.split(" : ")
            h_name = cluster_cfg.harmonize_gpu_from_nodes(nodes, gpu_type)
            if h_name is None or h_name not in known_gpus:
                nb_cannot_harmonize[(cluster_name, gpu_type)] += 1
            if full_name is not None:
                assert " : " in h_name
                assert h_name.startswith(full_name)

    if unknown_clusters:
        logger.warning("Unknown clusters: %s", unknown_clusters)
    if nb_cannot_harmonize:
        logger.warning("Cannot harmonize: %", nb_cannot_harmonize)


if __name__ == "__main__":
    main()
