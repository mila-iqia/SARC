import logging
from collections import Counter
from typing import Sequence

import sqlmodel
from sqlmodel import Session

from sarc.config import config
from sarc.db.job import SlurmJobDB
from sarc.db.support import GpuRguDB

logger = logging.getLogger(__name__)


# @register
def patch_gpu_types(sess: Session):
    pass


def get_jobs_without_harmonized_gpu_types(sess: Session) -> Sequence[SlurmJobDB]:
    query = (
        sqlmodel.select(SlurmJobDB)
        .outerjoin(
            GpuRguDB, sqlmodel.col(SlurmJobDB.allocated_gpu_type) == GpuRguDB.name
        )
        .where(
            sqlmodel.col(SlurmJobDB.allocated_gpu_type).is_not(None),
            sqlmodel.col(GpuRguDB.name).is_(None),
        )
    )
    return sess.exec(query).all()


def _get_gpu_to_rgu(sess: Session) -> dict[str, tuple[float, float]]:
    gs = sess.exec(sqlmodel.select(GpuRguDB)).all()
    return {g.name: (g.rgu, g.drac_rgu) for g in gs}


def main():
    no_matching: list[SlurmJobDB] = []
    many_matchings: list[tuple[SlurmJobDB, list[str]]] = []
    nb_no_cluster = Counter()

    cfg = config("scraping")
    cluster_configs = cfg.clusters
    with cfg.db.session() as sess:
        jobs = get_jobs_without_harmonized_gpu_types(sess)
        gpu_to_rgu = _get_gpu_to_rgu(sess)
        if jobs:
            logger.warning(f"Found {len(jobs)} jobs with no harmonized names")
        for job in jobs:
            cluster_name = job.cluster.name
            cluster_cfg = cluster_configs.get(job.cluster.name)
            if cluster_cfg is None:
                nb_no_cluster[cluster_name] += 1
                continue
            assert cluster_cfg is not None
            gpu_type = job.allocated_gpu_type
            assert gpu_type is not None
            harmonized_names: set[str] = set()
            for nodename in job.nodes or [""]:
                h_name = cluster_cfg.harmonize_gpu(nodename, gpu_type)
                if h_name is not None:
                    harmonized_names.add(h_name)

            if len(harmonized_names) == 0:
                no_matching.append(job)
            elif len(harmonized_names) == 1:
                harmonized_name = harmonized_names.pop()
                assert harmonized_name in gpu_to_rgu
            else:
                h_rgu_values = {gpu_to_rgu[h_name] for h_name in harmonized_names}
                if len(h_rgu_values) == 1:
                    h_names = sorted(harmonized_names)
                    harmonized_name = ", ".join(h_names)
                    (h_rgu_tuple,) = h_rgu_values
                    if harmonized_name in gpu_to_rgu:
                        assert gpu_to_rgu[harmonized_name] == h_rgu_tuple
                    else:
                        # Add compound gpu to GpuRguDB
                        gpu_to_rgu[harmonized_name] = h_rgu_tuple
                        rgu, drac_rgu = h_rgu_tuple
                        sess.add(
                            GpuRguDB(name=harmonized_name, rgu=rgu, drac_rgu=drac_rgu)
                        )
                else:
                    many_matchings.append((job, sorted(harmonized_names)))
            # sess.commit()

    if nb_no_cluster:
        logger.warning(f"GPU jobs with unknown clusters: {nb_no_cluster}")
    if no_matching:
        logger.warning(
            f"GPU jobs that cannot be harmonized: {len(no_matching)}: "
            f"{
                ', '.join(
                    f'{job.cluster.name}/{job.job_id}:{job.allocated_gpu_type}'
                    for job in no_matching
                )
            }"
        )
    if many_matchings:
        logger.warning(
            f"GPU jobs with many harmonized names with different RGU values: {len(many_matchings)}: "
            f"{
                ', '.join(
                    f'{job.cluster.name}/{job.job_id}:{job.allocated_gpu_type} => '
                    + (' | '.join(h_names))
                    for job, h_names in many_matchings
                )
            }"
        )


if __name__ == "__main__":
    main()
