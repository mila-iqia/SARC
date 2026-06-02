"""
Check jobs to harmonize GPU names.

Make sure each GPU name in SlurmJobDB table is a standard (harmonized) name
(either IGUANE name or "<iguane> : <mig>" name) also present in GpuRguDB table.

Won't modify job if GPU name cannot be harmonized.

Corner case
===========

Few jobs may run on many nodes with different GPU names but same RGU value.
Example: mila 6343581 gpu:a100l:4 nodes=['cn-g007', 'cn-i001'].
Here, a100l is A100-PCIe-80GB on node cn-i001, and A100-SXM4-80GB on node cn-g007.
Different names, but both have RGU 4.8.

Standard harmonization will fail because 2 different names are found.
This fix will instead use a workaround, so that we could still compute metrics
for such jobs, since RGU is the same:
- Set job GPU name to a compound name: for the example: 'A100-PCIe-80GB, A100-SXM4-80GB'
- Save RGU value associated to this compound name in GpuRguDB: for the example:
  'A100-PCIe-80GB, A100-SXM4-80GB' => 4.8
"""

import logging
from collections import Counter
from collections.abc import Sequence

import sqlmodel
from sqlmodel import Session

from sarc.config import config
from sarc.db.job import SlurmJobDB
from sarc.db.support import GpuRguDB

logger = logging.getLogger(__name__)


class HarmonizedNameNotInRguError(Exception):
    def __init__(self, job: SlurmJobDB, name: str):
        super().__init__(
            f"{job.cluster.name}/{job.job_id}:{job.allocated_gpu_type}: "
            f"Harmonized name not in GpuRguDB: {name}"
        )


def fix_gpu_types(sess: Session):
    """
    Check jobs to harmonize GPU names.

    Make sure each GPU name in SlurmJobDB table is a standard (harmonized) name
    (either IGUANE name or "<iguane> : <mig>" name) also present in GpuRguDB table.

    Won't modify job if GPU name cannot be harmonized. This may still happen,
    for e.g. if a job requested a GPU that doesn't exist on cluster.
    """

    no_matching: list[SlurmJobDB] = []
    many_matchings: list[tuple[SlurmJobDB, list[str]]] = []
    nb_no_cluster = Counter()

    # Get cluster configurations
    cluster_configs = config("scraping").clusters
    # Get GPU->RGU mapping from GpuRguDB
    gpu_to_rgu = _get_gpu_to_rgu(sess)
    # Find GPU jobs with missing harmonized names
    jobs = get_gpu_jobs_without_harmonized_gpu_types(sess)
    if jobs:
        logger.warning(f"Found {len(jobs)} jobs with no harmonized names")

    # Fix
    for job in jobs:
        cluster_name = job.cluster.name
        cluster_cfg = cluster_configs.get(cluster_name)
        if cluster_cfg is None:
            nb_no_cluster[cluster_name] += 1
            continue

        gpu_type = job.allocated_gpu_type
        assert gpu_type is not None
        harmonized_names: set[str] = set()
        for nodename in job.nodes or [""]:
            h_name = cluster_cfg.harmonize_gpu(nodename, gpu_type)
            if h_name is not None:
                harmonized_names.add(h_name)

        harmonized_name: str | None = None
        if len(harmonized_names) == 0:
            # No harmonized name found for this GPU. We'll log.
            no_matching.append(job)
        elif len(harmonized_names) == 1:
            # Harmonized name found. Ok.
            harmonized_name = harmonized_names.pop()
            if harmonized_name not in gpu_to_rgu:
                raise HarmonizedNameNotInRguError(job, harmonized_name)
        else:
            # Multiple harmonized names found.
            # This can happen a few time. Example: mila 6343581 gpu:a100l:4 nodes=['cn-g007', 'cn-i001']
            # Let's check if all harmonized names have same RGU value.
            h_rgu_values: set[tuple[float, float]] = set()
            for h_name in harmonized_names:
                if h_name not in gpu_to_rgu:
                    raise HarmonizedNameNotInRguError(job, h_name)
                h_rgu_values.add(gpu_to_rgu[h_name])

            if len(h_rgu_values) == 1:
                # All harmonized names have same RGU value.
                # Since RGU is the main value we need in SARC, we will save
                # both a custom GPU name (concatenation of harmonized names) to not forget job GPUs,
                # and the RGU values associated to this specific concatenation in GpuRguDB.
                harmonized_name = ", ".join(sorted(harmonized_names))
                (h_rgu_tuple,) = h_rgu_values
                if harmonized_name in gpu_to_rgu:
                    # Already saved in GpuRguDB.
                    assert gpu_to_rgu[harmonized_name] == h_rgu_tuple
                else:
                    # Add compound gpu to GpuRguDB
                    rgu, drac_rgu = h_rgu_tuple
                    sess.add(GpuRguDB(name=harmonized_name, rgu=rgu, drac_rgu=drac_rgu))
                    gpu_to_rgu[harmonized_name] = h_rgu_tuple
            else:
                # Different RGU values for different harmonized names. We'll log.
                many_matchings.append((job, sorted(harmonized_names)))

        if harmonized_name is not None:
            job.harmonized_gpu_type = harmonized_name

    sess.commit()

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


def get_gpu_jobs_without_harmonized_gpu_types(sess: Session) -> Sequence[SlurmJobDB]:
    query = (
        sqlmodel.select(SlurmJobDB)
        .outerjoin(
            GpuRguDB, sqlmodel.col(SlurmJobDB.harmonized_gpu_type) == GpuRguDB.name
        )
        .where(
            # We don't want CPU jobs
            sqlmodel.col(SlurmJobDB.allocated_gpu_type).is_not(None),
            # We look for GPU names not harmonized, i.e. not present in GpuRguDB
            sqlmodel.col(GpuRguDB.name).is_(None),
        )
    )
    return sess.exec(query).all()


def _get_gpu_to_rgu(sess: Session) -> dict[str, tuple[float, float]]:
    """
    Load GPU->RGU mapping from GpuRguDB.
    Since we currently keep 2 RGU values per GPU (default and DRAC),
    we return both as a tuple.
    """
    return {
        g.name: (g.rgu, g.drac_rgu) for g in sess.exec(sqlmodel.select(GpuRguDB)).all()
    }
