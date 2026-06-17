from types import SimpleNamespace

from iguane.fom import RAWDATA, fom_ugr
from sqlmodel import Session, select

from ..models.support import GpuRgu
from .sqlmodel import SQLModel


def get_meta():
    # We need to import those to register the tables
    from . import (  # noqa: F401
        allocation,
        cluster,
        diskusage,
        healthcheck,
        job,
        job_series,
        runstate,
        support,
        users,
    )

    return SQLModel.metadata


def init_insert() -> None:
    from sarc.config import config

    with config.db.session() as sess:
        insert_clusters(sess)
        insert_rgu(sess)
        sync_cluster_end_times(sess)
        sess.commit()


def sync_cluster_end_times(sess: Session) -> None:
    """Update end_time_sacct and end_time_prometheus based on latest cache entries.

    Scans the cache backward (most recent first) to find the latest entry per
    cluster and updates the DB fields only when the found time is more recent
    than the current value. Falls back to cluster start_date when no cache
    entry exists for a given cluster. Prometheus search is skipped for clusters
    without a prometheus_url to avoid scanning a potentially large cache for
    nothing.
    """
    from datetime import UTC, datetime

    from sarc.cache import Cache
    from sarc.config import config

    from .cluster import SlurmClusterDB

    if config.cache is None:
        return

    clusters_cfg = config.clusters
    db_clusters = {c.name: c for c in sess.exec(select(SlurmClusterDB)).all()}

    # -- jobs cache → end_time_sacct --
    pending = set(db_clusters.keys())
    sacct_times: dict[str, datetime] = {}
    for ce in Cache("jobs").read_backward():
        if not pending:
            break
        entry_time = ce.get_entry_datetime()
        for key in ce.keys():
            cluster_name = key.split("_")[0]
            if cluster_name in pending:
                sacct_times[cluster_name] = entry_time
                pending.discard(cluster_name)

    # -- prometheus cache → end_time_prometheus (skip clusters without URL) --
    prom_enabled = {
        name
        for name, cfg in clusters_cfg.items()
        if cfg.prometheus_url is not None and name in db_clusters
    }
    pending = set(prom_enabled)
    prom_times: dict[str, datetime] = {}
    for ce in Cache("prometheus").read_backward():
        if not pending:
            break
        entry_time = ce.get_entry_datetime()
        for key in ce.keys():
            cluster_name = key.split("$")[0]
            if cluster_name in pending:
                prom_times[cluster_name] = entry_time
                pending.discard(cluster_name)

    # -- apply to DB --
    for cluster_name, db_cluster in db_clusters.items():
        start_dt = datetime.combine(
            db_cluster.start_date, datetime.min.time(), tzinfo=UTC
        )

        new_sacct = sacct_times.get(cluster_name, start_dt)
        if db_cluster.end_time_sacct is None or new_sacct > db_cluster.end_time_sacct:
            db_cluster.end_time_sacct = new_sacct

        new_prom = prom_times.get(cluster_name, start_dt)
        if (
            db_cluster.end_time_prometheus is None
            or new_prom > db_cluster.end_time_prometheus
        ):
            db_cluster.end_time_prometheus = new_prom


def insert_clusters(sess: Session) -> None:
    # populate the db with default starting dates for each cluster
    from sarc.config import config

    from .cluster import SlurmClusterDB

    clusters = config.clusters
    for cluster_name, clust in clusters.items():
        db_cluster = sess.exec(
            select(SlurmClusterDB).where(SlurmClusterDB.name == cluster_name)
        ).one_or_none()
        if db_cluster is None:
            db_cluster = SlurmClusterDB(
                name=cluster_name,
                domain=clust.user_domain,
                start_date=clust.start_date,
                billing_is_gpu=clust.billing_is_gpu,
            )
            sess.add(db_cluster)
        else:
            db_cluster.domain = clust.user_domain
            db_cluster.start_date = clust.start_date
            db_cluster.billing_is_gpu = clust.billing_is_gpu
        sess.flush()


def insert_rgu(sess: Session) -> None:
    # populate the db with initial rgu data from iguane
    from sarc.client.rgumetrics import get_gpu_type_rgu
    from sarc.config import config

    from .support import GpuRguDB

    args = SimpleNamespace(fom_version="1.0", custom_weights=None, norm=False)
    rgu_map: dict[str, float] = {key: fom_ugr(key, args=args) for key in RAWDATA.keys()}

    mig_rgu_map: dict[str, GpuRgu] = {}

    for cluster_config in config.clusters.values():
        for nodename, gpus_per_nodes in cluster_config.gpus_per_nodes.items():
            for gpu_type in gpus_per_nodes.keys():
                # Harmonize each GPU name found in gpus_per_nodes.
                # Use harmonize_gpu() to handle recursive harmonization and avoid duplicated code.
                std_gpu_name = cluster_config.harmonize_gpu(nodename, gpu_type)
                assert std_gpu_name is not None
                if ":" in std_gpu_name:
                    # If harmonized name is a MIG, we compute default and DRAC RGU.
                    mig_rgu_default = get_gpu_type_rgu(std_gpu_name, mig_ref="mila")
                    mig_rgu_drac = get_gpu_type_rgu(std_gpu_name, mig_ref="drac")
                    gpu_rgu = GpuRgu(
                        name=std_gpu_name, rgu=mig_rgu_default, drac_rgu=mig_rgu_drac
                    )
                    if std_gpu_name in mig_rgu_map:
                        assert mig_rgu_map[std_gpu_name] == gpu_rgu
                    else:
                        mig_rgu_map[std_gpu_name] = gpu_rgu

    # Save IGUANE RGU map into database
    # Currently, DRAC provides RGU only for MIG, so we set drac_rgu to rgu for main GPUs.
    for key, rgu in rgu_map.items():
        sess.merge(GpuRguDB(name=key, rgu=rgu, drac_rgu=rgu))

    # Save MIG RGU map into database
    for mig_gpu_rgu in mig_rgu_map.values():
        sess.merge(GpuRguDB.model_validate(mig_gpu_rgu, from_attributes=True))
