import logging

from sarc.config import config, scraping_mode_required
from sarc.jobs.node_gpu_mapping import get_node_to_gpu

logger = logging.getLogger(__name__)


@scraping_mode_required
def check_prometheus_vs_slurmconfig(cluster_name=None):
    """
    Check if GPU types from Prometheus are the same as the ones in slurm config files.

    To get Prometheus GPU types, we make a Prometheus query:
    `slurm_job_utilization_gpu_memory{cluster=<cluster name>}`,
    then we collect GPU types from results.

    To get slurm config GPU types, we collect all GPU types
    from latest node => GPU mapping in database.
    Node => GPU mappings should have been collected using command line
    `sarc acquire slurmconfig -c <cluster name>`.

    Parameters
    ----------
    cluster_name: str
        Name of cluster to check. If None, all clusters are checked.
    """
    if cluster_name is None:
        clusters = config().clusters.values()
    else:
        clusters = [config().clusters[cluster_name]]

    for cluster in clusters:
        # We only check clusters which have a prometheus_url
        if not cluster.prometheus_url:
            continue

        query = f'slurm_job_utilization_gpu_memory{{cluster="{cluster.name}"}}'
        results = cluster.prometheus.custom_query(query)
        prometheus_gpu_types = {result["metric"]["gpu_type"] for result in results}

        # Get slurm config GPU types from latest
        # node => GPU mappings stored in database
        slurmconfig_gpu_types = set()
        mapping = get_node_to_gpu(cluster.name)
        if mapping:
            for gpu_types in mapping.node_to_gpu.values():
                # If gpu_types is a string, split it on commas
                if isinstance(gpu_types, str):
                    gpu_types = gpu_types.split(",")
                # If a GPU type is in format gpu:<name>:<count>, get <name>
                for gpu_type in gpu_types:
                    if gpu_type.startswith("gpu:") and gpu_type.count(":") == 2:
                        _, gpu_type, _ = gpu_type.split(":")
                    slurmconfig_gpu_types.add(gpu_type)
        if not slurmconfig_gpu_types:
            # Warn if there is no slurm config GPUs available.
            logger.warning(
                f"[prometheus][{cluster.name}] cannot find GPU types from slurm config file. "
                f"You may need to call `sarc acquire slurmconfig -c {cluster.name}`"
            )
        else:
            # Warn for each prometheus GPU not found in slurm config GPUs.
            only_in_prometheus = prometheus_gpu_types - slurmconfig_gpu_types
            for gpu_type in only_in_prometheus:
                logger.warning(
                    f"[prometheus][{cluster.name}] gpu_type not found in slurm config file: {gpu_type}"
                )
