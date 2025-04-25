from __future__ import annotations

import io
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List

from hostlist import expand_hostlist
from simple_parsing import field

from sarc.cache import CachePolicy, with_cache
from sarc.client.gpumetrics import _gpu_billing_collection
from sarc.config import ClusterConfig, config
from sarc.jobs.node_gpu_mapping import _node_gpu_mapping_collection

logger = logging.getLogger(__name__)


@dataclass
class AcquireSlurmConfig:
    cluster_name: str = field(alias=["-c"])
    day: str = field(
        alias=["-d"],
        required=False,
        help=(
            "Cluster config file date (format YYYY-MM-DD). "
            "Used for file versioning. Should represents a day when config file has been updated "
            "(e.g. for new GPU billings, node GPUs, etc.). "
            "If not specified, uses current day and downloads config file from cluster."
        ),
    )

    def execute(self) -> int:
        if self.cluster_name == "mila":
            logger.error("Cluster `mila` not yet supported.")
            return -1

        cluster_config = config().clusters[self.cluster_name]
        parser = SlurmConfigParser(cluster_config, self.day)
        slurm_conf = parser.get_slurm_config()
        _gpu_billing_collection().save_gpu_billing(
            self.cluster_name, parser.day, slurm_conf.gpu_to_billing
        )
        _node_gpu_mapping_collection().save_node_gpu_mapping(
            self.cluster_name, parser.day, slurm_conf.node_to_gpu
        )
        return 0


class FileContent:
    """
    Formatter for slurm conf file cache.
    Just read and write entire text content from file.
    """

    @classmethod
    def load(cls, file) -> str:
        return file.read()

    @classmethod
    def dump(cls, value: str, output_file):
        output_file.write(value)


class SlurmConfigParser:
    def __init__(self, cluster: ClusterConfig, day: str | None = None):
        if day is None:
            # No day given, get current day
            day = datetime.now().strftime("%Y-%m-%d")
            # Cache must download slurm conf file and save it locally.
            cache_policy = CachePolicy.use
            logger.info(f"Looking for config file at current date: {day}")
        else:
            # Day given. Slurm conf file must be retrieved from cache only.
            cache_policy = CachePolicy.always
        self.cluster = cluster
        self.day = day
        self.cache_policy = cache_policy

    def get_slurm_config(self) -> SlurmConfig:
        content = with_cache(
            self._get_slurm_conf,
            subdirectory="slurm_conf",
            key=self._cache_key,
            formatter=FileContent,
        )(cache_policy=self.cache_policy)
        return self.load(io.StringIO(content))

    def _get_slurm_conf(self) -> str:
        cmd = f"cat {self.cluster.slurm_conf_host_path}"
        result = self.cluster.ssh.run(cmd, hide=True)
        return result.stdout

    def _cache_key(self):
        return f"slurm.{self.cluster.name}.{self.day}.conf"

    def load(self, file) -> SlurmConfig:
        """
        Parse cached slurm conf file and return a SlurmConfig object
        containing node_to_gpu and gpu_to_billing.
        """
        partitions: List[Partition] = []
        node_to_gpu = {}

        # Parse lines: extract partitions and node_to_gpu
        for line_number, line in enumerate(file):
            line = line.strip()
            if line.startswith("PartitionName="):
                partitions.append(
                    Partition(
                        line_number=line_number + 1,
                        line=line,
                        info=dict(
                            option.split("=", maxsplit=1) for option in line.split()
                        ),
                    )
                )
            elif line.startswith("NodeName="):
                nodes_config = dict(
                    option.split("=", maxsplit=1) for option in line.split()
                )
                gpu_type = nodes_config.get("Gres")
                if gpu_type:
                    node_to_gpu.update(
                        {
                            node_name: gpu_type
                            for node_name in expand_hostlist(nodes_config["NodeName"])
                        }
                    )

        # Parse partitions: extract gpu_to_billing
        gpu_to_billing = self._parse_gpu_to_billing(partitions, node_to_gpu)

        # Return parsed data
        return SlurmConfig(node_to_gpu=node_to_gpu, gpu_to_billing=gpu_to_billing)

    def _parse_gpu_to_billing(
        self, partitions: List[Partition], node_to_gpu: Dict[str, str]
    ) -> Dict[str, float]:
        # Mapping of GPU to partition billing.
        # ALlow to check that inferred billing for a GPU is the same across partitions.
        # If not, an error will be raised with additional info about involved partitions.
        gpu_to_partition_billing: Dict[str, PartitionGPUBilling] = {}

        # Collection for all GPUs found in partition nodes.
        # We will later iterate on this collection to resolve any GPU without billing.
        all_partition_node_gpus = set()

        for partition in partitions:
            # Get all GPUs in partition nodes and all partition GPU billings.
            (
                local_gres,
                local_gpu_to_billing,
            ) = partition.get_gpus_and_partition_billings(node_to_gpu)

            # Merge local GPUs into global partition node GPUs.
            all_partition_node_gpus.update(local_gres)

            # Merge local GPU billings into global GPU billings
            for gpu_type, value in local_gpu_to_billing.items():
                new_billing = PartitionGPUBilling(
                    gpu_type=gpu_type, value=value, partition=partition
                )
                if gpu_type not in gpu_to_partition_billing:
                    # New GPU found, add it
                    gpu_to_partition_billing[gpu_type] = new_billing
                elif gpu_to_partition_billing[gpu_type].value != value:
                    # GPU already found, with a different billing. Problem.
                    raise InconsistentGPUBillingError(
                        gpu_type, gpu_to_partition_billing[gpu_type], new_billing
                    )

        # Generate GPU->billing mapping
        gpu_to_billing = {
            gpu_type: billing.value
            for gpu_type, billing in gpu_to_partition_billing.items()
        }

        # Resolve GPUs without billing
        for gpu_desc in all_partition_node_gpus:
            if gpu_desc not in gpu_to_billing:
                if gpu_desc.startswith("gpu:") and gpu_desc.count(":") == 2:
                    # GPU resource with format `gpu:<real-gpu-type>:<gpu-count>`
                    _, gpu_type, gpu_count = gpu_desc.split(":")
                    if gpu_type in gpu_to_billing:
                        billing = gpu_to_billing[gpu_type] * int(gpu_count)
                        gpu_to_billing[gpu_desc] = billing
                        logger.info(f"Inferred billing for {gpu_desc} := {billing}")
                    else:
                        logger.warning(
                            f"Cannot find GPU billing for GPU type {gpu_type} in GPU resource {gpu_desc}"
                        )
                else:
                    logger.warning(f"Cannot infer billing for GPU: {gpu_desc}")

        # We can finally return GPU->billing mapping.
        return gpu_to_billing


@dataclass
class SlurmConfig:
    """Parsed data from slurm config file"""

    node_to_gpu: Dict[str, str]
    gpu_to_billing: Dict[str, float]


@dataclass
class Partition:
    """Partition entry in slurm config file"""

    line_number: int
    line: str
    info: Dict[str, str]

    def get_gpus_and_partition_billings(self, node_to_gpu: Dict[str, str]):
        """
        Parse and return:
        - partition node GPUs
        - partition GPU billings inferred from field `TRESBillingWeights`
        """

        # Get partition node GPUs
        local_gres = self._get_node_gpus(node_to_gpu)

        # Get GPU weights from TRESBillingWeights
        tres_billing_weights = dict(
            option.split("=", maxsplit=1)
            for option in self.info.get("TRESBillingWeights", "").split(",")
            if option
        )
        gpu_weights = {
            key: value
            for key, value in tres_billing_weights.items()
            if key.startswith("GRES/gpu")
        }

        # Parse local GPU billings
        local_gpu_to_billing = {}
        for key, value in gpu_weights.items():
            value = float(value)
            if key == "GRES/gpu":
                if len(gpu_weights) == 1:
                    # We only have `GRES/gpu=<value>`
                    # Let's map value to each GPU found in partition nodes
                    local_gpu_to_billing.update(
                        {gpu_name: value for gpu_name in local_gres}
                    )
                else:
                    # We have both `GRES/gpu=<value>` and at least one `GRES/gpu:a_gpu=a_value`.
                    # Ambiguous case, cannot map `GRES/gpu=<value>` to a specific GPU.
                    logger.debug(
                        f"[line {self.line_number}] "
                        f"Ignored ambiguous GPU billing (cannot match to a specific GPU): `{key}={value}` "
                        f"| partition: {self.info['PartitionName']} "
                        # f"| nodes: {partition.info['Nodes']}, "
                        f"| nodes GPUs: {', '.join(local_gres)} "
                        f"| TRESBillingWeights: {self.info['TRESBillingWeights']}"
                    )
            else:
                # We have `GRES/gpu:a_gpu=a_value`.
                # We can parse.
                _, gpu_name = key.split(":", maxsplit=1)
                local_gpu_to_billing[gpu_name] = value

        return local_gres, local_gpu_to_billing

    def _get_node_gpus(self, node_to_gpu: Dict[str, str]) -> List[str]:
        """Return all GPUs found in partition nodes"""
        return sorted(
            {
                gres
                for node_name in expand_hostlist(self.info["Nodes"])
                for gres in node_to_gpu.get(node_name, "").split(",")
                if gres
            }
        )


@dataclass
class PartitionGPUBilling:
    """Represents a GPU billing found in a specific partition entry."""

    partition: Partition
    gpu_type: str
    value: float


class InconsistentGPUBillingError(Exception):
    def __init__(
        self,
        gpu_type: str,
        prev_billing: PartitionGPUBilling,
        new_billing: PartitionGPUBilling,
    ):
        super().__init__(
            f"\n"
            f"GPU billing differs.\n"
            f"GPU name: {gpu_type}\n"
            f"Previous value: {prev_billing.value}\n"
            f"From line: {prev_billing.partition.line_number}\n"
            f"{prev_billing.partition.line}\n"
            f"\n"
            f"New value: {new_billing.value}\n"
            f"From line: {new_billing.partition.line_number}\n"
            f"{new_billing.partition.line}\n"
        )
