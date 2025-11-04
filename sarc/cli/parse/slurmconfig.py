from __future__ import annotations

import glob
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Iterator, IO, cast

from hostlist import expand_hostlist
from pydantic import BaseModel
from simple_parsing import field

from sarc.cli.fetch.slurmconfig import SlurmConfigDownloader
from sarc.client.gpumetrics import _gpu_billing_collection
from sarc.config import config, ClusterConfig, UTC, TZLOCAL
from sarc.core.models.validators import datetime_utc
from sarc.jobs.node_gpu_mapping import _node_gpu_mapping_collection

logger = logging.getLogger(__name__)


@dataclass
class ParseSlurmConfig:
    cluster_name: str = field(alias=["-c"])
    threshold: float = field(
        alias=["-t"],
        required=False,
        default=0.1,
        help=(
            "Maximum allowed difference (in %%) between two billings for a GPU in different partitions. "
            "Default is 0.1 (%%)."
        ),
    )

    def execute(self) -> int:
        cluster_config = config("scraping").clusters[self.cluster_name]

        now = datetime.now(tz=TZLOCAL).astimezone(UTC)
        placeholder = SlurmConfigDownloader(cluster=cluster_config, date=now)
        slurm_conf_dir = placeholder.get_slurm_config.cache_dir
        if slurm_conf_dir is None or not slurm_conf_dir.is_dir():
            logger.error("No cache folder available")
            return -1

        prefix = f"slurm.{self.cluster_name}."
        suffix = ".conf"
        regex_day = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
        date_and_basename: list[tuple[datetime_utc, str]] = []
        for basename in glob.glob(
            f"slurm.{self.cluster_name}.*.conf", root_dir=slurm_conf_dir
        ):
            assert basename.startswith(prefix)
            assert basename.endswith(suffix)
            date_string = basename[len(prefix) : -len(suffix)]
            if regex_day.match(date_string):
                cache_date = (
                    datetime.strptime(date_string, "%Y-%m-%d")
                    .replace(tzinfo=TZLOCAL)
                    .astimezone(UTC)
                )
            else:
                cache_date = datetime.fromisoformat(date_string)
            date_and_basename.append((cache_date, basename))

        if date_and_basename:
            if cluster_config.billing_is_gpu:
                logger.warning(
                    f"GPU billing won't be parsed on cluster `{cluster_config.name}`, "
                    "since billing is directly expressed as number of GPUs on this cluster."
                )
            for cache_date, basename in sorted(date_and_basename):
                logger.info(f"Parsing {basename}, at {cache_date}")
                parser = SlurmConfigParser(
                    cluster=cluster_config, threshold=self.threshold
                )
                with open(
                    slurm_conf_dir / basename, mode="r", encoding="utf-8"
                ) as file:
                    slurm_conf = parser.load(file)
                if slurm_conf.gpu_to_billing is not None:
                    _gpu_billing_collection().save_gpu_billing(
                        self.cluster_name, cache_date, slurm_conf.gpu_to_billing
                    )
                _node_gpu_mapping_collection().save_node_gpu_mapping(
                    self.cluster_name, cache_date, slurm_conf.node_to_gpus
                )
        return 0


class SlurmConfigParser(BaseModel):
    cluster: ClusterConfig
    threshold: float

    @classmethod
    def _file_lines(cls, file) -> Iterator[tuple[int, str]]:
        """
        Iterate over lines from a file, and merge lines split with "\".
        """
        current_line_number = None
        current_line = ""

        for line_number, line in enumerate(file):
            if current_line_number is None:
                current_line_number = line_number

            line = line.strip()
            if line.endswith("\\") and not line.startswith("#"):
                current_line += line[:-1] + " "
            else:
                current_line += line
                yield current_line_number, current_line
                current_line_number = None
                current_line = ""

        assert current_line_number is None
        assert not current_line

    def load(self, file: IO[str]) -> SlurmConfig:
        """
        Parse cached slurm conf file and return a SlurmConfig object
        containing node_to_gpus and gpu_to_billing.
        """
        partitions: list[Partition] = []
        node_to_gpus: dict[str, list[str]] = {}

        # Parse lines: extract partitions and node_to_gpus
        for line_number, line in self._file_lines(file):
            line = line.strip()
            if line.startswith("PartitionName="):
                partitions.append(
                    Partition(
                        cluster_name=self.cluster.name,  # type: ignore[arg-type]
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
                gres = nodes_config.get("Gres")
                if gres:
                    # A node may have many GPUs, e.g. MIG GPUs
                    # Example on narval (2023-11-28):
                    # NodeName=ng20304 ... Gres=gpu:a100_1g.5gb:8,gpu:a100_2g.10gb:4,gpu:a100_3g.20gb:4
                    gpu_types = gres.split(",")
                    node_to_gpus.update(
                        {
                            node_name: gpu_types
                            for node_name in expand_hostlist(nodes_config["NodeName"])
                        }
                    )

        # Parse partitions: extract gpu_to_billing
        # only if billing is not GPU count on this cluster.
        gpu_to_billing = (
            None
            if self.cluster.billing_is_gpu
            else self._parse_gpu_to_billing(partitions, node_to_gpus)
        )

        # Return parsed data
        return SlurmConfig(node_to_gpus=node_to_gpus, gpu_to_billing=gpu_to_billing)

    def _parse_gpu_to_billing(
        self, partitions: list[Partition], node_to_gpus: dict[str, list[str]]
    ) -> dict[str, float]:
        # Mapping of GPU to partition billing.
        # Allow to check that inferred billing for a GPU is the same across partitions.
        # If not, an error will be raised with additional info about involved partitions.
        gpu_to_partition_billings: dict[str, list[PartitionGPUBilling]] = {}

        for partition in partitions:
            # Get billings from this partition
            parsed_partition = partition.parse(node_to_gpus)
            local_gpu_to_billing = parsed_partition.get_harmonized_gpu_to_billing(
                self.cluster
            )
            # Merge local GPU billings into global GPU billings
            for gpu_type, value in local_gpu_to_billing.items():
                new_billing = PartitionGPUBilling(
                    gpu_type=gpu_type, value=value, partition=partition
                )
                if gpu_type not in gpu_to_partition_billings:
                    gpu_to_partition_billings[gpu_type] = [new_billing]
                else:
                    ref_billing = gpu_to_partition_billings[gpu_type][0]
                    ref_value = ref_billing.value
                    if abs(ref_value - value) / ref_value > self.threshold / 100:
                        raise InconsistentGPUBillingError(
                            gpu_type, ref_billing, new_billing, self.threshold
                        )
                    gpu_to_partition_billings[gpu_type].append(new_billing)

        # Infer billing for each GPU across partitions
        gpu_to_billing: dict[str, float] = {}
        for gpu_type, partition_billings in gpu_to_partition_billings.items():
            average_value = sum(pb.value for pb in partition_billings) / len(
                partition_billings
            )
            gpu_to_billing[gpu_type] = average_value

        return gpu_to_billing


@dataclass
class SlurmConfig:
    """Parsed data from slurm config file"""

    node_to_gpus: dict[str, list[str]]
    gpu_to_billing: dict[str, float] | None


@dataclass
class Partition:
    """Partition entry in slurm config file"""

    cluster_name: str
    line_number: int
    line: str
    info: dict[str, str]

    @property
    def nodes(self) -> str:
        """Return hostlist of partition nodes"""
        return self.info.get("Nodes", "")

    def message(self, msg: str) -> str:
        """For logging: prepend given message with cluster name and partition name"""
        return f"[{self.cluster_name}][{self.info['PartitionName']}] {msg}"

    def parse(self, node_to_gpus: dict[str, list[str]]) -> ParsedPartition:
        """Parse partition's gpu => nodes, gpu => billing, and default billing"""

        # Map each partition GPU to belonging nodes
        gpu_to_nodes: dict[str, list[str]] = {}
        for node_name in expand_hostlist(self.nodes):
            for gpu_type in node_to_gpus.get(node_name, ()):
                # Parse `gpu:<real gpu name>:<count>` if necessary
                if gpu_type.startswith("gpu:") and gpu_type.count(":") == 2:
                    _, real_gpu_type, _ = gpu_type.split(":")
                    gpu_type = real_gpu_type
                gpu_to_nodes.setdefault(gpu_type, []).append(node_name)

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

        # Parse partition GPU billings
        default_billing = None
        gpu_to_billing = {}
        for key, val in gpu_weights.items():
            value = float(val)
            if key == "GRES/gpu":
                if len(gpu_weights) == 1:
                    # We only have `GRES/gpu=<value>`
                    # Save it as default billing for all partition GPUs
                    default_billing = value
                else:
                    # We have both `GRES/gpu=<value>` and at least one `GRES/gpu:a_gpu=a_value`.
                    # Ambiguous case, cannot map `GRES/gpu=<value>` to a specific GPU.
                    logger.debug(
                        self.message(
                            f"Ignored ambiguous GPU billing (cannot match to a specific GPU): `{key}={value}` "
                            f"| nodes GPUs: {', '.join(sorted(gpu_to_nodes.keys()))} "
                            f"| TRESBillingWeights: {self.info['TRESBillingWeights']}"
                        )
                    )
            else:
                # We have `GRES/gpu:a_gpu=a_value`.
                # We can parse.
                _, gpu_type = key.split(":", maxsplit=1)
                gpu_to_billing[gpu_type] = value

        return ParsedPartition(
            partition=self,
            gpu_to_nodes=gpu_to_nodes,
            gpu_to_billing=gpu_to_billing,
            default_billing=default_billing,
        )


@dataclass
class ParsedPartition:
    partition: Partition
    gpu_to_nodes: dict[str, list[str]]
    gpu_to_billing: dict[str, float]
    default_billing: float | None

    def get_harmonized_gpu_to_billing(self, cluster: ClusterConfig) -> dict[str, float]:
        """
        Convert GPU names read from slurm conf file into harmonized GPU names.

        Return harmonized GPU => billing mapping.
        """

        gpu_to_billing = self.gpu_to_billing.copy()
        gpus_not_billed = [
            gpu for gpu in self.gpu_to_nodes if gpu not in gpu_to_billing
        ]
        # If default billing is available,
        # set it as billing for all GPUs not yet billed in this partition.
        if self.default_billing is not None:
            for gpu_type in gpus_not_billed:
                gpu_to_billing[gpu_type] = self.default_billing

        # Build harmonized GPU => billing mapping.
        harmonized_gpu_to_billing = {}
        for gpu, billing in gpu_to_billing.items():
            if gpu in self.gpu_to_nodes:
                harmonized_gpu_names_raw = {
                    cluster.harmonize_gpu(node_name, gpu)
                    for node_name in self.gpu_to_nodes[gpu]
                }
                harmonized_gpu_names_raw.discard(None)
                harmonized_gpu_names = cast(set[str], harmonized_gpu_names_raw)
                if not harmonized_gpu_names:
                    logger.warning(
                        self.partition.message(
                            f"Cannot harmonize: {gpu} (keep this name as-is) : {self.partition.nodes}"
                        )
                    )
                    harmonized_gpu_to_billing[gpu] = billing
                else:
                    if len(harmonized_gpu_names) != 1:
                        # We may find many harmonized names for a same GPU name.
                        # Example on graham (2024-04-03), partition gpubase_bynode_b1:
                        # v100 => {'V100-SXM2-32GB', 'V100-PCIe-16GB'}
                        # Let's just associate billing to all harmonized names
                        logger.debug(
                            self.partition.message(
                                f"harmonize to multiple names: {gpu} => {harmonized_gpu_names} : {self.partition.nodes}"
                            )
                        )
                    for name in sorted(harmonized_gpu_names):
                        assert name not in harmonized_gpu_to_billing, (
                            name,
                            billing,
                            harmonized_gpu_to_billing,
                        )
                        harmonized_gpu_to_billing[name] = billing
            else:
                logger.debug(
                    self.partition.message(
                        f"GPU not in partition nodes: {gpu} (billing: {billing})"
                    )
                )
                # Try to harmonize GPU name anyway.
                # Passing None as nodename, harmonization will look for __DEFAULTS__
                # in `gpu_per_nodes` field of cluster config.
                h_name = cluster.harmonize_gpu(None, gpu)
                if h_name:
                    assert h_name not in harmonized_gpu_to_billing, (
                        h_name,
                        billing,
                        harmonized_gpu_to_billing,
                    )
                    harmonized_gpu_to_billing[h_name] = billing
                else:
                    logger.warning(
                        self.partition.message(
                            f"Cannot harmonize: {gpu} (keep this name as-is) : {self.partition.nodes}"
                        )
                    )
                    harmonized_gpu_to_billing[gpu] = billing
        return harmonized_gpu_to_billing


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
        threshold: float,
    ):
        super().__init__(
            f"\n"
            f"GPU billing differs (threshold {threshold} %).\n"
            f"GPU name: {gpu_type}\n"
            f"Previous value: {prev_billing.value}\n"
            f"From line: {prev_billing.partition.line_number}\n"
            f"{prev_billing.partition.line}\n"
            f"\n"
            f"New value: {new_billing.value}\n"
            f"From line: {new_billing.partition.line_number}\n"
            f"{new_billing.partition.line}\n"
        )
