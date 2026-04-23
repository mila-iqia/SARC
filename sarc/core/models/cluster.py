from pydantic import BaseModel

from sarc.core.models.validators import datetime_utc


class GPUBilling(BaseModel):
    """Holds data for a GPU Billing."""

    cluster_name: str
    since: datetime_utc
    gpu_to_billing: dict[str, float]


class NodeGPUMapping(BaseModel):
    """Holds data for a mapping <node name> -> <GPU type>."""

    cluster_name: str
    since: datetime_utc
    node_to_gpu: dict[str, list[str]]

    def __lt__(self, other):
        return self.since < other.since


class SlurmCluster(BaseModel):
    """Hold data for a Slurm cluster."""

    cluster_name: str
    start_date: str | None = None
    end_time_sacct: str | None = None
    end_time_prometheus: str | None = None
    billing_is_gpu: bool = False
