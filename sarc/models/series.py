from dataclasses import dataclass
from datetime import datetime

from .job import SlurmState
from .user import MemberType


@dataclass(kw_only=True)
class JobSeries:
    job_db_id: int

    # job identification
    cluster_id: int
    account: str
    job_id: int
    array_job_id: int | None
    task_id: int | None
    name: str
    cluster_user: str
    group: str

    # status
    job_state: SlurmState
    exit_code: int | None
    signal: int | None

    # allocation information
    partition: str
    nodes: list[str]

    work_dir: str
    submit_line: str | None

    # Miscellaneous
    constraints: str | None
    priority: int | None
    qos: str | None

    # Flags
    CLEAR_SCHEDULING: bool
    STARTED_ON_SUBMIT: bool
    STARTED_ON_SCHEDULE: bool
    STARTED_ON_BACKFILL: bool

    # temporal fields
    time_limit: int | None
    submit_time: datetime
    start_time: datetime | None
    end_time: datetime | None
    elapsed_time: float

    # tres
    requested_cpu: int | None
    requested_mem: int | None
    requested_node: int | None
    requested_billing: int | None
    requested_gres_gpu: int | None
    requested_gpu_type: str | None

    allocated_cpu: int | None
    allocated_mem: int | None
    allocated_node: int | None
    allocated_billing: int | None
    allocated_gres_gpu: int | None
    allocated_gpu_type: str | None
    harmonized_gpu_type: str | None

    # User ID
    sarc_user_id: int

    # Cost / waste (computed by the JobSeriesDB view)
    cpu_cost: float | None
    cpu_waste: float | None
    cpu_equivalent_cost: float | None
    cpu_equivalent_waste: float | None
    cpu_overbilling_cost: float | None
    gpu_cost: float | None
    gpu_waste: float | None
    gpu_equivalent_cost: float | None
    gpu_equivalent_waste: float | None
    gpu_overbilling_cost: float | None

    # Optional extra fields
    cluster_name: str | None = None
    statistics: dict[str, dict[str, float]] | None = None
    gpu_type_rgu: float | None = None
    gpu_type_rgu_drac: float | None = None
    rgu: float | None = None
    rgu_drac: float | None = None
    physical_rgu: float | None = None
    physical_rgu_drac: float | None = None
    display_name: str | None = None
    email: str | None = None
    member_type: MemberType | None = None
    supervisors: list[int] | None = None
