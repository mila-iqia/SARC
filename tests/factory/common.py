import random
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

from sarc.config import ClusterConfig
from sarc.models.user import MemberType, User

# ---------------------------------------------------------------------------
# User generation
# ---------------------------------------------------------------------------


@dataclass
class UserGenerationParameters:
    # Total number of users to generate
    total: int

    # Total of full professors to start with
    full_professors: int


@dataclass
class Valid[T]:
    user_id: int
    relationship: T
    start: datetime | None
    end: datetime | None


@dataclass
class Supervision:
    supervisor_ids: list[int]


@dataclass
class Credential:
    domain: str
    username: str


# ---------------------------------------------------------------------------
# Sacct job generation
# ---------------------------------------------------------------------------


@dataclass
class SacctGenerationParameters:
    # Average number of jobs to generate per raw output
    njobs_mean: int = 10
    # Standard deviation of the number of jobs per raw output
    njobs_std: float = 5
    # Start date for job gen
    start: date | None = None
    # End date for job gen
    end: date | None = None


# ---------------------------------------------------------------------------
# Primitive: the {set, infinite, number} envelope used throughout SLURM 23+
# ---------------------------------------------------------------------------


@dataclass
class SlurmNumber:
    set: bool
    infinite: bool
    number: int


# ---------------------------------------------------------------------------
# TRES (Trackable RESource) entries
# ---------------------------------------------------------------------------


@dataclass
class RawTRESEntry:
    """A single TRES record as it appears in flat lists (job-level tres)."""

    type: str  # e.g. "cpu", "mem", "gres", "node", "billing", "energy"
    name: str  # e.g. "" or "gpu", "disk"
    id: int
    count: int
    # Present in step tres.requested.{max,min} but absent in average/total
    task: int | None = None
    node: str | None = None


@dataclass
class RawStepTRESStats:
    """Step tres.requested / tres.consumed sub-object (has max/min/average/total)."""

    max: list[RawTRESEntry]
    min: list[RawTRESEntry]
    average: list[RawTRESEntry]
    total: list[RawTRESEntry]


@dataclass
class RawStepTRES:
    requested: RawStepTRESStats
    consumed: RawStepTRESStats
    allocated: list[RawTRESEntry]


@dataclass
class RawJobTRES:
    """Flat TRES used at the job (not step) level."""

    allocated: list[RawTRESEntry]
    requested: list[RawTRESEntry]


# ---------------------------------------------------------------------------
# Time structures
# ---------------------------------------------------------------------------


@dataclass
class RawDuration:
    """CPU/wall time with second + microsecond components."""

    seconds: int
    microseconds: int


@dataclass
class RawJobTime:
    """Top-level job time block (timestamps are raw Unix ints here)."""

    elapsed: int  # seconds
    eligible: int  # Unix timestamp
    end: int  # Unix timestamp
    start: int  # Unix timestamp
    submission: int  # Unix timestamp
    suspended: int  # seconds
    system: RawDuration
    total: RawDuration
    user: RawDuration
    limit: SlurmNumber  # minutes; set=False means no limit
    planned: SlurmNumber


@dataclass
class RawStepTime:
    """Step time block (start/end are SlurmNumbers, not raw ints)."""

    elapsed: int
    end: SlurmNumber
    start: SlurmNumber
    suspended: int
    system: RawDuration
    total: RawDuration
    user: RawDuration
    limit: SlurmNumber


# ---------------------------------------------------------------------------
# Exit codes and signals
# ---------------------------------------------------------------------------


@dataclass
class RawSignal:
    id: SlurmNumber
    name: str  # e.g. "TERM", ""


@dataclass
class RawExitCode:
    status: list[str]  # e.g. ["SUCCESS"], ["SIGNALED"], ["CANCELLED"]
    return_code: SlurmNumber
    signal: RawSignal


# ---------------------------------------------------------------------------
# Job state
# ---------------------------------------------------------------------------


@dataclass
class RawState:
    current: list[str]  # e.g. ["CANCELLED"], ["COMPLETED"]
    reason: str  # e.g. "None", "UserJobLimit"


# ---------------------------------------------------------------------------
# Array jobs
# ---------------------------------------------------------------------------


@dataclass
class RawArrayRunningTasks:
    tasks: int


@dataclass
class RawArrayMax:
    running: RawArrayRunningTasks


@dataclass
class RawArrayLimits:
    max: RawArrayMax


@dataclass
class RawArrayInfo:
    job_id: int
    limits: RawArrayLimits
    task_id: (
        SlurmNumber  # .number is the actual task id; set=False when not an array job
    )
    task: str  # string representation, e.g. "" or "1"


# ---------------------------------------------------------------------------
# Association, reservation, wckey, misc small blocks
# ---------------------------------------------------------------------------


@dataclass
class RawAssociation:
    account: str
    cluster: str
    partition: str
    user: str
    id: int


@dataclass
class RawComment:
    administrator: str
    job: str
    system: str


@dataclass
class RawHet:
    job_id: int
    job_offset: SlurmNumber


@dataclass
class RawMCS:
    label: str


@dataclass
class RawReservation:
    id: int
    name: str
    requested: str


@dataclass
class RawRequired:
    CPUs: int
    memory_per_cpu: SlurmNumber
    memory_per_node: SlurmNumber


@dataclass
class RawWckey:
    wckey: str
    flags: list[str]


# ---------------------------------------------------------------------------
# Step sub-structures
# ---------------------------------------------------------------------------


@dataclass
class RawFrequencyRange:
    min: SlurmNumber
    max: SlurmNumber


@dataclass
class RawStepCPUInfo:
    requested_frequency: RawFrequencyRange
    governor: str  # stringified int, e.g. "0"


@dataclass
class RawStepNodes:
    count: int
    range: str  # hostlist expression, e.g. "cn-c039"
    list: list[str]  # expanded list


@dataclass
class RawStepTasks:
    count: int


@dataclass
class RawStepStatsCPU:
    actual_frequency: int


@dataclass
class RawStepStatsEnergy:
    consumed: SlurmNumber


@dataclass
class RawStepStats:
    CPU: RawStepStatsCPU
    energy: RawStepStatsEnergy


@dataclass
class RawStepId:
    """The step.step sub-object identifying the step."""

    id: str  # e.g. "5000000.batch", "5000000.extern"
    name: str  # e.g. "batch", "extern"
    stderr: str
    stdin: str
    stdout: str
    stderr_expanded: str
    stdin_expanded: str
    stdout_expanded: str


@dataclass
class RawStepTask:
    distribution: str  # e.g. "Unknown", "Block:cyclic"


@dataclass
class RawStep:
    time: RawStepTime
    exit_code: RawExitCode
    nodes: RawStepNodes
    tasks: RawStepTasks
    pid: str
    CPU: RawStepCPUInfo
    kill_request_user: str
    state: list[str]
    statistics: RawStepStats
    step: RawStepId
    task: RawStepTask
    tres: RawStepTRES


# ---------------------------------------------------------------------------
# Top-level job
# ---------------------------------------------------------------------------


@dataclass
class RawSlurmJob:
    account: str
    allocation_nodes: int
    array: RawArrayInfo
    association: RawAssociation
    block: str
    cluster: str
    comment: RawComment
    constraints: str  # e.g. "x86_64"
    container: str
    derived_exit_code: RawExitCode
    exit_code: RawExitCode
    extra: str
    failed_node: str
    flags: list[str]  # e.g. ["STARTED_ON_BACKFILL", "START_RECEIVED"]
    group: str
    het: RawHet
    hold: bool
    job_id: int
    kill_request_user: str
    licenses: str
    mcs: RawMCS
    name: str
    nodes: str  # hostlist expression, e.g. "cn-c039" or "None assigned"
    partition: str
    priority: SlurmNumber
    qos: str
    qosreq: str
    required: RawRequired
    reservation: RawReservation
    restart_cnt: int
    script: str
    segment_size: int
    state: RawState
    stderr: str
    stderr_expanded: str
    stdin: str
    stdin_expanded: str
    stdout: str
    stdout_expanded: str
    steps: list[RawStep]
    submit_line: str
    tres: RawJobTRES
    time: RawJobTime
    used_gres: str
    user: str
    wckey: RawWckey
    working_directory: str


# ---------------------------------------------------------------------------
# Meta / top-level output envelope
# ---------------------------------------------------------------------------


@dataclass
class RawSlurmVersion:
    major: str
    minor: str
    micro: str


@dataclass
class RawSlurmPlugin:
    type: str
    name: str
    data_parser: str  # e.g. "data_parser/v0.0.43"
    accounting_storage: str  # e.g. "accounting_storage/slurmdbd"


@dataclass
class RawSlurmClient:
    source: str
    user: str
    group: str


@dataclass
class RawSlurmMetaSlurm:
    version: RawSlurmVersion
    release: str  # e.g. "25.05.2"
    cluster: str


@dataclass
class RawSlurmMeta:
    plugin: RawSlurmPlugin
    client: RawSlurmClient
    command: list[str]  # e.g. ["sacct", "-j", "5000000"]
    slurm: RawSlurmMetaSlurm


@dataclass
class RawSlurmOutput:
    """Root object of `sacct --json` output."""

    jobs: list[RawSlurmJob]
    meta: RawSlurmMeta
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Factory types
# ---------------------------------------------------------------------------


@dataclass
class Data:
    users: list[User] = field(default_factory=list)
    memberships: list[Valid[MemberType]] = field(default_factory=list)
    supervisions: list[Valid[Supervision]] = field(default_factory=list)
    github_usernames: list[Valid[str]] = field(default_factory=list)
    google_scholar_profile: list[Valid[str]] = field(default_factory=list)
    credentials: list[Valid[Credential]] = field(default_factory=list)
    scrapes: dict[str, RawSlurmOutput] = field(default_factory=dict)


@dataclass(kw_only=True)
class DataFactory:
    seed: int
    clusters: dict[str, ClusterConfig]
    t_start: date = date(2020, 1, 1)
    t_end: date = date(2025, 1, 1)
    tick: timedelta = timedelta(days=30)
    users: UserGenerationParameters
    slurm: dict[str, SacctGenerationParameters]

    def get_rng(self, name: str) -> random.Random:
        offset = int.from_bytes(name.encode(), "little") % (2**31)
        return random.Random(self.seed + offset)
