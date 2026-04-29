"""Generate synthetic `sacct --json` output (RawSlurmOutput) for each cluster per tick.

Jobs follow a PENDING → RUNNING → terminal lifecycle.  Successive scrapes for
the same cluster can show the same job_id at different states so that consumers
see realistic updates (start time added on RUNNING, end time on completion, …).
"""

import random
from dataclasses import dataclass
from datetime import datetime, timezone

from tests.factory.common import (
    # factory
    Data,
    DataFactory,
    # state / array / misc
    RawArrayInfo,
    RawArrayLimits,
    RawArrayMax,
    RawArrayRunningTasks,
    RawAssociation,
    RawComment,
    # time
    RawDuration,
    # exit codes
    RawExitCode,
    # step
    RawFrequencyRange,
    RawHet,
    RawJobTime,
    # TRES
    RawJobTRES,
    RawMCS,
    RawRequired,
    RawReservation,
    RawSignal,
    # job + envelope
    RawSlurmClient,
    RawSlurmJob,
    RawSlurmMeta,
    RawSlurmMetaSlurm,
    RawSlurmOutput,
    RawSlurmPlugin,
    RawSlurmVersion,
    RawState,
    RawStep,
    RawStepCPUInfo,
    RawStepId,
    RawStepNodes,
    RawStepStats,
    RawStepStatsCPU,
    RawStepStatsEnergy,
    RawStepTask,
    RawStepTasks,
    RawStepTime,
    RawStepTRES,
    RawStepTRESStats,
    RawTRESEntry,
    RawWckey,
    SlurmNumber,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Transition weights per tick.  Only non-terminal states have rows.
_TRANSITIONS: dict[str, list[tuple[str, int]]] = {
    "__INIT__": [
        ("COMPLETED", 50),
        ("PENDING", 10),
        ("RUNNING", 10),
        ("FAILED", 5),
        ("TIMEOUT", 1),
        ("CANCELLED", 3),
        ("OUT_OF_MEMORY", 1),
    ],
    "PENDING": [("RUNNING", 80), ("PENDING", 15), ("CANCELLED", 5)],
    "RUNNING": [
        ("COMPLETED", 65),
        ("RUNNING", 20),
        ("FAILED", 7),
        ("TIMEOUT", 4),
        ("CANCELLED", 3),
        ("OUT_OF_MEMORY", 1),
    ],
}
_TERMINAL_STATES = frozenset(
    {"COMPLETED", "FAILED", "TIMEOUT", "CANCELLED", "OUT_OF_MEMORY"}
)

_PARTITIONS = {
    "mila": ["long", "main", "short-unkillable", "gpu"],
    "drac": ["gpu", "cpu", "largemem"],
}
_JOB_NAMES = [
    "train",
    "eval",
    "finetune",
    "preprocess",
    "inference",
    "test",
    "sweep",
    "bench",
    "embed",
    "generate",
]
_CPU_COUNTS = [1, 2, 4, 8, 16]
_CPU_WEIGHTS = [5, 20, 40, 25, 10]
_GPU_COUNTS = [0, 1, 2, 4]
_GPU_WEIGHTS = [30, 50, 15, 5]
_TIME_LIMITS_MIN = [60, 120, 240, 480, 720, 1440]

_SLURM_VERSION = RawSlurmVersion(major="25", minor="05", micro="2")
_NO_SIGNAL = RawSignal(id=SlurmNumber(set=False, infinite=False, number=0), name="")
_ZERO_DURATION = RawDuration(seconds=0, microseconds=0)
_UNSET = SlurmNumber(set=False, infinite=False, number=0)
_DEFAULTS_KEY = "__DEFAULTS__"


# ---------------------------------------------------------------------------
# In-flight job state
# ---------------------------------------------------------------------------


@dataclass
class _Job:
    """All mutable state for a job being tracked across ticks."""

    job_id: int
    cluster_name: str
    domain: str
    username: str
    account: str
    partition: str
    qos: str
    priority: int
    job_name: str
    constraints: str
    work_dir: str
    # resources (fixed at creation)
    n_cpu: int
    mem_mb: int
    n_gpu: int
    gpu_slurm_name: str | None
    billing_is_gpu: bool
    time_limit_min: int
    # node is "None assigned" while PENDING, set on RUNNING transition
    node: str
    # lifecycle
    state: str
    flags: list[str]  # set once on PENDING→RUNNING, stable thereafter
    submission_ts: int
    start_ts: int  # 0 while PENDING
    end_ts: int  # 0 until terminal
    elapsed: int  # 0 while PENDING; updated each tick while RUNNING


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


def _num(n: int) -> SlurmNumber:
    return SlurmNumber(set=True, infinite=False, number=n)


def _exit_code(state: str) -> RawExitCode:
    if state == "COMPLETED":
        return RawExitCode(status=["SUCCESS"], return_code=_num(0), signal=_NO_SIGNAL)
    if state == "FAILED":
        return RawExitCode(status=["FAILED"], return_code=_num(1), signal=_NO_SIGNAL)
    if state == "TIMEOUT":
        return RawExitCode(
            status=["TIMEOUT"],
            return_code=_num(0),
            signal=RawSignal(id=_num(9), name="KILL"),
        )
    if state == "OUT_OF_MEMORY":
        return RawExitCode(
            status=["FAILED"],
            return_code=_num(0),
            signal=RawSignal(id=_num(9), name="KILL"),
        )
    # CANCELLED, RUNNING, PENDING
    return RawExitCode(status=[state], return_code=_UNSET, signal=_NO_SIGNAL)


def _tres_list(
    n_cpu: int,
    mem_mb: int,
    n_nodes: int,
    n_gpu: int,
    gpu_name: str | None,
    billing_is_gpu: bool,
) -> list[RawTRESEntry]:
    entries = [
        RawTRESEntry(type="cpu", name="", id=1, count=n_cpu),
        RawTRESEntry(type="mem", name="", id=2, count=mem_mb),
        RawTRESEntry(type="node", name="", id=4, count=n_nodes),
        RawTRESEntry(
            type="billing", name="", id=5, count=n_gpu if billing_is_gpu else n_cpu
        ),
    ]
    if n_gpu > 0:
        entries.append(
            RawTRESEntry(
                type="gres",
                name=f"gpu:{gpu_name}" if gpu_name else "gpu",
                id=1001,
                count=n_gpu,
            )
        )
    return entries


def _make_step(
    job_id: int,
    step_name: str,
    state: str,
    start_ts: int,
    end_ts: int,
    elapsed: int,
    n_cpu: int,
    mem_mb: int,
    n_gpu: int,
    gpu_name: str | None,
    node: str,
) -> RawStep:
    running = state == "RUNNING"
    alloc = [
        RawTRESEntry(type="cpu", name="", id=1, count=n_cpu),
        RawTRESEntry(type="mem", name="", id=2, count=mem_mb),
        RawTRESEntry(type="node", name="", id=4, count=1),
    ]
    if n_gpu > 0 and step_name == "batch":
        alloc.append(
            RawTRESEntry(
                type="gres",
                name=f"gpu:{gpu_name}" if gpu_name else "gpu",
                id=1001,
                count=n_gpu,
            )
        )
    empty = RawStepTRESStats(max=[], min=[], average=[], total=[])
    return RawStep(
        time=RawStepTime(
            elapsed=elapsed,
            end=SlurmNumber(
                set=not running, infinite=False, number=0 if running else end_ts
            ),
            start=_num(start_ts),
            suspended=0,
            system=_ZERO_DURATION,
            total=RawDuration(seconds=elapsed, microseconds=0),
            user=RawDuration(seconds=elapsed, microseconds=0),
            limit=_UNSET,
        ),
        exit_code=_exit_code(state),
        nodes=RawStepNodes(count=1, range=node, list=[node]),
        tasks=RawStepTasks(count=1),
        pid="",
        CPU=RawStepCPUInfo(
            requested_frequency=RawFrequencyRange(
                min=SlurmNumber(set=True, infinite=False, number=0),
                max=SlurmNumber(set=True, infinite=False, number=0),
            ),
            governor="0",
        ),
        kill_request_user="",
        state=[state],
        statistics=RawStepStats(
            CPU=RawStepStatsCPU(actual_frequency=0),
            energy=RawStepStatsEnergy(consumed=_UNSET),
        ),
        step=RawStepId(
            id=f"{job_id}.{step_name}",
            name=step_name,
            stderr="",
            stdin="",
            stdout="",
            stderr_expanded="",
            stdin_expanded="",
            stdout_expanded="",
        ),
        task=RawStepTask(distribution="Unknown"),
        tres=RawStepTRES(requested=empty, consumed=empty, allocated=alloc),
    )


def _make_meta(cluster_name: str) -> RawSlurmMeta:
    return RawSlurmMeta(
        plugin=RawSlurmPlugin(
            type="",
            name="",
            data_parser="data_parser/v0.0.43",
            accounting_storage="accounting_storage/slurmdbd",
        ),
        client=RawSlurmClient(source="", user="sacct", group="sacct"),
        command=["sacct", "--json"],
        slurm=RawSlurmMetaSlurm(
            version=_SLURM_VERSION, release="25.05.2", cluster=cluster_name
        ),
    )


# ---------------------------------------------------------------------------
# Serialise _Job → RawSlurmJob
# ---------------------------------------------------------------------------


def _to_raw(j: _Job) -> RawSlurmJob:
    pending = j.state == "PENDING"
    # running = j.state == "RUNNING"
    started = not pending

    tres_entries = _tres_list(
        j.n_cpu, j.mem_mb, 1, j.n_gpu, j.gpu_slurm_name, j.billing_is_gpu
    )
    steps = (
        []
        if pending
        else [
            _make_step(
                j.job_id,
                "batch",
                j.state,
                j.start_ts,
                j.end_ts,
                j.elapsed,
                j.n_cpu,
                j.mem_mb,
                j.n_gpu,
                j.gpu_slurm_name,
                j.node,
            ),
            _make_step(
                j.job_id,
                "extern",
                j.state,
                j.start_ts,
                j.end_ts,
                j.elapsed,
                j.n_cpu,
                j.mem_mb,
                0,
                None,
                j.node,
            ),
        ]
    )
    flags = j.flags

    return RawSlurmJob(
        account=j.account,
        allocation_nodes=1,
        array=RawArrayInfo(
            job_id=j.job_id,
            limits=RawArrayLimits(
                max=RawArrayMax(running=RawArrayRunningTasks(tasks=0))
            ),
            task_id=_UNSET,
            task="",
        ),
        association=RawAssociation(
            account=j.account,
            cluster=j.cluster_name,
            partition=j.partition,
            user=j.username,
            id=0,
        ),
        block="",
        cluster=j.cluster_name,
        comment=RawComment(administrator="", job="", system=""),
        constraints=j.constraints,
        container="",
        derived_exit_code=_exit_code(j.state),
        exit_code=_exit_code(j.state),
        extra="",
        failed_node=j.node if j.state == "FAILED" else "",
        flags=flags,
        group=j.username,
        het=RawHet(job_id=0, job_offset=_UNSET),
        hold=False,
        job_id=j.job_id,
        kill_request_user=j.username if j.state == "CANCELLED" else "",
        licenses="",
        mcs=RawMCS(label=""),
        name=j.job_name,
        nodes=j.node if started else "None assigned",
        partition=j.partition,
        priority=_num(j.priority),
        qos=j.qos,
        qosreq="",
        required=RawRequired(
            CPUs=j.n_cpu, memory_per_cpu=_UNSET, memory_per_node=_num(j.mem_mb)
        ),
        reservation=RawReservation(id=0, name="", requested=""),
        restart_cnt=0,
        script="",
        segment_size=0,
        state=RawState(current=[j.state], reason="Resources" if pending else "None"),
        stderr="",
        stderr_expanded="",
        stdin="",
        stdin_expanded="",
        stdout="",
        stdout_expanded="",
        steps=steps,
        submit_line="",
        tres=RawJobTRES(
            requested=tres_entries, allocated=tres_entries if started else []
        ),
        time=RawJobTime(
            elapsed=j.elapsed,
            eligible=j.submission_ts,
            end=j.end_ts,
            start=j.start_ts,
            submission=j.submission_ts,
            suspended=0,
            system=_ZERO_DURATION,
            total=RawDuration(seconds=j.elapsed, microseconds=0),
            user=RawDuration(seconds=j.elapsed, microseconds=0),
            limit=_num(j.time_limit_min),
            planned=_num(max(0, j.start_ts - j.submission_ts) if started else 0),
        ),
        used_gres="",
        user=j.username,
        wckey=RawWckey(wckey="", flags=[]),
        working_directory=j.work_dir,
    )


# ---------------------------------------------------------------------------
# Transition
# ---------------------------------------------------------------------------


def _state_transit(rng: random.Random, state: str):
    options = _TRANSITIONS.get(state)
    if options is None:  # already terminal, should not be in-flight
        return False

    states, weights = zip(*options)
    return rng.choices(states, weights=weights)[0]


def _transition(
    rng: random.Random, j: _Job, tick_ts: int, tick_sec: int, nodes: list[str]
) -> bool:
    """Advance j to its next state in place.  Returns True if state changed."""
    # options = _TRANSITIONS.get(j.state)
    # if options is None:  # already terminal, should not be in-flight
    #     return False

    # states, weights = zip(*options)
    # new_state = rng.choices(states, weights=weights)[0]
    if not (new_state := _state_transit(rng, j.state)):
        return False

    if new_state == j.state:
        # Staying in same state; update elapsed for RUNNING jobs
        if j.state == "RUNNING":
            j.elapsed = tick_ts - j.start_ts
        return False

    if new_state == "RUNNING":
        # PENDING → RUNNING: assign start time, node, and stable flags
        j.node = rng.choice(nodes)
        j.start_ts = j.submission_ts + rng.randint(60, min(3600, tick_sec))
        j.start_ts = min(j.start_ts, tick_ts)
        j.elapsed = tick_ts - j.start_ts
        j.flags = [rng.choice(["STARTED_ON_BACKFILL", "STARTED_ON_SCHEDULE"])]

    elif new_state in _TERMINAL_STATES:
        # RUNNING → terminal: assign end time
        max_elapsed = j.time_limit_min * 60
        if new_state == "TIMEOUT":
            j.elapsed = max_elapsed
        else:
            j.elapsed = rng.randint(60, max_elapsed)
        j.end_ts = min(j.start_ts + j.elapsed, tick_ts)
        j.elapsed = j.end_ts - j.start_ts

    j.state = new_state
    return True


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def generate_sacct(self: DataFactory, data: Data) -> None:
    rng = self.get_rng("sacct")
    tick_sec = int(self.tick.total_seconds())

    # PENDING and RUNNING jobs carried forward between ticks, keyed by cluster name
    in_flight: dict[str, list[_Job]] = {name: [] for name in self.clusters}

    job_ids: dict[str, int] = {name: 1 for name in self.clusters}
    t = self.t_start

    while t < self.t_end:
        dt = datetime(t.year, t.month, t.day, tzinfo=timezone.utc)
        tick_ts = int(dt.timestamp())

        for cluster_name, cluster in self.clusters.items():
            slurm_cfg = self.slurm.get(cluster_name)
            if slurm_cfg is None:
                continue

            # Honour per-cluster start/end window
            c_start = slurm_cfg.start or self.t_start
            c_end = slurm_cfg.end or self.t_end
            if not (c_start <= t < c_end):
                continue

            domain = cluster.user_domain

            active_creds = [
                (cred.user_id, cred.relationship.username)
                for cred in data.credentials
                if cred.relationship.domain == domain
                and (cred.start is None or cred.start <= dt)
                and (cred.end is None or dt < cred.end)
            ]
            if not active_creds:
                continue

            nodes = [n for n in cluster.gpus_per_nodes if n != _DEFAULTS_KEY]
            if not nodes:
                continue

            # ---- step 1: transition in-flight jobs ----
            next_in_flight: list[_Job] = []
            scrape_jobs: list[RawSlurmJob] = []

            for job in in_flight[cluster_name]:
                _transition(rng, job, tick_ts, tick_sec, nodes)
                scrape_jobs.append(_to_raw(job))
                if job.state not in _TERMINAL_STATES:
                    next_in_flight.append(job)
                # Terminal jobs appear in this scrape then are dropped

            in_flight[cluster_name] = next_in_flight

            # ---- step 2: create new jobs (start as PENDING) ----
            n_new = max(0, round(rng.gauss(slurm_cfg.njobs_mean, slurm_cfg.njobs_std)))

            for _ in range(n_new):
                _, username = rng.choice(active_creds)
                # Resources chosen at creation; node assigned on RUNNING transition
                n_cpu = rng.choices(_CPU_COUNTS, weights=_CPU_WEIGHTS)[0]
                mem_mb = rng.choice([4096, 8192, 16384, 32768, 40960])
                n_gpu = rng.choices(_GPU_COUNTS, weights=_GPU_WEIGHTS)[0]

                # Pre-pick the node that will be assigned on start (consistent GPU type)
                node = rng.choice(nodes)
                gpu_map = cluster.gpus_per_nodes.get(
                    node
                ) or cluster.gpus_per_nodes.get(_DEFAULTS_KEY, {})
                gpu_slurm_name = next(iter(gpu_map), None)
                if gpu_slurm_name is None:
                    n_gpu = 0

                time_limit_min = rng.choice(_TIME_LIMITS_MIN)
                account = (
                    rng.choice(cluster.accounts) if cluster.accounts else cluster_name
                )

                state = _state_transit(rng, "__INIT__")
                job = _Job(
                    job_id=job_ids[cluster_name],
                    cluster_name=cluster_name,
                    domain=domain,
                    username=username,
                    account=account,
                    partition=rng.choice(_PARTITIONS.get(domain, ["gpu", "cpu"])),
                    qos=rng.choices(["normal", "high"], weights=[85, 15])[0],
                    priority=rng.randint(1000, 100_000),
                    job_name=rng.choice(_JOB_NAMES),
                    constraints="x86_64" if domain == "mila" else "",
                    work_dir=f"/home/{domain}/{username[0]}/{username}/scratch",
                    n_cpu=n_cpu,
                    mem_mb=mem_mb,
                    n_gpu=n_gpu,
                    gpu_slurm_name=gpu_slurm_name,
                    billing_is_gpu=cluster.billing_is_gpu,
                    time_limit_min=time_limit_min,
                    node=node,  # reserved; becomes real on RUNNING transition
                    state=state,
                    flags=[],
                    submission_ts=tick_ts - rng.randint(7200, min(86400, tick_sec)),
                    start_ts=0,
                    end_ts=0,
                    elapsed=0,
                )
                job_ids[cluster_name] += 1
                if state != "PENDING":
                    job.start_ts = rng.randint(job.submission_ts, tick_ts - 1)
                    job.flags = [
                        rng.choice(["STARTED_ON_BACKFILL", "STARTED_ON_SCHEDULE"])
                    ]
                    if state in _TERMINAL_STATES:
                        max_elapsed = time_limit_min * 60
                        job.elapsed = (
                            max_elapsed
                            if state == "TIMEOUT"
                            else rng.randint(60, max_elapsed)
                        )
                        job.end_ts = min(job.start_ts + job.elapsed, tick_ts)
                        job.elapsed = job.end_ts - job.start_ts
                    else:
                        job.elapsed = tick_ts - job.start_ts
                scrape_jobs.append(_to_raw(job))
                if state not in _TERMINAL_STATES:
                    in_flight[cluster_name].append(job)

            data.scrapes[f"{cluster_name}_{tick_ts}"] = RawSlurmOutput(
                jobs=scrape_jobs, meta=_make_meta(cluster_name)
            )

        t += self.tick


# ---------------------------------------------------------------------------
# Diagnostics
# ---------------------------------------------------------------------------


def sacct_table(factory: DataFactory, data: Data) -> None:
    """Print RUNNING job counts per cluster per tick.

    Re-walks the same tick×cluster loop as generate_sacct (applying the same
    skip conditions) and consumes data.scrapes in order to match each scrape
    to its (tick, cluster) slot.
    """
    cluster_names = list(factory.clusters)
    scrape_iter = iter(data.scrapes.values())

    rows: list[tuple[str, dict[str, int | None]]] = []

    t = factory.t_start
    while t < factory.t_end:
        dt = datetime(t.year, t.month, t.day, tzinfo=timezone.utc)
        row: dict[str, int | None] = {}

        for cluster_name, cluster in factory.clusters.items():
            slurm_cfg = factory.slurm.get(cluster_name)
            if slurm_cfg is None:
                row[cluster_name] = None
                continue

            c_start = slurm_cfg.start or factory.t_start
            c_end = slurm_cfg.end or factory.t_end
            if not (c_start <= t < c_end):
                row[cluster_name] = None
                continue

            domain = cluster.user_domain
            nodes = [n for n in cluster.gpus_per_nodes if n != _DEFAULTS_KEY]
            if not nodes:
                row[cluster_name] = None
                continue

            active = any(
                c.relationship.domain == domain
                and (c.start is None or c.start <= dt)
                and (c.end is None or dt < c.end)
                for c in data.credentials
            )
            if not active:
                row[cluster_name] = None
                continue

            scrape = next(scrape_iter)
            row[cluster_name] = sum(
                1 for j in scrape.jobs if "RUNNING" in j.state.current
            )

        rows.append((str(t), row))
        t += factory.tick

    # --- render ---
    date_w = 12
    col_w = max(max(len(n) for n in cluster_names) + 1, 6)
    header = f"{'':>{date_w}}" + "".join(f"{n:>{col_w}}" for n in cluster_names)
    sep = "-" * len(header)
    print(header)
    print(sep)
    for date_str, row in rows:
        line = f"{date_str:>{date_w}}"
        for name in cluster_names:
            v = row[name]
            cell = "—" if v is None else str(v)
            line += f"{cell:>{col_w}}"
        print(line)


if __name__ == "__main__":
    from pathlib import Path

    from serieux import Serieux
    from serieux.features.fromfile import IncludeFile

    from .common import Data, DataFactory
    from .gen_users import generate_users

    srx = (Serieux + IncludeFile)()
    factory = srx.deserialize(
        DataFactory, Path(__file__).parent / "factory-config.yaml"
    )
    data = Data()
    generate_users(factory, data)
    generate_sacct(factory, data)
    sacct_table(factory, data)
