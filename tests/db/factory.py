from __future__ import annotations

import copy
import datetime as dt
from collections.abc import Iterable
from datetime import datetime, timedelta

from sarc.config import UTC
from sarc.db.allocation import AllocationDB
from sarc.db.cluster import GPUBillingDB, SlurmClusterDB
from sarc.db.diskusage import DiskUsageDB, DiskUsageGroupDB, DiskUsageUserDB
from sarc.db.job import SlurmJobDB
from sarc.db.users import UserDB
from sarc.scraping.users import Credentials, MemberType
from tests.common.dateutils import MTL

elapsed_time = 60 * 60 * 12
end_time = datetime(2023, 2, 14, 23, 48, 54, tzinfo=MTL).astimezone(UTC)
base_job = {
    "CLEAR_SCHEDULING": True,
    "STARTED_ON_BACKFILL": True,
    "STARTED_ON_SCHEDULE": False,
    "STARTED_ON_SUBMIT": False,
    "account": "mila",
    "cluster_name": "raisin",  # resolved to cluster_id via DEFAULT_CLUSTER_IDS
    "array_job_id": None,
    "constraints": "x86_64&(48gb|80gb)",
    "elapsed_time": elapsed_time,
    "end_time": end_time,
    "exit_code": 0,
    "group": "petitbonhomme",
    "job_id": 2831220,
    "job_state": "CANCELLED",
    "name": "main.sh",
    "nodes": ["cn-c021"],
    "partition": "long",
    "priority": 7152,
    "qos": "normal",
    "signal": None,
    "start_time": end_time - timedelta(seconds=elapsed_time),
    "submit_time": end_time - timedelta(seconds=elapsed_time + 60),
    "submit_line": None,
    "task_id": None,
    "time_limit": 43200,
    "user": "petitbonhomme",
    "work_dir": "/network/scratch/p/petitbonhomme/experience-demente",
    "allocated_billing": 1,
    "allocated_cpu": 4,
    "allocated_gres_gpu": 1,
    "allocated_gpu_type": None,
    "allocated_mem": 49152,
    "allocated_node": 1,
    "requested_billing": 1,
    "requested_cpu": 4,
    "requested_gres_gpu": 1,
    "requested_gpu_type": None,
    "requested_mem": 49152,
    "requested_node": 1,
}


def _flatten_tres(kwargs: dict) -> dict:
    """Expand allocated={...} and requested={...} kwargs to flattened fields."""
    result = {}
    for prefix in ("allocated", "requested"):
        if prefix in kwargs:
            for k, v in kwargs.pop(prefix).items():
                result[f"{prefix}_{k}"] = v
    return result


class JobFactory:
    def __init__(
        self,
        first_submit_time: None | datetime = None,
        first_job_id: int = 1,
        job_patch: dict | None = None,
        clusters: list = None,
        users: list = None,
    ):
        self.jobs: list[SlurmJobDB] = []
        self._first_submit_time = first_submit_time or datetime(
            2023, 2, 14, tzinfo=MTL
        ).astimezone(UTC)
        self._first_job_id = first_job_id
        self.job_patch = job_patch or {}
        self.clusters = {c.name: c for c in clusters}
        self.users = {u.email.split("@")[0]: u for u in users}

    @property
    def next_job_id(self):
        return self._first_job_id + len(self.jobs)

    @property
    def next_submit_time(self):
        return timedelta(hours=len(self.jobs) * 6) + self._first_submit_time

    def format_kwargs(self, kwargs):
        kwargs.setdefault("elapsed_time", base_job["elapsed_time"])
        kwargs.setdefault("submit_time", self.next_submit_time)
        kwargs.setdefault("start_time", kwargs["submit_time"] + timedelta(seconds=60))
        kwargs.setdefault("job_state", base_job["job_state"])

        if kwargs["job_state"] in ["RUNNING", "PENDING"]:
            kwargs.setdefault("end_time", None)
        else:
            kwargs.setdefault(
                "end_time",
                kwargs["start_time"]
                + timedelta(
                    seconds=kwargs.get("elapsed_time", base_job["elapsed_time"])
                ),
            )

        # Override elapsed_time to be coherent.
        if "elapsed_time" not in kwargs and kwargs["end_time"] is not None:
            kwargs["elapsed_time"] = int(
                (kwargs["end_time"] - kwargs["start_time"]).total_seconds()
            )
        elif "elapsed_time" not in kwargs and kwargs["job_state"] == "RUNNING":
            kwargs["elapsed_time"] = (
                datetime.now(tz=UTC) - kwargs["start_time"]
            ).total_seconds()

        kwargs.setdefault("elapsed_time", base_job["elapsed_time"])

        kwargs.setdefault("job_id", self.next_job_id)

        return kwargs

    def create_job(self, **kwargs) -> SlurmJobDB:
        job = copy.deepcopy(base_job)
        kwargs.update(_flatten_tres(kwargs))
        job.update(self.format_kwargs(kwargs))

        cluster_name = job.pop("cluster_name")
        if "cluster_id" not in job:
            job["cluster_id"] = self.clusters[cluster_name].id
        user_name = job["user"]
        if "user_id" not in job:
            job["user_id"] = self.users[user_name].id

        instance = SlurmJobDB(**job)
        if self.job_patch:
            instance = instance.model_copy(update=self.job_patch)
        return instance

    def add_job(self, **kwargs):
        self.jobs.append(self.create_job(**kwargs))

    def add_job_array(
        self, task_ids, job_id: None | int = None, submit_time: None | datetime = None
    ):
        job_id = self.next_job_id
        submit_time = self.next_submit_time
        for job_array_id_offset, task_id in enumerate(task_ids):
            self.add_job(
                submit_time=submit_time,
                job_id=job_array_id_offset + job_id,
                array_job_id=job_id,
                task_id=task_id,
            )


class UserFactory:
    def __init__(self):
        self.users: list[UserDB] = []

    def _create_user(
        self,
        id,
        display_name="Test User",
        email="test@example.com",
        match_ids=None,
        member_type=(),
        accounts=(("mila", "test"),),
        supervisor=(),
        cosupervisors=(),
        github_username=(),
        google_scholar_profile=(),
        session=None,
    ) -> UserDB:
        if match_ids is None:
            match_ids = {}
        u = UserDB(id=id, display_name=display_name, email=email)
        for k, v in match_ids.items():
            u.matching_ids[k] = v
        if session is not None:
            for mtype in member_type:
                u.member_type.insert(MemberType(mtype[0]), *mtype[1:])
            for acct in accounts:
                creds = u.associated_accounts.get(acct[0], Credentials())
                creds.insert(*acct[1:])
                u.associated_accounts[acct[0]] = creds
            for sup in supervisor:
                u.supervisor.insert(*sup)
            for cosup in cosupervisors:
                u.co_supervisors.insert(*cosup)
            for uname in github_username:
                u.github_username.insert(*uname)
            for profile in google_scholar_profile:
                u.google_scholar_profile.insert(*profile)
        return u

    def add_user(self, **kwargs) -> UserDB:
        u = self._create_user(**kwargs)
        self.users.append(u)
        return u


def create_users(user_factory=None) -> Iterable[UserDB]:
    if user_factory is None:
        user_factory = UserFactory()

    prof1 = user_factory.add_user(
        id=1,
        member_type=[
            (
                "professor",
                datetime(2020, 9, 1, tzinfo=dt.UTC),
                datetime(2027, 9, 1, tzinfo=dt.UTC),
            )
        ],
        display_name="Jane Doe",
        email="jdoe@example.com",
        match_ids={"mila_ldap": "doej@mila.quebec", "mymila": "111"},
        accounts=[("mila", "jdoe", None, None), ("drac", "doej001", None, None)],
    )

    prof2 = user_factory.add_user(
        id=2,
        display_name="John Smith",
        email="jsmith@example.com",
        match_ids={"mila_ldap": "smithj@mila.quebec", "mymila": "222"},
        member_type=[
            (
                "professor",
                datetime(2022, 9, 1, tzinfo=dt.UTC),
                datetime(2026, 5, 1, tzinfo=dt.UTC),
            ),
            (
                "phd",
                datetime(2018, 9, 1, tzinfo=dt.UTC),
                datetime(2021, 5, 1, tzinfo=dt.UTC),
            ),
        ],
        accounts=[("mila", "smithj", datetime(2018, 9, 1, tzinfo=dt.UTC))],
        supervisor=[
            (
                prof1.id,
                datetime(2018, 9, 1, tzinfo=dt.UTC),
                datetime(2021, 5, 1, tzinfo=dt.UTC),
            )
        ],
        google_scholar_profile=[
            ("https://scholar.google.com/citations?user=PataTe_000AJ&hl=en",)
        ],
    )
    user_factory.add_user(
        id=3,
        github_username=[
            (
                "testuser",
                datetime(2023, 3, 3, tzinfo=dt.UTC),
                datetime(2030, 12, 30, tzinfo=dt.UTC),
            )
        ],
        google_scholar_profile=[
            (
                "https://scholar.google.com/citations?user=PataTe_111AJ&hl=en",
                datetime(2019, 10, 11, tzinfo=dt.UTC),
                datetime(2030, 12, 30, tzinfo=dt.UTC),
            )
        ],
    )
    user_factory.add_user(id=4, supervisor=[(prof2.id, None, None)])
    user_factory.add_user(
        id=5,
        match_ids={"test_match": "cinq", "test1": "aaa"},
        member_type=[
            (
                "professor",
                datetime(2022, 1, 1, tzinfo=dt.UTC),
                datetime(2023, 1, 1, tzinfo=dt.UTC),
            )
        ],
        github_username=[
            (
                "test123",
                datetime(2022, 1, 1, tzinfo=dt.UTC),
                datetime(2023, 1, 1, tzinfo=dt.UTC),
            )
        ],
        google_scholar_profile=[
            (
                "profileA",
                datetime(2022, 1, 1, tzinfo=dt.UTC),
                datetime(2023, 1, 1, tzinfo=dt.UTC),
            )
        ],
        accounts=[
            (
                "test",
                "user",
                datetime(2022, 1, 1, tzinfo=dt.UTC),
                datetime(2023, 1, 1, tzinfo=dt.UTC),
            )
        ],
        supervisor=[
            (
                prof1.id,
                datetime(2022, 1, 1, tzinfo=dt.UTC),
                datetime(2023, 1, 1, tzinfo=dt.UTC),
            )
        ],
        cosupervisors=[
            (
                {prof2.id},
                datetime(2022, 1, 1, tzinfo=dt.UTC),
                datetime(2023, 1, 1, tzinfo=dt.UTC),
            )
        ],
    )
    user_factory.add_user(id=6, match_ids={"test_match": "quack"})
    user_factory.add_user(
        id=7,
        match_ids={"test_match": "abc", "test1": "bbb", "test2": "123"},
        display_name="Othername",
        member_type=[
            (
                "staff",
                datetime(2022, 1, 1, tzinfo=dt.UTC),
                datetime(2023, 1, 1, tzinfo=dt.UTC),
            )
        ],
        github_username=[
            (
                "test_abc",
                datetime(2022, 1, 1, tzinfo=dt.UTC),
                datetime(2023, 1, 1, tzinfo=dt.UTC),
            )
        ],
        google_scholar_profile=[
            (
                "profileB",
                datetime(2022, 1, 1, tzinfo=dt.UTC),
                datetime(2023, 1, 1, tzinfo=dt.UTC),
            )
        ],
        accounts=[
            (
                "test",
                "resu",
                datetime(2022, 1, 1, tzinfo=dt.UTC),
                datetime(2023, 1, 1, tzinfo=dt.UTC),
            ),
            ("cluster", "user"),
        ],
        supervisor=[
            (
                prof2.id,
                datetime(2022, 1, 1, tzinfo=dt.UTC),
                datetime(2023, 1, 1, tzinfo=dt.UTC),
            )
        ],
        cosupervisors=[
            (
                {prof1.id},
                datetime(2022, 1, 1, tzinfo=dt.UTC),
                datetime(2023, 1, 1, tzinfo=dt.UTC),
            )
        ],
    )

    fake_users = [
        (8, "bonhomme", None),
        (9, "petitbonhomme", "aaa-001"),
        (10, "beaubonhomme", "aaa-002"),
    ]

    for id, username, drac_account in fake_users:
        accts = [
            (
                "mila",
                f"{username}_mila",
                datetime(2024, 4, 11, 0, 0, tzinfo=dt.UTC),
                None,
            )
        ]
        if drac_account:
            accts.append(
                ("drac", username, datetime(2024, 4, 11, 0, 0, tzinfo=dt.UTC), None)
            )
        u = user_factory.add_user(
            id=id,
            display_name=f"M/Ms {username[0].upper()}{username[1:]}",
            email=f"{username}@mila.quebec",
            accounts=accts,
            match_ids={"mila_ldap": f"{username}@mila.quebec"},
        )
        if drac_account:
            u.matching_ids["drac_role"] = drac_account

    return user_factory.users


def create_jobs(
    job_factory: JobFactory | None = None,
    job_patch: dict | None = None,
    *,
    clusters: list,
    users: list,
):
    if job_factory is None:
        job_factory = JobFactory(job_patch=job_patch, clusters=clusters, users=users)

    for status in [
        "CANCELLED",
        "COMPLETED",
        "FAILED",
        "NODE_FAIL",
        "PREEMPTED",
        "TIMEOUT",
        "RUNNING",
        "PENDING",
    ]:
        job_factory.add_job(job_state=status)

    job_factory.add_job_array(task_ids=[1, 10, 13])

    for nodes in [["bart"], sorted(["cn-d001", "cn-c021", "cn-c022"])]:
        job_factory.add_job(nodes=nodes)

    for cluster_name in ["raisin", "fromage", "patate"]:
        job_factory.add_job(cluster_name=cluster_name)

    # # bonhomme has no drac account, so no user_id on a drac-domain cluster.
    # job_factory.add_job(user="bonhomme", user_id=None)

    job_factory.add_job(user="petitbonhomme")

    # # Note that user `grosbonhomme` won't be added to testing database.
    # # Thus, this job belongs to a non-existent user.
    # job_factory.add_job(user="grosbonhomme", cluster_name="mila", user_id=None)

    job_factory.add_job(user="beaubonhomme")

    job_factory.add_job(job_id=1_000_000, nodes=["cn-c017"], job_state="PREEMPTED")
    job_factory.add_job(job_id=1_000_000, nodes=["cn-b099"], job_state="OUT_OF_MEMORY")

    job_factory.add_job(
        allocated={
            "billing": 2,
            "cpu": 12,
            "gres_gpu": 1,
            "gpu_type": "A100",
            "mem": 39152,
            "node": 1,
        },
        requested={"billing": 2, "cpu": 12, "gres_gpu": 1, "mem": 59152, "node": 1},
    )

    # Add a job with requested and allocated GPU to 0.
    job_factory.add_job(
        job_id=999_999_999,
        elapsed_time=elapsed_time * 1.5,
        cluster_name="mila",
        user="petitbonhomme",
        allocated={
            "billing": 14,
            "cpu": 12,
            "gres_gpu": 0,
            "gpu_type": None,
            "mem": 39152,
            "node": 1,
        },
        requested={
            "billing": 14,
            "cpu": 12,
            "gres_gpu": 0,
            "gpu_type": None,
            "mem": 59152,
            "node": 1,
        },
    )

    return job_factory.jobs


# def create_cluster_entries():
#     """Generate cluster entries to fill collection `clusters`."""

#     # Get all cluster names from scraping config test file
#     cluster_names = sorted(config("scraping").clusters.keys())

#     cluster_entries = []

#     for i, cluster_name in enumerate(cluster_names):
#         cluster_end_time = end_time - timedelta(days=i)
#         cluster_start_time = cluster_end_time - timedelta(days=1)
#         if cluster_name == "patate":
#             # Make end_time_sacct older than end_time_prometheus
#             end_time_sacct = cluster_end_time - timedelta(minutes=300)
#         else:
#             # By default, end_time_sacct is more recent than end_time_prometheus,
#             # so that prometheus metrics will be scraped up to this date
#             # for auto-interval scraping.
#             end_time_sacct = cluster_end_time + timedelta(minutes=300)
#         cluster_entries.append(
#             SlurmClusterDB(
#                 id=i,
#                 domain=cluster_name,
#                 cluster_name=cluster_name,
#                 start_date=cluster_start_time.date(),
#                 end_time_sacct=end_time_sacct,
#                 end_time_prometheus=cluster_end_time,
#                 billing_is_gpu=True if cluster_name == "mila" else False,
#             )
#         )
#     return cluster_entries


def create_gpu_billings(clusters: list[SlurmClusterDB]) -> list[GPUBillingDB]:
    cluster_by_name = {c.name: c for c in clusters}
    return [
        GPUBillingDB(
            cluster_id=cluster_by_name["patate"].id,
            since=datetime(2023, 2, 15, tzinfo=MTL).astimezone(UTC),
            gpu_to_billing={
                "patate_gpu_no_rgu_with_billing": 120,
                "patate_gpu_with_rgu_with_billing": 90,
                "A100": 200,
            },
        ),
        GPUBillingDB(
            cluster_id=cluster_by_name["patate"].id,
            since=datetime(2023, 2, 18, tzinfo=MTL).astimezone(UTC),
            gpu_to_billing={
                "patate_gpu_no_rgu_with_billing": 240,  # / 2
                "patate_gpu_with_rgu_with_billing": 180,  # x 2
                # no billing for A100 since 2023-02-18
            },
        ),
        GPUBillingDB(
            cluster_id=cluster_by_name["raisin"].id,
            since=datetime(2023, 2, 15, tzinfo=MTL).astimezone(UTC),
            gpu_to_billing={
                "raisin_gpu_no_rgu_with_billing": 150,
                "raisin_gpu_with_rgu_with_billing": 50,
                "A100": 100,
            },
        ),
    ]


def create_diskusages() -> list[DiskUsageDB]:
    diskusages = []
    for cluster_name in ["botw", "totk"]:
        for timestamp in [
            datetime(2023, 2, 14, 0, 0, 0, tzinfo=UTC),
            datetime(2021, 12, 1, 0, 0, 0, tzinfo=UTC),
        ]:
            diskusages.append(
                DiskUsageDB(
                    cluster_name=cluster_name,
                    timestamp=timestamp,
                    groups=[
                        DiskUsageGroupDB(
                            group_name="gerudo",
                            users=[
                                DiskUsageUserDB(user="urbosa", nbr_files=2, size=0),
                                DiskUsageUserDB(
                                    user="riju", nbr_files=50, size=14484777205
                                ),
                                DiskUsageUserDB(user="mipha", nbr_files=2, size=0),
                            ],
                        ),
                        DiskUsageGroupDB(
                            group_name="piaf",
                            users=[
                                DiskUsageUserDB(
                                    user="revali", nbr_files=47085, size=4509715660
                                )
                            ],
                        ),
                    ],
                )
            )
    return diskusages


def create_allocations(clusters: list[SlurmClusterDB]) -> list[AllocationDB]:
    cluster_by_name = {c.name: c for c in clusters}
    ts = datetime(year=2023, month=2, day=1)
    fromage = cluster_by_name["fromage"].id
    patate = cluster_by_name["patate"].id
    return [
        AllocationDB(
            cluster_id=fromage,
            resource_name="fromage-gpu",
            group_name="rrg-bonhomme-ad",
            timestamp=ts,
            start=datetime(year=2017, month=4, day=1),
            end=datetime(year=2018, month=4, day=1),
            gpu_year=100,
            rgu_year=400,
        ),
        AllocationDB(
            cluster_id=fromage,
            resource_name="fromage-storage",
            group_name="rrg-bonhomme-ad",
            timestamp=ts,
            start=datetime(year=2017, month=4, day=1),
            end=datetime(year=2018, month=4, day=1),
            project_size="50TB",
            project_inodes="5e6",
            nearline="15TB",
        ),
        AllocationDB(
            cluster_id=fromage,
            resource_name="fromage-gpu",
            group_name="rrg-bonhomme-ad",
            timestamp=ts,
            start=datetime(year=2018, month=4, day=1),
            end=datetime(year=2019, month=4, day=1),
            gpu_year=100,
        ),
        AllocationDB(
            cluster_id=fromage,
            resource_name="fromage-storage",
            group_name="rrg-bonhomme-ad",
            timestamp=ts,
            start=datetime(year=2018, month=4, day=1),
            end=datetime(year=2019, month=4, day=1),
            project_size="70TB",
        ),
        AllocationDB(
            cluster_id=patate,
            resource_name="patate-gpu",
            group_name="rrg-bonhomme-ad",
            timestamp=ts,
            start=datetime(year=2019, month=4, day=1),
            end=datetime(year=2020, month=4, day=1),
            gpu_year=190,
            rgu_year=190,
        ),
        AllocationDB(
            cluster_id=patate,
            resource_name="patate-storage",
            group_name="rrg-bonhomme-ad",
            timestamp=ts,
            start=datetime(year=2019, month=4, day=1),
            end=datetime(year=2020, month=4, day=1),
            project_size="90TB",
            project_inodes="5e6",
            nearline="90TB",
        ),
        AllocationDB(
            cluster_id=fromage,
            resource_name="fromage-gpu",
            group_name="rrg-bonhomme-ad",
            timestamp=ts,
            start=datetime(year=2020, month=4, day=1),
            end=datetime(year=2021, month=4, day=1),
            gpu_year=130,
            rgu_year=450,
        ),
        AllocationDB(
            cluster_id=fromage,
            resource_name="fromage-storage",
            group_name="rrg-bonhomme-ad",
            timestamp=ts,
            start=datetime(year=2020, month=4, day=1),
            end=datetime(year=2021, month=4, day=1),
            project_size="30TB",
            project_inodes="5e6",
        ),
        AllocationDB(
            cluster_id=patate,
            resource_name="patate-compute",
            group_name="rrg-bonhomme-ad",
            timestamp=ts,
            start=datetime(year=2020, month=4, day=1),
            end=datetime(year=2021, month=4, day=1),
            cpu_year=219,
        ),
        AllocationDB(
            cluster_id=patate,
            resource_name="patate-gpu",
            group_name="rrg-bonhomme-ad",
            timestamp=ts,
            start=datetime(year=2020, month=4, day=1),
            end=datetime(year=2021, month=4, day=1),
            gpu_year=200,
            rgu_year=500,
        ),
        AllocationDB(
            cluster_id=patate,
            resource_name="patate-storage",
            group_name="rrg-bonhomme-ad",
            timestamp=ts,
            start=datetime(year=2020, month=4, day=1),
            end=datetime(year=2021, month=4, day=1),
            project_size="70TB",
            project_inodes="5e6",
            nearline="80TB",
        ),
    ]
