from dataclasses import dataclass, field
from datetime import datetime

from sqlmodel import col, func, select

from sarc.config import config
from sarc.db.job_series import JobSeriesDB

_TOP_JOBS_N = 5


@dataclass
class ClusterBreakdown:
    cluster: str
    gpu_hours: float
    wasted: float
    overbilled: float
    requested: float


@dataclass
class UnderuserJob:
    job_id: int
    cluster: str
    submit_time: datetime
    gpu_hours_unused: float
    # None when no gpu_utilization stat was recorded for this job
    gpu_utilization: float | None


@dataclass
class UnderuserRow:
    email: str
    display_name: str
    user_id: int
    # Total GPU-hours requested (denominator of waste_ratio; used for activity floor).
    # The floor is based on *requested* GPU-hours, not consumed, so that users who
    # request a lot but utilise very little are still captured by the alert.
    gpu_hours: float
    wasted: float
    overbilled: float
    requested: float
    waste_ratio: float
    # Average GPU utilisation fraction over the window.
    # Computed as 1 - wasted/requested using COALESCE-to-0 aggregates, so jobs
    # without a gpu_utilization stat are treated as 100% utilised (conservative).
    avg_utilization: float
    # GPU-hours unused due to low utilisation (= wasted; included as a human label).
    gpu_hours_unused: float
    by_cluster: list[ClusterBreakdown] = field(default_factory=list)
    # Top-5 GPU jobs by GPU-hours unused; sorted descending.
    top_jobs: list[UnderuserJob] = field(default_factory=list)


def get_underusers(
    start: datetime,
    end: datetime,
    *,
    min_ratio: float,
    min_gpu_hours: float,
    resource: str = "gpu",
) -> list[UnderuserRow]:
    if resource != "gpu":
        raise ValueError(f"Unsupported resource: {resource!r}")

    with config().db.session() as session:
        agg_rows = session.exec(
            select(
                JobSeriesDB.sarc_user_id,
                JobSeriesDB.email,
                JobSeriesDB.display_name,
                JobSeriesDB.cluster_name,
                func.coalesce(func.sum(JobSeriesDB.gpu_cost), 0).label("sum_requested"),
                func.coalesce(func.sum(JobSeriesDB.gpu_waste), 0).label("sum_wasted"),
                func.coalesce(func.sum(JobSeriesDB.gpu_overbilling_cost), 0).label(
                    "sum_overbilled"
                ),
            )
            .where(
                col(JobSeriesDB.submit_time) >= start,
                col(JobSeriesDB.submit_time) < end,
                col(JobSeriesDB.requested_gres_gpu) > 0,
            )
            .group_by(
                JobSeriesDB.sarc_user_id,
                JobSeriesDB.email,
                JobSeriesDB.display_name,
                JobSeriesDB.cluster_name,
            )
        ).all()

        # Build per-user aggregates and apply thresholds to find underusers.
        user_data: dict[int, dict] = {}
        for row in agg_rows:
            uid = row.sarc_user_id
            if uid not in user_data:
                user_data[uid] = {
                    "email": row.email,
                    "display_name": row.display_name,
                    "clusters": [],
                }
            requested_h = (row.sum_requested or 0.0) / 3600.0
            wasted_h = (row.sum_wasted or 0.0) / 3600.0
            overbilled_h = (row.sum_overbilled or 0.0) / 3600.0
            user_data[uid]["clusters"].append(
                ClusterBreakdown(
                    cluster=row.cluster_name or "unknown",
                    gpu_hours=requested_h,
                    wasted=wasted_h,
                    overbilled=overbilled_h,
                    requested=requested_h,
                )
            )

        underuser_ids: list[int] = []
        for uid, u in user_data.items():
            clusters = u["clusters"]
            total_requested = sum(c.requested for c in clusters)
            total_wasted = sum(c.wasted for c in clusters)
            total_overbilled = sum(c.overbilled for c in clusters)
            u["total_requested"] = total_requested
            u["total_wasted"] = total_wasted
            u["total_overbilled"] = total_overbilled
            if total_requested <= 0:
                continue
            waste_ratio = (total_wasted + total_overbilled) / total_requested
            u["waste_ratio"] = waste_ratio
            if waste_ratio >= min_ratio and total_requested >= min_gpu_hours:
                underuser_ids.append(uid)

        # Fetch per-job data for underusers to build the top-5 worst jobs list.
        jobs_by_user: dict[int, list[UnderuserJob]] = {uid: [] for uid in underuser_ids}
        if underuser_ids:
            job_rows = session.exec(
                select(
                    JobSeriesDB.job_db_id,
                    JobSeriesDB.sarc_user_id,
                    JobSeriesDB.cluster_name,
                    JobSeriesDB.submit_time,
                    JobSeriesDB.gpu_waste,
                    JobSeriesDB.gpu_cost,
                )
                .where(
                    col(JobSeriesDB.submit_time) >= start,
                    col(JobSeriesDB.submit_time) < end,
                    col(JobSeriesDB.requested_gres_gpu) > 0,
                    col(JobSeriesDB.sarc_user_id).in_(underuser_ids),
                )
            ).all()

            for jr in job_rows:
                gpu_waste_h = (jr.gpu_waste or 0.0) / 3600.0
                gpu_cost_h = (jr.gpu_cost or 0.0) / 3600.0
                if jr.gpu_waste is not None and gpu_cost_h > 0:
                    util = 1.0 - jr.gpu_waste / jr.gpu_cost
                else:
                    util = None
                jobs_by_user[jr.sarc_user_id].append(
                    UnderuserJob(
                        job_id=jr.job_db_id,
                        cluster=jr.cluster_name or "unknown",
                        submit_time=jr.submit_time,
                        gpu_hours_unused=gpu_waste_h,
                        gpu_utilization=util,
                    )
                )

    result = []
    for uid in underuser_ids:
        u = user_data[uid]
        clusters = u["clusters"]
        total_requested = u["total_requested"]
        total_wasted = u["total_wasted"]
        total_overbilled = u["total_overbilled"]
        waste_ratio = u["waste_ratio"]

        avg_utilization = 1.0 - total_wasted / total_requested if total_requested > 0 else 1.0

        by_cluster = sorted(
            clusters,
            key=lambda c: c.wasted + c.overbilled,
            reverse=True,
        )

        top_jobs = sorted(
            jobs_by_user[uid],
            key=lambda j: j.gpu_hours_unused,
            reverse=True,
        )[:_TOP_JOBS_N]

        result.append(
            UnderuserRow(
                email=u["email"],
                display_name=u["display_name"],
                user_id=uid,
                gpu_hours=total_requested,
                wasted=total_wasted,
                overbilled=total_overbilled,
                requested=total_requested,
                waste_ratio=waste_ratio,
                avg_utilization=avg_utilization,
                gpu_hours_unused=total_wasted,
                by_cluster=by_cluster,
                top_jobs=top_jobs,
            )
        )

    return result
