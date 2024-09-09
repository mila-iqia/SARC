import os
from datetime import datetime, timedelta

import pandas as pd
from tqdm import tqdm

from sarc.config import MTL, config
from sarc.jobs import get_jobs

# Clusters we want to compare
clusters = ["mila", "narval", "beluga", "cedar", "graham"]

# Subset of slurm fields we need to compute the trends
include_fields = {
    "cluster_name",
    "user",
    "start_time",
    "end_time",
    "elapsed_time",
    "job_id",
    "array_job_id",
    "task_id",
    "qos",
    "partition",
}


def get_jobs_dataframe(filename, start, end) -> pd.DataFrame:
    if filename and os.path.exists(filename):
        return pd.read_pickle(filename)

    df = None
    # Fetch all jobs from the clusters
    for cluster in tqdm(clusters, desc="clusters", position=0):
        dicts = []

        # Precompute the total number of jobs to display a progress bar
        # get_jobs is a generator so we don't get the total unless we pre-fetch all jobs
        # beforehand.
        total = config().mongo.database_instance.jobs.count_documents(
            {
                "cluster_name": cluster,
                "end_time": {"$gte": start},
                "start_time": {"$lt": end},
            }
        )

        for job in tqdm(
            get_jobs(cluster=cluster, start=start, end=end),
            total=total,
            desc="jobs",
            position=1,
            leave=False,
        ):
            if job.elapsed_time <= 0:
                continue

            if job.end_time is None:
                job.end_time = datetime.now(tz=MTL)

            # For some reason start time is not reliable, often equal to submit time,
            # so we infer it based on end_time and elapsed_time.
            job.start_time = job.end_time - timedelta(seconds=job.elapsed_time)

            # Clip the job to the time range we are interested in.
            if job.start_time < start:
                job.start_time = start
            if job.end_time > end:
                job.end_time = end
            job.elapsed_time = (job.end_time - job.start_time).total_seconds()

            # We only care about jobs that actually ran.
            if job.elapsed_time <= 0:
                continue

            # Create a small dict with the fields we need
            job_dict = job.dict(include=include_fields)
            # Add the allocation fields directry to dicts instead of nested as in the original job dict.
            job_dict.update(job.allocated.dict())

            dicts.append(job_dict)

        # Replace all NaNs by 0.
        cluster_df = pd.DataFrame(dicts).fillna(0)
        df = pd.concat([df, cluster_df])

        if filename:
            df.to_pickle(filename)

    assert isinstance(df, pd.DataFrame)

    return df


start = datetime(year=2022, month=1, day=1, tzinfo=MTL)
end = datetime(year=2023, month=1, day=1, tzinfo=MTL)
df = get_jobs_dataframe(
    "total_usage_demo_jobs.pkl",
    start=start,
    end=end,
)

# Compute the billed and used resource time in seconds
df["billed"] = df["elapsed_time"] * df["billing"]
df["used"] = df["elapsed_time"] * df["gres_gpu"]

df_mila = df[df["cluster_name"] == "mila"]
df_drac = df[df["cluster_name"] != "mila"]

print("Number of jobs:")
print("Mila-cluster", df_mila.shape[0])
print("DRAC clusters", df_drac.shape[0])

print("GPU hours:")
print("Mila-cluster", df_mila["used"].sum() / (3600))
print("DRAC clusters", df_drac["used"].sum() / (3600))


def compute_gpu_hours_per_duration(df):
    categories = {
        "< 1hour": (0, 3600),
        "1-24 hours": (3600, 24 * 3600),
        "1-28 days": (24 * 3600, 28 * 24 * 3600),
        ">= 28 days": (28 * 24 * 3600, None),
    }
    for key, (min_time, max_time) in categories.items():
        condition = df["elapsed_time"] >= min_time
        if max_time is not None:
            condition *= df["elapsed_time"] < max_time
        df[key] = condition.astype(bool) * df["used"]

    return df[list(categories.keys())].sum() / df["used"].sum()


print("GPU hours per job duration")
print("Mila-cluster:")
print(compute_gpu_hours_per_duration(df_mila))
print("DRAC clusters:")
print(compute_gpu_hours_per_duration(df_drac))


def compute_jobs_per_gpu_hours(df):
    categories = {
        "< 1 GPUhour": (0, 3600),
        "1-24 GPUhours": (3600, 24 * 3600),
        "1-28 GPUdays": (24 * 3600, 28 * 24 * 3600),
        ">= 28 GPUdays": (28 * 24 * 3600, None),
    }
    for key, (min_time, max_time) in categories.items():
        condition = df["used"] >= min_time
        if max_time is not None:
            condition *= df["used"] < max_time
        df[key] = condition.astype(bool) * df["used"]

    return df[list(categories.keys())].sum() / df["used"].sum()


print("Binned GPU hours")
print("Mila-cluster:")
print(compute_jobs_per_gpu_hours(df_mila))
print("DRAC clusters:")
print(compute_jobs_per_gpu_hours(df_drac))


def compute_gpu_hours_per_gpu_count(df):
    categories = {
        "1 GPU": (1, 2),
        "2-4 GPUs": (2, 5),
        "5-8 GPUs": (5, 9),
        "9-32 GPUs": (9, 33),
        ">= 33 PUdays": (33, None),
    }
    for key, (min_time, max_time) in categories.items():
        condition = df["gres_gpu"] >= min_time
        if max_time is not None:
            condition *= df["gres_gpu"] < max_time
        df[key] = condition.astype(bool) * df["used"]

    return df[list(categories.keys())].sum() / df["used"].sum()


print("GPU hours per gpu job count")
print("Mila-cluster:")
print(compute_gpu_hours_per_gpu_count(df_mila))
print("DRAC clusters:")
print(compute_gpu_hours_per_gpu_count(df_drac))
