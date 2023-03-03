import os
from datetime import date

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from tqdm import tqdm

from sarc.allocations import get_allocation_summaries
from sarc.config import config
from sarc.jobs import get_jobs

# Clusters we want to compare
clusters = ["narval", "beluga", "cedar", "graham"]


def get_jobs_dataframe(clusters, filename=None) -> pd.DataFrame:
    if filename and os.path.exists(filename):
        return pd.read_pickle(filename)

    # Subset of slurm fields we need to compute the trends
    include_fields = {
        "cluster_name",
        "user",
        "start_time",
        "end_time",
        "elapsed_time",
        "job_id",
    }

    df = None
    # Fetch all jobs from the clusters
    for cluster in tqdm(clusters, desc="clusters", position=0):
        dicts = []

        # Precompute the total number of jobs to display a progress bar
        # get_jobs is a generator so we don't get the total unless we pre-fetch all jobs
        # beforehand.
        total = config().mongo.get_database().jobs.count_documents({"cluster_name": cluster})
        for job in tqdm(
            get_jobs(cluster=cluster, start="2022-04-01"),
            total=total,
            desc="jobs",
            position=1,
            leave=False,
        ):
            # Create a small dict with the fields we need
            job_dict = job.dict(include=include_fields)
            # Add the allocation fields directry to dicts instead of nested as in the original job dict.
            job_dict.update(job.allocated.dict())
            # gres_gpu may be None or be a float.
            job_dict["gres_gpu"] = (
                int(job_dict["gres_gpu"]) if job_dict["gres_gpu"] else 0
            )

            dicts.append(job_dict)

        # Replace all NaNs by 0.
        cluster_df = pd.DataFrame(dicts).fillna(0)
        df = pd.concat([df, cluster_df])

        if filename:
            df.to_pickle(filename)

    assert isinstance(df, pd.DataFrame)

    return df


df = get_jobs_dataframe(clusters, "allocations_demo_jobs.pkl")

# Compute the billed resource time in seconds
df["billed"] = df["elapsed_time"] * df["billing"]

cost_per_day = (
    df.groupby(["cluster_name", "start_time"])["billed"].sum().fillna(0).to_xarray()
)

cost_per_month = cost_per_day.resample(start_time="1M").sum()

# Fetching allocations to compare with the usage.
allocations = get_allocation_summaries(
    cluster_name=clusters,
    start=date(year=2022, month=4, day=1),
)

# Select subset of allocation fields.
allocations = allocations[
    [
        "cluster_name",
        "start",
        "end",
        "resources.compute.gpu_year",
        "resources.compute.cpu_year",
    ]
]


# Concat all dates so that we can plot allocations as horizontal lines.
allocations = pd.concat(
    [
        allocations.drop(columns=["end"]),
        allocations.drop(columns=["start"]).rename(columns={"end": "start"}),
    ]
)

# Unstack the allocations so that we can index cluster data as columns
unstacked_gpu_allocations = (
    allocations[["start", "cluster_name", "resources.compute.gpu_year"]]
    .set_index(["start", "cluster_name"])["resources.compute.gpu_year"]
    .unstack("cluster_name")
)
print(unstacked_gpu_allocations)


def plot_allocations(jobs_analysis, allocations, filename, y_label):
    _, ax = plt.subplots()

    # Plot allocations as horizontal dashed lines.
    dashed_lines = allocations.plot(y=clusters, ax=ax, style="--")
    # Save labels to modify later.
    labels = [line.get_label() for line in dashed_lines.lines]

    # Reuse same colors and labels as in the previous plot.
    jobs_analysis.to_dataframe().fillna(0)["billed"].unstack("cluster_name").plot(
        y=clusters,
        ax=ax,
        color={line._label: line.get_color() for line in dashed_lines.lines},
    )

    # Set legend, adding `-allocation` to the dashed line labels.
    ax.legend([label + "-allocation" for label in labels] + labels, loc="upper left")

    ax.set_xlabel("Time")
    ax.set_ylabel(y_label)

    plt.savefig(filename)


# Computing what would be the total usage over the year if the usage
# for a given month was constant over the year.
cost_per_month_projected = cost_per_month * 12 / (365.25 * 24 * 3600)

print(cost_per_month_projected)

plot_allocations(
    cost_per_month_projected,
    unstacked_gpu_allocations,
    filename="allocations_per_month_test.png",
    y_label="Projected CPU/GPU years billed",
)

# Computing the cummulative usage over the year.
cummulative_usage = (cost_per_month / (365.25 * 24 * 3600)).cumsum("start_time")

plot_allocations(
    cummulative_usage,
    unstacked_gpu_allocations,
    filename="allocations_cummulative.png",
    y_label="Cummulative CPU/GPU years billed",
)
