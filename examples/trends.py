import os

import matplotlib.pyplot as plt
import pandas as pd
from tqdm import tqdm

from sarc.config import config
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
}


def get_jobs_dataframe(filename=None) -> pd.DataFrame:
    if filename and os.path.exists(filename):
        return pd.read_pickle(filename)

    df = None
    # Fetch all jobs from the clusters
    for cluster in tqdm(clusters, desc="clusters", position=0):
        dicts = []

        # Precompute the total number of jobs to display a progress bar
        # get_jobs is a generator so we don't get the total unless we pre-fetch all jobs
        # beforehand.
        total = config().mongo.database_instance.jobs.count_documents({"cluster_name": cluster})
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


df = get_jobs_dataframe("trends_demo_jobs.pkl")

# Compute the billed resource time in seconds
df["billed"] = df["elapsed_time"] * df["billing"]

# Sum billing for each cluster, each number of gpus, and converte to GPU/CPU years
stats_per_cluster = df.groupby(["cluster_name", "gres_gpu"])["billed"].sum() / (
    365.25 * 24 * 3600
)

# Unstack the cluster_name index to have a column for each cluster
stats_per_cluster = stats_per_cluster.unstack("cluster_name")
print(stats_per_cluster)

# Plot the results in a barplot, one bar per cluster, on x-index per number of gpus.
ax = stats_per_cluster.plot.bar(y=clusters, rot=0)
ax.set_xlabel("Number of GPUs per job")
ax.set_ylabel("CPU/GPU years billed")

plt.savefig("trends.png")
