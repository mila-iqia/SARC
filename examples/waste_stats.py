import os
from datetime import datetime, timedelta

import pandas as pd
from tqdm import tqdm

from sarc.config import ScraperConfig, _config_class, config
from sarc.jobs import get_jobs


def load_job_series(filename=None) -> pd.DataFrame:
    if filename and os.path.exists(filename):
        return pd.read_pickle(filename)

    cluster = "mila"
    total = config().mongo.database_instance.jobs.count_documents(
        {
            "cluster_name": cluster,
            "end_time": {"$gte": datetime(year=2023, month=2, day=10)},
        }
    )

    df = None

    # Fetch all jobs from the clusters
    for job in tqdm(get_jobs(cluster=cluster, start="2023-02-10"), total=total):
        if job.duration < timedelta(seconds=60):
            continue

        job_series = job.series(
            metric="slurm_job_utilization_gpu",
            measure="avg_over_time",
        )
        if job_series is not None:
            # TODO: Why is it possible to have billing smaller than gres_gpu???
            billing = job.allocated.billing or 0
            gres_gpu = job.allocated.gres_gpu or 0
            job_series["allocated"] = max(billing, gres_gpu)
            job_series["requested"] = gres_gpu
            job_series["duration"] = job.duration
            df = pd.concat([df, job_series], axis=0)

        if df is not None:
            df.to_pickle(filename)

    assert isinstance(df, pd.DataFrame)

    return df


# to access series, you need prometheus access rights. This is doable only with `SARC_MODE=scraping` for the moment
# check SARC_MODE env variable
config_class = _config_class(os.getenv("SARC_MODE", "none"))
if config_class is not ScraperConfig:
    print("SARC_MODE=scraping is required to access job series (prometheus))")
    exit(0)

filename = "mila_job_series4.pkl"
df = load_job_series(filename)

# Group jobs by user
grouped_by_user = df.groupby(["user"])

# Compute the total amount of compute time used, wasted and overbilled by user.
df["used"] = df["value"] / 100.0 * df["duration"]
df["wasted"] = (1 - df["value"] / 100.0) * df["duration"]
df["overbilled"] = (df["allocated"] - df["requested"]) * df["duration"]

waste_by_user = df.groupby(["user"])["wasted"].sum()
usage_by_user = df.groupby("user")["used"].sum()
overbilling_by_user = df.groupby("user")["overbilled"].sum()

# Compute the ratios of wasted time to used time. Close to 0 is good, between 0.5 and 1 is concerning, above 1 is bad.
ratios = waste_by_user / usage_by_user
ratios = ratios.reset_index().rename(columns={0: "ratio"})
ratios["wasted"] = waste_by_user.values
ratios["overbilled"] = overbilling_by_user.values
ratios["used"] = usage_by_user.values
ratios["total_wasted"] = ratios["wasted"] + ratios["overbilled"]
ratios["total_ratio"] = (ratios["wasted"] + ratios["overbilled"]) / ratios["used"]

# Print from worst offender to best usage.
print(ratios.reset_index().sort_values(ascending=False, by="total_ratio"))
