import logging
from datetime import datetime, timedelta

import pandas as pd

from sarc.client.series import load_job_series
from sarc.config import MTL


def find_missing_user_to_mila_emails(
    df: pd.DataFrame,
) -> list[str]:
    missing_mila_email = df["user.mila.email"].isna()

    n_missing = missing_mila_email.sum()
    if not n_missing:
        return []

    N = df.shape[0]
    print(f"'user.mila.email' is missing in {n_missing} jobs ({n_missing / N:.2%})")

    unique_users = df[missing_mila_email]["user"].unique()

    return sorted(unique_users)


# This check will check if the users of jobs from a specific period
# exist in the database, and have a mila email address.
# return value: list of users IDs that are not in the database
def check_users_in_jobs(
    time_interval: timedelta | None = timedelta(hours=24),
) -> list[str]:
    logging.info("Checking users in jobs; timedelta: %s", time_interval)

    # Parse time_interval
    start, end, clip_time = None, None, False
    if time_interval is not None:
        end = datetime.now(tz=MTL)
        start = end - time_interval
        clip_time = True

    df_jobs = load_job_series(start=start, end=end, clip_time=clip_time)

    # Find missing 'user.mila.email' in jobs
    missing_mila_email_users = find_missing_user_to_mila_emails(df_jobs)

    return missing_mila_email_users
