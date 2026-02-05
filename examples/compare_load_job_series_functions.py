"""Example code to compare both implementations of load_job_series functions."""

import calendar
import difflib
import math
import pprint
import uuid
from datetime import datetime, timedelta
from enum import Enum
from typing import Any
from unittest.mock import patch

import pandas as pd
from tqdm import tqdm

from sarc.config import UTC


def mongodb_load_job_series(*args, **kwargs) -> pd.DataFrame:
    from sarc.client.series import load_job_series

    return load_job_series(*args, **kwargs)


def rest_load_job_series(*args, **kwargs) -> pd.DataFrame:
    from sarc.rest.client import load_job_series

    return load_job_series(*args, **kwargs)


def _df_to_rows(df: pd.DataFrame) -> list[dict]:
    """Convert pandas dataframe to list of dicts."""
    # return [row.to_dict() for _, row in df.iterrows()]
    return df.to_dict(orient="records")


def deep_normalize(obj: Any) -> Any:
    """Normalize any value in a pandas data frame."""
    result = obj
    if isinstance(obj, (list, set)):
        normalized_list = [deep_normalize(i) for i in obj]
        try:
            result = sorted(normalized_list)
        except TypeError:
            result = normalized_list
    elif isinstance(obj, dict):
        result = {k: deep_normalize(v) for k, v in obj.items()}
    elif pd.isna(obj):
        # Must be checked after lists, sets and dicts, since
        # pdf.isna() will complain on empty containers
        result = None
    elif isinstance(obj, Enum):
        result = str(obj.value)
    elif isinstance(obj, uuid.UUID):
        result = str(obj)
    elif isinstance(obj, datetime):
        result = datetime.fromisoformat(obj.isoformat()).astimezone(tz=UTC)
    return result


def prepare_df_for_comparison(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize data frame before comparison"""
    for col in df.columns:
        if df[col].dtype == "O":
            # Normalize object columns
            df[col] = df[col].apply(deep_normalize)
        elif pd.api.types.is_numeric_dtype(df[col]):
            # In numeri columns, replace nan with None
            df[col] = df[col].replace({math.nan: None})
    return df


def _df_sorted(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sort dataframe rows.

    MongoDB and REST won't return jobs in same order,
    so we must sort rows before comparison.
    """
    basic_cols = ["cluster_name", "job_id", "submit_time", "user.uuid"]
    df = df.sort_values(by=basic_cols)
    return df


# Freeze current time
NOW = datetime.now(tz=UTC)


class FrozenDatetime(datetime):
    """
    Helper class to freeze current time.

    Since load_job_series() current implementation internally uses `datetime.now()`,
    some dates may vary and invalidate comparison.
    As a workaround, we mock datetime.now() so that
    all implementations and all calls to load_job_series()
    use same current time.
    """

    @classmethod
    def now(cls, *_args, **_kwargs):
        return NOW


def main():
    # Patch datetime.now()
    with patch("datetime.datetime", FrozenDatetime):
        _main()


def _main():
    # Data retrieval may be very slow with REST calls,
    # so we test only 6 cases (3 years combined to 3 months)
    for year in (2023, 2024, 2025):
        for month in (1, 5, 9):
            _, num_days = calendar.monthrange(year, month)
            start = datetime(year, month, 1, tzinfo=UTC)
            end = start + timedelta(days=num_days)
            print(f"[{start}] to [{end}]")

            df_mongo = mongodb_load_job_series(start=start, end=end)
            df_rest = rest_load_job_series(start=start, end=end)

            assert df_mongo.shape == df_rest.shape, (
                f"Mongo shape: {df_mongo.shape}, REST shape: {df_rest.shape}"
            )

            df_mongo = prepare_df_for_comparison(df_mongo)
            df_mongo = _df_sorted(df_mongo)

            df_rest = prepare_df_for_comparison(df_rest)
            df_rest = _df_sorted(df_rest)

            df_mongo_rows = _df_to_rows(df_mongo)
            df_rest_rows = _df_to_rows(df_rest)
            assert len(df_mongo_rows) == len(df_rest_rows)
            for i, (row_mongo, row_rest) in tqdm(
                enumerate(zip(df_mongo_rows, df_rest_rows)),
                total=len(df_mongo_rows),
                desc="Comparing rows",
            ):
                row_mongo.pop("id", None)
                row_rest.pop("id", None)
                if row_mongo != row_rest:
                    # Stop in first row diff, and print diff
                    lines_mongo = pprint.pformat(row_mongo).splitlines()
                    lines_rest = pprint.pformat(row_rest).splitlines()
                    diff = difflib.Differ()
                    result = list(diff.compare(lines_mongo, lines_rest))
                    print(f"[row {i}] DIFF FOUND / MONGO vs. REST:")
                    print("\n".join(result))
                    return


if __name__ == "__main__":
    main()
