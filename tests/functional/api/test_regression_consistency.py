"""
Check that regression files for REST's load_job_series tests
match regression files for MongoDB's load_job_series tests.

Both regression files must contain same jobs, while
not necessarly sorted in same order, since REST
API sort data to correctly manage pagination.

Generated with Gemini AI.
"""

import io
import re
from pathlib import Path

import pandas as pd
import pytest

# Absolute paths to regression result directories
BASE_DIR = Path(__file__).parent.parent.parent.parent
JOBS_REG_DIR = BASE_DIR / "tests/functional/jobs/test_func_load_job_series"
API_REG_DIR = BASE_DIR / "tests/functional/api/test_client_load_job_series"


def normalize_text(text):
    """
    Normalizes minor differences in string representation of objects,
    especially timezone formats which can vary between test environments.
    """
    if not isinstance(text, str):
        text = str(text)

    # Normalize various UTC offset representations to a common 'UTC' string.
    # This handles formats like FixedOffset(0, 'UTC'), TzInfo(0), etc.,
    # ensuring that differences in library versions or OS don't break the match.
    text = re.sub(
        r"tzinfo=FixedOffset\(datetime\.timedelta\(0\), 'UTC'\)", "tzinfo=UTC", text
    )
    text = re.sub(r"tzinfo=TzInfo\(0\)", "tzinfo=UTC", text)
    text = re.sub(r"FixedOffset\(datetime\.timedelta\(0\), 'UTC'\)", "UTC", text)
    text = re.sub(r"TzInfo\(0\)", "UTC", text)

    # Clean up whitespace often found in stringified Python objects (lists/dicts)
    text = text.replace(" '", "'").replace("' ", "'")

    return text


def load_regression_df(path):
    """
    Loads a SARC regression file and extracts the job data as a normalized DataFrame.
    Regression files start with a summary line 'Found X job(s):' followed by
    either a CSV or a Markdown table.
    """
    if not path.exists():
        return None

    with open(path, "r", encoding="utf-8") as f:
        lines = f.read().splitlines()

    if not lines:
        return pd.DataFrame()

    # Identify where the actual table (data) starts by skipping the header text
    table_start_index = -1
    for i, line in enumerate(lines):
        # A table header contains commas (CSV) or pipes (Markdown) and isn't the title
        if ("," in line or "|" in line) and not line.startswith("Found "):
            table_start_index = i
            break

    # Handle files representing zero results
    if table_start_index == -1:
        if any("Found 0 job(s):" in l for l in lines[:2]):
            return pd.DataFrame()
        return None

    table_content = "\n".join(lines[table_start_index:])

    if "|" in lines[table_start_index]:
        # Handle Markdown table format (often used for user-mapping tests)
        df = pd.read_csv(io.StringIO(table_content), sep="|", skipinitialspace=True)

        # Drop columns that are entirely empty (usually leading/trailing artifacts from pipes)
        df = df.dropna(axis=1, how="all")

        # Clean up column names
        df.columns = [c.strip() for c in df.columns]

        # Remove the Markdown separator line (e.g., |---|---|) which is sometimes parsed as data.
        # We filter out rows where the first column consists only of dashes and colons.
        df = df[~df.iloc[:, 0].astype(str).str.match(r"^[:\-\s]+$")]

        # Trim leading/trailing whitespace from all string cells
        df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
    else:
        # Handle standard CSV format
        df = pd.read_csv(io.StringIO(table_content))

    # Drop the first column if it's an unnamed index (common artifact from Pandas to_csv)
    if not df.empty and (df.columns[0] == "" or "Unnamed" in df.columns[0]):
        df = df.drop(df.columns[0], axis=1)

    # Convert everything to string to allow a content-based comparison
    df = df.astype(str)

    # Harmonize various string representations of 'null' or 'empty' values
    df = df.replace(["nan", "None", "", "nan "], "N/A")

    # Apply global text normalization (timezones, list formatting, etc.)
    df = df.map(normalize_text)

    return df


def get_regression_filenames():
    """Retrieve the list of regression filenames to be compared from the JOBS directory."""
    if not JOBS_REG_DIR.exists():
        return []
    files = [f.name for f in JOBS_REG_DIR.glob("*.txt")]

    # Exclude files known to be specific to MongoDB direct access (e.g., write-operation tests)
    excluded = ["test_load_job_series_with_bad_gpu_utilization.txt"]
    files = [f for f in files if f not in excluded]

    return sorted(files)


@pytest.mark.parametrize("filename", get_regression_filenames())
def test_regression_files_api_vs_jobs_consistency(filename):
    """
    Ensures that regression files generated via the REST API contain the exact same
    job data as those generated via direct MongoDB access.

    Order of jobs is ignored; only the set of jobs and their individual content must match.
    """
    jobs_path = JOBS_REG_DIR / filename
    api_path = API_REG_DIR / filename

    assert api_path.exists(), f"Corresponding API regression file {filename} is missing"

    df_jobs = load_regression_df(jobs_path)
    df_api = load_regression_df(api_path)

    assert df_jobs is not None, f"Failed to parse JOBS regression data from {filename}"
    assert df_api is not None, f"Failed to parse API regression data from {filename}"

    # Check if both represent an empty set of jobs
    if df_jobs.empty or df_api.empty:
        assert df_jobs.empty and df_api.empty, (
            f"One file is empty while the other is not: {filename}"
        )
        return

    # Verify that the number of jobs found is identical
    assert len(df_jobs) == len(df_api) > 0, f"Job count mismatch in {filename}"

    # Perform an order-independent content comparison using Multisets.
    # Each job (row) is converted into a sorted tuple of (column, value) pairs.
    def get_job_multiset(df):
        return sorted(
            [tuple(sorted(row.to_dict().items())) for _, row in df.iterrows()]
        )

    assert get_job_multiset(df_jobs) == get_job_multiset(df_api), (
        f"Mismatched job content found in {filename} after normalization"
    )
