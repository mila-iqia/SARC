"""
Check that regression files for REST's load_job_series tests
match regression files for SQL's load_job_series tests.

Both shims (`sql_load_job_series` and `rest_load_job_series`) share the
post-processing logic (`_finalize_records`) and the row ordering (sort by
`job_db_id`), so the resulting snapshots must be byte-identical.
"""

from pathlib import Path

import pytest

BASE_DIR = Path(__file__).parent.parent.parent.parent
SQL_REG_DIR = BASE_DIR / "tests/functional/job_series/test_func_load_job_series"
REST_REG_DIR = BASE_DIR / "tests/functional/api/test_rest_load_job_series"


def _regression_filenames() -> list[str]:
    if not SQL_REG_DIR.exists():
        return []
    return sorted(f.name for f in SQL_REG_DIR.glob("*.txt"))


@pytest.mark.parametrize("filename", _regression_filenames())
def test_regression_files_sql_vs_rest_consistency(filename: str):
    """Ensure SQL and REST adapters produce byte-identical snapshots."""
    sql_path = SQL_REG_DIR / filename
    rest_path = REST_REG_DIR / filename
    assert rest_path.exists(), f"REST regression file {filename} is missing"
    assert sql_path.read_text() == rest_path.read_text(), (
        f"Mismatched content in {filename}"
    )
