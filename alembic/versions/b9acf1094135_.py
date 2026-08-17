"""empty message

Revision ID: b9acf1094135
Revises: 4df0156c09ee
Create Date: 2026-08-13 00:52:46.467728+00:00

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic_utils.pg_function import PGFunction

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b9acf1094135"
down_revision: Union[str, Sequence[str], None] = "4df0156c09ee"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_END_PAYLOAD_GPU = [
    "start_time",
    "elapsed_time",
    "id",
    "harmonized_gpu_type",
    "allocated_gres_gpu",
    "cluster_id",
    "cluster_user",
    "sarc_user_id",
    "job_state",
]
_SUBMIT_PAYLOAD = [
    "id",
    "harmonized_gpu_type",
    "allocated_gres_gpu",
    "elapsed_time",
    "cluster_id",
    "cluster_user",
    "sarc_user_id",
]

public_slurm_job_end = PGFunction(
    schema="public",
    signature="slurm_job_end(job_start timestamptz, job_elapsed double precision)",
    definition="returns timestamptz\nlanguage sql\nimmutable\nstrict\nparallel safe\nas $$ select case when job_elapsed > 0 then job_start + make_interval(secs => job_elapsed) end $$",
)


def upgrade() -> None:
    """Upgrade schema."""
    op.create_entity(public_slurm_job_end)

    op.create_index(
        "ix_slurm_jobs_end_gpu",
        "slurm_jobs",
        [sa.literal_column("slurm_job_end(start_time, elapsed_time)")],
        unique=False,
        postgresql_include=_END_PAYLOAD_GPU,
        postgresql_where=sa.text(
            "allocated_gres_gpu > 0 AND harmonized_gpu_type IS NOT NULL"
        ),
    )
    # The /dash window filter moved off submit_time onto the index above,
    # leaving this one to serve the job table's submit_time *sort* only, which
    # reads the order and none of the payload.
    op.create_index("ix_slurm_jobs_submit", "slurm_jobs", ["submit_time"], unique=False)
    op.drop_index("ix_slurm_jobs_submit_gpu_type", table_name="slurm_jobs")


def downgrade() -> None:
    """Downgrade schema."""
    op.create_index(
        "ix_slurm_jobs_submit_gpu_type",
        "slurm_jobs",
        ["submit_time", "allocated_gpu_type"],
        unique=False,
        postgresql_include=_SUBMIT_PAYLOAD,
    )
    op.drop_index("ix_slurm_jobs_submit", table_name="slurm_jobs")
    op.drop_index("ix_slurm_jobs_end_gpu", table_name="slurm_jobs")
    op.drop_entity(public_slurm_job_end)
