"""Add jobstatistics_fetchdate table

Revision ID: c8f3a1b920d4
Revises: ff1795ca47a3
Create Date: 2026-06-29 00:00:00.000000+00:00

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c8f3a1b920d4"
down_revision: Union[str, Sequence[str], None] = "ff1795ca47a3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "jobstatistics_fetchdate",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("job_id", sa.Integer(), nullable=False),
        sa.Column("fetch_date", sa.DateTime(timezone=True), nullable=False),
        sa.Column("jobstatistic_id", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(["job_id"], ["slurm_jobs.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["jobstatistic_id"], ["jobstatisticdb.id"], ondelete="SET NULL"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("job_id", name="uq_jobstatistics_fetchdate_job_id"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("jobstatistics_fetchdate")
