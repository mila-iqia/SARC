"""empty message

Revision ID: a24d2967a725
Revises: 10c932c4bf84
Create Date: 2026-06-05 13:52:06.534147+00:00

"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a24d2967a725"
down_revision: Union[str, Sequence[str], None] = "10c932c4bf84"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.drop_constraint(
        constraint_name="slurm_jobs_check1", table_name="slurm_jobs", type_="check"
    )
    op.drop_constraint(
        constraint_name="slurm_jobs_check", table_name="slurm_jobs", type_="check"
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.create_check_constraint(
        "slurm_jobs_check", "slurm_jobs", "start_time <= end_time"
    )
    op.create_check_constraint(
        "slurm_jobs_check1", "slurm_jobs", "submit_time <= start_time"
    )
