"""create target_shares table

Revision ID: 3b7e1d9f4a52
Revises: 280c24c88ea6
Create Date: 2026-10-22 14:03:41.120335+00:00

"""

from typing import Sequence, Union

import sqlalchemy as sa

import sarc.db.sqlmodel
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "3b7e1d9f4a52"
down_revision: Union[str, Sequence[str], None] = "280c24c88ea6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "target_shares",
        sa.Column("cluster_id", sa.Integer(), nullable=False),
        sa.Column("allocation", sa.String(), nullable=True),
        sa.Column(
            "start_date", sarc.db.sqlmodel.UTCDateTime(timezone=True), nullable=False
        ),
        sa.Column(
            "end_date", sarc.db.sqlmodel.UTCDateTime(timezone=True), nullable=False
        ),
        sa.Column("rgu_year", sa.Float(), nullable=False),
        sa.ForeignKeyConstraint(["cluster_id"], ["clusters.id"]),
        sa.PrimaryKeyConstraint("cluster_id", "start_date"),
        sa.CheckConstraint(
            "end_date > start_date", name="target_shares_end_after_start"
        ),
    )
    # One target share per cluster at any instant (see TargetShareDB).
    op.execute(
        "ALTER TABLE target_shares ADD CONSTRAINT target_shares_no_overlap "
        "EXCLUDE USING gist ("
        "cluster_id WITH =, tstzrange(start_date, end_date, '[)') WITH &&)"
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("target_shares")
