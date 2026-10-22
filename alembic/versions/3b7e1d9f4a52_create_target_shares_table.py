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
    # The exclusion constraint below indexes the text column with gist.
    op.execute("CREATE EXTENSION IF NOT EXISTS btree_gist")
    op.create_table(
        "target_shares",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("cluster_id", sa.Integer(), nullable=False),
        sa.Column("allocation", sa.String(), nullable=True),
        sa.Column("start", sa.Date(), nullable=False),
        sa.Column("end", sa.Date(), nullable=False),
        sa.Column("rgu_hours", sa.Float(), nullable=False),
        sa.Column(
            "timestamp", sarc.db.sqlmodel.UTCDateTime(timezone=True), nullable=False
        ),
        sa.ForeignKeyConstraint(["cluster_id"], ["clusters.id"]),
        sa.PrimaryKeyConstraint("id"),
        # "end" is a reserved word: it must be quoted in the raw SQL.
        sa.CheckConstraint('"end" > start', name="target_shares_end_after_start"),
    )
    op.create_index(
        op.f("ix_target_shares_cluster_id"),
        "target_shares",
        ["cluster_id"],
        unique=False,
    )
    # Target-share periods must not overlap (see TargetShareDB):
    # same cluster and allocation, half-open periods [start, end).
    # coalesce() so NULL allocations (whole-cluster budgets) conflict
    # with each other; a gist exclusion predicate with a NULL operand
    # is never true.
    op.execute(
        "ALTER TABLE target_shares ADD CONSTRAINT target_shares_no_overlap "
        "EXCLUDE USING gist ("
        "cluster_id WITH =, coalesce(allocation, '') WITH =, "
        'daterange("start", "end", \'[)\') WITH &&)'
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    """Downgrade schema."""
    # btree_gist is left in place: it is a shared extension.
    op.drop_index(op.f("ix_target_shares_cluster_id"), table_name="target_shares")
    op.drop_table("target_shares")
