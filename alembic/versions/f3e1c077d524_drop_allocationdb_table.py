"""drop allocationdb table

Revision ID: f3e1c077d524
Revises: 3b7e1d9f4a52
Create Date: 2026-10-23 21:44:12.534643+00:00

"""

from typing import Sequence, Union

import sqlalchemy as sa

import sarc.db.sqlmodel
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "f3e1c077d524"
down_revision: Union[str, Sequence[str], None] = "3b7e1d9f4a52"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Superseded by target_shares, and never filled since the move to SQL.
    op.drop_index(op.f("ix_allocationdb_cluster_id"), table_name="allocationdb")
    op.drop_table("allocationdb")


def downgrade() -> None:
    """Downgrade schema."""
    op.create_table(
        "allocationdb",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("cluster_id", sa.Integer(), nullable=False),
        sa.Column("resource_name", sa.String(), nullable=False),
        sa.Column("group_name", sa.String(), nullable=False),
        sa.Column(
            "timestamp", sarc.db.sqlmodel.UTCDateTime(timezone=True), nullable=False
        ),
        sa.Column("start", sa.Date(), nullable=False),
        sa.Column("end", sa.Date(), nullable=False),
        sa.Column("gpu_year", sa.Integer(), nullable=True),
        sa.Column("cpu_year", sa.Integer(), nullable=True),
        sa.Column("rgu_year", sa.Integer(), nullable=True),
        sa.Column("vcpu_year", sa.Integer(), nullable=True),
        sa.Column("vgpu_year", sa.Integer(), nullable=True),
        sa.Column("project_inodes", sa.Float(), nullable=True),
        sa.Column("cloud_volume", sa.BigInteger(), nullable=True),
        sa.Column("cloud_shared", sa.BigInteger(), nullable=True),
        sa.Column("project_size", sa.BigInteger(), nullable=True),
        sa.Column("nearline_size", sa.BigInteger(), nullable=True),
        sa.Column("dCache", sa.BigInteger(), nullable=True),
        sa.Column("object", sa.BigInteger(), nullable=True),
        sa.ForeignKeyConstraint(["cluster_id"], ["clusters.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_allocationdb_cluster_id"), "allocationdb", ["cluster_id"], unique=False
    )
