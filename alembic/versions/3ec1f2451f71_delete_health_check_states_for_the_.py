"""Delete health check states for the removed cluster_scraping check

Revision ID: 3ec1f2451f71
Revises: 41ab28d1a677
Create Date: 2026-09-11 17:03:25.087039+00:00

"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "3ec1f2451f71"
down_revision: Union[str, Sequence[str], None] = "41ab28d1a677"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# Removed in the same commit as this migration, which leaves behind states
# referencing ClusterScrapingCheck.
REMOVED_CHECK_MODULE = "sarc.alerts.usage_alerts.cluster_scraping"


def upgrade() -> None:
    """Drop states whose check class no longer exists.

    `sarc health list` deserializes each state through `TaggedSubclass[HealthCheck]`,
    which imports the class named in `check_dict['$class']` and fails on these rows.
    A state is only cached last-result data: `sarc health run` recreates it from the
    config, so dropping it loses nothing.
    """
    op.execute(
        "DELETE FROM healthcheckstatedb "
        f"WHERE split_part(check_dict ->> '$class', ':', 1) = '{REMOVED_CHECK_MODULE}'"
    )


def downgrade() -> None:
    """Nothing to restore: the check these states belonged to no longer exists."""
