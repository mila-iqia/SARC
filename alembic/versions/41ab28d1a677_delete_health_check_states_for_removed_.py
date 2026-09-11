"""Delete health check states for removed check classes

Revision ID: 41ab28d1a677
Revises: 7fe5b57ffa1d
Create Date: 2026-09-10 19:24:34.687380+00:00

"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "41ab28d1a677"
down_revision: Union[str, Sequence[str], None] = "7fe5b57ffa1d"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# Removed by ec19d983 ("Remove the Prometheus stats occurrences checks"), which left
# behind states referencing PrometheusCpuStatCheck and PrometheusGpuStatCheck.
REMOVED_CHECK_MODULE = "sarc.alerts.usage_alerts.prometheus_stats_occurrences"


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
    """Nothing to restore: the checks these states belonged to no longer exist."""
