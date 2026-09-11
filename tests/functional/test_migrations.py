"""Data migration 41ab28d1a677: drop states whose health check class was removed."""

from typing import Callable

from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic.script import ScriptDirectory
from sqlmodel import Session, col, select

from sarc.db.healthcheck import HealthCheckStateDB

REVISION = "41ab28d1a677"
REMOVED_MODULE = "sarc.alerts.usage_alerts.prometheus_stats_occurrences"

SEEDED_STATES = {
    "prometheus_stats_cpu": f"{REMOVED_MODULE}:PrometheusCpuStatCheck",
    "prometheus_stats_gpu": f"{REMOVED_MODULE}:PrometheusGpuStatCheck",
    "old_running_jobs": "sarc.alerts.usage_alerts.old_running_jobs:OldRunningJobCheck",
    # Neighbouring module sharing the name prefix: must survive.
    "prometheus_gpu_type": "sarc.alerts.usage_alerts.prometheus_gpu_types:PrometheusGpuTypeCheck",
}
SURVIVORS = ["old_running_jobs", "prometheus_gpu_type"]


def _upgrade_function() -> Callable[[], None]:
    """The revision's own `upgrade()`.

    Called directly rather than through `alembic upgrade`, so that adding
    migrations on top of this one does not change what the test runs.
    """
    script = ScriptDirectory.from_config(Config(toml_file="pyproject.toml"))
    return script.get_revision(REVISION).module.upgrade


def _state_names(sess: Session) -> list[str]:
    return list(
        sess.exec(
            select(HealthCheckStateDB.name).order_by(col(HealthCheckStateDB.name))
        ).all()
    )


def test_migration_deletes_only_removed_check_states(empty_read_write_db: Session):
    sess = empty_read_write_db
    sess.add_all(
        HealthCheckStateDB(
            name=name,
            check_dict={"$class": class_ref, "active": True, "name": name},
            last_result_dict=None,
        )
        for name, class_ref in SEEDED_STATES.items()
    )
    sess.commit()
    assert _state_names(sess) == sorted(SEEDED_STATES)

    upgrade = _upgrade_function()
    with Operations.context(MigrationContext.configure(sess.connection())):
        upgrade()
        assert _state_names(sess) == SURVIVORS

        # Running it again on the cleaned-up database deletes nothing more.
        upgrade()
        assert _state_names(sess) == SURVIVORS
