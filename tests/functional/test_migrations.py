"""Data migrations dropping states whose health check class was removed."""

from typing import Callable, NamedTuple

import pytest
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic.script import ScriptDirectory
from sqlmodel import Session, col, select

from sarc.db.healthcheck import HealthCheckStateDB

OCCURRENCES_MODULE = "sarc.alerts.usage_alerts.prometheus_stats_occurrences"
SCRAPING_MODULE = "sarc.alerts.usage_alerts.cluster_scraping"


class Case(NamedTuple):
    revision: str
    seeded_states: dict[str, str]
    survivors: list[str]


CASES = [
    Case(
        revision="41ab28d1a677",
        seeded_states={
            "prometheus_stats_cpu": f"{OCCURRENCES_MODULE}:PrometheusCpuStatCheck",
            "prometheus_stats_gpu": f"{OCCURRENCES_MODULE}:PrometheusGpuStatCheck",
            "old_running_jobs": "sarc.alerts.usage_alerts.old_running_jobs:OldRunningJobCheck",
            # Neighbouring module sharing the name prefix: must survive.
            "prometheus_gpu_type": "sarc.alerts.usage_alerts.prometheus_gpu_types:PrometheusGpuTypeCheck",
        },
        survivors=["old_running_jobs", "prometheus_gpu_type"],
    ),
    Case(
        revision="3ec1f2451f71",
        seeded_states={
            "cluster_scraping": f"{SCRAPING_MODULE}:ClusterScrapingCheck",
            "old_running_jobs": "sarc.alerts.usage_alerts.old_running_jobs:OldRunningJobCheck",
            # Neighbouring module sharing the name prefix: must survive.
            "cluster_response": "sarc.alerts.usage_alerts.cluster_response:ClusterResponseCheck",
        },
        survivors=["cluster_response", "old_running_jobs"],
    ),
]


def _upgrade_function(revision: str) -> Callable[[], None]:
    """The revision's own `upgrade()`.

    Called directly rather than through `alembic upgrade`, so that adding
    migrations on top of one does not change what the test runs.
    """
    script = ScriptDirectory.from_config(Config(toml_file="pyproject.toml"))
    return script.get_revision(revision).module.upgrade


def _state_names(sess: Session) -> list[str]:
    return list(
        sess.exec(
            select(HealthCheckStateDB.name).order_by(col(HealthCheckStateDB.name))
        ).all()
    )


@pytest.mark.parametrize("case", CASES, ids=[case.revision for case in CASES])
def test_migration_deletes_only_removed_check_states(
    case: Case, empty_read_write_db: Session
):
    sess = empty_read_write_db
    sess.add_all(
        HealthCheckStateDB(
            name=name,
            check_dict={"$class": class_ref, "active": True, "name": name},
            last_result_dict=None,
        )
        for name, class_ref in case.seeded_states.items()
    )
    sess.commit()
    assert _state_names(sess) == sorted(case.seeded_states)

    upgrade = _upgrade_function(case.revision)
    with Operations.context(MigrationContext.configure(sess.connection())):
        upgrade()
        assert _state_names(sess) == case.survivors

        # Running it again on the cleaned-up database deletes nothing more.
        upgrade()
        assert _state_names(sess) == case.survivors
