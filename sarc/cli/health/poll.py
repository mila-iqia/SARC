"""Poll command for health checks.

This command is designed to be run periodically (e.g., by cron) and will
execute only the checks whose interval has expired since their last run.
State is persisted in MongoDB, and results are logged instead of written to files.
"""

import logging
from dataclasses import dataclass
from pathlib import Path

import gifnoc
from gifnoc.std import time

from sarc.alerts.common import CheckStatus
from sarc.alerts.healthcheck_state import HealthCheckStateRepository
from sarc.config import config

logger = logging.getLogger(__name__)


@dataclass
class HealthPollCommand:
    """Execute health checks that are due based on their configured interval.

    This is an alternative to the daemon-based `health check` command,
    designed for environments where long-running processes are costly (e.g., GCP).
    """

    config: Path | None = None

    def execute(self) -> int:
        hcfg = config().health_monitor
        with gifnoc.use(self.config):
            if hcfg is None:
                logger.error("No health_monitor configuration found")
                return 1

            # Get database and repository
            db = config().mongo.database_instance
            repo = HealthCheckStateRepository(db)

            # Get all check states from MongoDB
            db_states = repo.get_all_states()

            now = time.now()
            checks_run = 0
            checks_skipped = 0

            for name, check in hcfg.checks.items():
                # Skip inactive checks (from config)
                if not check.active:
                    logger.debug(f"Skipping '{name}': inactive in config")
                    checks_skipped += 1
                    continue

                # Check if disabled in database
                db_state = db_states.get(name)
                if db_state is not None and not db_state.active:
                    logger.debug(f"Skipping '{name}': disabled in database")
                    checks_skipped += 1
                    continue

                # Check if interval has expired
                if db_state is not None and db_state.last_run is not None:
                    next_run = db_state.last_run + check.interval
                    if now < next_run:
                        remaining = next_run - now
                        logger.debug(f"Skipping '{name}': next run in {remaining}")
                        checks_skipped += 1
                        continue

                # Check dependencies
                deps_ok = True
                for dep in check.depends:
                    dep_state = db_states.get(dep)
                    if dep_state is None or dep_state.last_status != "ok":
                        logger.warning(f"Skipping '{name}': dependency '{dep}' not OK")
                        deps_ok = False
                        break

                if not deps_ok:
                    checks_skipped += 1
                    continue

                # Execute the check
                logger.info(f"Running check: '{name}'")
                try:
                    result = check.wrapped_check()
                    checks_run += 1

                    # Log the result
                    result.log_result()

                    # Update MongoDB state
                    message = None
                    if result.exception:
                        message = f"{result.exception.type}: {result.exception.message}"
                    elif result.status == CheckStatus.FAILURE:
                        failures = list(result.get_failures().keys())
                        if failures:
                            message = f"Failed: {', '.join(failures)}"

                    repo.update_state(
                        name=name,
                        status=result.status,
                        message=message,
                        run_time=result.issue_date,
                    )

                    # Update local state for dependency checks
                    new_state = repo.get_state(name)
                    assert new_state is not None
                    db_states[name] = new_state

                except Exception as exc:  # pylint: disable=broad-except
                    logger.error(
                        f"Unexpected error running '{name}': {exc}",
                        exc_info=True,
                    )
                    repo.update_state(
                        name=name,
                        status=CheckStatus.ERROR,
                        message=str(exc),
                    )
                    checks_run += 1

            logger.info(
                f"Poll complete: {checks_run} checks run, {checks_skipped} skipped"
            )
            return 0
