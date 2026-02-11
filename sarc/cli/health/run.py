"""Poll command for health checks.

This command is designed to be run periodically (e.g., by cron) and will
execute only the checks whose interval has expired since their last run.
State is persisted in MongoDB, and results are logged instead of written to files.
"""

import logging
from dataclasses import dataclass
from pathlib import Path

import gifnoc
import simple_parsing

from sarc.alerts.common import CheckStatus
from sarc.alerts.healthcheck_state import (
    HealthCheckState,
    get_healthcheck_state_collection,
)
from sarc.config import config

logger = logging.getLogger(__name__)


@dataclass
class HealthRunCommand:
    """Execute health checks that are due based on their configured interval.

    This is an alternative to the daemon-based `health check` command,
    designed for environments where long-running processes are costly (e.g., GCP).
    """

    config: Path | None = None
    checks: list[str] | None = simple_parsing.field(
        default=None,
        help="Names of health checks to run. Mutually exclusive with --all",
    )
    all: bool = simple_parsing.field(
        action="store_true",
        help="Run all health checks. Mutually exclusive with --checks",
    )

    def execute(self) -> int:
        if self.config is None:
            return self._exec()
        with gifnoc.use(self.config):
            return self._exec()

    def _exec(self) -> int:
        hcfg = config().health_monitor

        if hcfg is None:
            logger.error("No health_monitor configuration found")
            return -1

        if not self.checks and not self.all:
            logger.error("No health checks to run. Use either --all or --checks")
            return -1
        if self.checks and self.all:
            logger.error("Arguments mutually exclusive: --all | --checks")
            return -1

        if self.checks:
            check_names = [
                check_name for check_name in self.checks if check_name in hcfg.checks
            ]
            logger.debug(f"Running {len(check_names)} health checks: {check_names}")
        else:
            check_names = list(hcfg.checks.keys())
            logger.debug(f"Running all {len(check_names)} health checks")

        # Get database and repository
        repo = get_healthcheck_state_collection()

        checks_run = 0
        checks_skipped = 0

        for name in check_names:
            state = repo.get_state(name)
            if state is None:
                # Get check from config file and save it in a new state in db
                state = HealthCheckState(check=hcfg.checks[name])
                repo.update_state(state)
            check = state.check

            # Skip inactive checks
            if not check.active:
                logger.debug(f"Skipping '{name}': inactive")
                checks_skipped += 1
                continue

            # Check dependencies
            deps_ok = True
            for dep in check.depends:
                dep_state = repo.get_state(dep) or HealthCheckState(
                    check=hcfg.checks[dep]
                )
                if (
                    dep_state.last_result is None
                    or dep_state.last_result.status != CheckStatus.OK
                ):
                    logger.warning(f"Skipping '{name}': dependency '{dep}' not OK")
                    deps_ok = False
                    break

            if not deps_ok:
                checks_skipped += 1
                continue

            # Execute the check
            logger.info(f"Running check: '{name}'")
            result = check.wrapped_check()
            checks_run += 1
            # Log the result
            message = result.log_result()
            # Update MongoDB state
            state.last_result = result
            state.last_message = message
            repo.update_state(state)

        logger.info(f"Poll complete: {checks_run} checks run, {checks_skipped} skipped")
        return 0
