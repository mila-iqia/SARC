"""Daemon to run the checks continuously."""

import datetime
import logging
from datetime import timedelta
from pathlib import Path
from typing import Generator, cast

from gifnoc.std import time as gtime

from .common import CheckResult, CheckStatus, HealthCheck

logger = logging.getLogger(__name__)


class CheckRunner:
    def __init__(self, directory: Path, checks: dict[str, HealthCheck]):
        self.directory = directory
        self.checks: dict[str, HealthCheck] = {
            name: check for name, check in checks.items() if check.active
        }
        self.state: dict[str, tuple[HealthCheck, CheckResult, datetime.datetime]] = {}

    def process(
        self, check: HealthCheck, result: CheckResult | None = None
    ) -> datetime.datetime:
        """Process a new result for the check, into the state.

        If result is None we fetch the latest result for the check.
        """
        if result is None:
            result = check.latest_result()
        elif result.status == CheckStatus.OK:
            logger.info(f"Check '{check.name}' succeeded.")
        elif result.status == CheckStatus.FAILURE:
            logger.error(f"Check '{check.name}' failed.")
        elif result.status == CheckStatus.ERROR:
            logger.error(f"Check '{check.name}' errored.")
        next_schedule = check.next_schedule(result)
        self.state[check.name] = (check, result, next_schedule)
        return next_schedule

    def iterate(self) -> Generator[float]:
        """Iterate over checks.

        This is implemented as a generator that continually generates how long to
        wait, in seconds, until the next check.
        """
        for check in self.checks.values():
            self.process(check)

        if not self.checks:
            logger.warning("There are no active checks to run!")
            return

        check_names = ", ".join(self.checks.keys())
        logger.info(f"Managing {len(self.checks)} active checks: {check_names}")

        wait = 0.0
        while True:
            yield wait
            delta = timedelta(days=1000)
            up_next = "?"
            for check, _, next_schedule in self.state.values():
                if any(
                    self.state[dep][1].status != CheckStatus.OK for dep in check.depends
                ):
                    logger.warning(
                        f"Skip check: '{check.name}' because dependency failed"
                    )
                    continue
                if gtime.now() >= next_schedule:
                    logger.info(f"Perform check: '{check.name}'")
                    try:
                        next_schedule = self.process(check, check())
                    except Exception as exc:  # pylint: disable=W0718
                        logger.error(
                            f"Unexpected {type(exc).__name__}: {exc}", exc_info=exc
                        )
                new_delta = next_schedule - cast(datetime.datetime, gtime.now())
                if new_delta < delta:
                    up_next = check.name
                    delta = new_delta

            wait = max(0.0, delta.total_seconds())
            _wait = int(wait)
            formatted = f"{_wait // 3600}:{(_wait % 3600) // 60:02}:{_wait % 60:02}"
            logger.info(f"Wait for {formatted}. Next check: '{up_next}'")

    def start(self, end_time: datetime.datetime | None = None) -> None:
        for wait in self.iterate():
            gtime.sleep(wait)
            if end_time is not None and gtime.now() >= end_time:
                break
