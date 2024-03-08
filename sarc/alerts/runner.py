import logging
from datetime import timedelta

from gifnoc.std import time as gtime

from .common import CheckStatus, HealthCheck

logger = logging.getLogger(__name__)


class CheckRunner:
    def __init__(self, directory, checks):
        self.directory = directory
        self.checks: list[HealthCheck] = [check for check in checks if check.active]
        self.state = {}

    def process(self, check, result=None):
        if result is None:
            result = check.latest_result()
        else:
            match result.status:
                case CheckStatus.OK:
                    logger.info(f"Check '{check.name}' succeeded.")
                case CheckStatus.FAILURE:
                    logger.error(f"Check '{check.name}' failed.")
                case CheckStatus.ERROR:
                    logger.error(f"Check '{check.name}' errored.")
        next_schedule = check.next_schedule(result)
        self.state[check.name] = (check, result, next_schedule)
        return next_schedule

    def start(self):
        for check in self.checks:
            self.process(check)

        if not self.checks:
            logger.warning("There are no active checks to run!")
            return

        check_names = ", ".join(check.name for check in self.checks)
        logger.info(f"Managing {len(self.checks)} active checks: {check_names}")

        while True:
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
                    except Exception as exc:
                        logger.error(
                            f"Unexpected {type(exc).__name__}: {exc}", exc_info=exc
                        )
                new_delta = next_schedule - gtime.now()
                if new_delta < delta:
                    up_next = check.name
                    delta = new_delta

            wait = max(0, delta.total_seconds())
            _wait = int(wait)
            formatted = f"{_wait // 3600}:{(_wait % 3600) // 60:02}:{_wait % 60:02}"
            logger.info(f"Wait for {formatted}. Next check: '{up_next}'")
            gtime.sleep(max(0.1, wait))
