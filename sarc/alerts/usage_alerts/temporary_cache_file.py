import logging
from dataclasses import dataclass

from sarc.alerts.common import CheckResult, HealthCheck

logger = logging.getLogger(__name__)


@dataclass
class TemporaryCacheFileCheck(HealthCheck):
    """
    Check if temporary files are present in SARC cache.

    New cache system is expected to create temporary cache files,
    which should no longer exist after any SARC operation is finished.
    """

    def check(self) -> CheckResult:
        from sarc.config import config

        cache = config().cache
        if cache is None:
            return self.ok()

        found = False
        for element in cache.rglob("*.current"):
            logger.error(f"Found temporary cache file: {element}")
            found = True

        return self.fail() if found else self.ok()
