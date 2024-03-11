import logging
from dataclasses import dataclass
from pathlib import Path
from pprint import pprint

import gifnoc

from sarc.alerts.common import config
from sarc.alerts.runner import CheckRunner

logger = logging.getLogger(__name__)


@dataclass
class HealthCheckCommand:
    config: Path = None

    name: str = None

    def execute(self) -> int:
        with gifnoc.use(self.config):
            if self.name:
                check = config.checks[self.name]
                results = check.check()
                pprint(results)
                print(f"{results.status.name}")
            else:
                try:
                    runner = CheckRunner(
                        directory=config.directory, checks=config.checks
                    )
                    runner.start()
                except KeyboardInterrupt:
                    logger.info("Execution ended due to KeyboardInterrupt")
                    return 0
