from dataclasses import dataclass
from pathlib import Path

import gifnoc
from gifnoc.std import time

from sarc.alerts.common import config
from sarc.alerts.monitor import HealthMonitor


@dataclass
class HealthMonitorCommand:
    config: Path = None

    def execute(self) -> int:
        with gifnoc.use(self.config):
            monitor = HealthMonitor(logdir=config.directory, checks=config.checks)
            try:
                while True:
                    print(monitor.status)
                    time.sleep(1)
            except KeyboardInterrupt:
                return 0
