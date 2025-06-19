from dataclasses import dataclass
from pathlib import Path

import gifnoc
from gifnoc.std import time

from sarc.alerts.monitor import HealthMonitor
from sarc.config import config


@dataclass
class HealthMonitorCommand:
    config: Path | None = None

    def execute(self) -> int:
        hcfg = config().health_monitor
        with gifnoc.use(self.config):
            assert hcfg is not None
            monitor = HealthMonitor(
                directory=hcfg.directory.as_uri(), checks=hcfg.checks
            )
            try:
                while True:
                    print(monitor.status)
                    time.sleep(1)
            except KeyboardInterrupt:
                return 0
