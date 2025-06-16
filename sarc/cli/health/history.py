import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import gifnoc
import simple_parsing
from serieux import TaggedSubclass, deserialize

from sarc.alerts.common import CheckResult
from sarc.config import config


def parse_date(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d").astimezone()


@dataclass
class HealthHistoryCommand:
    config: Path | None = None
    start: datetime | None = simple_parsing.field(type=parse_date, default=None)
    end: datetime | None = simple_parsing.field(type=parse_date, default=None)
    name: str | None = None

    def execute(self) -> int:
        hcfg = config().health_monitor
        with gifnoc.use(self.config):
            assert hcfg is not None
            config_files = sorted(
                hcfg.directory.glob("**/*.json"),
                key=lambda x: x.name,
            )
            for file in config_files:
                content = json.loads(file.read_text())
                results = deserialize(
                    TaggedSubclass[CheckResult],
                    content,
                )
                if self.start and results.issue_date < self.start:
                    continue
                if self.end and results.issue_date > self.end:
                    continue
                if self.name and results.name != self.name:
                    continue
                for k, status in results.get_failures().items():
                    timestring = results.issue_date.strftime("%Y-%m-%d-%H-%M-%S")
                    print(f"[{timestring}]  {k:30} {status.name}")
        return 0
