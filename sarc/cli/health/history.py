import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import gifnoc
from serieux import TaggedSubclass, deserialize

from sarc.alerts.common import CheckResult, config


def parse_date(s):
    return datetime.strptime(s, "%Y-%m-%d").astimezone()


@dataclass
class HealthHistoryCommand:
    config: Path = None
    start: parse_date = None
    end: parse_date = None
    name: str = None

    def execute(self) -> int:
        with gifnoc.use(self.config):
            config_files = sorted(
                config.directory.glob("**/*.json"),
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
