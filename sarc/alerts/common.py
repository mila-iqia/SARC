import json
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Optional

import gifnoc
from apischema import ValidationError, deserialize, deserializer, serialize, serializer
from gifnoc import TaggedSubclass
from gifnoc.std import time

logger = logging.getLogger(__name__)


class CheckStatus(Enum):
    """Possible statuses for a health check."""

    # Check succeeded
    OK = "ok"

    # Check failed
    FAILURE = "failure"

    # Check raised an unexpected exception
    ERROR = "error"

    # There is no check
    ABSENT = "absent"

    # Check has been made too long ago and is now stale
    STALE = "stale"


@dataclass(frozen=True)
class CheckException:
    """Exception data for checks with ERROR status."""

    type: str
    message: str

    @staticmethod
    def from_exception(exc):
        return CheckException(
            type=type(exc).__qualname__,
            message=str(exc),
        )


@dataclass
class CheckResult:
    """Result of a check."""

    # Name of the health check that has this result
    name: str = None

    # Status of the check
    status: CheckStatus = CheckStatus.ABSENT

    # Information about the exception, if the check has ERROR status
    exception: Optional[CheckException] = None

    # Date at which the check finished
    issue_date: datetime = field(default_factory=lambda: time.now())

    # Date at which the check will be considered STALE
    expiry: Optional[datetime] = None

    # Complementary data
    data: Optional[dict] = field(default_factory=dict)


@dataclass
class HealthCheck:
    """Base class for health checks."""

    # Name of the check
    name: str

    # Whether the check is active or not
    active: bool

    # Interval at which to activate the check
    interval: timedelta

    # Directory in which to serialize results
    # Note: this is typically set by the parent to be root_check_dir/check.name
    directory: Path = None

    def result(self, status: CheckStatus, **kwargs) -> CheckResult:
        """Generate a result with the given status."""
        now = time.now()
        expiry = now + self.interval + timedelta(hours=1)
        if status not in iter(CheckStatus):
            opts = ", ".join(item.value for item in CheckStatus)
            raise ValueError(f"Invalid status: {status}. Valid statuses are: {opts}")
        return CheckResult(
            name=self.name,
            status=status,
            issue_date=now,
            expiry=expiry,
            **kwargs,
        )

    def ok(self, **data) -> CheckResult:
        """Shortcut to generate OK status."""
        return self.result(CheckStatus.OK, data=data)

    def fail(self, **data) -> CheckResult:
        """Shortcut to generate FAIL status."""
        return self.result(CheckStatus.FAILURE, data=data)

    def check(self) -> CheckResult | CheckStatus:
        """Perform the check and return a result or status."""
        raise NotImplementedError("Please override in subclass.")

    def read_result(self, path) -> CheckResult:
        """Read results from the file at the given path."""
        data = json.loads(path.read_text())
        return deserialize(CheckResult, data)

    def latest_result(self) -> CheckResult:
        """Return the latest result for this check."""
        assert self.directory, "The check is not associated to a directory."
        if not self.directory.exists():
            os.makedirs(self.directory, exist_ok=True)

        files = [
            (file.stat().st_ctime_ns, file)
            for file in self.directory.iterdir()
            if file.suffix == ".json"
        ]
        if not files:
            return self.result(CheckStatus.ABSENT)
        else:
            files.sort(reverse=True)
            return self.read_result(files[0][1])

    def next_schedule(self, latest: CheckResult) -> datetime:
        """Return the latest result for this check."""
        if latest.status is CheckStatus.ABSENT:
            return time.now()
        else:
            return latest.issue_date + self.interval

    def __call__(self):
        os.makedirs(self.directory, exist_ok=True)
        try:
            results = self.check()
            if not isinstance(results, CheckResult):
                if results in (self.ok, self.fail):
                    results = results()
                else:
                    results = self.result(status=results)
            serialized = serialize(results)
        except BaseException as exc:
            results = self.result(
                CheckStatus.ERROR, exception=CheckException.from_exception(exc)
            )
            serialized = serialize(results)
        finally:
            timestring = results.issue_date.strftime("%Y-%m-%d-%H-%M-%S")
            dest = self.directory / f"{timestring}.json"
            dest.write_text(json.dumps(serialized, indent=4))
            logger.debug(f"Wrote {dest}")
            return results


@serializer
def _serialize_timedelta(td: timedelta) -> str:
    seconds = int(td.total_seconds())
    if td.microseconds:
        return f"{seconds}{td.microseconds:06}us"
    else:
        return f"{seconds}s"


@deserializer
def _deserialize_timedelta(s: str) -> timedelta:
    units = {
        "d": "days",
        "h": "hours",
        "m": "minutes",
        "s": "seconds",
        "ms": "milliseconds",
        "us": "microseconds",
    }
    sign = 1
    if s.startswith("-"):
        s = s[1:]
        sign = -1
    kw = {}
    parts = re.split(string=s, pattern="([a-z ]+)")
    if parts[-1] != "":
        raise ValidationError("timedelta representation must end with a unit")
    for i in range(len(parts) // 2):
        n = parts[i * 2]
        unit = parts[i * 2 + 1].strip()
        if unit not in units:
            raise ValidationError(f"'{unit}' is not a valid timedelta unit")
        try:
            kw[units[unit]] = float(n)
        except ValueError as err:
            raise ValidationError(f"Could not convert '{n}' ({units[unit]}) to float")
    return sign * timedelta(**kw)


@dataclass
class HealthMonitorConfig:
    # Root directory for check results
    directory: Path

    # List of checks to execute. TaggedSubclass[T] makes it so that we can serialize
    # and deserialize subclasses of HealthCheck (the class reference is in the "class"
    # field of each check).
    checks: list[TaggedSubclass[HealthCheck]]

    # # How to handle time. Use 'class: FrozenTime' to pretend time is frozen.
    # time: TaggedSubclass[NormalTime] = field(default_factory=NormalTime)

    def __post_init__(self):
        # Set each check's directory based on the root directory and the check's name.
        for check in self.checks:
            check.directory = self.directory / check.name


config = gifnoc.define(
    field="sarc_health_monitor",
    model=HealthMonitorConfig,
)
