import itertools
import json
import logging
import os
import re
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from enum import Enum
from graphlib import TopologicalSorter
from pathlib import Path
from typing import Optional, Union

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


StatusDict = dict[str, Union[CheckStatus, "StatusDict"]]


@dataclass
class CheckResult:
    """Results of a check."""

    # Name of the health check that has this result
    name: str = None

    # Global status of the check
    status: CheckStatus = CheckStatus.ABSENT

    # Statuses of individual checks
    statuses: StatusDict = field(default_factory=dict)

    # Information about the exception, if the check has ERROR status
    exception: Optional[CheckException] = None

    # Date at which the check finished
    issue_date: datetime = field(default_factory=lambda: time.now().astimezone())

    # Date at which the check will be considered STALE
    expiry: Optional[datetime] = None

    def get_failures(self):
        failure_statuses = (CheckStatus.FAILURE, CheckStatus.ERROR)
        results = {}
        if self.status in failure_statuses:
            results[self.name] = self.status
        results.update(
            {
                f"{self.name}/{k}": status
                for k, status in self.statuses.items()
                if status in failure_statuses
            }
        )
        return results

    def save(self, directory):
        os.makedirs(directory, exist_ok=True)
        serialized = serialize(TaggedSubclass[CheckResult], self)
        timestring = self.issue_date.strftime("%Y-%m-%d-%H-%M-%S")
        dest = directory / f"{timestring}.json"
        dest.write_text(json.dumps(serialized, indent=4))
        logger.debug(f"Wrote {dest}")


@dataclass
class HealthCheck:
    """Base class for health checks."""

    # Whether the check is active or not
    active: bool

    # Interval at which to activate the check
    interval: timedelta

    # Name of the check
    name: str = None

    # Parameters of the check
    parameters: dict[str, str] = field(default_factory=dict)

    # Directory in which to serialize results
    # Note: this is typically set by the parent to be root_check_dir/check.name
    directory: Path = None

    # Other checks on which this check depends. If these checks fail, this
    # check will not be run.
    depends: str | list[str] = field(default_factory=list)

    def __post_init__(self):
        if isinstance(self.depends, str):
            self.depends = [self.depends]

    def parameterize(self, **parameters):
        """Parameterize this check."""
        return replace(
            self,
            parameters=parameters,
            depends=[dep.format(**parameters) for dep in self.depends],
        )

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
        return self.result(CheckStatus.OK)

    def fail(self, **data) -> CheckResult:
        """Shortcut to generate FAIL status."""
        return self.result(CheckStatus.FAILURE)

    def check(self) -> CheckResult | CheckStatus:
        """Perform the check and return a result or status."""
        raise NotImplementedError("Please override in subclass.")

    def read_result(self, path) -> CheckResult:
        """Read results from the file at the given path."""
        data = json.loads(path.read_text())
        return deserialize(TaggedSubclass[CheckResult], data)

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

    def wrapped_check(self):
        try:
            results = self.check()
            if not isinstance(results, CheckResult):
                if isinstance(results, dict):
                    statuses = {
                        k: CheckStatus.OK if success else CheckStatus.FAILURE
                        for k, success in results.items()
                    }
                    results = self.result(CheckStatus.OK, statuses=statuses)
                elif results in (self.ok, self.fail):
                    results = results()
                else:
                    results = self.result(status=results)
        except Exception as exc:
            results = self.result(
                CheckStatus.ERROR, exception=CheckException.from_exception(exc)
            )
        return results

    def __call__(self, write=True):
        results = self.wrapped_check()
        if write:
            results.save(self.directory)
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

    # Parameterizations for the checks
    parameterizations: dict[str, list[str]] = field(default_factory=dict)

    # List of checks to execute. TaggedSubclass[T] makes it so that we can serialize
    # and deserialize subclasses of HealthCheck (the class reference is in the "class"
    # field of each check).
    checks: dict[str, TaggedSubclass[HealthCheck]] = field(default_factory=dict)

    def __post_init__(self):

        all_checks = {}

        # Parameterize the checks
        for name, check in self.checks.items():
            params = re.findall(pattern=r"\{([a-z_]+)\}", string=name)
            if params:
                for comb in itertools.product(
                    *[self.parameterizations[p] for p in params]
                ):
                    zipped = dict(zip(params, comb))
                    concrete_name = name.format(**zipped)
                    all_checks[concrete_name] = check.parameterize(**zipped)
            else:
                all_checks[name] = check

        self.checks = all_checks

        # Set each check's directory based on the root directory and the check's name.
        for name, check in self.checks.items():
            check.name = name
            check.directory = self.directory / check.name

        # Topological sort of the checks
        graph = {name: check.depends for name, check in self.checks.items()}
        order = TopologicalSorter(graph).static_order()
        self.checks = {name: self.checks[name] for name in order}


config = gifnoc.define(
    field="sarc.health_monitor",
    model=HealthMonitorConfig,
)
