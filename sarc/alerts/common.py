"""Core classes for the health monitor."""

from __future__ import annotations

import itertools
import logging
import re
from dataclasses import dataclass, field, replace
from datetime import datetime
from enum import Enum
from graphlib import TopologicalSorter
from typing import Callable, Self, cast

from gifnoc.std import time
from serieux import TaggedSubclass

logger = logging.getLogger(__name__)


class CheckStatus(str, Enum):
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
    def from_exception(exc: Exception) -> CheckException:
        return CheckException(
            type=type(exc).__qualname__,
            message=str(exc),
        )


@dataclass
class CheckResult:
    """Results of a check."""

    # Name of the health check that has this result
    name: str

    # Global status of the check
    status: CheckStatus = CheckStatus.ABSENT

    # Statuses of individual checks
    # Always a dict in current code
    statuses: dict[str, CheckStatus] = field(default_factory=dict)

    # Information about the exception, if the check has ERROR status
    exception: CheckException | None = None

    # Date at which the check finished
    issue_date: datetime = field(default_factory=lambda: time.now().astimezone())

    # Check object
    check: TaggedSubclass[HealthCheck] | None = None

    def get_failures(self) -> dict[str, CheckStatus]:
        """Return a dictionary of status/substatus: CheckStatus for failures."""
        failure_statuses = (CheckStatus.FAILURE, CheckStatus.ERROR)
        results: dict[str, CheckStatus] = {}
        if self.status in failure_statuses:
            results[self.name] = self.status
        results.update(
            {
                f"{self.name}/{k}": cast(CheckStatus, status)
                for k, status in self.statuses.items()
                if status in failure_statuses
            }
        )
        return results

    def log_result(self) -> str:
        """Log check result with appropriate level for polling mode. Return logged message."""
        prefix = f"[{self.name}] {self.status.name}"
        if self.status == CheckStatus.FAILURE:
            failures = ", ".join(self.get_failures().keys())
            desc = f"{prefix}: {failures}"
            logger.warning(desc)
        elif self.status == CheckStatus.ERROR:
            msg = (
                f"{self.exception.type}: {self.exception.message}"
                if self.exception
                else "Unknown error"
            )
            desc = f"{prefix}: {msg}"
            logger.error(desc)
        else:
            desc = prefix
            logger.info(desc)
        return desc


@dataclass
class HealthCheck:
    """Base class for health checks."""

    __result_class__ = CheckResult

    # Whether the check is active or not
    active: bool

    # Name of the check
    name: str = "NOTSET"

    # Parameters of the check
    parameters: dict[str, str] = field(default_factory=dict)

    # Other checks on which this check depends. If these checks fail, this
    # check will not be run.
    depends: list[str] | str = field(default_factory=list)

    def __post_init__(self):
        if isinstance(self.depends, str):
            self.depends = [self.depends]

    def parameterize(self, **parameters) -> Self:
        """Parameterize this check.

        * Set the `parameters` field
        * Fill in the parameters in the name of each dependency in `depends`
        """
        return replace(
            self,
            parameters=parameters,
            depends=[dep.format(**parameters) for dep in self.depends],
        )

    def result(self, status: CheckStatus, **kwargs) -> CheckResult:
        """Generate a result with the given status."""
        now = time.now()
        if status not in iter(CheckStatus):
            opts = ", ".join(item.value for item in CheckStatus)
            raise ValueError(f"Invalid status: {status}. Valid statuses are: {opts}")
        return self.__result_class__(
            name=self.name,
            status=status,
            issue_date=now,
            check=self,
            **kwargs,
        )

    def ok(self, **kwargs) -> CheckResult:
        """Shortcut to generate OK status."""
        return self.result(CheckStatus.OK, **kwargs)

    def fail(self, **kwargs) -> CheckResult:
        """Shortcut to generate FAIL status."""
        return self.result(CheckStatus.FAILURE, **kwargs)

    def check(
        self,
    ) -> CheckResult | CheckStatus | dict[str, bool] | Callable[[], CheckResult]:
        """Perform the check and return a result or status."""
        raise NotImplementedError("Please override in subclass.")

    def wrapped_check(self) -> CheckResult:
        """Wrap the check function.

        * If returns self.ok or self.fail, generate a CheckResult from that.
        * If there is an exception, generate ERROR result.
        """
        try:
            raw_results = self.check()
            if not isinstance(raw_results, CheckResult):
                if isinstance(raw_results, dict):
                    statuses = {
                        k: CheckStatus.OK if success else CheckStatus.FAILURE
                        for k, success in raw_results.items()
                    }
                    results = self.result(CheckStatus.OK, statuses=statuses)
                elif isinstance(raw_results, CheckStatus):
                    results = self.result(status=raw_results)
                else:
                    results = raw_results()
            else:
                results = raw_results
        except Exception as exc:  # pylint: disable=W0718
            results = self.result(
                CheckStatus.ERROR, exception=CheckException.from_exception(exc)
            )
        return results

    def __call__(self) -> CheckResult:
        """Perform the check"""
        return self.wrapped_check()


@dataclass
class HealthMonitorConfig:
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

        # Set each check's name.
        for name, check in self.checks.items():
            check.name = name

        # Topological sort of the checks
        graph = {name: check.depends for name, check in self.checks.items()}
        order = TopologicalSorter(graph).static_order()
        self.checks = {name: self.checks[name] for name in order}
