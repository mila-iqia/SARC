import logging
import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import timedelta
from functools import partial
from types import SimpleNamespace
from typing import Optional

from sarc.alerts.cache import Timespan
from sarc.jobs.job import SlurmJob

from .common import CheckException, HealthCheck
from .fixtures import latest_jobs

logger = logging.getLogger(__name__)


def build_environment(job, methods, extra):
    job_fields = {k: getattr(job, k) for k in dir(job) if not k.startswith("_")}
    methods = {method.__name__: partial(method, job) for method in methods}
    return {
        **job_fields,
        **methods,
        **extra,
        "job": job,
    }


utilities = []


def register_utility(fn):
    utilities.append(fn)
    return fn


@register_utility
def ran_for(job: SlurmJob, **timeparts):
    delta = timedelta(**timeparts)
    return job.elapsed_time >= delta.total_seconds()


@register_utility
def allocated_gpu(job: SlurmJob):
    return bool(job.allocated.gres_gpu)


@dataclass
class FilterResults:
    results: list[SlurmJob] = field(default_factory=list)
    total_count: int = 0
    count: int = 0
    exception_count: int = 0
    exc: Optional[CheckException] = None
    counts: dict[str, int] = field(default_factory=dict)


@dataclass
class FilterCheckBase(HealthCheck):
    # Python expressions to test
    filters: list[str] = field(default_factory=list)

    # Exception messages to ignore when evaluating a filter
    ignore: list[str] = field(default_factory=list)

    def __post_init__(self):
        super().__post_init__()

        def subfn(m):
            prop = m.groups()[0]
            self.ignore.append(f"'NoneType' object has no attribute '{prop}'")
            return f".{prop}"

        self.filters = [
            re.sub(string=expr, pattern=r"\?\.([A-Za-z0-9_]+)", repl=subfn)
            for expr in self.filters
        ]

    def filter_jobs(self, jobs):
        params = SimpleNamespace(**self.parameters)
        f = FilterResults(
            results=[],
            total_count=len(jobs),
            counts={expr: 0 for expr in self.filters},
        )

        for job in jobs:
            for expr in self.filters:
                try:
                    env = build_environment(job, utilities, {"params": params})
                    if not eval(expr, {}, env):
                        break
                except Exception as _exc:
                    if str(_exc) not in self.ignore:
                        logger.warning(
                            f"Error with filter '{expr}': {_exc}", exc_info=_exc
                        )
                        f.exc = _exc
                        f.exception_count += 1
                    break
                f.counts[expr] += 1
            else:
                f.results.append(job)
                f.count += 1

        return f

    def fetch_jobs(self, period):
        jobs = latest_jobs(period)
        return self.filter_jobs(jobs)


@dataclass
class FilterCheck(FilterCheckBase):
    # How far back to fetch data
    period: Timespan = Timespan("1h")

    # Minimum number of results expected
    min_count: int | None = None

    def check(self):
        f = self.fetch_jobs(self.period)

        data = {
            "count": f.count,
            "total_count": f.total_count,
            "exception_count": f.exception_count,
            "exception_sample": f.exc and CheckException.from_exception(f.exc),
            "counts_after_filters": f.counts,
        }
        if f.count >= self.min_count:
            return self.ok(data=data)
        else:
            return self.fail(data=data)
