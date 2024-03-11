import logging
import re
from dataclasses import dataclass, field
from datetime import timedelta
from functools import partial
from types import SimpleNamespace

from gifnoc.std import time

from .common import CheckException, HealthCheck
from .fixtures import jobs_last_hour

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


def ran_for(job, **timeparts):
    delta = timedelta(**timeparts)
    return job.start_time and ((job.end_time or time.now()) - job.start_time) >= delta


@dataclass
class FilterCheck(HealthCheck):
    # How far back to fetch data
    period: timedelta = None  # timedelta(hours=1)

    # Python expressions to test
    filters: list[str] = field(default_factory=list)

    # Exception messages to ignore when evaluating a filter
    ignore: list[str] = field(default_factory=list)

    # Minimum number of results expected
    min_count: int | None = None

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

    def check(self):
        params = SimpleNamespace(**self.parameters)
        jobs = jobs_last_hour()
        count = 0
        counts = {expr: 0 for expr in self.filters}
        total_count = len(jobs)
        exception_count = 0
        exc = None
        for job in jobs:
            for expr in self.filters:
                try:
                    env = build_environment(job, [ran_for], {"params": params})
                    if not eval(expr, {}, env):
                        break
                except Exception as _exc:
                    if str(_exc) not in self.ignore:
                        logger.warning(
                            f"Error with filter '{expr}': {_exc}", exc_info=_exc
                        )
                        exc = _exc
                        exception_count += 1
                    break
                counts[expr] += 1
            else:
                count += 1
        data = {
            "count": count,
            "total_count": total_count,
            "exception_count": exception_count,
            "exception_sample": exc and CheckException.from_exception(exc),
            "counts_after_filters": counts,
        }
        if count >= self.min_count:
            return self.ok(data=data)
        else:
            return self.fail(data=data)
