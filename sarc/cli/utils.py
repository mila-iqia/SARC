from __future__ import annotations

from collections.abc import Iterable
from functools import cache

from sarc.client.job import _jobs_collection


@cache
def get_clusters() -> Iterable[str]:
    """Fetch all possible clusters"""
    # NB: Is this function still useful ? Currently used only in sarc.cli.utils
    jobs = _jobs_collection().get_collection()
    return jobs.distinct("cluster_name", {})


class ChoicesContainer[T]:
    def __init__(self, choices: list[T]):
        self.choices = choices

    def __contains__(self, item: T):
        return item in self.choices

    def __iter__(self) -> Iterable[T]:
        return iter(self.choices)


clusters = ChoicesContainer(list(get_clusters()))
