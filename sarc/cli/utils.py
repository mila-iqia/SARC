from __future__ import annotations

from functools import cache

from sarc.client.job import _jobs_collection


@cache
def get_clusters():
    """Fetch all possible clusters"""
    # NB: Is this function still useful ? Currently used only in sarc.cli.utils
    jobs = _jobs_collection().get_collection()
    return jobs.distinct("cluster_name", {})


class ChoicesContainer:
    def __init__(self, choices):
        self.choices = choices

    def __contains__(self, item):
        return item in self.choices

    def __iter__(self):
        return iter(self.choices)


clusters = ChoicesContainer(list(get_clusters()))
