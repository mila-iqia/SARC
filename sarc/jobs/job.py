from __future__ import annotations

from functools import cache

from ..client.job import _jobs_collection


@cache
def get_clusters():
    """Fetch all possible clusters"""
    # NB: Is this function still useful ? Currently used only in sarc.cli.utils
    jobs = _jobs_collection().get_collection()
    return jobs.distinct("cluster_name", {})
