class ClusterNotFound(Exception):
    pass


class FetchTimeout(Exception):
    """Raised when fetching data from a cluster exceeds the cluster's timeout."""
