"""QThread worker classes for async SARC API calls."""
from __future__ import annotations

from PyQt6.QtCore import QThread, pyqtSignal

from sarc.rest.client import SarcApiClient


class ConnectWorker(QThread):
    success = pyqtSignal(list)   # list of cluster names
    error = pyqtSignal(str)

    def __init__(self, client: SarcApiClient):
        super().__init__()
        self.client = client

    def run(self):
        try:
            clusters = self.client.cluster_list()
            self.success.emit(clusters)
        except Exception as exc:
            self.error.emit(str(exc))


class SummaryWorker(QThread):
    success = pyqtSignal(int, int, list)   # job_count, user_count, clusters
    error = pyqtSignal(str)

    def __init__(self, client: SarcApiClient):
        super().__init__()
        self.client = client

    def run(self):
        try:
            job_count = self.client.job_count()
            user_list = self.client.user_list(per_page=1)
            clusters = self.client.cluster_list()
            self.success.emit(job_count, user_list.total, clusters)
        except Exception as exc:
            self.error.emit(str(exc))


class ClustersWorker(QThread):
    success = pyqtSignal(list)   # list of (cluster_name, count)
    error = pyqtSignal(str)

    def __init__(self, client: SarcApiClient):
        super().__init__()
        self.client = client

    def run(self):
        try:
            clusters = self.client.cluster_list()
            counts: list[tuple[str, int]] = []
            for cl in clusters:
                try:
                    cnt = self.client.job_count(cluster=cl)
                    counts.append((cl, cnt))
                except Exception:
                    counts.append((cl, -1))
            self.success.emit(counts)
        except Exception as exc:
            self.error.emit(str(exc))


class ClusterListWorker(QThread):
    """Fetches cluster list only (for populating dropdowns)."""
    success = pyqtSignal(list)
    error = pyqtSignal(str)

    def __init__(self, client: SarcApiClient):
        super().__init__()
        self.client = client

    def run(self):
        try:
            clusters = self.client.cluster_list()
            self.success.emit(clusters)
        except Exception as exc:
            self.error.emit(str(exc))


class UsersWorker(QThread):
    success = pyqtSignal(object)
    error = pyqtSignal(str)

    def __init__(self, client: SarcApiClient, query, mtype, page, per_page):
        super().__init__()
        self.client = client
        self.query = query
        self.mtype = mtype
        self.page = page
        self.per_page = per_page

    def run(self):
        try:
            result = self.client.user_list(
                display_name=self.query,
                member_type=self.mtype,
                page=self.page,
                per_page=self.per_page,
            )
            self.success.emit(result)
        except Exception as exc:
            self.error.emit(str(exc))


class UserDetailsWorker(QThread):
    success = pyqtSignal(object, dict)   # user, job_counts
    error = pyqtSignal(str)

    def __init__(self, client: SarcApiClient, uuid_str: str):
        super().__init__()
        self.client = client
        self.uuid_str = uuid_str

    def run(self):
        try:
            user = self.client.user_by_id(self.uuid_str)
            clusters = self.client.cluster_list()
            job_counts: dict[str, int] = {}
            for cl in clusters:
                try:
                    login = user.matching_ids.get(cl)
                    if login:
                        count = self.client.job_count(cluster=cl, username=login)
                        job_counts[cl] = count
                except Exception:
                    job_counts[cl] = -1
            self.success.emit(user, job_counts)
        except Exception as exc:
            self.error.emit(str(exc))


class JobsWorker(QThread):
    success = pyqtSignal(object)
    error = pyqtSignal(str)

    def __init__(self, client: SarcApiClient, cluster, username, job_state, start, end, page, per_page):
        super().__init__()
        self.client = client
        self.cluster = cluster
        self.username = username
        self.job_state = job_state
        self.start_dt = start
        self.end_dt = end
        self.page = page
        self.per_page = per_page

    def run(self):
        try:
            result = self.client.job_list(
                cluster=self.cluster,
                username=self.username,
                job_state=self.job_state,
                start=self.start_dt,
                end=self.end_dt,
                page=self.page,
                per_page=self.per_page,
            )
            self.success.emit(result)
        except Exception as exc:
            self.error.emit(str(exc))


class JobDetailsWorker(QThread):
    success = pyqtSignal(object)
    error = pyqtSignal(str)

    def __init__(self, client: SarcApiClient, job_internal_id: str):
        super().__init__()
        self.client = client
        self.job_internal_id = job_internal_id

    def run(self):
        try:
            job = self.client.job_by_id(self.job_internal_id)
            self.success.emit(job)
        except Exception as exc:
            self.error.emit(str(exc))
