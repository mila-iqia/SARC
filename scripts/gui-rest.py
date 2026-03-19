"""
SARC Database Browser - PyQt6 GUI for browsing SARC via REST API.

Usage:
    python scripts/gui-rest.py
"""
from __future__ import annotations

import sys
import os
from datetime import datetime, timedelta, timezone
from typing import Any

# Make sarc importable when running from scripts/ directory
_script_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_script_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QTabWidget, QTableWidget, QTableWidgetItem, QLineEdit, QPushButton,
    QLabel, QComboBox, QDialog, QScrollArea, QGroupBox, QFormLayout,
    QHeaderView, QSplitter, QFrame, QDateEdit, QMessageBox, QSizePolicy,
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QDate
from PyQt6.QtGui import QFont, QColor

from sarc.rest.client import SarcApiClient
from sarc.client.job import SlurmState
from sarc.core.models.users import MemberType


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def fmt_datetime(dt: datetime | None) -> str:
    if dt is None:
        return "N/A"
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def fmt_elapsed(seconds: float | None) -> str:
    if seconds is None or seconds != seconds:  # NaN check
        return "N/A"
    try:
        secs = int(seconds)
        h = secs // 3600
        m = (secs % 3600) // 60
        s = secs % 60
        return f"{h}h {m}m {s}s"
    except (TypeError, ValueError):
        return "N/A"


def fmt_memory(mem_mb: int | None) -> str:
    if mem_mb is None:
        return "N/A"
    if mem_mb >= 1024:
        return f"{mem_mb / 1024:.1f} GB"
    return f"{mem_mb} MB"


def fmt_float(v: float | None) -> str:
    if v is None:
        return "N/A"
    try:
        if v != v:  # NaN check
            return "N/A"
        return f"{v:.2f}"
    except (TypeError, ValueError):
        return "N/A"


def get_member_type(user_data) -> str:
    """Safely extract current member type from a ValidField."""
    try:
        mt = user_data.member_type
        if mt is None:
            return "N/A"
        val = mt.get_value()
        if val is None:
            return "N/A"
        if hasattr(val, "value"):
            return val.value
        return str(val)
    except Exception:
        try:
            if user_data.member_type.values:
                last = user_data.member_type.values[-1]
                v = last.value
                if hasattr(v, "value"):
                    return v.value
                return str(v)
        except Exception:
            pass
        return "N/A"


# ---------------------------------------------------------------------------
# Worker threads (QThread-based)
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Job Details Dialog
# ---------------------------------------------------------------------------

class JobDetailsDialog(QDialog):
    def __init__(self, parent: QWidget, job):
        super().__init__(parent)
        self.setWindowTitle(f"Job Details — {job.name} ({job.job_id})")
        self.resize(720, 650)

        layout = QVBoxLayout(self)

        self._scroll = QScrollArea()
        self._scroll.setWidgetResizable(True)
        layout.addWidget(self._scroll)

        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.accept)
        layout.addWidget(close_btn, alignment=Qt.AlignmentFlag.AlignRight)

        self._show_job(job)

    def _show_job(self, job):
        content = QWidget()
        layout = QVBoxLayout(content)
        layout.setAlignment(Qt.AlignmentFlag.AlignTop)

        def add_section(title: str, rows: list[tuple[str, str]]):
            group = QGroupBox(title)
            form = QFormLayout(group)
            form.setLabelAlignment(Qt.AlignmentFlag.AlignRight)
            for label, value in rows:
                val_label = QLabel(str(value))
                val_label.setWordWrap(True)
                val_label.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
                form.addRow(label + ":", val_label)
            layout.addWidget(group)

        # Basic Info
        add_section("Basic Info", [
            ("Job ID (Slurm)", str(job.job_id)),
            ("Internal ID", str(job.id)),
            ("Name", job.name),
            ("User", job.user),
            ("Group", job.group),
            ("Account", job.account),
            ("Cluster", job.cluster_name),
            ("State", job.job_state.value),
            ("Partition", job.partition),
            ("QOS", job.qos or "N/A"),
            ("Priority", str(job.priority) if job.priority is not None else "N/A"),
            ("Exit Code", str(job.exit_code) if job.exit_code is not None else "N/A"),
            ("Signal", str(job.signal) if job.signal is not None else "N/A"),
            ("Nodes", ", ".join(job.nodes) if job.nodes else "N/A"),
            ("Work Dir", job.work_dir),
            ("Constraints", job.constraints or "N/A"),
        ])

        # Timing
        add_section("Timing", [
            ("Submit Time", fmt_datetime(job.submit_time)),
            ("Start Time", fmt_datetime(job.start_time)),
            ("End Time", fmt_datetime(job.end_time)),
            ("Elapsed Time", fmt_elapsed(job.elapsed_time)),
            ("Time Limit", fmt_elapsed((job.time_limit or 0) * 60) if job.time_limit else "N/A"),
        ])

        # Resources Requested
        req = job.requested
        add_section("Resources Requested", [
            ("CPUs", str(req.cpu) if req.cpu is not None else "N/A"),
            ("Memory", fmt_memory(req.mem)),
            ("Nodes", str(req.node) if req.node is not None else "N/A"),
            ("Billing", str(req.billing) if req.billing is not None else "N/A"),
            ("GPUs (gres)", str(req.gres_gpu) if req.gres_gpu is not None else "N/A"),
            ("GPU Type", req.gpu_type or "N/A"),
        ])

        # Resources Allocated
        alloc = job.allocated
        add_section("Resources Allocated", [
            ("CPUs", str(alloc.cpu) if alloc.cpu is not None else "N/A"),
            ("Memory", fmt_memory(alloc.mem)),
            ("Nodes", str(alloc.node) if alloc.node is not None else "N/A"),
            ("Billing", str(alloc.billing) if alloc.billing is not None else "N/A"),
            ("GPUs (gres)", str(alloc.gres_gpu) if alloc.gres_gpu is not None else "N/A"),
            ("GPU Type", alloc.gpu_type or "N/A"),
        ])

        # Statistics
        stats = job.stored_statistics
        if stats is not None and not stats.empty():
            stat_rows: list[tuple[str, str]] = []
            if stats.gpu_utilization is not None:
                gu = stats.gpu_utilization
                stat_rows += [
                    ("GPU Util (mean)", fmt_float(gu.mean)),
                    ("GPU Util (median)", fmt_float(gu.median)),
                    ("GPU Util (max)", fmt_float(gu.max)),
                ]
            if stats.gpu_memory is not None:
                gm = stats.gpu_memory
                stat_rows += [
                    ("GPU Memory (mean)", fmt_float(gm.mean)),
                    ("GPU Memory (median)", fmt_float(gm.median)),
                    ("GPU Memory (max)", fmt_float(gm.max)),
                ]
            if stats.cpu_utilization is not None:
                cu = stats.cpu_utilization
                stat_rows += [
                    ("CPU Util (mean)", fmt_float(cu.mean)),
                    ("CPU Util (median)", fmt_float(cu.median)),
                    ("CPU Util (max)", fmt_float(cu.max)),
                ]
            if stats.gpu_power is not None:
                gp = stats.gpu_power
                stat_rows += [
                    ("GPU Power (mean)", fmt_float(gp.mean)),
                    ("GPU Power (max)", fmt_float(gp.max)),
                ]
            if stat_rows:
                add_section("Job Statistics", stat_rows)

        layout.addStretch()
        self._scroll.setWidget(content)


# ---------------------------------------------------------------------------
# User Details Panel (embedded widget)
# ---------------------------------------------------------------------------

class UserDetailsPanel(QFrame):
    """Panel showing details for a single user, embedded in the Users tab."""

    def __init__(self, parent: QWidget | None = None):
        super().__init__(parent)
        self.setFrameShape(QFrame.Shape.StyledPanel)
        self.client: SarcApiClient | None = None
        self._worker: UserDetailsWorker | None = None

        layout = QVBoxLayout(self)

        title = QLabel("User Details")
        title_font = QFont()
        title_font.setBold(True)
        title_font.setPointSize(11)
        title.setFont(title_font)
        layout.addWidget(title)

        self._status_label = QLabel("Select a user to see details.")
        layout.addWidget(self._status_label)

        # Scroll area for user info
        self._scroll = QScrollArea()
        self._scroll.setWidgetResizable(True)
        self._scroll.setVisible(False)
        layout.addWidget(self._scroll)

    def load_user(self, uuid_str: str):
        self._scroll.setVisible(False)
        self._status_label.setVisible(True)
        self._status_label.setStyleSheet("")
        self._status_label.setText("Loading...")

        if self._worker is not None:
            self._worker.success.disconnect()
            self._worker.error.disconnect()

        self._worker = UserDetailsWorker(self.client, uuid_str)
        self._worker.success.connect(self._show_user)
        self._worker.error.connect(self._show_error)
        QApplication.setOverrideCursor(Qt.CursorShape.WaitCursor)
        self._worker.start()

    def _show_error(self, msg: str):
        QApplication.restoreOverrideCursor()
        self._status_label.setText(f"Error: {msg}")
        self._status_label.setStyleSheet("color: red;")

    def _show_user(self, user, job_counts: dict[str, int]):
        QApplication.restoreOverrideCursor()
        self._status_label.setVisible(False)
        self._scroll.setVisible(True)

        content = QWidget()
        layout = QFormLayout(content)
        layout.setLabelAlignment(Qt.AlignmentFlag.AlignRight)

        def add_row(label: str, value: str):
            val_lbl = QLabel(value)
            val_lbl.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
            layout.addRow(label + ":", val_lbl)

        add_row("Name", user.display_name)
        add_row("Email", user.email)
        add_row("UUID", str(user.uuid))
        add_row("Member Type", get_member_type(user))

        if user.matching_ids:
            sep = QLabel("Cluster Logins")
            bold = QFont()
            bold.setBold(True)
            sep.setFont(bold)
            layout.addRow(sep)
            for cluster, login in user.matching_ids.items():
                add_row(f"  {cluster}", login)

        if job_counts:
            sep2 = QLabel("Job Counts")
            bold2 = QFont()
            bold2.setBold(True)
            sep2.setFont(bold2)
            layout.addRow(sep2)
            for cluster, count in job_counts.items():
                count_str = str(count) if count >= 0 else "error"
                add_row(f"  {cluster}", count_str)

        self._scroll.setWidget(content)


# ---------------------------------------------------------------------------
# Summary Tab
# ---------------------------------------------------------------------------

class SummaryTab(QWidget):
    go_to_jobs = pyqtSignal()
    go_to_users = pyqtSignal()
    go_to_jobs_cluster = pyqtSignal(str)

    def __init__(self, parent: QWidget | None = None):
        super().__init__(parent)
        self._worker: SummaryWorker | None = None
        self._build_ui()

    def _build_ui(self):
        layout = QVBoxLayout(self)

        # Header
        header = QHBoxLayout()
        title = QLabel("Summary")
        font = QFont()
        font.setBold(True)
        font.setPointSize(14)
        title.setFont(font)
        header.addWidget(title)
        header.addStretch()
        refresh_btn = QPushButton("Refresh")
        refresh_btn.clicked.connect(lambda: self.refresh())
        header.addWidget(refresh_btn)
        layout.addLayout(header)

        # Cards
        cards_layout = QHBoxLayout()

        jobs_group = QGroupBox("Total Jobs")
        jobs_vbox = QVBoxLayout(jobs_group)
        self._jobs_label = QLabel("—")
        jobs_font = QFont()
        jobs_font.setBold(True)
        jobs_font.setPointSize(24)
        self._jobs_label.setFont(jobs_font)
        self._jobs_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        jobs_vbox.addWidget(self._jobs_label)
        view_jobs_btn = QPushButton("View Jobs")
        view_jobs_btn.clicked.connect(self.go_to_jobs)
        jobs_vbox.addWidget(view_jobs_btn)
        cards_layout.addWidget(jobs_group)

        users_group = QGroupBox("Total Users")
        users_vbox = QVBoxLayout(users_group)
        self._users_label = QLabel("—")
        users_font = QFont()
        users_font.setBold(True)
        users_font.setPointSize(24)
        self._users_label.setFont(users_font)
        self._users_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        users_vbox.addWidget(self._users_label)
        view_users_btn = QPushButton("View Users")
        view_users_btn.clicked.connect(self.go_to_users)
        users_vbox.addWidget(view_users_btn)
        cards_layout.addWidget(users_group)

        layout.addLayout(cards_layout)

        # Clusters list
        clusters_group = QGroupBox("Clusters")
        clusters_layout = QVBoxLayout(clusters_group)
        self._clusters_table = QTableWidget(0, 1)
        self._clusters_table.setHorizontalHeaderLabels(["Cluster Name"])
        self._clusters_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self._clusters_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._clusters_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._clusters_table.doubleClicked.connect(self._on_cluster_double_click)
        clusters_layout.addWidget(self._clusters_table)
        hint = QLabel("Double-click a cluster to view its jobs.")
        hint.setStyleSheet("color: gray;")
        clusters_layout.addWidget(hint)
        layout.addWidget(clusters_group)

    def refresh(self, client: SarcApiClient | None = None):
        if client is not None:
            self._client = client
        if not hasattr(self, "_client") or self._client is None:
            return
        self._jobs_label.setText("...")
        self._users_label.setText("...")
        self._worker = SummaryWorker(self._client)
        self._worker.success.connect(self._on_success)
        self._worker.error.connect(self._on_error)
        QApplication.setOverrideCursor(Qt.CursorShape.WaitCursor)
        self._worker.start()

    def set_client(self, client: SarcApiClient):
        self._client = client

    def _on_success(self, job_count: int, user_count: int, clusters: list):
        QApplication.restoreOverrideCursor()
        self._jobs_label.setText(str(job_count))
        self._users_label.setText(str(user_count))
        self._clusters_table.setRowCount(0)
        for cl in clusters:
            row = self._clusters_table.rowCount()
            self._clusters_table.insertRow(row)
            item = QTableWidgetItem(cl)
            item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            self._clusters_table.setItem(row, 0, item)

    def _on_error(self, msg: str):
        QApplication.restoreOverrideCursor()
        self._jobs_label.setText("error")
        self._users_label.setText("error")
        QMessageBox.critical(self, "Error", f"Failed to load summary:\n{msg}")

    def _on_cluster_double_click(self, index):
        row = index.row()
        item = self._clusters_table.item(row, 0)
        if item:
            self.go_to_jobs_cluster.emit(item.text())


# ---------------------------------------------------------------------------
# Users Tab
# ---------------------------------------------------------------------------

class UsersTab(QWidget):
    def __init__(self, parent: QWidget | None = None):
        super().__init__(parent)
        self.client: SarcApiClient | None = None
        self._page = 1
        self._per_page = 50
        self._total = 0
        self._uuid_map: dict[int, str] = {}  # row -> uuid
        self._worker: UsersWorker | None = None
        self._build_ui()

    def _build_ui(self):
        layout = QVBoxLayout(self)

        # Search bar
        search_layout = QHBoxLayout()
        search_layout.addWidget(QLabel("Search (name/email):"))
        self._search_input = QLineEdit()
        self._search_input.setPlaceholderText("Enter name or email...")
        self._search_input.setMaximumWidth(250)
        self._search_input.returnPressed.connect(self._do_search)
        search_layout.addWidget(self._search_input)

        search_layout.addWidget(QLabel("Member type:"))
        self._type_combo = QComboBox()
        self._type_combo.addItem("(all)")
        for m in MemberType:
            self._type_combo.addItem(m.value)
        search_layout.addWidget(self._type_combo)

        search_btn = QPushButton("Search")
        search_btn.clicked.connect(lambda: self._do_search())
        search_layout.addWidget(search_btn)
        search_layout.addStretch()
        layout.addLayout(search_layout)

        # Splitter: table left, details right
        splitter = QSplitter(Qt.Orientation.Horizontal)

        left_widget = QWidget()
        left_layout = QVBoxLayout(left_widget)
        left_layout.setContentsMargins(0, 0, 0, 0)

        self._table = QTableWidget(0, 3)
        self._table.setHorizontalHeaderLabels(["Name", "Email", "Member Type"])
        self._table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self._table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        self._table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        self._table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._table.itemSelectionChanged.connect(self._on_selection_changed)
        left_layout.addWidget(self._table)

        # Pagination
        page_layout = QHBoxLayout()
        self._prev_btn = QPushButton("< Prev")
        self._prev_btn.clicked.connect(self._prev_page)
        page_layout.addWidget(self._prev_btn)
        self._page_label = QLabel("Page 1")
        page_layout.addWidget(self._page_label)
        self._next_btn = QPushButton("Next >")
        self._next_btn.clicked.connect(self._next_page)
        page_layout.addWidget(self._next_btn)
        self._total_label = QLabel("")
        page_layout.addWidget(self._total_label)
        page_layout.addStretch()
        left_layout.addLayout(page_layout)

        splitter.addWidget(left_widget)

        self._details_panel = UserDetailsPanel()
        splitter.addWidget(self._details_panel)
        splitter.setStretchFactor(0, 2)
        splitter.setStretchFactor(1, 1)

        layout.addWidget(splitter)

    def trigger_search(self):
        self._do_search()

    def _do_search(self, page: int = 1):
        if self.client is None:
            QMessageBox.warning(self, "Not Connected", "Please connect to a SARC API server first.")
            return
        self._details_panel.client = self.client
        self._page = page
        query = self._search_input.text().strip() or None
        mtype_str = self._type_combo.currentText()
        mtype = None
        if mtype_str != "(all)":
            try:
                mtype = MemberType(mtype_str)
            except ValueError:
                pass

        self._table.setRowCount(0)
        self._uuid_map.clear()

        if self._worker is not None:
            self._worker.success.disconnect()
            self._worker.error.disconnect()

        self._worker = UsersWorker(self.client, query, mtype, page, self._per_page)
        self._worker.success.connect(self._on_users_loaded)
        self._worker.error.connect(self._on_error)
        QApplication.setOverrideCursor(Qt.CursorShape.WaitCursor)
        self._worker.start()

    def _on_users_loaded(self, result):
        QApplication.restoreOverrideCursor()
        self._total = result.total
        self._table.setRowCount(0)
        self._uuid_map.clear()

        for user in result.users:
            row = self._table.rowCount()
            self._table.insertRow(row)
            mt = get_member_type(user)
            for col, text in enumerate([user.display_name, user.email, mt]):
                item = QTableWidgetItem(text)
                item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
                self._table.setItem(row, col, item)
            self._uuid_map[row] = str(user.uuid)

        total_pages = max(1, (result.total + self._per_page - 1) // self._per_page)
        self._page_label.setText(f"Page {self._page} / {total_pages}")
        self._total_label.setText(f"Total: {result.total} users")

    def _on_error(self, msg: str):
        QApplication.restoreOverrideCursor()
        QMessageBox.critical(self, "Error", f"Failed to load users:\n{msg}")

    def _on_selection_changed(self):
        selected = self._table.selectedItems()
        if not selected:
            return
        row = self._table.currentRow()
        uuid_str = self._uuid_map.get(row)
        if uuid_str and self.client:
            self._details_panel.client = self.client
            self._details_panel.load_user(uuid_str)

    def _prev_page(self):
        if self._page > 1:
            self._do_search(page=self._page - 1)

    def _next_page(self):
        total_pages = max(1, (self._total + self._per_page - 1) // self._per_page)
        if self._page < total_pages:
            self._do_search(page=self._page + 1)


# ---------------------------------------------------------------------------
# Clusters Tab
# ---------------------------------------------------------------------------

class ClustersTab(QWidget):
    go_to_jobs_cluster = pyqtSignal(str)

    def __init__(self, parent: QWidget | None = None):
        super().__init__(parent)
        self.client: SarcApiClient | None = None
        self._worker: ClustersWorker | None = None
        self._build_ui()

    def _build_ui(self):
        layout = QVBoxLayout(self)

        header = QHBoxLayout()
        title = QLabel("Clusters")
        font = QFont()
        font.setBold(True)
        font.setPointSize(14)
        title.setFont(font)
        header.addWidget(title)
        header.addStretch()
        refresh_btn = QPushButton("Refresh")
        refresh_btn.clicked.connect(lambda: self.load_clusters())
        header.addWidget(refresh_btn)
        layout.addLayout(header)

        hint = QLabel("Double-click a cluster to view its jobs.")
        hint.setStyleSheet("color: gray;")
        layout.addWidget(hint)

        self._table = QTableWidget(0, 2)
        self._table.setHorizontalHeaderLabels(["Cluster Name", "Total Jobs"])
        self._table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self._table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        self._table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._table.doubleClicked.connect(self._on_double_click)
        layout.addWidget(self._table)

    def load_clusters(self):
        if self.client is None:
            QMessageBox.warning(self, "Not Connected", "Please connect to a SARC API server first.")
            return
        self._table.setRowCount(0)

        if self._worker is not None:
            self._worker.success.disconnect()
            self._worker.error.disconnect()

        self._worker = ClustersWorker(self.client)
        self._worker.success.connect(self._on_loaded)
        self._worker.error.connect(self._on_error)
        QApplication.setOverrideCursor(Qt.CursorShape.WaitCursor)
        self._worker.start()

    def _on_loaded(self, counts: list):
        QApplication.restoreOverrideCursor()
        self._table.setRowCount(0)
        for cl, cnt in counts:
            row = self._table.rowCount()
            self._table.insertRow(row)
            count_str = str(cnt) if cnt >= 0 else "error"
            for col, text in enumerate([cl, count_str]):
                item = QTableWidgetItem(text)
                item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
                self._table.setItem(row, col, item)

    def _on_error(self, msg: str):
        QApplication.restoreOverrideCursor()
        QMessageBox.critical(self, "Error", f"Failed to load clusters:\n{msg}")

    def _on_double_click(self, index):
        row = index.row()
        item = self._table.item(row, 0)
        if item:
            self.go_to_jobs_cluster.emit(item.text())


# ---------------------------------------------------------------------------
# Jobs Tab
# ---------------------------------------------------------------------------

class JobsTab(QWidget):
    def __init__(self, parent: QWidget | None = None):
        super().__init__(parent)
        self.client: SarcApiClient | None = None
        self._page = 1
        self._per_page = 50
        self._total = 0
        self._job_map: dict[int, object] = {}  # row -> SlurmJob
        self._worker: JobsWorker | None = None
        self._cluster_worker: ClusterListWorker | None = None
        self._pending_cluster: str | None = None  # cluster to select after combo loads
        self._build_ui()

    def _build_ui(self):
        layout = QVBoxLayout(self)

        # Filters
        filters_group = QGroupBox("Filters")
        filters_layout = QVBoxLayout(filters_group)

        # Row 1: cluster, username, state
        row1 = QHBoxLayout()
        row1.addWidget(QLabel("Cluster:"))
        self._cluster_combo = QComboBox()
        self._cluster_combo.addItem("(all)")
        self._cluster_combo.setMinimumWidth(140)
        row1.addWidget(self._cluster_combo)

        row1.addWidget(QLabel("Username:"))
        self._username_input = QLineEdit()
        self._username_input.setMaximumWidth(160)
        self._username_input.returnPressed.connect(self._do_search)
        row1.addWidget(self._username_input)

        row1.addWidget(QLabel("State:"))
        self._state_combo = QComboBox()
        self._state_combo.addItem("(all)")
        for s in SlurmState:
            self._state_combo.addItem(s.value)
        self._state_combo.setMinimumWidth(130)
        row1.addWidget(self._state_combo)
        row1.addStretch()
        filters_layout.addLayout(row1)

        # Row 2: time period + custom dates + search button
        row2 = QHBoxLayout()
        row2.addWidget(QLabel("Period:"))
        self._period_combo = QComboBox()
        for p in ["Last 7 days", "Last 30 days", "Last 90 days", "Custom"]:
            self._period_combo.addItem(p)
        self._period_combo.setCurrentText("Last 30 days")
        self._period_combo.currentTextChanged.connect(self._on_period_change)
        row2.addWidget(self._period_combo)

        row2.addWidget(QLabel("From:"))
        self._start_date = QDateEdit()
        self._start_date.setCalendarPopup(True)
        self._start_date.setDate(QDate.currentDate().addDays(-30))
        self._start_date.setDisplayFormat("yyyy-MM-dd")
        self._start_date.setVisible(False)
        row2.addWidget(self._start_date)

        row2.addWidget(QLabel("To:"))
        self._end_date = QDateEdit()
        self._end_date.setCalendarPopup(True)
        self._end_date.setDate(QDate.currentDate())
        self._end_date.setDisplayFormat("yyyy-MM-dd")
        self._end_date.setVisible(False)
        row2.addWidget(self._end_date)

        self._start_label = row2.itemAt(2).widget()  # "From:" label
        self._end_label = row2.itemAt(4).widget()    # "To:" label
        self._start_label.setVisible(False)
        self._end_label.setVisible(False)

        row2.addStretch()
        search_btn = QPushButton("Search")
        search_btn.clicked.connect(lambda: self._do_search(page=1))
        row2.addWidget(search_btn)
        filters_layout.addLayout(row2)

        layout.addWidget(filters_group)

        # Jobs table
        self._table = QTableWidget(0, 8)
        self._table.setHorizontalHeaderLabels(
            ["Job ID", "Name", "User", "Cluster", "State", "Submit Time", "Elapsed", "GPUs"]
        )
        self._table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        self._table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(5, QHeaderView.ResizeMode.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(6, QHeaderView.ResizeMode.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(7, QHeaderView.ResizeMode.ResizeToContents)
        self._table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._table.doubleClicked.connect(self._on_job_double_click)
        layout.addWidget(self._table)

        # Pagination
        page_layout = QHBoxLayout()
        self._prev_btn = QPushButton("< Prev")
        self._prev_btn.clicked.connect(self._prev_page)
        page_layout.addWidget(self._prev_btn)
        self._page_label = QLabel("Page 1")
        page_layout.addWidget(self._page_label)
        self._next_btn = QPushButton("Next >")
        self._next_btn.clicked.connect(self._next_page)
        page_layout.addWidget(self._next_btn)
        self._total_label = QLabel("")
        page_layout.addWidget(self._total_label)
        page_layout.addStretch()
        hint = QLabel("Double-click a row to see job details.")
        hint.setStyleSheet("color: gray;")
        page_layout.addWidget(hint)
        layout.addLayout(page_layout)

    def _on_period_change(self, text: str):
        is_custom = (text == "Custom")
        self._start_label.setVisible(is_custom)
        self._start_date.setVisible(is_custom)
        self._end_label.setVisible(is_custom)
        self._end_date.setVisible(is_custom)

    def set_cluster_filter(self, cluster: str):
        """Set the cluster combo to the given cluster name if present."""
        idx = self._cluster_combo.findText(cluster)
        if idx >= 0:
            self._cluster_combo.setCurrentIndex(idx)

    def refresh_cluster_combo(self):
        """Reload the cluster list into the combo box."""
        if self.client is None:
            return
        # Don't start a second worker if one is already running
        if self._cluster_worker is not None and self._cluster_worker.isRunning():
            return

        self._cluster_worker = ClusterListWorker(self.client)
        self._cluster_worker.success.connect(self._on_clusters_loaded)
        QApplication.setOverrideCursor(Qt.CursorShape.WaitCursor)
        self._cluster_worker.start()

    def _on_clusters_loaded(self, clusters: list):
        QApplication.restoreOverrideCursor()
        self._cluster_combo.clear()
        self._cluster_combo.addItem("(all)")
        for cl in clusters:
            self._cluster_combo.addItem(cl)
        # Apply pending cluster filter (set before combo was populated)
        if self._pending_cluster is not None:
            idx = self._cluster_combo.findText(self._pending_cluster)
            if idx >= 0:
                self._cluster_combo.setCurrentIndex(idx)
            self._pending_cluster = None
            self._do_search(page=1)

    def _get_time_range(self) -> tuple[datetime | None, datetime | None] | None:
        period = self._period_combo.currentText()
        now = datetime.now(timezone.utc)
        if period == "Last 7 days":
            return now - timedelta(days=7), None
        elif period == "Last 30 days":
            return now - timedelta(days=30), None
        elif period == "Last 90 days":
            return now - timedelta(days=90), None
        else:  # Custom
            qstart = self._start_date.date()
            qend = self._end_date.date()
            start = datetime(qstart.year(), qstart.month(), qstart.day(), tzinfo=timezone.utc)
            end = datetime(qend.year(), qend.month(), qend.day(), tzinfo=timezone.utc)
            return start, end

    def _do_search(self, page: int = 1):
        if self.client is None:
            QMessageBox.warning(self, "Not Connected", "Please connect to a SARC API server first.")
            return

        self._page = page

        # Populate cluster combo on first search if still empty
        if self._cluster_combo.count() <= 1:
            self.refresh_cluster_combo()

        time_range = self._get_time_range()
        if time_range is None:
            return
        start, end = time_range

        cluster_val = self._cluster_combo.currentText()
        cluster = None if cluster_val == "(all)" else cluster_val

        username = self._username_input.text().strip() or None

        state_val = self._state_combo.currentText()
        job_state: SlurmState | None = None
        if state_val != "(all)":
            try:
                job_state = SlurmState(state_val)
            except ValueError:
                pass

        self._table.setRowCount(0)
        self._job_map.clear()

        if self._worker is not None:
            try:
                self._worker.success.disconnect()
                self._worker.error.disconnect()
            except Exception:
                pass

        self._worker = JobsWorker(
            self.client, cluster, username, job_state,
            start, end, page, self._per_page
        )
        self._worker.success.connect(self._on_jobs_loaded)
        self._worker.error.connect(self._on_error)
        QApplication.setOverrideCursor(Qt.CursorShape.WaitCursor)
        self._worker.start()

    def _on_jobs_loaded(self, result):
        QApplication.restoreOverrideCursor()
        self._total = result.total
        self._table.setRowCount(0)
        self._job_map.clear()

        for job in result.jobs:
            gpus = ""
            if job.requested.gres_gpu is not None:
                gpus = str(job.requested.gres_gpu)
                if job.requested.gpu_type:
                    gpus += f" ({job.requested.gpu_type})"

            row = self._table.rowCount()
            self._table.insertRow(row)
            values = [
                str(job.job_id),
                job.name,
                job.user,
                job.cluster_name,
                job.job_state.value,
                fmt_datetime(job.submit_time),
                fmt_elapsed(job.elapsed_time),
                gpus or "N/A",
            ]
            for col, text in enumerate(values):
                item = QTableWidgetItem(text)
                item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
                self._table.setItem(row, col, item)

            self._job_map[row] = job

        total_pages = max(1, (result.total + self._per_page - 1) // self._per_page)
        self._page_label.setText(f"Page {self._page} / {total_pages}")
        self._total_label.setText(f"Total: {result.total} jobs")

    def _on_error(self, msg: str):
        QApplication.restoreOverrideCursor()
        QMessageBox.critical(self, "Error", f"Failed to load jobs:\n{msg}")

    def _on_job_double_click(self, index):
        row = index.row()
        job = self._job_map.get(row)
        if job is None:
            return
        dialog = JobDetailsDialog(self, job)
        dialog.exec()

    def _prev_page(self):
        if self._page > 1:
            self._do_search(page=self._page - 1)

    def _next_page(self):
        total_pages = max(1, (self._total + self._per_page - 1) // self._per_page)
        if self._page < total_pages:
            self._do_search(page=self._page + 1)


# ---------------------------------------------------------------------------
# Main Window
# ---------------------------------------------------------------------------

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("SARC Database Browser")
        self.resize(1100, 750)

        self.client: SarcApiClient | None = None
        self._connect_worker: ConnectWorker | None = None

        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)
        main_layout.setContentsMargins(4, 4, 4, 4)

        # Connection bar
        conn_frame = QFrame()
        conn_frame.setFrameShape(QFrame.Shape.StyledPanel)
        conn_layout = QHBoxLayout(conn_frame)
        conn_layout.addWidget(QLabel("Host:Port:"))
        self._host_input = QLineEdit()
        self._host_input.setPlaceholderText("localhost:1234")
        self._host_input.setText("localhost:1234")
        self._host_input.setMaximumWidth(220)
        self._host_input.returnPressed.connect(self._connect)
        conn_layout.addWidget(self._host_input)

        self._connect_btn = QPushButton("Connect")
        self._connect_btn.clicked.connect(self._connect)
        conn_layout.addWidget(self._connect_btn)

        self._status_label = QLabel("Disconnected")
        self._status_label.setStyleSheet("color: red;")
        self._status_label.setMinimumWidth(180)
        conn_layout.addWidget(self._status_label)

        self._loading_label = QLabel("")
        conn_layout.addWidget(self._loading_label)
        conn_layout.addStretch()
        main_layout.addWidget(conn_frame)

        # Tabs
        self._tabs = QTabWidget()
        main_layout.addWidget(self._tabs)

        self._summary_tab = SummaryTab()
        self._summary_tab.go_to_jobs.connect(self._go_to_jobs)
        self._summary_tab.go_to_users.connect(self._go_to_users)
        self._summary_tab.go_to_jobs_cluster.connect(self._go_to_jobs_with_cluster)
        self._tabs.addTab(self._summary_tab, "Summary")

        self._users_tab = UsersTab()
        self._tabs.addTab(self._users_tab, "Users")

        self._clusters_tab = ClustersTab()
        self._clusters_tab.go_to_jobs_cluster.connect(self._go_to_jobs_with_cluster)
        self._tabs.addTab(self._clusters_tab, "Clusters")

        self._jobs_tab = JobsTab()
        self._tabs.addTab(self._jobs_tab, "Jobs")

    def _connect(self):
        host_port = self._host_input.text().strip()
        if not host_port:
            QMessageBox.critical(self, "Error", "Please enter a host:port value.")
            return
        url = f"http://{host_port}"
        try:
            self.client = SarcApiClient(remote_url=url)
        except Exception as exc:
            QMessageBox.critical(self, "Connection Error", str(exc))
            return

        self._set_loading(True, "Testing connection...")
        self._connect_btn.setEnabled(False)

        self._connect_worker = ConnectWorker(self.client)
        self._connect_worker.success.connect(self._on_connected)
        self._connect_worker.error.connect(self._on_connect_failed)
        self._connect_worker.start()

    def _on_connected(self, clusters: list):
        self._set_loading(False)
        self._connect_btn.setEnabled(True)
        self._status_label.setText(f"Connected ({len(clusters)} clusters)")
        self._status_label.setStyleSheet("color: green;")

        # Propagate client to all tabs
        self._users_tab.client = self.client
        self._clusters_tab.client = self.client
        self._jobs_tab.client = self.client
        self._summary_tab.set_client(self.client)

        # Load summary automatically
        self._summary_tab.refresh()

    def _on_connect_failed(self, msg: str):
        self._set_loading(False)
        self._connect_btn.setEnabled(True)
        self._status_label.setText("Connection failed")
        self._status_label.setStyleSheet("color: red;")
        self.client = None
        QMessageBox.critical(self, "Connection Failed", f"Could not connect:\n{msg}")

    def _set_loading(self, loading: bool, msg: str = ""):
        self._loading_label.setText(msg if loading else "")

    def _go_to_jobs(self):
        self._tabs.setCurrentWidget(self._jobs_tab)
        self._jobs_tab._do_search(page=1)

    def _go_to_users(self):
        self._tabs.setCurrentWidget(self._users_tab)
        self._users_tab.trigger_search()

    def _go_to_jobs_with_cluster(self, cluster: str):
        self._tabs.setCurrentWidget(self._jobs_tab)
        if self._jobs_tab._cluster_combo.count() <= 1:
            # Combo not yet populated — store the cluster and let _on_clusters_loaded apply it
            self._jobs_tab._pending_cluster = cluster
            self._jobs_tab.refresh_cluster_combo()
        else:
            self._jobs_tab.set_cluster_filter(cluster)
            self._jobs_tab._do_search(page=1)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())
