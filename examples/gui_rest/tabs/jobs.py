"""Jobs tab for the SARC GUI."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QGroupBox, QLabel, QPushButton,
    QLineEdit, QComboBox, QTableWidget, QTableWidgetItem, QHeaderView,
    QApplication, QMessageBox, QDateEdit,
)
from PyQt6.QtCore import Qt, QDate

from sarc.rest.client import SarcApiClient
from sarc.client.job import SlurmState

from ..utils import fmt_datetime, fmt_elapsed
from ..workers import JobsWorker, ClusterListWorker
from ..dialogs import JobDetailsDialog


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
