"""Summary tab for the SARC GUI."""
from __future__ import annotations

from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QGroupBox, QLabel, QPushButton,
    QTableWidget, QTableWidgetItem, QHeaderView, QApplication, QMessageBox,
)
from PyQt6.QtCore import Qt, pyqtSignal
from PyQt6.QtGui import QFont

from sarc.rest.client import SarcApiClient

from ..workers import SummaryWorker


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
