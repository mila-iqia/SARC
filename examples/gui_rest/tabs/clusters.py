"""Clusters tab for the SARC GUI."""
from __future__ import annotations

from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QTableWidget, QTableWidgetItem, QHeaderView, QApplication, QMessageBox,
)
from PyQt6.QtCore import pyqtSignal
from PyQt6.QtGui import QFont

from sarc.rest.client import SarcApiClient

from ..workers import ClustersWorker


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
