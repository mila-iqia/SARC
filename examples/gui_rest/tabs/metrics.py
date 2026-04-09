"""Metrics tab for the SARC GUI."""

from __future__ import annotations

from datetime import date, timedelta

from PyQt6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QHeaderView,
    QDialog,
    QMessageBox,
    QApplication,
)
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QFont, QCursor

from sarc.rest.client import SarcApiClient

from gui_rest.workers import MetricsWorker, MetricsHistoryWorker


def _fmt_wait(seconds: float | None) -> str:
    if seconds is None:
        return "N/A"
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    return f"{h}h {m:02d}m"


class MetricsHistoryWindow(QDialog):
    """Window showing 30-day daily history and averages for a metric."""

    def __init__(
        self,
        client: SarcApiClient,
        clusters: list[str],
        metric_key: str,
        metric_name: str,
        parent: QWidget | None = None,
    ):
        super().__init__(parent)
        self.setWindowTitle(f"History: {metric_name}")
        self.resize(860, 520)

        self._client = client
        self._clusters = clusters
        self._metric_key = metric_key
        self._metric_name = metric_name
        self._worker: MetricsHistoryWorker | None = None

        layout = QVBoxLayout(self)

        # Status / progress
        self._status = QLabel("Loading…")
        layout.addWidget(self._status)

        # Summary averages
        self._avg_label = QLabel("")
        self._avg_label.setWordWrap(True)
        layout.addWidget(self._avg_label)

        # Matplotlib canvas (created lazily after data arrives)
        self._canvas_placeholder = QWidget()
        self._canvas_placeholder.setMinimumHeight(380)
        layout.addWidget(self._canvas_placeholder)
        self._canvas = None

        self._fetch()

    def _fetch(self):
        self._worker = MetricsHistoryWorker(self._client, self._clusters, self._metric_key)
        self._worker.success.connect(self._on_data)
        self._worker.error.connect(self._on_error)
        self._worker.progress.connect(lambda msg: self._status.setText(msg))
        QApplication.setOverrideCursor(Qt.CursorShape.WaitCursor)
        self._worker.start()

    def _on_error(self, msg: str):
        QApplication.restoreOverrideCursor()
        self._status.setText(f"Error: {msg}")

    def _on_data(self, data: dict):
        QApplication.restoreOverrideCursor()
        self._status.setText("")
        self._render(data)

    def _render(self, data: dict):
        from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg
        from matplotlib.figure import Figure

        today = date.today()
        cutoff_7 = today - timedelta(days=7)
        cutoff_30 = today - timedelta(days=30)

        # Build averages summary text
        lines = []
        for cluster, series in sorted(data.items()):
            if not series:
                continue
            vals_7 = [v for d, v in series if d >= cutoff_7]
            vals_30 = [v for d, v in series if d >= cutoff_30]
            avg7 = sum(vals_7) / len(vals_7) if vals_7 else None
            avg30 = sum(vals_30) / len(vals_30) if vals_30 else None
            if self._metric_key == "avg_wait":
                s7 = _fmt_wait(avg7)
                s30 = _fmt_wait(avg30)
            else:
                s7 = f"{avg7:.1f}" if avg7 is not None else "N/A"
                s30 = f"{avg30:.1f}" if avg30 is not None else "N/A"
            lines.append(f"<b>{cluster}</b>: 7-day avg = {s7} &nbsp;|&nbsp; 30-day avg = {s30}")
        self._avg_label.setText("<br>".join(lines))

        # Draw chart
        fig = Figure(figsize=(8, 4), tight_layout=True)
        ax = fig.add_subplot(111)

        for cluster, series in sorted(data.items()):
            if not series:
                continue
            days, values = zip(*series)
            if self._metric_key == "avg_wait":
                values = tuple(v / 3600 for v in values)  # convert to hours
            ax.plot(days, values, marker="o", markersize=3, label=cluster)

        ax.set_title(f"{self._metric_name} — last 30 days")
        ax.set_xlabel("Date")
        y_label = "Hours" if self._metric_key == "avg_wait" else "Jobs"
        ax.set_ylabel(y_label)
        fig.autofmt_xdate()
        if len(data) > 1:
            ax.legend(fontsize=8)

        canvas = FigureCanvasQTAgg(fig)

        # Replace placeholder with canvas
        parent_layout = self._canvas_placeholder.parent().layout()
        idx = parent_layout.indexOf(self._canvas_placeholder)
        self._canvas_placeholder.hide()
        parent_layout.insertWidget(idx, canvas)
        self._canvas = canvas


class MetricsTab(QWidget):
    def __init__(self, parent: QWidget | None = None):
        super().__init__(parent)
        self.client: SarcApiClient | None = None
        self._clusters: list[str] = []
        self._worker: MetricsWorker | None = None
        self._history_windows: list[MetricsHistoryWindow] = []

        layout = QVBoxLayout(self)

        # Header row
        header = QHBoxLayout()
        header.addWidget(QLabel("<b>Last 24 hours — by cluster</b>"))
        header.addStretch()
        self._refresh_btn = QPushButton("Refresh")
        self._refresh_btn.clicked.connect(self.refresh)
        header.addWidget(self._refresh_btn)
        layout.addLayout(header)

        hint = QLabel("Click a metric column header to see 30-day history.")
        hint.setStyleSheet("color: gray; font-size: 11px;")
        layout.addWidget(hint)

        # Table
        self._table = QTableWidget(0, 3)
        self._table.setHorizontalHeaderLabels(["Cluster", "Avg Wait Time ▸", "Submitted Jobs ▸"])
        self._table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self._table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        self._table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._table.verticalHeader().setVisible(False)

        # Make metric headers look clickable
        header_font = QFont()
        header_font.setUnderline(True)
        self._table.horizontalHeaderItem(1).setFont(header_font)
        self._table.horizontalHeaderItem(2).setFont(header_font)
        self._table.horizontalHeader().setCursor(QCursor(Qt.CursorShape.PointingHandCursor))
        self._table.horizontalHeader().sectionClicked.connect(self._on_header_clicked)

        layout.addWidget(self._table)

        self._status = QLabel("")
        self._status.setStyleSheet("color: gray;")
        layout.addWidget(self._status)

    def refresh(self):
        if self.client is None:
            return
        self._status.setText("Loading…")
        self._refresh_btn.setEnabled(False)
        self._table.setRowCount(0)

        # Fetch cluster list then metrics
        from gui_rest.workers import ClusterListWorker

        self._cluster_worker = ClusterListWorker(self.client)
        self._cluster_worker.success.connect(self._on_clusters)
        self._cluster_worker.error.connect(self._on_error)
        self._cluster_worker.start()

    def _on_clusters(self, clusters: list[str]):
        self._clusters = clusters
        self._worker = MetricsWorker(self.client, clusters)
        self._worker.success.connect(self._on_metrics)
        self._worker.error.connect(self._on_error)
        QApplication.setOverrideCursor(Qt.CursorShape.WaitCursor)
        self._worker.start()

    def _on_metrics(self, data: dict):
        QApplication.restoreOverrideCursor()
        self._refresh_btn.setEnabled(True)
        self._status.setText("")
        self._table.setRowCount(len(data))
        for row, (cluster, metrics) in enumerate(sorted(data.items())):
            self._table.setItem(row, 0, QTableWidgetItem(cluster))
            self._table.setItem(row, 1, QTableWidgetItem(_fmt_wait(metrics["avg_wait"])))
            count_item = QTableWidgetItem(str(metrics["job_count"]))
            count_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            self._table.setItem(row, 2, count_item)

    def _on_error(self, msg: str):
        QApplication.restoreOverrideCursor()
        self._refresh_btn.setEnabled(True)
        self._status.setText(f"Error: {msg}")
        QMessageBox.critical(self, "Metrics Error", msg)

    def _on_header_clicked(self, column: int):
        if self.client is None or not self._clusters:
            return
        if column == 1:
            win = MetricsHistoryWindow(
                self.client, self._clusters, "avg_wait", "Avg Wait Time", self
            )
            self._history_windows.append(win)
            win.show()
        elif column == 2:
            win = MetricsHistoryWindow(
                self.client, self._clusters, "job_count", "Submitted Jobs", self
            )
            self._history_windows.append(win)
            win.show()
