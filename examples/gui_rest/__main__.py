# /// script
# dependencies = ["PyQt6>=6.9,<7", "sarc"]
#
# [tool.uv.sources]
# sarc = { path = "../.." }
# ///
"""
SARC Database Browser - PyQt6 GUI for browsing SARC via REST API.

Usage:
    uv run examples/gui_rest/__main__.py
"""

from __future__ import annotations

import sys
import os
import tempfile

# If no SARC config is set, inject a minimal one so SarcApiClient can be
# instantiated with explicit parameters (mongo is unused by the GUI).
if not os.environ.get("SARC_CONFIG"):
    _cfg = tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", delete=False
    )
    _cfg.write("sarc:\n  mongo:\n    connection_string: mongodb://localhost:27017\n    database_name: sarc\n")
    _cfg.close()
    os.environ["SARC_CONFIG"] = _cfg.name

# Make sarc and gui_rest importable when running directly from the project root
_script_dir = os.path.dirname(os.path.abspath(__file__))
_examples_dir = os.path.dirname(_script_dir)
_project_root = os.path.dirname(_examples_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)
if _examples_dir not in sys.path:
    sys.path.insert(0, _examples_dir)

from PyQt6.QtWidgets import (
    QApplication,
    QMainWindow,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QTabWidget,
    QLineEdit,
    QPushButton,
    QLabel,
    QFrame,
    QMessageBox,
)
from PyQt6.QtCore import Qt, QSettings

from sarc.rest.client import SarcApiClient

from gui_rest.workers import ConnectWorker
from gui_rest.tabs.summary import SummaryTab
from gui_rest.tabs.users import UsersTab
from gui_rest.tabs.clusters import ClustersTab
from gui_rest.tabs.jobs import JobsTab


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("SARC Database Browser")
        self.resize(1100, 750)

        self.client: SarcApiClient | None = None
        self._connect_worker: ConnectWorker | None = None
        self._settings = QSettings("SARC", "DatabaseBrowser")

        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)
        main_layout.setContentsMargins(4, 4, 4, 4)

        # Connection bar
        conn_frame = QFrame()
        conn_frame.setFrameShape(QFrame.Shape.StyledPanel)
        conn_layout = QHBoxLayout(conn_frame)
        conn_layout.addWidget(QLabel("URL:"))
        self._host_input = QLineEdit()
        self._host_input.setPlaceholderText("https://hostname or localhost:1234")
        self._host_input.setText(self._settings.value("connection/host", "localhost:1234"))
        self._host_input.setMaximumWidth(220)
        self._host_input.returnPressed.connect(self._connect)
        conn_layout.addWidget(self._host_input)

        conn_layout.addWidget(QLabel("Token:"))
        self._token_input = QLineEdit()
        self._token_input.setPlaceholderText("OAuth2 bearer token (optional)")
        self._token_input.setEchoMode(QLineEdit.EchoMode.Password)
        self._token_input.setText(self._settings.value("connection/token", ""))
        self._token_input.setMaximumWidth(240)
        self._token_input.returnPressed.connect(self._connect)
        conn_layout.addWidget(self._token_input)

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
        if host_port.startswith("http://") or host_port.startswith("https://"):
            url = host_port
        else:
            url = f"https://{host_port}"
        token = self._token_input.text().strip() or None
        try:
            kwargs = {"remote_url": url}
            if token is not None:
                kwargs["oauth2_token"] = token
            self.client = SarcApiClient(**kwargs)
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
        self._settings.setValue("connection/host", self._host_input.text().strip())
        self._settings.setValue("connection/token", self._token_input.text().strip())
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


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())
