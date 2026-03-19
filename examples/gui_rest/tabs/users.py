"""Users tab and user details panel for the SARC GUI."""
from __future__ import annotations

from PyQt6.QtWidgets import (
    QWidget, QFrame, QVBoxLayout, QHBoxLayout, QFormLayout, QScrollArea,
    QLabel, QPushButton, QLineEdit, QComboBox, QTableWidget, QTableWidgetItem,
    QHeaderView, QSplitter, QApplication, QMessageBox,
)
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QFont

from sarc.rest.client import SarcApiClient
from sarc.core.models.users import MemberType

from ..utils import get_member_type
from ..workers import UsersWorker, UserDetailsWorker


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
