"""Metrics tab for the SARC GUI."""

from __future__ import annotations

from PyQt6.QtWidgets import QWidget, QVBoxLayout, QLabel
from PyQt6.QtCore import Qt


class MetricsTab(QWidget):
    def __init__(self, parent: QWidget | None = None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        label = QLabel("Metrics — coming soon")
        label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(label)
