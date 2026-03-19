"""Dialog windows for the SARC GUI."""

from __future__ import annotations

from PyQt6.QtWidgets import (
    QDialog,
    QWidget,
    QVBoxLayout,
    QScrollArea,
    QGroupBox,
    QFormLayout,
    QLabel,
    QPushButton,
)
from PyQt6.QtCore import Qt

from .utils import fmt_datetime, fmt_elapsed, fmt_memory, fmt_float


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
                val_label.setTextInteractionFlags(
                    Qt.TextInteractionFlag.TextSelectableByMouse
                )
                form.addRow(label + ":", val_label)
            layout.addWidget(group)

        # Basic Info
        add_section(
            "Basic Info",
            [
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
                (
                    "Exit Code",
                    str(job.exit_code) if job.exit_code is not None else "N/A",
                ),
                ("Signal", str(job.signal) if job.signal is not None else "N/A"),
                ("Nodes", ", ".join(job.nodes) if job.nodes else "N/A"),
                ("Work Dir", job.work_dir),
                ("Constraints", job.constraints or "N/A"),
            ],
        )

        # Timing
        add_section(
            "Timing",
            [
                ("Submit Time", fmt_datetime(job.submit_time)),
                ("Start Time", fmt_datetime(job.start_time)),
                ("End Time", fmt_datetime(job.end_time)),
                ("Elapsed Time", fmt_elapsed(job.elapsed_time)),
                (
                    "Time Limit",
                    fmt_elapsed((job.time_limit or 0) * 60)
                    if job.time_limit
                    else "N/A",
                ),
            ],
        )

        # Resources Requested
        req = job.requested
        add_section(
            "Resources Requested",
            [
                ("CPUs", str(req.cpu) if req.cpu is not None else "N/A"),
                ("Memory", fmt_memory(req.mem)),
                ("Nodes", str(req.node) if req.node is not None else "N/A"),
                ("Billing", str(req.billing) if req.billing is not None else "N/A"),
                (
                    "GPUs (gres)",
                    str(req.gres_gpu) if req.gres_gpu is not None else "N/A",
                ),
                ("GPU Type", req.gpu_type or "N/A"),
            ],
        )

        # Resources Allocated
        alloc = job.allocated
        add_section(
            "Resources Allocated",
            [
                ("CPUs", str(alloc.cpu) if alloc.cpu is not None else "N/A"),
                ("Memory", fmt_memory(alloc.mem)),
                ("Nodes", str(alloc.node) if alloc.node is not None else "N/A"),
                ("Billing", str(alloc.billing) if alloc.billing is not None else "N/A"),
                (
                    "GPUs (gres)",
                    str(alloc.gres_gpu) if alloc.gres_gpu is not None else "N/A",
                ),
                ("GPU Type", alloc.gpu_type or "N/A"),
            ],
        )

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
