"""NVIDIA DCGM sentinel values and helpers.

DCGM publishes special "BLANK" values when a metric is unavailable
(e.g. profiling paused, GPU not accessible, MIG slice not enabled). These
sentinels reach SARC via Prometheus exporters such as dcgm-exporter and
pollute statistics if not filtered before aggregation.

Constants and semantics mirror NVIDIA/DCGM ``testing/python3/dcgmvalue.py``:
https://github.com/NVIDIA/DCGM/blob/master/testing/python3/dcgmvalue.py
"""

DCGM_INT64_BLANK = 0x7FFF_FFFF_FFFF_FFF0
DCGM_INT32_BLANK = 0x7FFF_FFF0
DCGM_FP64_BLANK = 140737488355328.0  # 2**47 == 0x8000_0000_0000

DCGM_FP64_NOT_FOUND = DCGM_FP64_BLANK + 1.0
DCGM_FP64_NOT_SUPPORTED = DCGM_FP64_BLANK + 2.0
DCGM_FP64_NOT_PERMISSIONED = DCGM_FP64_BLANK + 3.0
