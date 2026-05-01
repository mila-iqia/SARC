"""
Dedicated module to compute RGU for a given GPU, with MIG support
and alternative MIG value reference, either "drac" (default) or "mila".

TODO Integrate into SlurmJob and update_job_series_rgu.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum
from typing import Literal

from sarc.db.job import get_rgus


class GpuError(Exception):
    """Specific exception for this module"""


class MigType(StrEnum):
    """Standardized enumeration for known GPUs which have MIGs."""

    H100 = "H100-SXM5-80GB"
    A100 = "A100-SXM4-40GB"
    A100L = "A100-SXM4-80GB"


@dataclass(slots=True, frozen=True)
class Gpu:
    """
    Help class to represent a GPU.
    Fields:
        type: GPU harmonized name. Either "<iguane-name>", or "<iguane-name> : <MIG description>" if GPU is a MIG.
        mig_type: typed GPU name, if GPU is a MIG, or None otherwise.
            ** Currently used to compute MIG RGU.
        mig_name: standard MIG name, e.g "1g.20gb", if GPU is a MIG, or None otherwise.
            Currently unused, for info only.
        mig_number: MIG number, e.g `1` in "1g.20gb", if GPU is a MIG, or None otherwise.
            ** Currently used to compute MIG RGU.
        mig_memory_gb: MIG memory in gigabytes, e.g. `20` in "1g.20gb", if GPU is a MIG, or None otherwise.
            Currently unused, for info only.
    """

    type: str
    mig_type: MigType | None = None
    mig_name: str | None = None
    mig_number: int | None = None
    mig_memory_gb: int | None = None


_DRAC_MIG_RGU_RAW: dict[MigType, dict[int, float]] = {
    MigType.H100: {1: 1.74, 2: 3.48, 3: 6.1}
}
"""
Format: MIG GPU => MIG number => MIG RGU

RGU raw value for MIGs, as provided by DRAC (2026/04/30):
https://docs.alliancecan.ca/wiki/Allocations_and_compute_scheduling#Ratios_in_bundles
"""

_MILA_MIG_RGU_FRAC: dict[MigType, dict[int, tuple[int, int]]] = {
    # Specific fractions for H100
    MigType.H100: {1: (1, 8), 2: (2, 8), 3: (5, 11), 4: (1, 2)},
    # 1/7 rule
    MigType.A100: {1: (1, 7), 2: (2, 7), 3: (3, 7), 4: (4, 7)},
    # 1/7 rule
    MigType.A100L: {1: (1, 7), 2: (2, 7), 3: (3, 7), 4: (4, 7)},
}
"""
Format: MIG GPU => MIG number => fraction to be applied to main GPU RGU: tuple[numerator, denominator]

Fractions used to compute default RGU value for MIGs (when DRAC is ignored or bypassed).
For a given MIG, RGU value = (RGU value for main GPU) * numerator / denominator
"""

_REGEX_MIG = re.compile(r"(([0-9]+)g\.([0-9]+)gb)")
"""
Regex used to parse a MIG name from GPU harmonized name.
Captures MIG name, MIG number and MIG memory.
"""


def get_gpu_type_rgu(
    gpu_type: str, rgu_version: str = "1.0", mig_ref: Literal["mila", "drac"] = "drac"
) -> float:
    """Compute and return RGU value for a single given GPU name."""
    gpu_type_to_rgu = get_rgus(rgu_version=rgu_version)

    gpu = _parse_gpu(gpu_type)
    if gpu.mig_type is None:
        if gpu.type not in gpu_type_to_rgu:
            raise GpuError(f"No RGU for {gpu.type}")
        return gpu_type_to_rgu[gpu.type]
    else:
        mig_rgu = None
        if mig_ref == "drac":
            mig_rgu = _get_drac_mig_rgu(gpu)
        # Anyway, fallback to mila mig_ref
        if mig_rgu is None:
            mig_rgu = _get_mila_mig_rgu(gpu_type_to_rgu, gpu)
        return mig_rgu


def _parse_gpu(gpu_type: str) -> Gpu:
    """Parse a GPU harmonized name and return a GPU object."""
    if ":" not in gpu_type:
        return Gpu(gpu_type)

    main_gpu, mig_desc = gpu_type.split(":")
    main_gpu = main_gpu.strip()
    mig_matches = _REGEX_MIG.findall(mig_desc)
    assert len(mig_matches) == 1
    ((mig_name, mig_number_str, mig_mem_str),) = mig_matches
    return Gpu(
        type=main_gpu,
        mig_type=_get_mig_main_type(main_gpu),
        mig_name=mig_name,
        mig_number=int(mig_number_str),
        mig_memory_gb=int(mig_mem_str),
    )


def _get_mig_main_type(main_gpu: str) -> MigType:
    if main_gpu == "H100-SXM5-80GB":
        return MigType.H100
    if main_gpu == "A100-SXM4-40GB":
        return MigType.A100
    if main_gpu in ("A100-SXM4-80GB", "a100l"):
        return MigType.A100L
    raise GpuError(f"Unknown GPU type for MIGs: {main_gpu}")


def _get_drac_mig_rgu(gpu: Gpu) -> float | None:
    """Get RGU value given by DRAC for a MIG. May return None if not defined in DRAC dict."""
    assert gpu.mig_type is not None
    assert gpu.mig_number is not None
    return _DRAC_MIG_RGU_RAW.get(gpu.mig_type, {}).get(gpu.mig_number)


def _get_mila_mig_rgu(gpu_type_to_rgu: dict[str, float], gpu: Gpu) -> float:
    """Compute RGU value for a MIG, as fraction of main GPU."""
    assert gpu.mig_type is not None
    assert gpu.mig_number is not None
    # NB: Main GPU for a MIG is expected to be present in gpu_type_to_rgu and _MILA_MIG_RGU_FRAC
    main_rgu = gpu_type_to_rgu[gpu.mig_type.value]
    num, den = _MILA_MIG_RGU_FRAC[gpu.mig_type][gpu.mig_number]
    return main_rgu * num / den
