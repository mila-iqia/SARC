from __future__ import annotations

import logging
from types import SimpleNamespace

from iguane.fom import RAWDATA, fom_ugr

logger = logging.getLogger(__name__)


def get_rgus(rgu_version: str = "1.0") -> dict[str, float]:
    """
    Return GPU->RGU mapping for given RGU version.

    Get mapping from package IGUANE.
    """
    args = SimpleNamespace(fom_version=rgu_version, custom_weights=None, norm=False)
    gpus = sorted(RAWDATA.keys())
    return {gpu: fom_ugr(gpu, args=args) for gpu in gpus}
