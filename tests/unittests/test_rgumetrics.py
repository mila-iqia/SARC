import pytest

from sarc.client.rgumetrics import get_gpu_type_rgu

THRESHOLD = 1e-10

RGU_H100 = 12.168321513002363  # from iguane: uv run python -m iguane --json


@pytest.mark.parametrize(
    "gpu_type,rgu",
    [
        ("A100-SXM4-40GB", 4.0),
        ("A100-SXM4-80GB", 4.8),
        ("H100-SXM5-80GB", RGU_H100),
        ("A100-SXM4-40GB: 1g.123456789gb", 4.0 * 1 / 7),
        ("A100-SXM4-40GB: 2g.123456789gb", 4.0 * 2 / 7),
        ("A100-SXM4-40GB: 3g.123456789gb", 4.0 * 3 / 7),
        ("A100-SXM4-40GB: 4g.123456789gb", 4.0 * 4 / 7),
        ("A100-SXM4-40GB: a100_1g.123456789gb_anything", 4.0 * 1 / 7),
        ("A100-SXM4-40GB: a100aomething1g.123456789gb_anything", 4.0 * 1 / 7),
        ("A100-SXM4-80GB: 1g.10gb", 4.8 * 1 / 7),
        ("A100-SXM4-80GB: 2g.20gb", 4.8 * 2 / 7),
        ("A100-SXM4-80GB: 3g.30gb", 4.8 * 3 / 7),
        ("A100-SXM4-80GB: 4g.40gb", 4.8 * 4 / 7),
    ],
)
def test_get_gpu_type_rgu(gpu_type, rgu):
    rgu_mig_ref_drac = get_gpu_type_rgu(gpu_type, mig_ref="drac")
    rgu_mig_ref_mila = get_gpu_type_rgu(gpu_type, mig_ref="mila")
    assert abs(rgu_mig_ref_mila - rgu) < THRESHOLD
    assert abs(rgu_mig_ref_drac - rgu) < THRESHOLD


@pytest.mark.parametrize(
    "gpu_type,rgu",
    [
        ("H100-SXM5-80GB: 1g.10gb", 1.74),  # drac
        ("H100-SXM5-80GB: 2g.20gb", 3.48),  # drac
        ("H100-SXM5-80GB: 3g.30gb", 6.1),  # drac
        ("H100-SXM5-80GB: 4g.40gb", RGU_H100 * 1 / 2),  # not in drac, fallback to mila
    ],
)
def test_get_gpu_type_rgu_drac(gpu_type, rgu):
    assert abs(get_gpu_type_rgu(gpu_type) - rgu) < THRESHOLD


@pytest.mark.parametrize(
    "gpu_type,rgu",
    [
        (
            "H100-SXM5-80GB: 1g.10gb",
            RGU_H100 * 1 / 8,
        ),  # uses fraction with mig_ref=mila
        (
            "H100-SXM5-80GB: 2g.20gb",
            RGU_H100 * 2 / 8,
        ),  # uses fraction with mig_ref=mila
        (
            "H100-SXM5-80GB: 3g.30gb",
            RGU_H100 * 5 / 11,
        ),  # uses fraction with mig_ref=mila
        ("H100-SXM5-80GB: 4g.40gb", RGU_H100 * 1 / 2),  # mila anyway
    ],
)
def test_get_gpu_type_rgu_mila(gpu_type, rgu):
    assert abs(get_gpu_type_rgu(gpu_type, mig_ref="mila") - rgu) < THRESHOLD
