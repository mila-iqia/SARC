from datetime import timedelta

import pytest
from gifnoc.std import time

from sarc.alerts.common import CheckException, CheckResult, CheckStatus
from .definitions import BeanCheck, BeanResult


@pytest.fixture
def beancheck(tmpdir):
    def fn(beans, interval=1):
        return BeanCheck(
            name="beano",
            beans=beans,
            active=True,
            interval=timedelta(hours=interval),
        )

    yield fn


def test_CheckResult_get_failures():
    cr = CheckResult(
        name="test",
        status=CheckStatus.ERROR,
        statuses={
            "a": CheckStatus.OK,
            "b": CheckStatus.FAILURE,
            "c": CheckStatus.FAILURE,
        },
    )
    assert cr.get_failures() == {
        "test": CheckStatus.ERROR,
        "test/b": CheckStatus.FAILURE,
        "test/c": CheckStatus.FAILURE,
    }


def test_HealthCheck(beancheck, frozen_gifnoc_time):
    hc = beancheck(beans=13)
    result = hc()
    assert result == BeanResult(
        name="beano",
        status=CheckStatus.OK,
        statuses={},
        issue_date=time.now(),
        expiry=time.now() + timedelta(hours=2),
        check=hc,
        more=0,
    )


def test_HealthCheck_failure(beancheck, frozen_gifnoc_time):
    hc = beancheck(beans=3)
    result = hc()
    assert result == BeanResult(
        name="beano",
        status=CheckStatus.FAILURE,
        statuses={},
        issue_date=time.now(),
        expiry=time.now() + timedelta(hours=2),
        check=hc,
        more=7,
    )


def test_HealthCheck_error(beancheck, frozen_gifnoc_time):
    hc = beancheck(beans=666)
    result = hc()
    assert result == BeanResult(
        name="beano",
        status=CheckStatus.ERROR,
        exception=CheckException(
            type="ValueError",
            message="What a beastly number",
        ),
        statuses={},
        issue_date=time.now(),
        expiry=time.now() + timedelta(hours=2),
        check=hc,
        more=0,
    )


def test_HealthCheck_multiple_statuses(beancheck, frozen_gifnoc_time):
    hc = beancheck(beans=-15)
    result = hc()
    assert result == BeanResult(
        name="beano",
        status=CheckStatus.OK,
        statuses={
            "positive": CheckStatus.FAILURE,
            "negative": CheckStatus.OK,
            "fillbelly": CheckStatus.FAILURE,
        },
        issue_date=time.now(),
        expiry=time.now() + timedelta(hours=2),
        check=hc,
        more=0,
    )


def test_HealthCheck_latest_result(beancheck, frozen_gifnoc_time):
    hc = beancheck(beans=3)
    result1 = hc()
    assert result1.status is not CheckStatus.ABSENT
    time.sleep(3600)
    hc()
    time.sleep(3600)
    result2 = hc()
    assert result1.issue_date < result2.issue_date


def test_HealthCheck_next_schedule(beancheck, frozen_gifnoc_time):
    hc = beancheck(beans=3, interval=1)
    result = hc()
    assert hc.next_schedule(result) == result.issue_date + timedelta(hours=1)
    time.sleep(1800)
    assert hc.next_schedule(result) == result.issue_date + timedelta(hours=1)
    result2 = hc()
    assert hc.next_schedule(result2) == result2.issue_date + timedelta(hours=1)
