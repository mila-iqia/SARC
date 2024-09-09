import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from apischema import ValidationError, deserialize, serialize
from gifnoc import TaggedSubclass
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
            directory=Path(tmpdir) / "beano",
        )

    yield fn


@pytest.mark.parametrize(
    "td",
    [
        timedelta(hours=1, seconds=14),
        timedelta(days=1, hours=18),
        timedelta(minutes=-5),
    ],
)
def test_timedelta_serializer(td):
    assert (ser := serialize(timedelta, td)) == f"{int(td.total_seconds())}s"
    assert deserialize(timedelta, ser) == td


def test_timedelta_serializer_microseconds():
    td = timedelta(seconds=3, microseconds=651)
    assert (ser := serialize(timedelta, td)) == "3000651us"
    assert deserialize(timedelta, ser) == td


def test_timedelta_deserializer():
    assert deserialize(timedelta, "1h14s") == timedelta(hours=1, seconds=14)
    assert deserialize(timedelta, "1d18h") == timedelta(days=1, hours=18)
    assert deserialize(timedelta, "1d2h3m4s") == timedelta(
        days=1, hours=2, minutes=3, seconds=4
    )
    assert deserialize(timedelta, "-5h3m") == timedelta(hours=-5, minutes=-3)
    assert deserialize(timedelta, "2.5h") == timedelta(hours=2, minutes=30)


def test_date_deserializer():
    assert deserialize(datetime, "2024-02-01T15:00:00Z") == datetime(
        year=2024, month=2, day=1, hour=15, tzinfo=timezone.utc
    )


@pytest.mark.parametrize("inv", ["1h14", "3", "quack", "h", "1H13M"])
def test_timedelta_invalid(inv):
    with pytest.raises(ValidationError):
        deserialize(timedelta, inv)


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


def test_CheckResult_save(tmpdir):
    cr = BeanResult(
        name="test",
        status=CheckStatus.ERROR,
        statuses={
            "a": CheckStatus.OK,
            "b": CheckStatus.FAILURE,
            "c": CheckStatus.FAILURE,
        },
        more=666,
    )
    cr.save(tmpdir)
    pth = cr.get_save_path(tmpdir)
    recovered = deserialize(
        TaggedSubclass[CheckResult], json.loads(pth.read_text(encoding="utf8"))
    )
    assert cr == recovered
    assert recovered.more == 666


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
    assert hc.latest_result() == result


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
    assert hc.latest_result() == result


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
    assert hc.latest_result() == result


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
    assert hc.latest_result() == result


def test_HealthCheck_latest_result(beancheck, frozen_gifnoc_time):
    hc = beancheck(beans=3)
    assert hc.latest_result().status is CheckStatus.ABSENT
    result1 = hc()
    assert result1.status is not CheckStatus.ABSENT
    assert hc.latest_result() == result1
    time.sleep(3600)
    hc()
    time.sleep(3600)
    result2 = hc()
    assert result1.issue_date < result2.issue_date
    assert hc.latest_result() == result2


def test_HealthCheck_next_schedule(beancheck, frozen_gifnoc_time):
    hc = beancheck(beans=3, interval=1)
    result = hc()
    assert hc.next_schedule(result) == result.issue_date + timedelta(hours=1)
    time.sleep(1800)
    assert hc.next_schedule(result) == result.issue_date + timedelta(hours=1)
    result2 = hc()
    assert hc.next_schedule(result2) == result2.issue_date + timedelta(hours=1)


def test_HealthCheck_initial_next_schedule(beancheck, frozen_gifnoc_time):
    hc = beancheck(beans=3, interval=1)
    result = hc.latest_result()
    assert result.status is CheckStatus.ABSENT
    assert hc.next_schedule(result) == time.now()
