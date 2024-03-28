from collections import Counter
from datetime import timedelta

import pytest
from gifnoc.std import time
from pytest import fixture

from sarc.alerts.cache import Timespan, spancache


@fixture
def spancached(frozen_gifnoc_time):
    cnts = Counter()
    expected_cnts = Counter()

    @spancache
    def fn(span, should_execute):
        if not should_execute:
            raise AssertionError("Result should still be cached at this time.")
        cnts[span.key] += 1
        return cnts[span.key]

    def checker(span, should_execute):
        result = fn(span, should_execute)
        if should_execute:
            expected_cnts[span.key] += 1
        assert result == expected_cnts[span.key]

    return checker


def test_timespan_shorthand():
    t1 = Timespan(duration="1h", offset="2h", validity="3h")
    t2 = Timespan(
        duration=timedelta(hours=1),
        offset=timedelta(hours=2),
        validity=timedelta(hours=3),
    )
    assert t1 == t2


def test_timespan_bounds(frozen_gifnoc_time):
    t1 = Timespan(duration="1h", offset="2h", validity="3h")
    start = time.now() - timedelta(hours=3)
    end = time.now() - timedelta(hours=2)
    assert t1.bounds == (start, end)


def test_timespan_to_str():
    t1 = Timespan(duration="1h", offset="2h", validity="3h")
    assert str(t1) == "1:00:00 2:00:00 ago"
    t2 = Timespan(duration="1h", validity="3h")
    assert str(t2) == "1:00:00"


def test_spancache(spancached):
    with pytest.raises(AssertionError):
        # Just checking that it isn't a dud
        spancached(Timespan(duration="1h"), False)

    spancached(Timespan(duration="1h"), should_execute=True)
    spancached(Timespan(duration="1h"), should_execute=False)
    time.sleep(3600)
    spancached(Timespan(duration="1h"), should_execute=True)


def test_spancache_multiple(spancached):
    spancached(Timespan(duration="1h", validity="1h"), should_execute=True)
    spancached(Timespan(duration="2h", validity="2h"), should_execute=True)
    spancached(Timespan(duration="3h", validity="3h"), should_execute=True)
    spancached(Timespan(duration="1h", validity="1h"), should_execute=False)
    spancached(Timespan(duration="2h", validity="2h"), should_execute=False)
    spancached(Timespan(duration="3h", validity="3h"), should_execute=False)
    time.sleep(3600)
    spancached(Timespan(duration="1h", validity="1h"), should_execute=True)
    spancached(Timespan(duration="2h", validity="2h"), should_execute=False)
    spancached(Timespan(duration="3h", validity="3h"), should_execute=False)
    time.sleep(3600)
    spancached(Timespan(duration="1h", validity="1h"), should_execute=True)
    spancached(Timespan(duration="2h", validity="2h"), should_execute=True)
    spancached(Timespan(duration="3h", validity="3h"), should_execute=False)
    time.sleep(3600)
    spancached(Timespan(duration="1h", validity="1h"), should_execute=True)
    spancached(Timespan(duration="2h", validity="2h"), should_execute=False)
    spancached(Timespan(duration="3h", validity="3h"), should_execute=True)


def test_spancache_validity(spancached):
    spancached(Timespan(duration="1h", validity="2h"), should_execute=True)
    time.sleep(3600)
    spancached(Timespan(duration="1h", validity="2h"), should_execute=False)
    spancached(Timespan(duration="1h", validity="3h"), should_execute=False)
    spancached(Timespan(duration="1h", validity="30m"), should_execute=True)
    time.sleep(7200)
    spancached(Timespan(duration="1h", validity="3h"), should_execute=False)
    spancached(Timespan(duration="1h", validity="2h"), should_execute=True)


def test_no_spancache_on_methods():
    with pytest.raises(TypeError):

        class CachedCounter:
            def __init__(self):
                self.list = []

            @spancache
            def __call__(self, span, x):
                self.list.append(x)
