import time as real_time

# from sarc.alerts.common import config
from sarc.alerts.monitor import HealthMonitor
from sarc.config import config

from .test_runner import day_runner


def test_monitor(beans_config, caplog):
    # caplog.set_level(1000)
    hc = config().health_monitor
    monitor = HealthMonitor(
        directory=hc.directory,
        checks=hc.checks,
    )
    monitor.start()
    assert monitor.status == {
        "evil_beans": "ABSENT",
        "little_beans": "ABSENT",
        "many_beans": "ABSENT",
    }
    it = day_runner().iterate()
    next(it)  # First next() does nothing
    next(it)
    expected = {
        "evil_beans": "ERROR",
        "little_beans": "FAILURE",
        "many_beans": "OK",
    }
    for i in range(9):
        real_time.sleep(2**i / 100)
        if monitor.status == expected:
            break
    else:
        assert monitor.status == expected
    monitor.stop()
    monitor.join()
