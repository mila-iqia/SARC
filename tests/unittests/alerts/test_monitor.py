import time as real_time

from sarc.alerts.common import config
from sarc.alerts.monitor import HealthMonitor

from .test_runner import day_runner


def test_monitor(beans_config, caplog):
    # caplog.set_level(1000)
    monitor = HealthMonitor(
        directory=config.directory,
        checks=config.checks,
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
    real_time.sleep(0.1)
    assert monitor.status == {
        "evil_beans": "ERROR",
        "little_beans": "FAILURE",
        "many_beans": "OK",
    }
    monitor.stop()
    monitor.join()
