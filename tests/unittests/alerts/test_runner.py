from datetime import timedelta

from gifnoc.std import time

from sarc.alerts.common import CheckStatus
from sarc.alerts.runner import CheckRunner
from sarc.config import config


def day_runner():
    hc = config().health_monitor
    return CheckRunner(
        directory=hc.directory,
        checks=hc.checks,
    )


def test_runner(beans_config, caplog):
    caplog.set_level(1000)
    start = time.now()
    day_runner().start(end_time=start + timedelta(hours=24))

    expected_status = {
        "many_beans": CheckStatus.OK,
        "little_beans": CheckStatus.FAILURE,
        "evil_beans": CheckStatus.ERROR,
        "sleepy_beans": CheckStatus.OK,
    }

    for check in beans_config.checks.values():
        print(f"Checking: {check.name}")
        results = list(check.all_results(ascending=True))
        if check.active:
            duration = check.interval.total_seconds()
            expected_n = int((24 * 3600) / duration)
            assert all(res.status is expected_status[check.name] for res in results)
            assert len(results) == expected_n
            assert [res.issue_date for res in results] == [
                start + timedelta(seconds=i * duration) for i in range(expected_n)
            ]
        else:
            assert len(results) == 0


def test_runner_deps(deps_config, caplog):
    caplog.set_level(1000)
    day_runner().start(end_time=time.now() + timedelta(hours=24))

    assert len(list(deps_config.checks["evil_beans"].all_results())) == 24
    assert len(list(deps_config.checks["many_beans"].all_results())) == 0


def test_runner_parameters(params_config, caplog):
    caplog.set_level(1000)
    day_runner().start(end_time=time.now() + timedelta(hours=24))

    assert len(list(params_config.checks["isbeta_alpha"].all_results())) == 24
    assert len(list(params_config.checks["isbeta_beta"].all_results())) == 24
    assert len(list(params_config.checks["isbeta_gamma"].all_results())) == 24

    assert len(list(params_config.checks["beanz_alpha"].all_results())) == 0
    assert len(list(params_config.checks["beanz_beta"].all_results())) == 16
    assert len(list(params_config.checks["beanz_gamma"].all_results())) == 0
