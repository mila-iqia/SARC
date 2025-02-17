import random
from dataclasses import dataclass
from datetime import timedelta

from sarc.alerts.common import CheckResult, HealthCheck
from sarc.alerts.db_sanity_checks.users_accounts import check_users_in_jobs
from sarc.alerts.usage_alerts.cluster_response import check_cluster_response
from sarc.alerts.usage_alerts.cluster_scraping import check_nb_jobs_per_cluster_per_time


# this is a simple check that will fail 50% of the time
# it uses a custom result class to add more context to the result
@dataclass
class HelloWorldResult(CheckResult):
    custom_comment: str = ""


@dataclass
class HelloWorldCheck(HealthCheck):
    __result_class__ = HelloWorldResult

    def check(self):
        if random.random() < 0.5:
            return self.fail(
                custom_comment="Hello, HealthMonitor World! You were chosen randomly to fail..."
            )
        return self.ok(custom_comment="Hello, HealthMonitor World!")


# this is a simple check that will fail 50% of the time
# it uses the statuses dictionnary to add more context information to the result
@dataclass
class HelloWorld2Check(HealthCheck):
    example_additionnal_param: str = "default_value"

    def check(self):
        random_number = random.random()
        if random_number < 0.5:
            return self.fail(
                statuses={
                    "comment": "Hello, HealthMonitor World! You were chosen randomly to fail...",
                    "random_number": random_number,
                    "example_additionnal_param": self.example_additionnal_param,
                }
            )
        return self.ok(
            statuses={
                "comment": "Hello, HealthMonitor World!",
                "random_number": random_number,
                "example_additionnal_param": self.example_additionnal_param,
            }
        )


# cheks if the cluster responded in the last `days` days
@dataclass
class ClusterResponseCheck(HealthCheck):
    days: int = 7

    def check(self):
        cluster_name = self.parameters["cluster_name"]
        days = self.days
        # days = 7
        if check_cluster_response(
            time_interval=timedelta(days=days), cluster_name=cluster_name
        ):
            return self.ok
        return self.fail(
            statuses={
                "comment": f"  Cluster {cluster_name} has not been scraped in the last {days} days."
            }
        )


@dataclass
class ClusterJobScrapingCheck(HealthCheck):
    time_interval: int = 7
    time_unit: int = 1
    stddev: int = 2
    verbose: bool = False

    def check(self):
        time_interval = timedelta(days=self.time_interval)
        time_unit = timedelta(days=self.time_unit)
        cluster_name = self.parameters["cluster_name"]
        nb_stddev = self.stddev
        verbose = self.verbose
        if check_nb_jobs_per_cluster_per_time(
            time_interval=time_interval,
            time_unit=time_unit,
            cluster_names=[cluster_name],
            nb_stddev=nb_stddev,
            verbose=verbose,
        ):
            return self.ok
        return self.fail(
            statuses={
                "comment": f"Cluster {cluster_name} has not enough jobs scrapped",
                "time_interval": time_interval,
                "time_unit": time_unit,
                "stddev": nb_stddev,
            }
        )


@dataclass
class UsersInJobsCheck(HealthCheck):
    time_interval: int = 7  # days

    def check(self):
        time_interval = timedelta(days=self.time_interval)
        missing_users = check_users_in_jobs(time_interval=time_interval)
        if not missing_users:
            return self.ok
        return self.fail(
            statuses={
                "comment": f"Missing users in jobs: {missing_users}",
                "time_interval": time_interval,
            }
        )
