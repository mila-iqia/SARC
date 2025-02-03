import logging
import random
from dataclasses import dataclass
from datetime import timedelta

from sarc.alerts.common import CheckResult, HealthCheck
from sarc.alerts.usage_alerts.cluster_response import check_cluster_response


# this is a simple check that will fail 50% of the time
# it uses a custom result class to add more context to the result
@dataclass
class HelloWorldResult(CheckResult):
    custom_comment: str = ""


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
class HelloWorld2Check(HealthCheck):
    def check(self):
        random_number = random.random()
        if random_number < 0.5:
            return self.fail(
                statuses={
                    "comment": "Hello, HealthMonitor World! You were chosen randomly to fail...",
                    "random_number": random_number,
                }
            )
        return self.ok(
            statuses={
                "comment": "Hello, HealthMonitor World!",
                "random_number": random_number,
            }
        )


# cheks if the cluster responded in the last `days` days
class ClusterResponseCheck(HealthCheck):
    def check(self):
        logging.warning("Checking cluster response...")
        cluster_name = self.parameters["cluster_name"]
        # days = self.parameters["days"]
        days = 7
        if check_cluster_response(
            time_interval=timedelta(days=days), cluster_name=cluster_name
        ):
            return self.ok
        return self.fail(
            statuses={
                "comment": f"  Cluster {cluster_name} has not been scraped in the last {days} days."
            }
        )
