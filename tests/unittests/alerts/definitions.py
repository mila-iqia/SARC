from dataclasses import dataclass

from sarc.alerts.common import CheckResult, HealthCheck


@dataclass
class BeanResult(CheckResult):
    more: int = 0


@dataclass
class BeanCheck(HealthCheck):
    __result_class__ = BeanResult

    beans: int = 0

    def check(self):
        if self.beans == 666:
            raise ValueError("What a beastly number")
        elif self.beans < 0:
            return {
                "positive": False,
                "negative": True,
                "fillbelly": False,
            }
        elif self.beans < 10:
            return self.fail(more=10 - self.beans)
        else:
            return self.ok


@dataclass
class LetterCheck(HealthCheck):
    def check(self):
        letter = self.parameters["letter"]
        if letter == "alpha":
            return self.fail
        elif letter == "beta":
            return self.ok
        elif letter == "gamma":
            return self.fail
        else:
            raise ValueError("I DO NOT KNOW THIS LETTER")
