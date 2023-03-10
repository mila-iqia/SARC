from sarc.config import config


class ChoicesContainer:
    def __init__(self, choices):
        self.choices = choices

    def __contains__(self, item):
        return item in self.choices

    def __iter__(self):
        return iter(self.choices)


clusters = ChoicesContainer(list(config().clusters.keys()))
