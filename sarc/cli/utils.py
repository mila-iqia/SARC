from sarc.jobs.job import get_clusters


class ChoicesContainer:
    def __init__(self, choices):
        self.choices = choices

    def __contains__(self, item):
        return item in self.choices

    def __iter__(self):
        return iter(self.choices)


clusters = ChoicesContainer(list(get_clusters()))
