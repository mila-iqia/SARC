import json
import logging
from pathlib import Path

from apischema import deserialize
from gifnoc.std import time
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver

from .common import CheckResult, HealthCheck

logger = logging.getLogger(__name__)


class MonitorHandler(FileSystemEventHandler):
    def __init__(self, monitor):
        self.monitor = monitor

    def on_created(self, event):
        logger.debug(f"Filesystem event: {event.event_type} @ {event.src_path}")
        self.monitor.process(Path(event.src_path))


class HealthMonitor:
    def __init__(self, directory, checks, poll=False):
        self.directory = directory
        self.checks: list[HealthCheck] = checks
        self.poll = poll
        self.observer = None
        self.state = {}

    def recover_state(self):
        for check in self.checks:
            self.state[check.name] = check.latest_result()

    def process(self, file):
        data = deserialize(
            type=CheckResult,
            data=json.loads(file.read_text()),
        )
        self.state[data.name] = data

    @property
    def status(self):
        now = time.now()
        return {
            name: (
                data.status.name if (not data.expiry or data.expiry > now) else "STALE"
            )
            for name, data in self.state.items()
        }

    def start(self):
        self.recover_state()
        self.observer = (PollingObserver if self.poll else Observer)()
        self.observer.schedule(MonitorHandler(self), self.directory, recursive=True)
        self.observer.start()

    def join(self):
        self.observer.join()
