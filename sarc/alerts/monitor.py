import json
import logging
from pathlib import Path

from apischema import deserialize
from gifnoc import TaggedSubclass
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
        pth = Path(event.src_path)
        if pth.is_file() and pth.suffix == ".json":
            self.monitor.process(pth)


class HealthMonitor:
    def __init__(self, directory, checks, poll=False):
        self.directory = directory
        self.checks: dict[str, HealthCheck] = checks
        self.poll = poll
        self.observer = None
        self.state = {}

    def recover_state(self):
        for name, check in self.checks.items():
            if check.active:
                self.state[name] = check.latest_result()

    def process(self, file):
        try:
            data = deserialize(
                type=TaggedSubclass[CheckResult],
                data=json.loads(file.read_text()),
            )
            if data.name in self.checks:
                check = self.checks[data.name]
                if check.active:
                    self.state[data.name] = data
        except Exception as exc:
            logger.error(f"Unexpected {type(exc).__name__}: {exc}", exc_info=exc)

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

    def stop(self):
        self.observer.stop()

    def join(self):
        self.observer.join()
