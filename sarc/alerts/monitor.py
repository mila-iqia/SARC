import json
import logging
from pathlib import Path

from gifnoc.std import time
from serieux import TaggedSubclass, deserialize
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
    def __init__(self, directory, checks, poll=0):
        self.directory = directory
        self.checks: dict[str, HealthCheck] = checks
        self.poll = poll
        self.observer = None
        self.state = {}

    def recover_state(self):
        """Recover the current state from the latest result for each check."""
        for name, check in self.checks.items():
            if check.active:
                self.state[name] = check.latest_result()

    def process(self, file):
        """Process the contents of a new file."""
        try:
            data = deserialize(
                TaggedSubclass[CheckResult],
                json.loads(file.read_text()),
            )
            if data.name in self.checks:
                check = self.checks[data.name]
                if check.active:
                    self.state[data.name] = data
        except Exception as exc:  # pylint: disable=W0718
            logger.error(f"Unexpected {type(exc).__name__}: {exc}", exc_info=exc)

    @property
    def status(self) -> dict[str, str]:
        """Current status as a {status_name: status} dict."""
        now = time.now()
        return {
            name: (
                data.status.name if (not data.expiry or data.expiry > now) else "STALE"
            )
            for name, data in self.state.items()
        }

    def start(self):
        """Start the monitor."""
        self.recover_state()
        self.observer = PollingObserver(timeout=self.poll) if self.poll else Observer()
        self.observer.schedule(MonitorHandler(self), self.directory, recursive=True)
        self.observer.start()

    def stop(self):
        """Stop the monitor."""
        self.observer.stop()

    def join(self):
        """Wait for the monitor to finish."""
        self.observer.join()
