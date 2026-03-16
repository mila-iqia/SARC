import logging
from dataclasses import dataclass

from sarc.alerts.healthcheck_state import get_healthcheck_state_collection

logger = logging.getLogger(__name__)


@dataclass
class HealthListCommand:
    """Show health check states saved in database."""

    def execute(self) -> int:
        repo = get_healthcheck_state_collection()
        nb_states = repo.get_collection().count_documents({})
        logger.info(f"There are {nb_states} health check states saved in database.")
        for state in repo.get_states():
            print(state.model_dump_json(indent=2))
        return 0
