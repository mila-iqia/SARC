import json
import logging
from dataclasses import dataclass
from pathlib import Path

from simple_parsing import field

from sarc.config import config, using_sarc_mode
from sarc.scraping.users import UserMatch, update_user

logger = logging.getLogger(__name__)


@dataclass
class UsermatchApply:
    path: Path = field(positional=True)

    def execute(self) -> int:
        raw = json.loads(self.path.read_text())
        entries = [UserMatch.model_validate(e) for e in raw]

        with using_sarc_mode("scraping"):
            with config().db.session() as sess:
                for i, user_match in enumerate(entries, 1):
                    logger.info(
                        "Applying %d/%d: %s:%s → %s:%s",
                        i,
                        len(entries),
                        user_match.matching_id.name,
                        user_match.matching_id.mid,
                        *next(
                            (m.name, m.mid)
                            for m in user_match.known_matches
                        ),
                    )
                    update_user(sess, user_match)
                sess.commit()

        print(f"Applied {len(entries)} user match{'es' if len(entries) != 1 else ''}.")
        return 0
