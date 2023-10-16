from __future__ import annotations

from dataclasses import dataclass

from simple_parsing import field

import sarc.ldap.acquire


@dataclass
class AcquireUsers:
    prompt: bool = field(
        action="store_true",
        help="Provide a prompt for manual matching if automatic matching fails (default: False)",
    )

    def execute(self) -> int:
        sarc.ldap.acquire.run(prompt=self.prompt)
        return 0
