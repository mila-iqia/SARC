from __future__ import annotations

from dataclasses import dataclass

import sarc.ldap.acquire


@dataclass
class AcquireUsers:
    def execute(self) -> int:
        sarc.ldap.acquire.run()
        return 0
