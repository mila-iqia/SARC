from __future__ import annotations

from dataclasses import dataclass

import sarc.ldap.acquire


@dataclass
class AcquireLDAP:
    def execute(self) -> int:
        sarc.ldap.acquire.run()
        return 0
