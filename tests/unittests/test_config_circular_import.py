"""
Regression test to prevent circular imports.

Previously, `gifnoc.set_sources("${envfile:SARC_CONFIG}")`, which is called globally
inside `sarc.config` module, would immediately deserialize config YAML file. If file
contains a health monitor definition, then health check classes would be imported,
then sarc.db symbols would be imported, then sarc.config would be imported, leading
to a circular import.

To prevent this, we now import sarc.config inside sarc.db.__init__ functions.
"""

import os
import subprocess
import sys
import textwrap
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_import_sarc_config_does_not_trigger_circular_import(tmp_path):
    cfg = tmp_path / "sarc.yaml"
    cfg.write_text(
        textwrap.dedent("""\
        sarc:
          db:
            host: localhost
            name: sarc-test
            auto_upgrade: false
          clusters: {}
          patches: patches
          health_monitor:
            checks:
              cluster_scraping:
                $class: "sarc.alerts.usage_alerts.cluster_scraping:ClusterScrapingCheck"
                active: true
    """)
    )
    result = subprocess.run(
        [sys.executable, "-c", "from sarc.config import config; config().db"],
        env={**os.environ, "SARC_MODE": "scraping", "SARC_CONFIG": str(cfg)},
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, (
        "Likely circular-import regression in sarc/config.py.\n"
        f"--- stderr ---\n{result.stderr}\n"
        f"--- stdout ---\n{result.stdout}"
    )
