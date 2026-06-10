from __future__ import annotations

import logging
import smtplib
from email.message import EmailMessage
from typing import TYPE_CHECKING

from sarc.notifications.slack import SendResult, SendStatus

if TYPE_CHECKING:
    from sarc.config import EmailConfig

logger = logging.getLogger(__name__)


class EmailClient:
    def __init__(self, config: EmailConfig | None) -> None:
        self._config = config

    def send_plaintext(self, to_address: str, subject: str, body: str) -> SendResult:
        if self._config is None:
            logger.warning("SMTP not configured; skipping email to %s", to_address)
            return SendResult(SendStatus.FAILED, "smtp_not_configured")

        msg = EmailMessage()
        msg["From"] = self._config.from_address
        msg["To"] = to_address
        msg["Subject"] = subject
        msg.set_content(body)

        try:
            with smtplib.SMTP(self._config.host, self._config.port) as smtp:
                if self._config.username and self._config.password is not None:
                    smtp.starttls()
                    smtp.login(self._config.username, self._config.password)
                smtp.send_message(msg)
            return SendResult(SendStatus.OK)
        except Exception as exc:
            logger.error("Email send failed to %s: %s", to_address, exc)
            return SendResult(SendStatus.FAILED, str(exc))
