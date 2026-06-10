import logging
from unittest.mock import MagicMock, patch

from sarc.config import EmailConfig
from sarc.notifications.email import EmailClient
from sarc.notifications.slack import SendStatus


def _smtp_mock():
    mock = MagicMock()
    mock.__enter__ = lambda s: mock
    mock.__exit__ = MagicMock(return_value=False)
    return mock


def test_send_success():
    cfg = EmailConfig(host="smtp.example.com", port=587, from_address="noreply@example.com")
    smtp = _smtp_mock()

    with patch("smtplib.SMTP", return_value=smtp):
        result = EmailClient(cfg).send_plaintext("user@example.com", "Subject", "Body")

    assert result.status == SendStatus.OK
    smtp.send_message.assert_called_once()
    smtp.starttls.assert_not_called()


def test_send_with_credentials():
    cfg = EmailConfig(
        host="smtp.example.com",
        port=587,
        from_address="noreply@example.com",
        username="sender",
        password="s3cr3t",
    )
    smtp = _smtp_mock()

    with patch("smtplib.SMTP", return_value=smtp):
        result = EmailClient(cfg).send_plaintext("user@example.com", "Subject", "Body")

    assert result.status == SendStatus.OK
    smtp.starttls.assert_called_once()
    smtp.login.assert_called_once_with("sender", "s3cr3t")


def test_send_not_configured(caplog):
    with caplog.at_level(logging.WARNING):
        result = EmailClient(None).send_plaintext("user@example.com", "Subject", "Body")

    assert result.status == SendStatus.FAILED
    assert "smtp_not_configured" in result.detail
    assert "SMTP not configured" in caplog.text


def test_send_smtp_error():
    cfg = EmailConfig(host="smtp.example.com", port=587, from_address="noreply@example.com")
    smtp = _smtp_mock()
    smtp.send_message.side_effect = Exception("Connection refused")

    with patch("smtplib.SMTP", return_value=smtp):
        result = EmailClient(cfg).send_plaintext("user@example.com", "Subject", "Body")

    assert result.status == SendStatus.FAILED
    assert "Connection refused" in result.detail
