"""Public report uploads: ``POST /upload-report``.

Anyone may upload a Markdown report; it is saved under the ``user-reports``
subdirectory of the cache (``config.cache / config.server.report_upload``),
inside a per-category subdirectory chosen from a configurable whitelist.
Uploads are rate limited per client IP and globally; all limits, the
whitelist, and the window come from ``ReportUploadConfig``.

The limiter keeps its sliding-window logs in process memory, so it only
enforces limits for a single API process. Multiple workers each get their
own budget; tighten ``max_per_ip``/``max_total`` accordingly if the API is
run with more than one worker.
"""

import logging
import math
import re
import threading
import time
from collections import deque
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from fastapi import APIRouter, File, HTTPException, Query, Request, UploadFile, status
from pydantic import BaseModel

from sarc.config import config

logger = logging.getLogger(__name__)

router = APIRouter()

# Chunk size used when draining the upload while enforcing the size limit.
_CHUNK_SIZE = 64 * 1024

_SAFE_STEM = re.compile(r"[^A-Za-z0-9._-]+")


def _now() -> float:
    """Monotonic clock used by the rate limiter; monkeypatched in tests."""
    return time.monotonic()


class RateLimiter:
    """Sliding-window log rate limiter with a global and a per-key budget.

    Each allowed call appends its timestamp to the key's window and to the
    global window; a call is rejected when either window is already at its
    limit. Slots are consumed by the attempt, not by the saved report: a
    request that passes the limiter and later fails (e.g. oversize) still
    counts, so failed uploads are no cheaper than successful ones. Expired
    timestamps are pruned on every call, and keys idle for a full window
    are swept periodically, so memory stays proportional to the recent
    request rate.

    The limits are not stored here: they are passed to `try_acquire` so that
    live config changes take effect immediately.
    """

    def __init__(self) -> None:
        self._global: deque[float] = deque()
        self._per_key: dict[str, deque[float]] = {}
        self._last_sweep = 0.0
        self._lock = threading.Lock()

    def reset(self) -> None:
        """Drop all recorded activity (used by tests)."""
        with self._lock:
            self._global.clear()
            self._per_key.clear()
            self._last_sweep = 0.0

    def try_acquire(
        self, key: str, now: float, limit_per_key: int, limit_total: int, window: float
    ) -> float | None:
        """Consume one slot for ``key``.

        Returns ``None`` when the call is allowed (and its timestamp is
        recorded), or the seconds until a slot may free up when it is
        rejected.
        """
        with self._lock:
            cutoff = now - window
            while self._global and self._global[0] <= cutoff:
                self._global.popleft()

            per = self._per_key.get(key)
            if per is not None:
                while per and per[0] <= cutoff:
                    per.popleft()
                if not per:
                    del self._per_key[key]
                    per = None

            if limit_per_key <= 0 or limit_total <= 0:
                return window
            if len(self._global) >= limit_total or (
                per is not None and len(per) >= limit_per_key
            ):
                return self._retry_after(per, now, limit_per_key, limit_total, window)

            if now - self._last_sweep >= window:
                self._sweep(cutoff)
                self._last_sweep = now

            self._global.append(now)
            self._per_key.setdefault(key, deque()).append(now)
            return None

    def _retry_after(
        self,
        per: deque[float] | None,
        now: float,
        limit_per_key: int,
        limit_total: int,
        window: float,
    ) -> float:
        """Seconds until the caller's request may next succeed.

        A request needs a free slot in *both* windows, so it waits for the
        latest expiry among the saturated ones. Caller must hold the lock.
        """
        waits = []
        if self._global and len(self._global) >= limit_total:
            waits.append(self._global[0] + window - now)
        if per is not None and len(per) >= limit_per_key:
            waits.append(per[0] + window - now)
        return max(waits, default=window)

    def _sweep(self, cutoff: float) -> None:
        """Forget keys whose window is fully expired. Caller holds the lock."""
        for key in [k for k, dq in self._per_key.items() if dq[-1] <= cutoff]:
            del self._per_key[key]


limiter = RateLimiter()


class UploadReportResponse(BaseModel):
    filename: str
    """Name the report was saved under, inside the category's directory."""
    category: str
    """Whitelisted category (subdirectory) the report was stored in."""
    size: int
    """Report size in bytes."""


def _safe_stem(filename: str) -> str:
    """Validate the uploaded name is a ``.md`` file and return a safe stem.

    Keeps only path-free, shell-safe characters so the saved name cannot
    escape the report directory.
    """
    name = Path(filename).name
    if not name.lower().endswith(".md"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Report must be a .md file"
        )
    stem = _SAFE_STEM.sub("-", name[:-3]).strip("-.")
    return stem[:100] or "report"


@router.post("/upload-report", status_code=status.HTTP_201_CREATED)
async def upload_report(
    request: Request,
    category: str = Query(
        description=(
            "Report category; must be one of the configured categories, and "
            "is stored as a subdirectory."
        )
    ),
    file: UploadFile = File(description="Markdown report to upload, as .md."),
) -> UploadReportResponse:
    """Upload a Markdown report; anyone may post one.

    The report must be a ``.md`` file of at most ``max_size`` bytes.
    ``category`` is required and selects the subdirectory the report lands
    in; it must be one of ``categories``. An unknown or non-whitelisted
    category is refused with a 400, and — like any other failed upload — it
    spends the caller's slot; an omitted category is a plain 422. Each
    client IP may upload ``max_per_ip`` reports per ``window_seconds``, and
    no more than ``max_total`` per window are accepted overall; a
    rate-limited request gets a 429 with a ``Retry-After`` header. Uploads
    rejected for size still consume the caller's slot, so spamming oversized
    files is no cheaper than uploading. The report is saved in
    ``<cache>/<subdirectory>/<category>/`` under a unique name derived from
    the uploaded filename.
    """
    cfg = config.server.report_upload

    # The extension check runs before the slot is taken: it is free and
    # stateless, so a wrong file type costs the caller nothing. The category
    # whitelist is checked just after, so category abuse is charged like any
    # other failed upload while still being refused before we touch the body.
    stem = _safe_stem(file.filename or "")

    client = request.client.host if request.client else "unknown"
    retry_after = limiter.try_acquire(
        client, _now(), cfg.max_per_ip, cfg.max_total, cfg.window_seconds
    )
    if retry_after is not None:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Rate limit exceeded, try again later",
            headers={"Retry-After": str(max(1, math.ceil(retry_after)))},
        )
    if category not in cfg.categories:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"Unknown category '{category}'; expected one of "
                f"{sorted(cfg.categories)}"
            ),
        )

    buf = bytearray()
    while chunk := await file.read(_CHUNK_SIZE):
        buf += chunk
        if len(buf) > cfg.max_size:
            raise HTTPException(
                status_code=status.HTTP_413_CONTENT_TOO_LARGE,
                detail=f"Report larger than {cfg.max_size} bytes",
            )

    if config.cache is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Cache directory not configured; cannot store reports",
        )
    reports_dir = config.cache / cfg.subdirectory / category
    reports_dir.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
    filename = f"{stem}-{stamp}-{uuid4().hex[:8]}.md"
    (reports_dir / filename).write_bytes(bytes(buf))

    logger.info(
        "Stored uploaded report %s/%s (%d bytes) from %s",
        category,
        filename,
        len(buf),
        client,
    )
    return UploadReportResponse(filename=filename, category=category, size=len(buf))
