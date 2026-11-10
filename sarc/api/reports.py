"""Public report uploads (``POST /upload-report``) and an admin-only browse
interface for what they stored (``/reports``).

Anyone may upload a Markdown report; it is saved under the configured
``subdirectory`` of the storage directory (``config.storage /
config.server.report_upload``), inside a per-category subdirectory chosen
from a configurable whitelist. Uploads are rate limited per client IP and
globally; all limits, the whitelist, and the window come from
``ReportUploadConfig``.

The limiter keeps its sliding-window logs in process memory, so it only
enforces limits for a single API process. Multiple workers each get their
own budget; tighten ``max_per_ip``/``max_total`` accordingly if the API is
run with more than one worker.

``/reports`` is a minimalist HTML page to browse the categories, view a
report rendered from Markdown, and download the file. It is locked to the
``admin`` capability: guests are redirected to the OAuth login (like the
``/dash`` pages) and signed-in non-admins get a 403.
"""

import logging
import math
import re
import threading
import time
from collections import deque
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import quote
from uuid import uuid4

import mistune
from fastapi import (
    APIRouter,
    Depends,
    File,
    HTTPException,
    Query,
    Request,
    UploadFile,
    status,
)
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from sarc.api.v0 import require_admin
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
    ``<storage>/<subdirectory>/<category>/`` under a unique name
    derived from the uploaded filename.
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

    reports_dir = _reports_root() / category
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


# --------------------------------------------------------------------------- #
# Admin-only interface to browse, view and download the stored reports.
# --------------------------------------------------------------------------- #

_templates = Jinja2Templates(directory=Path(__file__).parent)

# Reports are uploaded by anyone but read here by admins, so the renderer is
# the XSS boundary: escape=True makes mistune render raw HTML in a report as
# text, and its HTML renderer already neutralizes dangerous link targets
# (javascript:... becomes a dead link). Safe to embed in the page.
_markdown = mistune.create_markdown(escape=True, plugins=["table", "strikethrough"])


async def _reports_login_redirect(request: Request) -> None:
    """Router-level gate for ``/reports``, mirroring metrics'
    ``_dash_login_redirect``: an anonymous browser request is redirected to
    the OAuth login page (with the target URL recorded in the session, so the
    admin lands back here) instead of getting the API's 401. Registered
    *before* ``require_admin`` so guests redirect while signed-in non-admins
    fall through to its 403. No-op when auth is disabled, where every request
    is admin anyway."""
    auth = config.server.auth
    if auth is None:
        return
    await auth.ensure_email(request)


browser_router = APIRouter(
    prefix="/reports",
    dependencies=[Depends(_reports_login_redirect), Depends(require_admin)],
)


def _reports_root() -> Path:
    """The directory ``POST /upload-report`` stores reports in, or a 503 when
    no storage directory is configured. Resolved so that the containment
    checks on browsed paths compare canonical paths."""
    if config.storage is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Storage directory not configured; reports are unavailable",
        )
    root = config.storage / config.server.report_upload.subdirectory
    return root.resolve()


def _category_dir(root: Path, category: str) -> Path:
    """The directory of ``category``, or 404.

    A 404 rather than the upload's 400: here a bad category just names a page
    that does not exist, and the message should not hint at a whitelist. The
    name must be a plain directory name (no separators, not ``.``/``..``, not
    empty) resolving inside the reports root, so no query parameter can
    traverse out of it; a configured-but-never-uploaded category simply has
    no directory yet.
    """
    detail = f"No such category: {category!r}"
    if not category or category in (".", "..") or Path(category).name != category:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=detail)
    path = (root / category).resolve()
    if not path.is_relative_to(root) or not path.is_dir():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=detail)
    return path


def _report_files(directory: Path) -> list[Path]:
    """The reports stored in ``directory``: plain files whose name ends in
    ``.md`` (case-insensitive, like the upload's own check). ``iterdir``
    rather than ``glob``, which would read metacharacters in names."""
    return [
        p for p in directory.iterdir() if p.is_file() and p.name.lower().endswith(".md")
    ]


def _report_path(root: Path, category: str, filename: str) -> Path:
    """The stored report ``category/filename``, or 404 unless it is a plain
    ``.md`` file inside the category's directory (same traversal guards as
    the category itself)."""
    detail = f"No such report: {category}/{filename}"
    directory = _category_dir(root, category)
    if (
        not filename
        or not filename.lower().endswith(".md")
        or Path(filename).name != filename
    ):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=detail)
    path = (directory / filename).resolve()
    if not path.is_relative_to(root) or not path.is_file():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=detail)
    return path


def _human_size(num: int) -> str:
    """A byte count as a short string (``1.2 KB``)."""
    size = float(num)
    for unit in ("B", "KB", "MB"):
        if size < 1024:
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} GB"


def _fmt_time(ts: float) -> str:
    """A file mtime as the UTC timestamp shown in the tables."""
    return datetime.fromtimestamp(ts, UTC).strftime("%Y-%m-%d %H:%M")


@browser_router.get("", response_class=HTMLResponse)
def browse_reports(
    request: Request,
    category: str | None = Query(
        default=None, description="Category (subdirectory) to list."
    ),
    file: str | None = Query(
        default=None, description="Report (.md file) to view; needs a category."
    ),
):
    """Serve the report browser: one page, three modes picked by the query
    parameters, so the whole interface is two links deep with no client-side
    state.

    * ``/reports`` — the categories present on disk (one appears when its
      first report is uploaded), with report counts and last-upload times.
    * ``/reports?category=...`` — that category's reports, newest first.
    * ``/reports?category=...&file=...`` — one report rendered from Markdown;
      ``/reports/download`` serves the file itself.

    Admin-only through the router's dependencies.
    """
    root = _reports_root()
    if category is None:
        if file is not None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="file requires a category",
            )
        categories = []
        if root.is_dir():
            for entry in sorted(root.iterdir()):
                # Hidden directories are not browsable categories.
                if not entry.is_dir() or entry.name.startswith("."):
                    continue
                files = _report_files(entry)
                categories.append(
                    {
                        "name": entry.name,
                        "count": len(files),
                        "latest": (
                            _fmt_time(max(f.stat().st_mtime for f in files))
                            if files
                            else "—"
                        ),
                        "url": f"/reports?category={quote(entry.name)}",
                    }
                )
        return _templates.TemplateResponse(
            request, "reports.html", {"mode": "home", "categories": categories}
        )

    if file is None:
        files = sorted(
            _report_files(_category_dir(root, category)),
            key=lambda p: p.stat().st_mtime,
            reverse=True,  # newest first: the last upload is the interesting one
        )
        return _templates.TemplateResponse(
            request,
            "reports.html",
            {
                "mode": "category",
                "category": category,
                "files": [
                    {
                        "name": p.name,
                        "size": _human_size(p.stat().st_size),
                        "mtime": _fmt_time(p.stat().st_mtime),
                        "view_url": f"/reports?category={quote(category)}&file={quote(p.name)}",
                        "download_url": (
                            f"/reports/download?category={quote(category)}&file={quote(p.name)}"
                        ),
                    }
                    for p in files
                ],
            },
        )

    path = _report_path(root, category, file)
    stat = path.stat()
    return _templates.TemplateResponse(
        request,
        "reports.html",
        {
            "mode": "report",
            "category": category,
            "category_url": f"/reports?category={quote(category)}",
            "report": {
                "name": path.name,
                # The one `| safe` in the template: mistune ran with
                # escape=True (see _markdown), so no report content can
                # inject markup into this admin page.
                "html": _markdown(path.read_bytes().decode("utf-8", errors="replace")),
                "size": _human_size(stat.st_size),
                "mtime": _fmt_time(stat.st_mtime),
                "download_url": f"/reports/download?category={quote(category)}&file={quote(path.name)}",
            },
        },
    )


@browser_router.get("/download")
def download_report(
    category: str = Query(description="Category (subdirectory) of the report."),
    file: str = Query(description="Report (.md file) to download."),
) -> FileResponse:
    """Serve one stored report as a file attachment; the page above renders
    it instead. Admin-only like the rest of the router."""
    return FileResponse(
        _report_path(_reports_root(), category, file),
        media_type="text/markdown",
        filename=file,
    )
