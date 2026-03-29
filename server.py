"""
NOC dashboard server: ThousandEyes MCP (streamable HTTP) + Flask.

All ThousandEyes data is gathered via MCP only; no direct ThousandEyes REST API calls.
See: https://docs.thousandeyes.com/product-documentation/integration-guides/thousandeyes-mcp-server

MCP collection practices: batched test filters, bounded retries with backoff on 429,
stagger between batches to avoid rate limits, parallel independent calls only where
merge order stays deterministic; cache TTL aligned to refresh interval.
"""

import asyncio
import csv
import hashlib
import hmac
import io
import json
import logging
import os
import secrets
import random
import re
import sqlite3
import threading
import time

import httpx
from collections import defaultdict
from datetime import datetime, timezone, timedelta

from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv
from flask import Flask, jsonify, redirect, request, send_file, session, url_for

load_dotenv()

# Log level: ERROR (default) = only errors/traces; WARNING, INFO, DEBUG for more verbosity
_LOG_LEVEL_NAME = (os.getenv("LOG_LEVEL") or "ERROR").strip().upper()
_LOG_LEVEL = getattr(logging, _LOG_LEVEL_NAME, logging.ERROR)
if _LOG_LEVEL_NAME == "TRACE":
    _LOG_LEVEL = logging.DEBUG
logging.basicConfig(
    level=_LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

app = Flask(__name__)
# Werkzeug access logs: off unless LOG_LEVEL is INFO or DEBUG (reduces noise at default ERROR)
if _LOG_LEVEL <= logging.DEBUG:
    logging.getLogger("werkzeug").setLevel(logging.DEBUG)
elif _LOG_LEVEL <= logging.INFO:
    logging.getLogger("werkzeug").setLevel(logging.INFO)
else:
    logging.getLogger("werkzeug").setLevel(logging.WARNING)

# --- Config (env); no secrets logged ---
MCP_URL = os.getenv("MCP_URL", "https://api.thousandeyes.com/mcp").strip()
MCP_TOKEN = (os.getenv("TE_TOKEN") or "").strip()


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (ValueError, TypeError):
        log.warning("Invalid env var %s, using default %d", name, default)
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (ValueError, TypeError):
        log.warning("Invalid env var %s, using default %g", name, default)
        return default


REFRESH_MINUTES = max(1, min(120, _env_int("REFRESH_MINUTES", 15)))
MCP_BATCH_SIZE = max(5, min(50, _env_int("MCP_BATCH_SIZE", 15)))
# Delay between batch requests; higher reduces 429s (default 1.0s; min 0.5s when set)
MCP_INTER_BATCH_DELAY = max(0.5, _env_float("MCP_INTER_BATCH_DELAY_SEC", 1.0))
# Extra KPIs: longer delay between batches to avoid 429 (default 2.0s). Set MCP_EXTRA_KPI_DELAY_SEC higher if 429s persist.
MCP_EXTRA_KPI_BATCH_DELAY = max(1.0, _env_float("MCP_EXTRA_KPI_DELAY_SEC", 2.0))
# Max concurrent get_network_app_synthetics_metrics calls (1 = sequential, reduces 429s)
MCP_SYNTH_CONCURRENCY = max(1, min(10, _env_int("MCP_SYNTH_CONCURRENCY", 1)))
# Global MCP rate limit: max requests per minute (e.g. 240 rpm = 4 req/s); min interval between calls
MCP_MAX_RPM = max(10, min(500, _env_int("MCP_MAX_RPM", 240)))
_mcp_min_interval_sec = 60.0 / MCP_MAX_RPM
# MCP uses httpx; ConnectTimeout = TCP/TLS to MCP_URL did not finish in time (firewall, proxy, slow egress).
# Default connect 90s (was 60): many corporate paths need more than 60s for first TLS to api.thousandeyes.com.
MCP_CONNECT_TIMEOUT_SEC = max(5.0, min(600.0, _env_float("MCP_CONNECT_TIMEOUT_SEC", 90.0)))
MCP_READ_TIMEOUT_SEC = max(30.0, min(3600.0, _env_float("MCP_READ_TIMEOUT_SEC", 300.0)))
# Retries per MCP tool call (transport errors, HTTP 5xx, broken stream). Increase if logs show repeated ConnectTimeout.
MCP_RETRIES = max(1, min(20, _env_int("MCP_RETRIES", 6)))
# Exponential backoff base for ConnectTimeout / ConnectError / ReadTimeout between retries (capped).
MCP_TRANSPORT_BACKOFF_BASE_SEC = max(2.0, min(120.0, _env_float("MCP_TRANSPORT_BACKOFF_BASE_SEC", 8.0)))
MCP_TRANSPORT_BACKOFF_MAX_SEC = max(
    MCP_TRANSPORT_BACKOFF_BASE_SEC,
    min(600.0, _env_float("MCP_TRANSPORT_BACKOFF_MAX_SEC", 120.0)),
)

# Optional dashboard login: set DASHBOARD_USERNAME and DASHBOARD_PASSWORD in .env
AUTH_USERNAME = (os.getenv("DASHBOARD_USERNAME") or "").strip()
AUTH_PASSWORD = (os.getenv("DASHBOARD_PASSWORD") or "").strip()
AUTH_ENABLED = bool(AUTH_USERNAME and AUTH_PASSWORD)
_flask_secret = (os.getenv("SECRET_KEY") or os.getenv("FLASK_SECRET_KEY") or "").strip()
if AUTH_ENABLED and not _flask_secret:
    _flask_secret = secrets.token_hex(32)
    log.warning(
        "Dashboard auth enabled but SECRET_KEY/FLASK_SECRET_KEY not set; using ephemeral signing key "
        "(sessions reset on restart). Set SECRET_KEY in .env for production."
    )
elif not _flask_secret:
    _flask_secret = secrets.token_hex(16)
app.secret_key = _flask_secret
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_SECURE"] = (os.getenv("SESSION_COOKIE_SECURE") or "").strip().lower() in (
    "1",
    "true",
    "yes",
)
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(hours=max(1, min(168, _env_int("SESSION_HOURS", 8))))

# Single shared event loop for all MCP async calls — eliminates per-thread event-loop races and
# ensures the rate-limiter and semaphore are shared across all Flask worker threads.
_async_loop_ready = threading.Event()
_mcp_rate_limit_lock: asyncio.Lock  # created inside the loop thread
_mcp_last_call_time: float = 0.0
_synth_semaphore: asyncio.Semaphore  # created inside the loop thread


def _run_async_loop() -> None:
    """Thread target: owns the shared event loop for all MCP coroutines."""
    global _async_loop, _mcp_rate_limit_lock, _synth_semaphore
    _async_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_async_loop)
    _mcp_rate_limit_lock = asyncio.Lock()
    _synth_semaphore = asyncio.Semaphore(MCP_SYNTH_CONCURRENCY)
    _async_loop_ready.set()
    _async_loop.run_forever()


_async_loop: asyncio.AbstractEventLoop  # assigned by _run_async_loop
_async_loop_thread = threading.Thread(target=_run_async_loop, daemon=True, name="mcp-async-loop")
_async_loop_thread.start()
_async_loop_ready.wait()  # block until the loop is ready before accepting requests


def run_async(coro):
    """Submit a coroutine to the shared MCP event loop; block the calling thread until done."""
    future = asyncio.run_coroutine_threadsafe(coro, _async_loop)
    return future.result()


async def _mcp_rate_limit_wait() -> None:
    """Enforce global MCP request rate (MCP_MAX_RPM). All coroutines share one lock/counter."""
    global _mcp_last_call_time
    async with _mcp_rate_limit_lock:
        now = time.monotonic()
        wait = max(0.0, _mcp_min_interval_sec - (now - _mcp_last_call_time))
        if wait > 0:
            await asyncio.sleep(wait)
        _mcp_last_call_time = time.monotonic()


async def _get_synth_sem() -> asyncio.Semaphore:
    """Return the shared semaphore for MCP synth calls."""
    return _synth_semaphore

# Cache window metrics at least one refresh cycle (capped)
METRICS_TTL_SECONDS = max(300, min(3600, REFRESH_MINUTES * 60))

# Local KPI history (SQLite) for trending without re-querying MCP for past intervals
_default_kpi_db = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "kpi_history.sqlite")
KPI_DB_PATH = (os.getenv("KPI_DB_PATH") or _default_kpi_db).strip()
KPI_HISTORY_DISABLED = (os.getenv("KPI_HISTORY_DISABLED") or "").strip().lower() in ("1", "true", "yes")
KPI_HISTORY_RETENTION_DAYS = max(1, min(730, _env_int("KPI_HISTORY_RETENTION_DAYS", 90)))

# Hourly rollup retention (per-test + scalar KPIs stored each hour)
KPI_HOURLY_RETENTION_DAYS = max(1, min(90, _env_int("KPI_HOURLY_RETENTION_DAYS", 14)))
# Daily aggregate retention (computed from hourly data)
KPI_DAILY_RETENTION_DAYS = max(1, min(730, _env_int("KPI_DAILY_RETENTION_DAYS", 60)))
# Minimum number of hourly buckets required before serving multi-hour views from DB
_HOURLY_MIN_BUCKETS_FOR_DB_VIEW = 20

WINDOWS = {
    "1h": 1,
    "6h": 6,
    "12h": 12,
    "24h": 24,
    "2d": 48,
    "7d": 168,
}

# First paint: load a small window first, then 1h hourly + 24h cache fallback in background
INITIAL_BOOTSTRAP_DISABLED = (os.getenv("INITIAL_BOOTSTRAP_DISABLED") or "").strip().lower() in ("1", "true", "yes")
_ibw = (os.getenv("INITIAL_BOOTSTRAP_WINDOW") or "1h").strip()
INITIAL_BOOTSTRAP_WINDOW = _ibw if _ibw in WINDOWS else "1h"
if _ibw not in WINDOWS:
    log.warning("INITIAL_BOOTSTRAP_WINDOW=%r invalid; using %s", _ibw, INITIAL_BOOTSTRAP_WINDOW)

# Business services: type_rules map service name -> list of test types; services is display order with name, icon, patterns (substrings, case-insensitive)
_DEFAULT_BUSINESS_SERVICES = {
    "type_rules": {
        "Network Infrastructure": ["agent-to-agent", "agent-to-server", "bgp"],
        "Voice": ["voice", "sip-server"],
    },
    "services": [
        {"name": "Collaboration - Microsoft", "icon": "\uD83D\uDCE7", "patterns": ["MSTeams"]},
        {"name": "Health Care", "icon": "\u2695", "patterns": ["MyChart", "BSWHealth"]},
        {"name": "Voice", "icon": "\uD83D\uDCDE", "patterns": []},
        {"name": "Network Infrastructure", "icon": "\uD83D\uDD17", "patterns": ["SD-WAN", "DataCenter", "Site-DC", "Stores", "TOR-NYC", "SFTP"]},
        {"name": "Collaboration - Webex", "icon": "\uD83D\uDCDE", "patterns": ["Webex"]},
        {"name": "Microsoft O365", "icon": "\uD83D\uDCE7", "patterns": ["MSO365", "MS O365", "SD-WAN DIA", "O365"]},
        {"name": "Retail Operations", "icon": "\uD83C\uDFEA", "patterns": ["RetailDemo", "RetailSite", "Public_Sector"]},
        {"name": "Pseudoco Platform", "icon": "\uD83D\uDDA5", "patterns": ["Pseudoco", "Psuedoco", "pseudoco."]},
        {"name": "AI Services", "icon": "\uD83E\uDD16", "patterns": ["Bedrock"]},
        {"name": "AWS Cloud Services", "icon": "\u2601", "patterns": ["AWS"]},
        {"name": "Azure Cloud Services", "icon": "\u2601", "patterns": ["Azure"]},
        {"name": "Salesforce & CRM", "icon": "\uD83D\uDCC8", "patterns": ["Salesforce", "SAP", "salesforce.com", "lightning.force"]},
        {"name": "Workday HR", "icon": "\uD83D\uDC64", "patterns": ["Workday", "RetailDemo_Workday"]},
        {"name": "Facebook & Social", "icon": "\uD83C\uDF10", "patterns": ["Facebook"]},
        {"name": "Data Center Management", "icon": "\uD83D\uDCBB", "patterns": ["ESXi", "TrueNAS", "Xen Management", "Splunk"]},
        {"name": "Demo & Monitoring", "icon": "\uD83D\uDD2C", "patterns": ["Mar Demo", "Boutique", "Internet Test", "ThousandEyes", "thousandeyes.com"]},
        {"name": "Other Services", "icon": "\uD83D\uDCE6", "patterns": []},
    ],
}


def _load_business_services_config() -> dict:
    """Load business services config from env (JSON) or file path, else default. No secrets."""
    raw = os.getenv("BUSINESS_SERVICES_CONFIG", "").strip()
    if raw:
        try:
            out = json.loads(raw)
            if isinstance(out, dict) and "services" in out:
                return out
        except json.JSONDecodeError:
            log.warning("BUSINESS_SERVICES_CONFIG invalid JSON, using default")
    path = os.getenv("BUSINESS_SERVICES_CONFIG_FILE", "").strip()
    if path and os.path.isfile(path):
        try:
            with open(path, encoding="utf-8") as f:
                out = json.load(f)
            if isinstance(out, dict) and "services" in out:
                return out
        except (json.JSONDecodeError, OSError) as e:
            log.warning("BUSINESS_SERVICES_CONFIG_FILE load failed: %s, using default", e)
    return _DEFAULT_BUSINESS_SERVICES


_business_services_config: dict = _load_business_services_config()

COORD_LOOKUP = {
    "toronto, canada": (43.65, -79.38),
    "singapore, singapore": (1.35, 103.82),
    "central singapore, singapore": (1.35, 103.82),
    "slough, england, uk": (51.51, -0.60),
    "london, england, uk": (51.51, -0.13),
    "santa clara, california, us": (37.35, -121.95),
    "san jose, california, us": (37.34, -121.89),
    "new jersey, us": (40.74, -74.17),
    "raleigh, north carolina, us": (35.78, -78.64),
    "sydney, australia": (-33.87, 151.21),
    "new south wales, australia": (-33.87, 151.21),
    "melbourne, australia": (-37.81, 144.96),
    "amsterdam, netherlands": (52.37, 4.90),
    "bengaluru, karnataka, india": (12.97, 77.59),
    "new york, us": (40.71, -74.01),
    "new york, new york, us": (40.71, -74.01),
    "washington, us": (47.61, -122.33),
    "washington d.c., district of columbia, us": (38.91, -77.04),
    "austin, texas, us": (30.27, -97.74),
    "atlanta, georgia, us": (33.75, -84.39),
    "louisville, kentucky, us": (38.25, -85.76),
    "nashville, tennessee, us": (36.16, -86.78),
    "los angeles, california, us": (33.94, -118.41),
    "seattle, washington, us": (47.61, -122.33),
    "phoenix, arizona, us": (33.45, -112.07),
    "pittsburgh, pennsylvania, us": (40.44, -79.99),
    "columbus, ohio, us": (39.96, -83.00),
    "ashburn, virginia, us": (39.04, -77.49),
    "chicago, illinois, us": (41.88, -87.63),
    "stockholm, sweden": (59.33, 18.07),
    "guadalajara, jalisco, mexico": (20.67, -103.35),
    "cancún, quintana roo, mexico": (21.16, -86.85),
    "rio de janeiro, rio de janeiro, brazil": (-22.91, -43.17),
    "são paulo, são paulo, brazil": (-23.55, -46.63),
    "tokyo, tokyo, japan": (35.68, 139.69),
    "boston, massachusetts, us": (42.36, -71.06),
    "hamburg, germany": (53.55, 9.99),
    "indianapolis, indiana, us": (39.77, -86.16),
    "dallas, texas, us": (32.78, -96.80),
    "denver, colorado, us": (39.74, -104.98),
    "miami, florida, us": (25.76, -80.19),
    "paris, france": (48.86, 2.35),
    "zurich, switzerland": (47.37, 8.54),
    "frankfurt am main, germany": (50.11, 8.68),
    "leipzig, germany": (51.34, 12.37),
    "seoul, south korea": (37.57, 126.98),
    "kuala lumpur, malaysia": (3.14, 101.69),
    "cape town, south africa": (-33.93, 18.42),
    "cairo, egypt": (30.04, 31.24),
    "tsuen wan, hong kong": (22.32, 114.17),
    "manila, philippines": (14.60, 120.98),
    "buenos aires, argentina": (-34.60, -58.38),
    "moscow, moscow, russia": (55.76, 37.62),
    "warsaw, poland": (52.23, 21.01),
    "madrid, spain": (40.42, -3.70),
    "vienna, austria": (48.21, 16.37),
    "brussels, belgium": (50.85, 4.35),
    "istanbul, turkey": (41.01, 28.98),
    "dublin, ireland": (53.35, -6.26),
    "shanghai, shanghai municipality, china": (31.23, 121.47),
    "beijing, beijing municipality, china": (39.90, 116.40),
    "milan, italy": (45.46, 9.19),
    "osaka, ōsaka, japan": (34.69, 135.50),
    "taipei, taiwan": (25.03, 121.57),
    "dubai, united arab emirates": (25.20, 55.27),
    "helsinki, finland": (60.17, 24.94),
    "bangkok, thailand": (13.76, 100.50),
    "prague, czech republic": (50.08, 14.44),
    "vancouver, canada": (49.28, -123.12),
    "montreal, canada": (45.50, -73.57),
    "calgary, canada": (51.05, -114.07),
    "newark, new jersey, us": (40.74, -74.17),
    "cleveland, ohio, us": (41.50, -81.69),
    "charlotte, north carolina, us": (35.23, -80.84),
    "philadelphia, pennsylvania, us": (39.95, -75.17),
    "las vegas, nevada, us": (36.17, -115.14),
    "buffalo, new york, us": (42.89, -78.88),
    "albuquerque, new mexico, us": (35.08, -106.65),
    "kansas city, missouri, us": (39.10, -94.58),
    "sacramento, california, us": (38.58, -121.49),
    "minneapolis, minnesota, us": (44.98, -93.27),
    "edison, new jersey, us": (40.52, -74.41),
    "cary, north carolina, us": (35.79, -78.78),
    "frisco, texas, us": (33.15, -96.82),
    "mumbai, india": (19.08, 72.88),
    "krakow, poland": (50.06, 19.94),
    "brisbane, australia": (-27.47, 153.03),
    "frankfurt, germany": (50.11, 8.68),
    "mexico city, mexico": (19.43, -99.13),
    "port st. lucie, fl": (27.29, -80.35),
    "port saint lucie, florida, us": (27.29, -80.35),
    "flint, mi, us": (43.01, -83.69),
    "flint, michigan, us": (43.01, -83.69),
    "connecticut, us": (41.60, -72.76),
    "south carolina, us": (34.00, -81.03),
    "louisiana, us": (30.98, -91.96),
    "amadora, portugal": (38.75, -9.23),
    "queluz, portugal": (38.75, -9.25),
    "bellevue, washington, us": (47.61, -122.20),
    "benito juarez, mexico city, mexico": (19.40, -99.16),
    "carlsbad, california, us": (33.16, -117.35),
    "city of london, england, uk": (51.51, -0.09),
    "concord, california, us": (37.98, -122.03),
    "derby, england, uk": (52.92, -1.47),
    "glendale, arizona, us": (33.54, -112.19),
    "hawaii, us": (21.31, -157.86),
    "manchester, new hampshire, us": (42.99, -71.46),
    "mumbai, maharashtra, india": (19.08, 72.88),
    "munster, ireland": (52.35, -8.98),
    "paterson, new jersey, us": (40.92, -74.17),
    "providence, rhode island, us": (41.82, -71.41),
    "saint paul, minnesota, us": (44.94, -93.09),
    "sants-montjuïc, spain": (41.37, 2.15),
    "seongnam-si, south korea": (37.42, 127.13),
    "sheffield, england, uk": (53.38, -1.47),
    "sunnyvale, california, us": (37.37, -122.04),
    "woking, england, uk": (51.32, -0.56),
    "brisbane, queensland, australia": (-27.47, 153.03),
}

_base_cache = {}
_metrics_cache = {}
_extra_kpi_cache = {}
_agent_perf_cache: dict[str, dict] = {}  # test_id -> {"data": {...}, "ts": float}
_cache_lock = threading.Lock()


def _full_metrics_ready() -> bool:
    """True when the system can serve a meaningful default view (1h cache or 24h from DB/cache)."""
    with _cache_lock:
        if _metrics_cache.get("1h") and _extra_kpi_cache.get("1h"):
            return True
        if _metrics_cache.get("24h") and _extra_kpi_cache.get("24h"):
            return True
    return False


def _metrics_first_paint_ready() -> bool:
    """True when the UI can show metrics: any cached window data."""
    if INITIAL_BOOTSTRAP_DISABLED:
        return _full_metrics_ready()
    bw = INITIAL_BOOTSTRAP_WINDOW
    with _cache_lock:
        if _metrics_cache.get(bw) and _extra_kpi_cache.get(bw):
            return True
        if _metrics_cache.get("1h") and _extra_kpi_cache.get("1h"):
            return True
    return False


def _resolve_metrics_cache_entry(requested_window: str):
    """Pick best cached metrics for the requested window.

    Priority: exact cache hit → 1h cache → bootstrap window cache → None.
    For multi-hour windows, the caller should check DB hourly data first.
    """
    with _cache_lock:
        if _metrics_cache.get(requested_window):
            return requested_window, _metrics_cache[requested_window]
        if _metrics_cache.get("1h"):
            return "1h", _metrics_cache["1h"]
        if _metrics_cache.get("24h"):
            return "24h", _metrics_cache["24h"]
        if not INITIAL_BOOTSTRAP_DISABLED:
            bw = INITIAL_BOOTSTRAP_WINDOW
            if _metrics_cache.get(bw):
                return bw, _metrics_cache[bw]
    return requested_window, None


def _resolve_extra_cache_entry(requested_window: str):
    with _cache_lock:
        if _extra_kpi_cache.get(requested_window):
            return requested_window, _extra_kpi_cache[requested_window]
        if _extra_kpi_cache.get("1h"):
            return "1h", _extra_kpi_cache["1h"]
        if _extra_kpi_cache.get("24h"):
            return "24h", _extra_kpi_cache["24h"]
        if not INITIAL_BOOTSTRAP_DISABLED:
            bw = INITIAL_BOOTSTRAP_WINDOW
            if _extra_kpi_cache.get(bw):
                return bw, _extra_kpi_cache[bw]
    return requested_window, None


# KPI SQLite: one connection is used under _kpi_db_lock (Flask is multi-threaded)
_kpi_db_lock = threading.Lock()
_kpi_conn: sqlite3.Connection | None = None

_EXTRA_KPI_SCALAR_KEYS = (
    "avg_response_ms",
    "p50_response_ms",
    "p75_response_ms",
    "p95_response_ms",
    "p99_response_ms",
    "avg_packet_loss",
    "loss_affected_paths",
    "p50_loss",
    "p75_loss",
    "p95_loss",
    "p99_loss",
)


_COUNT_KPI_KEYS = ("alert_count", "event_count", "outage_count")


def _kpi_known_keys() -> frozenset[str]:
    return (
        frozenset(("availability_mean", "availability_count"))
        | frozenset(_EXTRA_KPI_SCALAR_KEYS)
        | frozenset(_COUNT_KPI_KEYS)
    )


def _kpi_get_conn() -> sqlite3.Connection | None:
    """Return shared SQLite connection; None if KPI history is disabled or open failed."""
    global _kpi_conn
    if KPI_HISTORY_DISABLED:
        return None
    with _kpi_db_lock:
        if _kpi_conn is not None:
            return _kpi_conn
        try:
            parent = os.path.dirname(KPI_DB_PATH)
            if parent:
                os.makedirs(parent, exist_ok=True)
            conn = sqlite3.connect(KPI_DB_PATH, timeout=30.0, check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS kpi_point (
                    sampled_at TEXT NOT NULL,
                    window_key TEXT NOT NULL,
                    kpi_key TEXT NOT NULL,
                    value REAL NOT NULL,
                    PRIMARY KEY (sampled_at, window_key, kpi_key)
                );
                CREATE INDEX IF NOT EXISTS idx_kpi_point_lookup
                    ON kpi_point (kpi_key, window_key, sampled_at);

                CREATE TABLE IF NOT EXISTS test_hourly (
                    hour_bucket TEXT NOT NULL,
                    test_id TEXT NOT NULL,
                    test_name TEXT NOT NULL DEFAULT '',
                    metric_key TEXT NOT NULL,
                    value REAL NOT NULL,
                    PRIMARY KEY (hour_bucket, test_id, metric_key)
                );
                CREATE INDEX IF NOT EXISTS idx_test_hourly_lookup
                    ON test_hourly (test_id, metric_key, hour_bucket);
                CREATE INDEX IF NOT EXISTS idx_test_hourly_bucket
                    ON test_hourly (hour_bucket);

                CREATE TABLE IF NOT EXISTS kpi_hourly (
                    hour_bucket TEXT NOT NULL,
                    kpi_key TEXT NOT NULL,
                    value REAL NOT NULL,
                    PRIMARY KEY (hour_bucket, kpi_key)
                );
                CREATE INDEX IF NOT EXISTS idx_kpi_hourly_lookup
                    ON kpi_hourly (kpi_key, hour_bucket);

                CREATE TABLE IF NOT EXISTS kpi_daily (
                    day_bucket TEXT NOT NULL,
                    kpi_key TEXT NOT NULL,
                    avg_value REAL NOT NULL,
                    min_value REAL,
                    max_value REAL,
                    sample_count INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (day_bucket, kpi_key)
                );
                CREATE INDEX IF NOT EXISTS idx_kpi_daily_lookup
                    ON kpi_daily (kpi_key, day_bucket);
                """
            )
            conn.commit()
            _kpi_conn = conn
            return conn
        except OSError as e:
            log.error("KPI history DB init failed (path=%s): %s", KPI_DB_PATH, e)
            return None


def _kpi_prune_old(conn: sqlite3.Connection) -> None:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=KPI_HISTORY_RETENTION_DAYS)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    conn.execute("DELETE FROM kpi_point WHERE sampled_at < ?", (cutoff,))
    conn.commit()


def _kpi_persist_snapshot(window_key: str, metrics: dict, extra: dict) -> None:
    """Append one timestamped row per scalar KPI for trending charts."""
    if KPI_HISTORY_DISABLED or window_key not in WINDOWS:
        return
    conn = _kpi_get_conn()
    if not conn:
        return

    sampled_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows: list[tuple[str, str, str, float]] = []

    if metrics:
        vals = [float(v) for v in metrics.values() if isinstance(v, (int, float))]
        if vals:
            rows.append((sampled_at, window_key, "availability_mean", sum(vals) / len(vals)))
            rows.append((sampled_at, window_key, "availability_count", float(len(vals))))

    for k in _EXTRA_KPI_SCALAR_KEYS:
        if k not in extra:
            continue
        v = extra.get(k)
        if v is None:
            continue
        if isinstance(v, bool):
            continue
        if isinstance(v, (int, float)):
            rows.append((sampled_at, window_key, k, float(v)))

    if not rows:
        return

    with _kpi_db_lock:
        try:
            conn.executemany(
                "INSERT OR REPLACE INTO kpi_point (sampled_at, window_key, kpi_key, value) VALUES (?,?,?,?)",
                rows,
            )
            _kpi_prune_old(conn)
        except sqlite3.Error as e:
            log.error("KPI history write failed: %s", e)


# Mapping from extra-KPI detail list keys to (metric_key, value_field) for test_hourly storage
_EXTRA_DETAIL_TO_METRIC = {
    "resp_by_test": ("response_ms", "ms"),
    "loss_by_test": ("loss", "loss"),
    "jitter_by_test": ("jitter", "jitter"),
    "latency_by_test": ("latency", "latency"),
    "mos_by_test": ("mos", "value"),
    "voip_latency_by_test": ("voip_latency", "value"),
    "voip_loss_by_test": ("voip_loss", "value"),
    "pdv_by_test": ("pdv", "value"),
    "bgp_reach_by_test": ("bgp_reach", "value"),
    "api_completion_by_test": ("api_completion", "value"),
    "api_txn_time_by_test": ("api_txn_time", "value"),
    "txn_completion_by_test": ("txn_completion", "value"),
    "page_completion_by_test": ("page_completion", "value"),
}

_EP_METRIC_KEYS = ("latency", "rssi", "gateway_latency", "gateway_loss", "cpu", "memory")

_last_daily_agg_day: str | None = None


def _hour_bucket(dt: datetime | None = None) -> str:
    """Return the UTC hour bucket string for *dt* (default: now), truncated to the hour."""
    if dt is None:
        dt = datetime.now(timezone.utc)
    return dt.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def _kpi_persist_hourly(metrics: dict, extra: dict) -> None:
    """Store per-test + scalar KPI data into hourly tables.

    *metrics*: {test_name: availability_float} from fetch_metrics_async
    *extra*: full extra-KPI dict from fetch_extra_kpis_async
    """
    if KPI_HISTORY_DISABLED:
        return
    conn = _kpi_get_conn()
    if not conn:
        return

    bucket = _hour_bucket()
    name_to_id = _get_metrics_synth_tests()
    id_to_name = {v: k for k, v in name_to_id.items()}

    # -- per-test rows for test_hourly --
    test_rows: list[tuple[str, str, str, str, float]] = []

    if metrics:
        for tname, val in metrics.items():
            if not isinstance(val, (int, float)):
                continue
            tid = name_to_id.get(tname, "")
            if not tid:
                continue
            test_rows.append((bucket, tid, tname, "availability", float(val)))

    for detail_key, (metric_key, val_field) in _EXTRA_DETAIL_TO_METRIC.items():
        detail_list = extra.get(detail_key)
        if not detail_list or not isinstance(detail_list, list):
            continue
        for entry in detail_list:
            tid = entry.get("id", "")
            tname = entry.get("test", "")
            val = entry.get(val_field)
            if not tid or val is None or not isinstance(val, (int, float)):
                continue
            test_rows.append((bucket, str(tid), tname, metric_key, float(val)))

    ep_performers = extra.get("ep_worst_performers")
    if ep_performers and isinstance(ep_performers, list):
        for ep in ep_performers:
            agent_id = ep.get("id", "")
            agent_name = ep.get("name", "")
            if not agent_id:
                continue
            ep_tid = f"ep-{agent_id}"
            for mk in _EP_METRIC_KEYS:
                val = ep.get(mk)
                if val is not None and isinstance(val, (int, float)):
                    test_rows.append((bucket, ep_tid, agent_name, f"ep_{mk}", float(val)))

    # -- scalar KPI rows for kpi_hourly --
    kpi_rows: list[tuple[str, str, float]] = []
    if metrics:
        vals = [float(v) for v in metrics.values() if isinstance(v, (int, float))]
        if vals:
            kpi_rows.append((bucket, "availability_mean", sum(vals) / len(vals)))
            kpi_rows.append((bucket, "availability_count", float(len(vals))))

    for k in _EXTRA_KPI_SCALAR_KEYS:
        v = extra.get(k)
        if v is None or isinstance(v, bool) or not isinstance(v, (int, float)):
            continue
        kpi_rows.append((bucket, k, float(v)))

    with _cache_lock:
        base = dict(_base_cache) if _base_cache else {}
    counts = base.get("COUNTS") or {}
    alert_feed = base.get("ALERT_FEED") or []
    live_events = base.get("LIVE_EVENTS") or []
    app_out = counts.get("app_outages", 0) or 0
    net_out = counts.get("net_outages", 0) or 0
    count_map = {
        "alert_count": float(len(alert_feed)),
        "event_count": float(len(live_events)),
        "outage_count": float(app_out + net_out),
    }
    for ck, cv in count_map.items():
        kpi_rows.append((bucket, ck, cv))

    if not test_rows and not kpi_rows:
        return

    with _kpi_db_lock:
        try:
            if test_rows:
                conn.executemany(
                    "INSERT OR REPLACE INTO test_hourly "
                    "(hour_bucket, test_id, test_name, metric_key, value) VALUES (?,?,?,?,?)",
                    test_rows,
                )
            if kpi_rows:
                conn.executemany(
                    "INSERT OR REPLACE INTO kpi_hourly "
                    "(hour_bucket, kpi_key, value) VALUES (?,?,?)",
                    kpi_rows,
                )
            conn.commit()
            log.info("Hourly persist: bucket=%s test_rows=%d kpi_rows=%d", bucket, len(test_rows), len(kpi_rows))
        except sqlite3.Error as e:
            log.error("Hourly persist failed: %s", e)
            return

    _kpi_maybe_aggregate_daily(conn)
    _kpi_prune_hourly(conn)


def _kpi_prune_hourly(conn: sqlite3.Connection) -> None:
    """Remove hourly data older than KPI_HOURLY_RETENTION_DAYS and daily data older than KPI_DAILY_RETENTION_DAYS."""
    hourly_cutoff = (datetime.now(timezone.utc) - timedelta(days=KPI_HOURLY_RETENTION_DAYS)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    daily_cutoff = (datetime.now(timezone.utc) - timedelta(days=KPI_DAILY_RETENTION_DAYS)).strftime("%Y-%m-%d")
    with _kpi_db_lock:
        try:
            conn.execute("DELETE FROM test_hourly WHERE hour_bucket < ?", (hourly_cutoff,))
            conn.execute("DELETE FROM kpi_hourly WHERE hour_bucket < ?", (hourly_cutoff,))
            conn.execute("DELETE FROM kpi_daily WHERE day_bucket < ?", (daily_cutoff,))
            conn.commit()
        except sqlite3.Error as e:
            log.error("Hourly prune failed: %s", e)


def _backfill_kpi_hourly_from_legacy() -> int:
    """One-time migration: aggregate kpi_point rows into kpi_hourly.

    Groups legacy snapshot rows by hour bucket and KPI key, computes the
    average, and inserts them with INSERT OR IGNORE so existing hourly data
    is preserved.  Returns the number of rows backfilled.
    """
    if KPI_HISTORY_DISABLED:
        return 0
    conn = _kpi_get_conn()
    if not conn:
        return 0

    with _kpi_db_lock:
        try:
            existing = conn.execute("SELECT COUNT(*) FROM kpi_hourly").fetchone()
            legacy = conn.execute("SELECT COUNT(*) FROM kpi_point").fetchone()
        except sqlite3.Error:
            return 0

    if not legacy or legacy[0] == 0:
        return 0

    if existing and existing[0] >= legacy[0]:
        return 0

    with _kpi_db_lock:
        try:
            cur = conn.execute(
                """
                SELECT
                    substr(sampled_at, 1, 13) || ':00:00Z' AS hour_bucket,
                    kpi_key,
                    AVG(value) AS value
                FROM kpi_point
                GROUP BY hour_bucket, kpi_key
                """
            )
            rows = cur.fetchall()
            if rows:
                conn.executemany(
                    "INSERT OR IGNORE INTO kpi_hourly (hour_bucket, kpi_key, value) VALUES (?,?,?)",
                    rows,
                )
                conn.commit()
                log.info("Backfilled %d kpi_hourly rows from legacy kpi_point", len(rows))
                return len(rows)
        except sqlite3.Error as e:
            log.error("kpi_hourly backfill from legacy failed: %s", e)
    return 0


def _kpi_maybe_aggregate_daily(conn: sqlite3.Connection) -> None:
    """Roll up completed days from kpi_hourly into kpi_daily.

    Only aggregates days that are fully in the past (before today UTC).
    Tracks the last aggregated day to avoid redundant work.
    """
    global _last_daily_agg_day
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    with _kpi_db_lock:
        try:
            cur = conn.execute(
                "SELECT DISTINCT substr(hour_bucket, 1, 10) AS d FROM kpi_hourly "
                "WHERE substr(hour_bucket, 1, 10) < ? ORDER BY d",
                (today,),
            )
            past_days = [row[0] for row in cur]
        except sqlite3.Error as e:
            log.error("Daily aggregation query failed: %s", e)
            return

    if not past_days:
        return

    if _last_daily_agg_day:
        past_days = [d for d in past_days if d > _last_daily_agg_day]
    if not past_days:
        return

    with _kpi_db_lock:
        try:
            for day in past_days:
                day_start = f"{day}T00:00:00Z"
                day_end = f"{day}T23:59:59Z"
                rows = conn.execute(
                    "SELECT kpi_key, AVG(value), MIN(value), MAX(value), COUNT(*) "
                    "FROM kpi_hourly WHERE hour_bucket >= ? AND hour_bucket <= ? "
                    "GROUP BY kpi_key",
                    (day_start, day_end),
                ).fetchall()
                if rows:
                    conn.executemany(
                        "INSERT OR REPLACE INTO kpi_daily "
                        "(day_bucket, kpi_key, avg_value, min_value, max_value, sample_count) "
                        "VALUES (?,?,?,?,?,?)",
                        [(day, r[0], r[1], r[2], r[3], r[4]) for r in rows],
                    )
            conn.commit()
            _last_daily_agg_day = past_days[-1]
            log.info("Daily aggregation: processed %d day(s) up to %s", len(past_days), _last_daily_agg_day)
        except sqlite3.Error as e:
            log.error("Daily aggregation write failed: %s", e)


def _hourly_bucket_count(conn: sqlite3.Connection) -> int:
    """Return the number of distinct hour buckets in kpi_hourly (approximate coverage indicator)."""
    with _kpi_db_lock:
        try:
            row = conn.execute("SELECT COUNT(DISTINCT hour_bucket) FROM kpi_hourly").fetchone()
            return row[0] if row else 0
        except sqlite3.Error:
            return 0


def _has_enough_hourly_data(hours: int) -> bool:
    """True when the DB has enough hourly buckets to serve a view spanning *hours*."""
    if KPI_HISTORY_DISABLED:
        return False
    conn = _kpi_get_conn()
    if not conn:
        return False
    count = _hourly_bucket_count(conn)
    needed = min(hours, _HOURLY_MIN_BUCKETS_FOR_DB_VIEW)
    return count >= needed


def _db_inventory() -> dict:
    """Inspect existing hourly data in the DB to decide what startup can skip.

    Returns a dict with:
      - total_hourly_buckets: int
      - recent_hourly_buckets: int  (within last 25 hours, for 24h view)
      - latest_bucket: str | None   (most recent hour_bucket)
      - latest_bucket_age_hours: float | None  (hours since the most recent bucket)
      - test_count: int             (distinct test_ids in recent hourly data)
      - has_24h_coverage: bool      (enough recent buckets for a 24h view from DB)
      - has_recent_data: bool       (latest bucket is less than 2 hours old)
    """
    result = {
        "total_hourly_buckets": 0,
        "recent_hourly_buckets": 0,
        "latest_bucket": None,
        "latest_bucket_age_hours": None,
        "test_count": 0,
        "has_24h_coverage": False,
        "has_recent_data": False,
    }
    if KPI_HISTORY_DISABLED:
        return result
    conn = _kpi_get_conn()
    if not conn:
        return result

    _backfill_kpi_hourly_from_legacy()

    now = datetime.now(timezone.utc)
    cutoff_24h = _hour_bucket(now - timedelta(hours=25))

    with _kpi_db_lock:
        try:
            row = conn.execute("SELECT COUNT(DISTINCT hour_bucket) FROM kpi_hourly").fetchone()
            result["total_hourly_buckets"] = row[0] if row else 0

            row = conn.execute(
                "SELECT COUNT(DISTINCT hour_bucket) FROM kpi_hourly WHERE hour_bucket >= ?",
                (cutoff_24h,),
            ).fetchone()
            result["recent_hourly_buckets"] = row[0] if row else 0

            row = conn.execute("SELECT MAX(hour_bucket) FROM kpi_hourly").fetchone()
            if row and row[0]:
                result["latest_bucket"] = row[0]
                try:
                    latest_dt = datetime.strptime(row[0], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                    age_h = (now - latest_dt).total_seconds() / 3600.0
                    result["latest_bucket_age_hours"] = round(age_h, 2)
                    result["has_recent_data"] = age_h < 2.0
                except ValueError:
                    pass

            row = conn.execute(
                "SELECT COUNT(DISTINCT test_id) FROM test_hourly WHERE hour_bucket >= ?",
                (cutoff_24h,),
            ).fetchone()
            result["test_count"] = row[0] if row else 0

            result["has_24h_coverage"] = result["recent_hourly_buckets"] >= _HOURLY_MIN_BUCKETS_FOR_DB_VIEW
        except sqlite3.Error as e:
            log.error("DB inventory query failed: %s", e)

    return result


def _warm_caches_from_db(inv: dict) -> bool:
    """Pre-populate in-memory metrics/extra caches from stored hourly data.

    Called at startup when the DB has sufficient recent data. This avoids
    the expensive 24h MCP fallback fetch.

    Returns True if caches were successfully warmed.
    """
    if not inv.get("has_24h_coverage"):
        return False

    m24 = _build_metrics_from_hourly(24)
    e24 = _build_extra_kpis_from_hourly(24)
    if not m24 or not e24:
        return False

    with _cache_lock:
        _metrics_cache["24h"] = {"data": m24, "ts": time.monotonic()}
        _extra_kpi_cache["24h"] = {"data": e24, "ts": time.monotonic()}

    log.info("Caches warmed from DB: 24h view with %d tests", len(m24))
    return True


def _build_metrics_from_hourly(hours: int) -> dict[str, float] | None:
    """Reconstruct per-test availability by averaging hourly buckets over the last *hours*.

    Returns {test_name: mean_availability} or None if insufficient data.
    """
    if KPI_HISTORY_DISABLED:
        return None
    conn = _kpi_get_conn()
    if not conn:
        return None

    cutoff = _hour_bucket(datetime.now(timezone.utc) - timedelta(hours=hours))
    with _kpi_db_lock:
        try:
            rows = conn.execute(
                "SELECT test_name, test_id, AVG(value) FROM test_hourly "
                "WHERE metric_key = 'availability' AND hour_bucket >= ? "
                "GROUP BY test_id",
                (cutoff,),
            ).fetchall()
        except sqlite3.Error as e:
            log.error("_build_metrics_from_hourly query failed: %s", e)
            return None

    if not rows:
        return None
    return {row[0]: round(row[2], 4) for row in rows if row[0]}


def _percentile(sorted_vals: list[float], p: float) -> float:
    """Return the p-th percentile (0-1) from a pre-sorted list."""
    if not sorted_vals:
        return 0.0
    idx = min(int(len(sorted_vals) * p), len(sorted_vals) - 1)
    return sorted_vals[idx]


def _build_extra_kpis_from_hourly(hours: int) -> dict | None:
    """Reconstruct the full EXTRA_KPI dict by averaging hourly per-test data from DB.

    Returns a dict compatible with the structure produced by fetch_extra_kpis_async,
    or None if insufficient data.
    """
    if KPI_HISTORY_DISABLED:
        return None
    conn = _kpi_get_conn()
    if not conn:
        return None

    cutoff = _hour_bucket(datetime.now(timezone.utc) - timedelta(hours=hours))

    with _kpi_db_lock:
        try:
            rows = conn.execute(
                "SELECT test_id, test_name, metric_key, AVG(value) "
                "FROM test_hourly WHERE hour_bucket >= ? "
                "GROUP BY test_id, metric_key",
                (cutoff,),
            ).fetchall()
        except sqlite3.Error as e:
            log.error("_build_extra_kpis_from_hourly query failed: %s", e)
            return None

    if not rows:
        return None

    # Organize: {test_id: {"name": str, metrics: {metric_key: avg_value}}}
    by_test: dict[str, dict] = {}
    for tid, tname, mkey, avg_val in rows:
        if tid not in by_test:
            by_test[tid] = {"name": tname, "metrics": {}}
        by_test[tid]["metrics"][mkey] = avg_val

    extra: dict = {
        "avg_response_ms": None, "p50_response_ms": None,
        "p75_response_ms": None, "p95_response_ms": None,
        "p99_response_ms": None, "avg_packet_loss": None,
        "loss_affected_paths": 0, "p50_loss": None,
        "p75_loss": None, "p95_loss": None, "p99_loss": None,
    }

    # Helper to build a per-test detail list from a metric_key
    def _detail_list(metric_key: str, val_field: str, sort_key: str | None = None,
                     reverse: bool = True, round_digits: int = 2) -> list[dict]:
        items = []
        for tid, info in by_test.items():
            if tid.startswith("ep-"):
                continue
            val = info["metrics"].get(metric_key)
            if val is None:
                continue
            entry = {"test": info["name"], "id": tid, val_field: round(val, round_digits)}
            items.append(entry)
        sk = sort_key or val_field
        items.sort(key=lambda x: x.get(sk, 0), reverse=reverse)
        return items

    # Response time
    resp_list = _detail_list("response_ms", "ms", round_digits=0)
    if resp_list:
        vals = [e["ms"] for e in resp_list]
        extra["avg_response_ms"] = round(sum(vals) / len(vals))
        sv = sorted(vals)
        extra["p50_response_ms"] = round(_percentile(sv, 0.50))
        extra["p75_response_ms"] = round(_percentile(sv, 0.75))
        extra["p95_response_ms"] = round(_percentile(sv, 0.95))
        extra["p99_response_ms"] = round(_percentile(sv, 0.99))
        extra["resp_by_test"] = resp_list

    # Loss
    loss_list = _detail_list("loss", "loss", round_digits=3)
    if loss_list:
        vals = [e["loss"] for e in loss_list]
        extra["avg_packet_loss"] = round(sum(vals) / len(vals), 2)
        extra["loss_affected_paths"] = sum(1 for v in vals if v > 0)
        sv = sorted(vals)
        extra["p50_loss"] = round(_percentile(sv, 0.50), 3)
        extra["p75_loss"] = round(_percentile(sv, 0.75), 3)
        extra["p95_loss"] = round(_percentile(sv, 0.95), 3)
        extra["p99_loss"] = round(_percentile(sv, 0.99), 3)
        extra["loss_by_test"] = loss_list

    # Jitter, latency
    jitter_list = _detail_list("jitter", "jitter")
    if jitter_list:
        extra["jitter_by_test"] = jitter_list

    lat_list = _detail_list("latency", "latency")
    if lat_list:
        extra["latency_by_test"] = lat_list

    # VoIP and type-specific metrics
    _simple_details = {
        "mos": ("mos_by_test", "value", False),
        "voip_latency": ("voip_latency_by_test", "value", True),
        "voip_loss": ("voip_loss_by_test", "value", True),
        "pdv": ("pdv_by_test", "value", True),
        "bgp_reach": ("bgp_reach_by_test", "value", False),
        "api_completion": ("api_completion_by_test", "value", False),
        "api_txn_time": ("api_txn_time_by_test", "value", False),
        "txn_completion": ("txn_completion_by_test", "value", False),
        "page_completion": ("page_completion_by_test", "value", False),
    }
    for mkey, (extra_key, vfield, rev) in _simple_details.items():
        detail = _detail_list(mkey, vfield, reverse=rev)
        if detail:
            extra[extra_key] = detail

    # Endpoint agent metrics
    ep_data: dict[str, dict] = {}
    for tid, info in by_test.items():
        if not tid.startswith("ep-"):
            continue
        agent_id = tid[3:]
        ep_entry = {"name": info["name"], "id": agent_id, "loc": "", "device": ""}
        for mk in _EP_METRIC_KEYS:
            ep_entry[mk] = info["metrics"].get(f"ep_{mk}")
        ep_data[agent_id] = ep_entry

    if ep_data:
        with _cache_lock:
            ep_agents = list(_base_cache.get("ENDPOINT_AGENTS") or [])
        for ep in ep_agents:
            aid = ep.get("id", "")
            if aid in ep_data:
                ep_data[aid]["loc"] = ep.get("loc", "")
                ep_data[aid]["device"] = ep.get("device", "")
        ep_perf = list(ep_data.values())
        ep_perf.sort(key=lambda x: (
            x.get("latency") is None,
            -(x.get("latency") or 0),
            x.get("rssi") is None,
            x.get("rssi") or 0,
        ))
        extra["ep_worst_performers"] = ep_perf

    return extra


# Refresh status for initial load and startup script / UI (thread-safe)
_refresh_status_lock = threading.Lock()
_refresh_status = {
    "phase": "idle",       # idle | base | metrics | extra_kpis | done | error
    "message": "",
    "current": 0,
    "total": 0,
    "error": None,
    "started_at": None,
    "completed_at": None,
}

# Startup timing: tracks wall-clock seconds for each phase of initial load
_startup_timing_lock = threading.Lock()
_startup_timing: dict[str, float | str | bool | None] = {
    "base_sec": None,
    "bootstrap_sec": None,
    "db_warmup_sec": None,
    "hourly_1h_sec": None,
    "fallback_24h_sec": None,
    "total_sec": None,
    "skipped_24h_fallback": False,
    "db_inventory": None,
}
_startup_t0: float | None = None  # perf_counter at startup begin


def _startup_mark(phase: str, value) -> None:
    """Record a startup phase value (seconds are rounded; dicts/bools stored as-is)."""
    with _startup_timing_lock:
        _startup_timing[phase] = round(value, 2) if isinstance(value, (int, float)) else value


def _startup_get() -> dict:
    """Return a copy of the startup timing dict (safe for JSON serialization)."""
    with _startup_timing_lock:
        return dict(_startup_timing)


def _set_refresh_status(
    phase: str | None = None,
    message: str | None = None,
    current: int | None = None,
    total: int | None = None,
    error: str | None = None,
):
    with _refresh_status_lock:
        if phase is not None:
            _refresh_status["phase"] = phase
        if message is not None:
            _refresh_status["message"] = message
        if current is not None:
            _refresh_status["current"] = current
        if total is not None:
            _refresh_status["total"] = total
        if error is not None:
            _refresh_status["error"] = error
            _refresh_status["phase"] = "error"
        if phase == "done" or phase == "error" or _refresh_status["phase"] == "error":
            _refresh_status["completed_at"] = datetime.now(timezone.utc).isoformat()
        if phase and phase not in ("done", "error") and _refresh_status["started_at"] is None:
            _refresh_status["started_at"] = datetime.now(timezone.utc).isoformat()


def _unwrap_exception(e: Exception) -> Exception:
    """Unwrap ExceptionGroup/TaskGroup (Python 3.11+) to get the root cause for clearer logging."""
    sub = getattr(e, "exceptions", None)
    if sub and type(e).__name__ in ("ExceptionGroup", "BaseExceptionGroup"):
        if len(sub) == 1:
            return _unwrap_exception(sub[0])
        return sub[0]
    return e


def _find_httpx_transport_error(exc: BaseException, _seen: set | None = None) -> BaseException | None:
    """Find ConnectTimeout / ConnectError / ReadTimeout in exc, __cause__, __context__, or ExceptionGroup."""
    if _seen is None:
        _seen = set()
    eid = id(exc)
    if eid in _seen:
        return None
    _seen.add(eid)
    if isinstance(exc, (httpx.ConnectTimeout, httpx.ConnectError, httpx.ReadTimeout)):
        return exc
    sub = getattr(exc, "exceptions", None)
    if sub and type(exc).__name__ in ("ExceptionGroup", "BaseExceptionGroup"):
        for s in sub:
            if isinstance(s, BaseException):
                found = _find_httpx_transport_error(s, _seen)
                if found is not None:
                    return found
    cause = getattr(exc, "__cause__", None)
    if isinstance(cause, BaseException):
        found = _find_httpx_transport_error(cause, _seen)
        if found is not None:
            return found
    ctx = getattr(exc, "__context__", None)
    if isinstance(ctx, BaseException) and ctx is not cause:
        found = _find_httpx_transport_error(ctx, _seen)
        if found is not None:
            return found
    return None


_mcp_streamable_fn = None
_mcp_create_http_client_fn = None  # None = use legacy streamable client without custom httpx timeout


def _ensure_mcp_streamable_helpers():
    """Resolve MCP SDK entry points once; prefer custom httpx timeouts when supported."""
    global _mcp_streamable_fn, _mcp_create_http_client_fn
    if _mcp_streamable_fn is not None:
        return
    import inspect

    mod = __import__("mcp.client.streamable_http", fromlist=["streamable_http_client"])
    _mcp_streamable_fn = getattr(mod, "streamable_http_client", None) or getattr(
        mod, "streamablehttp_client", None
    )
    if _mcp_streamable_fn is None:
        raise RuntimeError("mcp.client.streamable_http has no streamable HTTP client function")
    try:
        from mcp.shared._httpx_utils import create_mcp_http_client as _create

        if "http_client" in inspect.signature(_mcp_streamable_fn).parameters:
            _mcp_create_http_client_fn = _create
    except Exception:
        _mcp_create_http_client_fn = None


class _McpToolRateLimited(Exception):
    """Tool response indicated HTTP 429; outer loop should backoff and retry."""


async def _mcp_execute_tool(session, tool_name: str, arguments: dict | None) -> dict | list:
    await session.initialize()
    result = await session.call_tool(tool_name, arguments or {})
    if getattr(result, "isError", False):
        texts = [c.text for c in result.content if hasattr(c, "text")]
        err_text = "\n".join(texts)
        if "429" in err_text or "Too Many Requests" in err_text:
            raise _McpToolRateLimited()
        log.error("MCP tool error: tool=%s, error=%s", tool_name, err_text[:500])
        raise RuntimeError(f"MCP tool error: {err_text[:200]}")
    texts = [c.text for c in result.content if hasattr(c, "text")]
    combined = "\n".join(texts)
    try:
        return json.loads(combined)
    except json.JSONDecodeError:
        return {"raw": combined}


async def call_mcp_tool(tool_name: str, arguments: dict | None = None, retries: int | None = None):
    from mcp import ClientSession

    _ensure_mcp_streamable_helpers()
    streamable = _mcp_streamable_fn
    create_mcp_http = _mcp_create_http_client_fn

    attempts = MCP_RETRIES if retries is None else max(1, min(20, int(retries)))

    async def _do() -> dict | list:
        headers = {"Authorization": f"Bearer {MCP_TOKEN}"}
        timeout = httpx.Timeout(
            connect=MCP_CONNECT_TIMEOUT_SEC,
            read=MCP_READ_TIMEOUT_SEC,
            write=max(60.0, MCP_CONNECT_TIMEOUT_SEC),
            pool=MCP_CONNECT_TIMEOUT_SEC,
        )
        last_err = None
        for attempt in range(attempts):
            await _mcp_rate_limit_wait()
            try:
                if create_mcp_http is not None:
                    client = create_mcp_http(headers=headers, timeout=timeout)
                    async with client:
                        async with streamable(url=MCP_URL, http_client=client) as _streams:
                            read, write = _streams[0], _streams[1]
                            async with ClientSession(read, write) as session:
                                return await _mcp_execute_tool(session, tool_name, arguments)
                else:
                    async with streamable(url=MCP_URL, headers=headers) as _streams:
                        read, write = _streams[0], _streams[1]
                        async with ClientSession(read, write) as session:
                            return await _mcp_execute_tool(session, tool_name, arguments)
            except _McpToolRateLimited:
                wait = min(60, 2 ** (attempt + 2))
                jitter = random.uniform(0, min(5, wait * 0.2))
                log.error(
                    "MCP 429 rate limited: tool=%s, attempt=%d/%d, retry_in=%.1fs",
                    tool_name, attempt + 1, attempts, wait + jitter,
                )
                await asyncio.sleep(wait + jitter)
                continue
            except RuntimeError:
                raise
            except Exception as e:
                root = _unwrap_exception(e)
                last_err = root
                err_str = (str(root) or repr(root) or type(root).__name__).strip()
                err_str = err_str or type(root).__name__
                transport_exc = _find_httpx_transport_error(e) or _find_httpx_transport_error(root)
                logged_specific = False
                if isinstance(root, httpx.HTTPStatusError) and getattr(root, "response", None):
                    status_code = getattr(root.response, "status_code", None)
                    log.error(
                        "ThousandEyes API HTTP error: status=%s, url=%s; will retry (attempt %d/%d).",
                        status_code, str(getattr(root.response, "url", MCP_URL)), attempt + 1, attempts,
                    )
                    logged_specific = True
                elif transport_exc is not None:
                    log.error(
                        "MCP HTTP %s to %s (connect_timeout=%.1fs read_timeout=%.1fs). "
                        "Allow outbound HTTPS to this host; set HTTPS_PROXY/HTTP_PROXY if a proxy is required; "
                        "increase MCP_CONNECT_TIMEOUT_SEC (e.g. 180) or MCP_RETRIES on slow links; "
                        "tune MCP_TRANSPORT_BACKOFF_* for longer pauses between retries.",
                        type(transport_exc).__name__,
                        MCP_URL,
                        MCP_CONNECT_TIMEOUT_SEC,
                        MCP_READ_TIMEOUT_SEC,
                    )
                    logged_specific = True
                elif type(root).__name__ == "BrokenResourceError":
                    log.warning(
                        "MCP stream closed unexpectedly (attempt %d/%d); retrying with a new connection.",
                        attempt + 1, attempts,
                    )
                    logged_specific = True
                is_429 = (
                    "429" in str(e) or "Too Many Requests" in str(e)
                    or "429" in str(root) or "Too Many Requests" in str(root)
                )
                if is_429:
                    wait = min(60, 2 ** (attempt + 2))
                    jitter = random.uniform(0, min(5, wait * 0.2))
                    log.error(
                        "MCP 429 rate limited: tool=%s, attempt=%d/%d, retry_in=%.1fs",
                        tool_name, attempt + 1, attempts, wait + jitter,
                    )
                elif transport_exc is not None:
                    wait = min(
                        MCP_TRANSPORT_BACKOFF_MAX_SEC,
                        MCP_TRANSPORT_BACKOFF_BASE_SEC * (2**attempt),
                    )
                    jitter = random.uniform(0, min(12, wait * 0.2))
                else:
                    wait = min(30, 2 ** (attempt + 2))
                    jitter = random.uniform(0, min(3, wait * 0.2))
                    if not logged_specific:
                        log.error(
                            "MCP transient error: tool=%s, attempt=%d/%d, retry_in=%.1fs, error=%s",
                            tool_name, attempt + 1, attempts, wait + jitter, err_str,
                        )
                await asyncio.sleep(wait + jitter)
                continue
        root = _unwrap_exception(last_err) if last_err else last_err
        err_str = (str(root) or repr(root) or type(root).__name__).strip() or type(root).__name__
        log.error("MCP failed after %d retries: tool=%s, last_error=%s", attempts, tool_name, err_str)
        raise RuntimeError(f"Failed after {attempts} retries on {tool_name}: {err_str}")

    if tool_name == "get_network_app_synthetics_metrics":
        async with await _get_synth_sem():
            return await _do()
    return await _do()


async def _mcp_synth_metrics_batch(
    metric_ids: list[str], batch: list[str], start: str, end: str
) -> list[dict | None]:
    """One MCP call per metric_id, same batch. Semaphore + rate limit applied inside call_mcp_tool."""

    async def _one(mid: str):
        try:
            return await call_mcp_tool(
                "get_network_app_synthetics_metrics",
                {
                    "metric_id": mid,
                    "start_date": start,
                    "end_date": end,
                    "aggregation_type": "MEAN",
                    "group_by": "TEST",
                    "filter_dimension": "TEST",
                    "filter_values": batch,
                },
            )
        except Exception as e:
            root = _unwrap_exception(e) if isinstance(e, Exception) else e
            err_str = (str(root) or repr(root) or type(root).__name__).strip() or type(root).__name__
            log.error("MCP synth metric failed: metric_id=%s, error=%s", mid, err_str)
            return None

    return list(await asyncio.gather(*[_one(mid) for mid in metric_ids]))


async def safe_call(tool_name, args=None):
    try:
        return await call_mcp_tool(tool_name, args)
    except Exception as e:
        log.error("MCP call failed: tool=%s, error=%s", tool_name, e)
        return None


def resolve_coords(location_str: str):
    if not location_str:
        return None, None
    key = location_str.lower().strip()
    for pattern, coords in COORD_LOOKUP.items():
        if pattern in key or key in pattern:
            return coords
    parts = key.split(",")
    if len(parts) >= 2:
        city_key = parts[0].strip()
        for pattern, coords in COORD_LOOKUP.items():
            if city_key in pattern:
                return coords
    return None, None


def parse_csv_metrics(data: dict) -> dict[str, float]:
    csv_text = data.get("csv", "")
    agg_map = data.get("names", {}).get("aggregatesMap", {})
    names_map = agg_map.get("TEST", agg_map.get("EYEBROW_TEST", {}))
    test_col = "TEST"
    if "EYEBROW_TEST" in csv_text.split("\n", 1)[0]:
        test_col = "EYEBROW_TEST"
    sums = defaultdict(float)
    cnts = defaultdict(int)
    reader = csv.DictReader(io.StringIO(csv_text))
    for row in reader:
        tid = row.get(test_col, "")
        v = row.get("v", "")
        if tid and v:
            try:
                sums[tid] += float(v)
                cnts[tid] += 1
            except ValueError:
                pass
    result = {}
    for tid in sums:
        name = names_map.get(tid, tid)
        result[name] = round(sums[tid] / cnts[tid], 2)
    return result


def _merge_parsed_first_wins(responses_in_order: list[dict | None]) -> dict[str, float]:
    """Same test name: first non-empty metric in list order wins (deterministic)."""
    out: dict[str, float] = {}
    for resp in responses_in_order:
        if not resp:
            continue
        for name, val in parse_csv_metrics(resp).items():
            if name not in out:
                out[name] = val
    return out


def classify_agent_type(agent_type_str: str) -> str:
    t = agent_type_str.lower()
    if "cloud" in t:
        return "cloud"
    if "enterprise" in t:
        return "enterprise"
    return "endpoint"


def derive_city(agent: dict) -> str:
    loc = agent.get("location") or agent.get("locationName") or ""
    if "," in loc:
        return loc.split(",")[0].strip()
    return loc or "Unknown"


def _deconflict_coords(agents: list):
    seen: dict[tuple, list] = {}
    for a in agents:
        if a.get("lat") is None:
            continue
        key = (round(a["lat"], 4), round(a["lng"], 4))
        seen.setdefault(key, []).append(a)
    offsets = [
        (0.018, 0.018), (-0.018, 0.018), (0.018, -0.018), (-0.018, -0.018),
        (0.025, 0.0), (-0.025, 0.0), (0.0, 0.025), (0.0, -0.025),
    ]
    for key, group in seen.items():
        if len(group) <= 1:
            continue
        ea = [a for a in group if a["type"] == "enterprise"]
        for i, a in enumerate(ea):
            off = offsets[i % len(offsets)]
            a["lat"] += off[0]
            a["lng"] += off[1]


def _parse_list(resp):
    if isinstance(resp, list):
        return resp
    if isinstance(resp, dict):
        return resp.get("result", resp.get("tests", []))
    return []


# ---------------------------------------------------------------------------
# Base data: agents, tests, alerts, outages, events, endpoint agents
# ---------------------------------------------------------------------------

async def refresh_base_data_async():
    log.info("Refreshing base data via MCP...")
    _set_refresh_status(phase="base", message="Fetching base data (tests, agents, alerts)...")

    # Single gather for all base calls to minimize wall time
    (
        tests_resp,
        ep_tests_resp,
        acct_groups_resp,
        ce_agents_resp,
        ep_agents_resp,
        alerts_resp,
        events_resp,
        outages_resp,
    ) = await asyncio.gather(
        safe_call("list_network_app_synthetics_tests"),
        safe_call("list_endpoint_agent_tests"),
        safe_call("get_account_groups"),
        safe_call("list_cloud_enterprise_agents"),
        safe_call("list_endpoint_agents"),
        safe_call("list_alerts", {"state": "TRIGGER"}),
        safe_call("list_events"),
        safe_call("search_outages", {"window": "24h"}),
    )

    # --- Account Group Name ---
    account_group_name = ""
    ag_list = []
    if isinstance(acct_groups_resp, dict):
        ag_list = acct_groups_resp.get("accountGroups", acct_groups_resp.get("results", []))
    elif isinstance(acct_groups_resp, list):
        ag_list = acct_groups_resp
    for ag in ag_list:
        if ag.get("isCurrentAccountGroup") or ag.get("isCurrent") or ag.get("isDefault"):
            account_group_name = ag.get("accountGroupName", ag.get("name", ""))
            break
    if not account_group_name and ag_list:
        account_group_name = ag_list[0].get("accountGroupName", ag_list[0].get("name", ""))

    # --- Tests ---
    agent_tests = {}
    test_ids = {}
    all_tests = []
    for t in _parse_list(tests_resp):
        tid = str(t.get("testid", ""))
        tname = t.get("name", "")
        ttype = t.get("type", "")
        target = t.get("target", "")
        enabled = t.get("enabled", True)
        if not tid or not tname or not enabled:
            continue
        test_ids[tname] = tid
        test_agents = []
        for ag in (t.get("agents") or []):
            ag_id = str(ag.get("agentId", ""))
            ag_name = ag.get("agentName", "")
            ag_loc = ag.get("location", "")
            if ag_id:
                agent_tests.setdefault(ag_id, [])
                if tname not in agent_tests[ag_id]:
                    agent_tests[ag_id].append(tname)
            test_agents.append({"name": ag_name, "loc": ag_loc})
        all_tests.append({
            "id": tid, "name": tname, "type": ttype, "target": target,
            "agents_count": len(t.get("agents") or []),
            "agents": test_agents[:5],
        })

    relevant_agent_ids = set(agent_tests.keys())

    # --- Cloud & Enterprise Agents ---
    all_agents = []
    agent_id_set = set()
    cloud_count = 0
    enterprise_count = 0

    for a in _parse_list(ce_agents_resp):
        aid = str(a.get("agentId", ""))
        if not aid or aid in agent_id_set or aid not in relevant_agent_ids:
            continue
        agent_id_set.add(aid)
        loc = (a.get("location") or "").strip()
        lat, lng = resolve_coords(loc)
        atype = classify_agent_type(a.get("agentType", ""))
        agent_name = a.get("agentName", "")
        # Use geographic location for site grouping; fall back to agent name when location is empty
        city = loc if loc and atype == "enterprise" else (derive_city(a) if atype == "cloud" else agent_name)
        if atype == "cloud":
            cloud_count += 1
        else:
            enterprise_count += 1
        agent_entry = {
            "id": aid, "name": agent_name, "type": atype,
            "city": city or agent_name, "loc": loc,
        }
        if lat is not None:
            agent_entry["lat"] = lat
            agent_entry["lng"] = lng
        all_agents.append(agent_entry)

    _deconflict_coords(all_agents)

    # --- Endpoint Agents ---
    endpoint_agents = []
    for a in _parse_list(ep_agents_resp):
        aid = str(a.get("id", ""))
        if not aid or aid in agent_id_set:
            continue
        agent_id_set.add(aid)
        loc = a.get("locationName", "")
        name = a.get("name") or a.get("computerName", "")
        mfr = a.get("manufacturer", "")
        model = a.get("model", "")
        device = f"{mfr} {model}".strip() if (mfr or model) else ""
        endpoint_agents.append({
            "id": aid,
            "name": name,
            "loc": loc,
            "device": device,
        })
        lat, lng = resolve_coords(loc)
        ep_entry = {
            "id": aid, "name": name, "type": "endpoint",
            "city": loc, "loc": loc, "device": device,
        }
        if lat is not None:
            ep_entry["lat"] = lat
            ep_entry["lng"] = lng
        all_agents.append(ep_entry)

    _deconflict_coords(all_agents)

    ep_test_names = []
    for t in _parse_list(ep_tests_resp):
        tid = str(t.get("testId", ""))
        tname = t.get("testName", "")
        if not tid or not tname:
            continue
        test_ids[tname] = f"ep-{tid}"
        ep_test_names.append(tname)

    # --- Alerts ---
    known_issues = {}
    alert_list_raw = []
    if isinstance(alerts_resp, dict):
        alert_list_raw = alerts_resp.get("results", [])
    elif isinstance(alerts_resp, list):
        alert_list_raw = alerts_resp

    alert_feed = []
    for a in alert_list_raw:
        test_info = a.get("test", {})
        rule_info = a.get("rule", {})
        test_name = test_info.get("name", "") if isinstance(test_info, dict) else a.get("testName", "")
        severity = a.get("severity", "INFO")
        start_time = a.get("start", "")
        rule_name = rule_info.get("name", "") if isinstance(rule_info, dict) else a.get("ruleName", "")
        alert_type = rule_info.get("type", "") if isinstance(rule_info, dict) else a.get("type", "")
        if test_name:
            known_issues[test_name] = {
                "severity": severity,
                "rule": rule_name,
                "type": alert_type,
                "state": a.get("state", ""),
            }
        time_str = ""
        time_iso = ""
        if start_time:
            try:
                if isinstance(start_time, (int, float)):
                    ts = float(start_time)
                    # ThousandEyes sometimes returns epoch days rather than epoch seconds.
                    # Epoch seconds for any recent date are > 1 billion; values < 50000
                    # are almost certainly day counts (e.g. 17738 days ≈ 2018-07-26).
                    if ts < 50_000:
                        ts *= 86400
                    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                else:
                    try:
                        dt = datetime.fromisoformat(str(start_time).replace("Z", "+00:00"))
                    except (ValueError, TypeError):
                        # Numeric string (e.g. "17738") — parse and apply day/second heuristic
                        ts = float(str(start_time))
                        if ts < 50_000:
                            ts *= 86400
                        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                time_str = dt.strftime("%b %d %H:%M")
                time_iso = dt.isoformat().replace("+00:00", "Z")
            except (ValueError, TypeError, OSError):
                time_str = ""
        sev_color = "red" if severity.upper() in ("MAJOR", "CRITICAL") else "yellow" if severity.upper() == "MINOR" else "green"
        alert_id = str(a.get("alertId", a.get("id", "")))
        alert_feed.append({
            "time": time_str,
            "timeISO": time_iso,
            "msg": f"{severity.upper()} - {test_name}: {rule_name} ({alert_type})",
            "sev": sev_color,
            "severity": severity.upper(),
            "alertId": alert_id,
        })

    # --- Events ---
    live_events = []
    event_list = []
    if isinstance(events_resp, dict):
        event_list = events_resp.get("results", events_resp.get("events", []))
    elif isinstance(events_resp, list):
        event_list = events_resp
    now_utc = datetime.now(timezone.utc)
    for e in event_list:
        ongoing_raw = e.get("ongoing", e.get("state", ""))
        if isinstance(ongoing_raw, bool):
            is_ongoing = ongoing_raw
        else:
            is_ongoing = str(ongoing_raw).lower() in ("yes", "true", "1", "ongoing", "active")
        if not is_ongoing:
            continue

        event_id = str(e.get("eventId", e.get("id", "")))
        event_type = e.get("type", "")
        impact = e.get("impact", e.get("severity", "")).upper()
        if impact not in ("HIGH", "MEDIUM", "LOW"):
            raw_sev = e.get("severity", "").upper()
            if raw_sev in ("CRITICAL", "MAJOR"):
                impact = "HIGH"
            elif raw_sev in ("MINOR", "WARNING"):
                impact = "MEDIUM"
            else:
                impact = "LOW"

        agents_affected = e.get("agentsAffected", e.get("affectedAgents", e.get("agents", 0)))
        if isinstance(agents_affected, list):
            agents_affected = len(agents_affected)
        else:
            try:
                agents_affected = int(agents_affected)
            except (TypeError, ValueError):
                agents_affected = 0

        start_date = e.get("startDate", e.get("start", ""))
        start_dt = None
        time_str = ""
        if start_date:
            try:
                if isinstance(start_date, (int, float)):
                    start_dt = datetime.fromtimestamp(start_date, tz=timezone.utc)
                else:
                    start_dt = datetime.fromisoformat(str(start_date).replace("Z", "+00:00"))
                time_str = start_dt.strftime("%H:%M")
            except (ValueError, TypeError, OSError):
                time_str = str(start_date)[:5]

        duration_min = 0
        end_date = e.get("endDate", e.get("end", ""))
        if end_date:
            try:
                if isinstance(end_date, (int, float)):
                    end_dt = datetime.fromtimestamp(end_date, tz=timezone.utc)
                else:
                    end_dt = datetime.fromisoformat(str(end_date).replace("Z", "+00:00"))
                if start_dt:
                    duration_min = max(0, int((end_dt - start_dt).total_seconds() / 60))
            except (ValueError, TypeError, OSError):
                pass
        elif start_dt:
            duration_min = max(0, int((now_utc - start_dt).total_seconds() / 60))

        duration_str = ""
        if duration_min >= 1440:
            duration_str = f"{duration_min // 1440}d {(duration_min % 1440) // 60}h"
        elif duration_min >= 60:
            duration_str = f"{duration_min // 60}h {duration_min % 60}m"
        else:
            duration_str = f"{duration_min}m"

        time_iso = ""
        if start_dt:
            time_iso = start_dt.isoformat().replace("+00:00", "Z")
        live_events.append({
            "eventId": event_id,
            "title": e.get("title", e.get("type", "Event")),
            "type": event_type,
            "impact": impact,
            "agentsAffected": agents_affected,
            "duration": duration_str,
            "durationMin": duration_min,
            "time": time_str,
            "timeISO": time_iso,
            "timestamp": str(start_date),
            "ongoing": True,
        })

    # --- Outages ---
    def parse_outages(resp):
        items = []
        if isinstance(resp, list):
            items = resp
        elif isinstance(resp, dict):
            items = resp.get("outages", resp.get("results", []))
        outages = []
        for o in items:
            provider = o.get("providerName") or o.get("provider_name") or "Unknown"
            otype = o.get("type") or o.get("outageType") or ""
            severity = o.get("severity", "")
            if isinstance(severity, str):
                severity = severity.lower().strip()
            if severity not in ("high", "medium", "low"):
                total_affected = (o.get("affectedServersCount", 0) or 0) + (o.get("affectedInterfacesCount", 0) or 0)
                severity = "high" if total_affected >= 20 else "medium" if total_affected >= 5 else "low"
            affected_servers = o.get("affectedServersCount", o.get("affectedServerCount", 0))
            affected_interfaces = o.get("affectedInterfacesCount", o.get("affectedInterfaceCount", 0))
            affected_locations = o.get("affectedLocationsCount", o.get("locationCount", 0))
            start = o.get("startDate", o.get("start", ""))
            end = o.get("endDate", o.get("end", ""))

            start_str = ""
            if start:
                try:
                    dt = datetime.fromisoformat(start.replace("Z", "+00:00"))
                    start_str = dt.strftime("%H:%M UTC")
                except (ValueError, TypeError):
                    start_str = str(start)[:10]

            raw_dur = o.get("duration")
            if raw_dur and isinstance(raw_dur, (int, float)) and raw_dur > 0:
                duration_min = int(raw_dur / 60) if raw_dur >= 120 else int(raw_dur)
            elif start and end:
                try:
                    s = datetime.fromisoformat(start.replace("Z", "+00:00"))
                    e_dt = datetime.fromisoformat(end.replace("Z", "+00:00"))
                    duration_min = int((e_dt - s).total_seconds() / 60)
                except (ValueError, TypeError):
                    duration_min = 0
            else:
                duration_min = 0

            if affected_servers:
                affected_str = f"{affected_servers} servers, {affected_locations} locations"
            elif affected_interfaces:
                affected_str = f"{affected_interfaces} interfaces, {affected_locations} locations"
            else:
                affected_str = f"{affected_locations} locations"

            impact = min(100, max(10, (affected_servers or affected_interfaces or 0) + affected_locations * 3))

            raw_type = otype.lower().strip()
            display_type = "SAAS" if raw_type == "app" else "ISP (Net)" if raw_type == "net" else otype
            outage_id = o.get("id", "")
            outage_name = o.get("name", provider)
            outages.append({
                "id": outage_id,
                "provider": provider,
                "name": outage_name,
                "type": display_type,
                "_raw_type": raw_type,
                "severity": severity,
                "affected": affected_str,
                "start": start_str,
                "start_iso": start if isinstance(start, str) else "",
                "duration": duration_min,
                "impact": impact,
            })
        return outages

    all_parsed_outages = parse_outages(outages_resp)
    app_outages = [o for o in all_parsed_outages if o.get("_raw_type") == "app"]
    net_outages = [o for o in all_parsed_outages if o.get("_raw_type") != "app"]
    all_outages = sorted(all_parsed_outages, key=lambda x: x.get("impact", 0), reverse=True)[:15]

    # --- Test type distribution ---
    type_counts = defaultdict(int)
    for t in all_tests:
        ttype = t["type"]
        type_counts[ttype] += 1

    type_label_map = {
        "http-server": "HTTP Server", "page-load": "Page Load", "dns-server": "DNS Server",
        "agent-to-server": "Agent-to-Server", "agent-to-agent": "Agent-to-Agent",
        "web-transactions": "Web Txn", "api": "API", "voice": "Voice/SIP",
        "bgp": "BGP", "DnsTrace": "DNS Trace", "sip-server": "SIP Server",
        "ftp-server": "FTP Server", "Dnssec": "DNSSEC",
    }
    gauges = [
        {"name": type_label_map.get(k, k), "val": v}
        for k, v in sorted(type_counts.items(), key=lambda x: -x[1])
        if v > 0
    ][:8]

    test_types = {t["name"]: t["type"] for t in all_tests if t.get("name") and t.get("type")}

    base = {
        "ALL_AGENTS": all_agents,
        "ALL_TESTS": all_tests,
        "TEST_IDS": test_ids,
        "TEST_TYPES": test_types,
        "AGENT_TESTS": agent_tests,
        "KNOWN_ISSUES": known_issues,
        "ALERT_FEED": alert_feed,
        "LIVE_EVENTS": live_events,
        "OUTAGES": all_outages,
        "ENDPOINT_AGENTS": endpoint_agents,
        "GAUGES": gauges,
        "COUNTS": {
            "tests": len(all_tests),
            "test_types": len(type_counts),
            "cloud_agents": cloud_count,
            "enterprise_agents": enterprise_count,
            "endpoint_agents": len(endpoint_agents),
            "total_ce_agents": cloud_count + enterprise_count,
            "alerts": len(alert_feed),
            "events": len(live_events),
            "app_outages": len(app_outages),
            "net_outages": len(net_outages),
        },
        "ACCOUNT_GROUP": account_group_name,
        "lastRefresh": datetime.now(timezone.utc).isoformat(),
    }

    with _cache_lock:
        _base_cache.update(base)

    _set_refresh_status(message="Base data loaded")
    log.info(
        "Base refresh complete: %d tests, %d agents (%d cloud + %d enterprise), "
        "%d endpoints, %d alerts, %d outages",
        len(all_tests), cloud_count + enterprise_count, cloud_count, enterprise_count,
        len(endpoint_agents), len(alert_feed), len(all_outages),
    )
    return base


# ---------------------------------------------------------------------------
# Metrics: availability per time window
# ---------------------------------------------------------------------------

def _get_metrics_synth_tests() -> dict[str, str]:
    """Return name -> test_id for all synthetic tests (excludes only endpoint tests ep-)."""
    with _cache_lock:
        test_ids_by_name = dict(_base_cache.get("TEST_IDS") or {})
    return {n: tid for n, tid in test_ids_by_name.items() if not tid.startswith("ep-")}


async def fetch_metrics_async(hours: int) -> dict[str, float]:
    now = datetime.now(timezone.utc)
    start = (now - timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    synth_ids = _get_metrics_synth_tests()
    if not synth_ids:
        return {}

    all_synth_tids = list(synth_ids.values())
    log.info("Fetching metrics for %dh window (%d synth tests)...", hours, len(synth_ids))
    test_availability = {}
    batch_size = MCP_BATCH_SIZE
    delay = MCP_INTER_BATCH_DELAY
    num_batches = (len(all_synth_tids) + batch_size - 1) // batch_size
    _set_refresh_status(
        phase="metrics",
        message=f"Fetching {hours}h availability ({len(synth_ids)} tests, {num_batches} batches)...",
        total=num_batches,
        current=0,
    )

    for i in range(0, len(all_synth_tids), batch_size):
        batch = all_synth_tids[i : i + batch_size]
        _set_refresh_status(current=(i // batch_size) + 1)
        resps = await _mcp_synth_metrics_batch(
            ["WEB_AVAILABILITY", "DNS_TRACE_AVAILABILITY"], batch, start, end
        )
        merged = _merge_parsed_first_wins(resps)
        for name, val in merged.items():
            if name not in test_availability:
                test_availability[name] = val
        await asyncio.sleep(delay + random.uniform(0, 0.4))

    loss_metrics = ["NET_LOSS", "ONE_WAY_NET_LOSS_TO_TARGET"]
    for loss_metric in loss_metrics:
        missing = [tid for tname, tid in synth_ids.items()
                   if tname not in test_availability]
        if not missing:
            break
        for i in range(0, len(missing), batch_size):
            batch = missing[i : i + batch_size]
            try:
                resp = await call_mcp_tool("get_network_app_synthetics_metrics", {
                    "metric_id": loss_metric,
                    "start_date": start,
                    "end_date": end,
                    "aggregation_type": "MEAN",
                    "group_by": "TEST",
                    "filter_dimension": "TEST",
                    "filter_values": batch,
                })
                parsed = parse_csv_metrics(resp)
                for name, val in parsed.items():
                    if name not in test_availability:
                        test_availability[name] = round(100.0 - val, 2)
            except Exception as e:
                log.error("MCP metrics batch error: metric=%s, batch_index=%s, error=%s", loss_metric, i, e)
            await asyncio.sleep(delay + random.uniform(0, 0.4))

    _set_refresh_status(message=f"{hours}h availability loaded ({len(test_availability)} tests)")
    log.info("Metrics for %dh: %d tests with availability", hours, len(test_availability))
    return test_availability


def get_or_fetch_metrics(window_key: str) -> dict[str, float] | None:
    hours = WINDOWS.get(window_key)
    if hours is None:
        return None

    with _cache_lock:
        entry = _metrics_cache.get(window_key)
        if entry and (time.monotonic() - entry["ts"]) < METRICS_TTL_SECONDS:
            return entry["data"]

    try:
        metrics = run_async(fetch_metrics_async(hours))
    except Exception as e:
        log.error("Metrics fetch failed: window=%s, error=%s", window_key, e)
        metrics = {}

    if metrics:
        with _cache_lock:
            _metrics_cache[window_key] = {"data": metrics, "ts": time.monotonic()}
        return metrics

    with _cache_lock:
        fallback = _metrics_cache.get("24h")
        if fallback:
            log.info("Falling back to 24h metrics for window %s", window_key)
            return fallback["data"]
    return {}


# ---------------------------------------------------------------------------
# Extra KPI metrics
# ---------------------------------------------------------------------------

async def fetch_extra_kpis_async(hours: int) -> dict:
    now = datetime.now(timezone.utc)
    start = (now - timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    synth_ids_dict = _get_metrics_synth_tests()
    if not synth_ids_dict:
        return {"avg_response_ms": None, "p95_response_ms": None, "avg_packet_loss": None, "loss_affected_paths": 0}
    synth_ids = list(synth_ids_dict.values())
    test_ids_by_name = synth_ids_dict
    batch_size = MCP_BATCH_SIZE
    delay = MCP_EXTRA_KPI_BATCH_DELAY
    _set_refresh_status(
        phase="extra_kpis",
        message=f"Fetching extra KPIs (response, loss, jitter, latency) for {len(synth_ids)} tests...",
    )
    extra = {"avg_response_ms": None, "p95_response_ms": None,
             "avg_packet_loss": None, "loss_affected_paths": 0}

    resp_by_test = {}
    resp_metric_order = ["WEB_TTFB"]
    for i in range(0, len(synth_ids), batch_size):
        batch = synth_ids[i : i + batch_size]
        resps = await _mcp_synth_metrics_batch(resp_metric_order, batch, start, end)
        merged = _merge_parsed_first_wins(resps)
        resp_by_test.update({k: v for k, v in merged.items() if k not in resp_by_test})
        await asyncio.sleep(delay + random.uniform(0, 0.5))

    if resp_by_test:
        vals = list(resp_by_test.values())
        extra["avg_response_ms"] = round(sum(vals) / len(vals))
        sorted_v = sorted(vals)
        n = len(sorted_v)
        extra["p50_response_ms"] = round(sorted_v[min(int(n * 0.50), n - 1)])
        extra["p75_response_ms"] = round(sorted_v[min(int(n * 0.75), n - 1)])
        extra["p95_response_ms"] = round(sorted_v[min(int(n * 0.95), n - 1)])
        extra["p99_response_ms"] = round(sorted_v[min(int(n * 0.99), n - 1)])
        resp_detail = []
        for test_name, ms_val in resp_by_test.items():
            numeric_id = test_ids_by_name.get(test_name, "")
            resp_detail.append({"test": test_name, "id": numeric_id, "ms": round(ms_val)})
        resp_detail.sort(key=lambda x: x["ms"], reverse=True)
        extra["resp_by_test"] = resp_detail

    loss_by_test = {}
    # Include ONE_WAY_NET_LOSS_TO_TARGET so agent-to-agent tests get loss data
    # (_merge_parsed_first_wins uses the first metric that returns a value per test)
    loss_metric_order = ["NET_LOSS", "ONE_WAY_NET_LOSS_TO_TARGET"]
    await asyncio.sleep(delay * 0.5)
    for i in range(0, len(synth_ids), batch_size):
        batch = synth_ids[i : i + batch_size]
        resps = await _mcp_synth_metrics_batch(loss_metric_order, batch, start, end)
        merged = _merge_parsed_first_wins(resps)
        loss_by_test.update({k: v for k, v in merged.items() if k not in loss_by_test})
        await asyncio.sleep(delay + random.uniform(0, 0.5))

    if loss_by_test:
        vals = list(loss_by_test.values())
        extra["avg_packet_loss"] = round(sum(vals) / len(vals), 2)
        extra["loss_affected_paths"] = sum(1 for v in vals if v > 0)
        sorted_l = sorted(vals)
        nl = len(sorted_l)
        extra["p50_loss"] = round(sorted_l[min(int(nl * 0.50), nl - 1)], 3)
        extra["p75_loss"] = round(sorted_l[min(int(nl * 0.75), nl - 1)], 3)
        extra["p95_loss"] = round(sorted_l[min(int(nl * 0.95), nl - 1)], 3)
        extra["p99_loss"] = round(sorted_l[min(int(nl * 0.99), nl - 1)], 3)
        loss_detail = []
        for test_name, loss_val in loss_by_test.items():
            numeric_id = test_ids_by_name.get(test_name, "")
            loss_detail.append({"test": test_name, "id": numeric_id, "loss": round(loss_val, 3)})
        loss_detail.sort(key=lambda x: x["loss"], reverse=True)
        extra["loss_by_test"] = loss_detail

    jitter_by_test = {}
    jitter_order = ["NET_JITTER", "ONE_WAY_NET_JITTER_TO_TARGET"]
    await asyncio.sleep(delay * 0.5)
    for i in range(0, len(synth_ids), batch_size):
        batch = synth_ids[i : i + batch_size]
        resps = await _mcp_synth_metrics_batch(jitter_order, batch, start, end)
        merged = _merge_parsed_first_wins(resps)
        jitter_by_test.update({k: v for k, v in merged.items() if k not in jitter_by_test})
        await asyncio.sleep(delay + random.uniform(0, 0.5))

    if jitter_by_test:
        jitter_detail = []
        for test_name, j_val in jitter_by_test.items():
            numeric_id = test_ids_by_name.get(test_name, "")
            jitter_detail.append({"test": test_name, "id": numeric_id, "jitter": round(j_val, 2)})
        jitter_detail.sort(key=lambda x: x["jitter"], reverse=True)
        extra["jitter_by_test"] = jitter_detail

    latency_by_test = {}
    lat_order = ["NET_LATENCY", "ONE_WAY_NET_LATENCY_TO_TARGET"]
    await asyncio.sleep(delay * 0.5)
    for i in range(0, len(synth_ids), batch_size):
        batch = synth_ids[i : i + batch_size]
        resps = await _mcp_synth_metrics_batch(lat_order, batch, start, end)
        merged = _merge_parsed_first_wins(resps)
        latency_by_test.update({k: v for k, v in merged.items() if k not in latency_by_test})
        await asyncio.sleep(delay + random.uniform(0, 0.5))

    if latency_by_test:
        lat_detail = []
        for test_name, lat_val in latency_by_test.items():
            numeric_id = test_ids_by_name.get(test_name, "")
            lat_detail.append({"test": test_name, "id": numeric_id, "latency": round(lat_val, 2)})
        lat_detail.sort(key=lambda x: x["latency"], reverse=True)
        extra["latency_by_test"] = lat_detail

    await asyncio.sleep(delay)
    voip_metrics = {
        "VOIP_MOS": "mos_by_test",
        "VOIP_LATENCY": "voip_latency_by_test",
        "VOIP_LOSS": "voip_loss_by_test",
        "VOIP_PDV": "pdv_by_test",
    }
    for metric_id, extra_key in voip_metrics.items():
        by_test = {}
        for i in range(0, len(synth_ids), batch_size):
            batch = synth_ids[i : i + batch_size]
            try:
                resp = await call_mcp_tool("get_network_app_synthetics_metrics", {
                    "metric_id": metric_id,
                    "start_date": start, "end_date": end,
                    "aggregation_type": "MEAN",
                    "group_by": "TEST",
                    "filter_dimension": "TEST", "filter_values": batch,
                })
                by_test.update(parse_csv_metrics(resp))
            except Exception as e:
                log.error("MCP extra KPI batch error: metric_id=%s, batch_index=%s, error=%s", metric_id, i, e)
            await asyncio.sleep(delay + random.uniform(0, 0.5))
        await asyncio.sleep(delay * 0.5)
        if by_test:
            detail = []
            for test_name, val in by_test.items():
                numeric_id_v = test_ids_by_name.get(test_name, "")
                detail.append({"test": test_name, "id": numeric_id_v, "value": round(val, 2)})
            detail.sort(key=lambda x: x["value"], reverse=(metric_id != "VOIP_MOS"))
            extra[extra_key] = detail

    await asyncio.sleep(delay)
    extra_type_metrics = {
        "BGP_REACHABILITY": "bgp_reach_by_test",
        "API_REQUEST_COMPLETION": "api_completion_by_test",
        "API_TRANSACTION_TIME": "api_txn_time_by_test",
        "WEB_TRANSACTION_COMPLETION": "txn_completion_by_test",
        "WEB_PAGE_LOAD_COMPLETION_RATE": "page_completion_by_test",
    }
    for metric_id, extra_key in extra_type_metrics.items():
        by_test = {}
        for i in range(0, len(synth_ids), batch_size):
            batch = synth_ids[i : i + batch_size]
            try:
                resp = await call_mcp_tool("get_network_app_synthetics_metrics", {
                    "metric_id": metric_id,
                    "start_date": start, "end_date": end,
                    "aggregation_type": "MEAN",
                    "group_by": "TEST",
                    "filter_dimension": "TEST", "filter_values": batch,
                })
                by_test.update(parse_csv_metrics(resp))
            except Exception as e:
                log.error("MCP extra KPI batch error: metric_id=%s, batch_index=%s, error=%s", metric_id, i, e)
            await asyncio.sleep(delay + random.uniform(0, 0.5))
        await asyncio.sleep(delay * 0.5)
        if by_test:
            detail = []
            for test_name, val in by_test.items():
                numeric_id_v = test_ids_by_name.get(test_name, "")
                detail.append({"test": test_name, "id": numeric_id_v, "value": round(val, 2)})
            detail.sort(key=lambda x: x["value"])
            extra[extra_key] = detail

    # --- Endpoint Agent metrics (test net latency, RSSI, gateway, CPU, memory) ---
    async def _fetch_ep_metric(metric_id: str):
        try:
            return await call_mcp_tool("get_endpoint_agent_metrics", {
                "metric_id": metric_id,
                "start_date": start, "end_date": end,
                "aggregation_type": "MEAN",
                "group_by": "ENDPOINT_AGENT_MACHINE_ID",
            })
        except Exception as e:
            log.debug("Endpoint metric %s failed: %s", metric_id, e)
            return None

    ep_results = await asyncio.gather(
        _fetch_ep_metric("ENDPOINT_TEST_NET_LATENCY"),
        _fetch_ep_metric("ENDPOINT_GATEWAY_WIRELESS_RSSI"),
        _fetch_ep_metric("ENDPOINT_GATEWAY_LATENCY"),
        _fetch_ep_metric("ENDPOINT_GATEWAY_LOSS"),
        _fetch_ep_metric("ENDPOINT_NET_CPU_LOAD_PERCENT"),
        _fetch_ep_metric("ENDPOINT_NET_MEMORY_LOAD_PERCENT"),
    )
    ep_responses = {
        "latency": ep_results[0],
        "rssi": ep_results[1],
        "gateway_latency": ep_results[2],
        "gateway_loss": ep_results[3],
        "cpu": ep_results[4],
        "memory": ep_results[5],
    }

    # Candidate column names used by different endpoint metric types in the CSV response.
    # Network metrics (latency, RSSI, gateway) use EYEBROW_MACHINE_ID; system metrics
    # (CPU, memory) may use ENDPOINT_AGENT_MACHINE_ID or MACHINE_ID.
    _EP_MID_COLS = ("EYEBROW_MACHINE_ID", "ENDPOINT_AGENT_MACHINE_ID", "MACHINE_ID")

    def _parse_ep_csv(resp):
        if not resp or not isinstance(resp, dict):
            return {}
        csv_text = resp.get("csv", "")
        if not csv_text:
            return {}
        agg_map = resp.get("names", {}).get("aggregatesMap", {})
        # Try multiple aggregatesMap keys — different metrics use different dimension names
        machine_names = {}
        for key in _EP_MID_COLS:
            machine_names = agg_map.get(key) or {}
            if machine_names:
                break
        header = csv_text.split("\n", 1)[0]
        mid_col = next((c for c in _EP_MID_COLS if c in header), None)
        if not mid_col:
            log.debug("EP CSV: no known machine-ID column in header: %s", header[:120])
            return {}
        sums = defaultdict(float)
        cnts = defaultdict(int)
        reader = csv.DictReader(io.StringIO(csv_text))
        for row in reader:
            mid = row.get(mid_col, "")
            v = row.get("v", "")
            if mid and v:
                try:
                    sums[mid] += float(v)
                    cnts[mid] += 1
                except ValueError:
                    pass
        result = {}
        for mid in sums:
            name = machine_names.get(mid, mid)
            result[name] = round(sums[mid] / cnts[mid], 2)
        return result

    def _ep_norm(name: str) -> str:
        """Normalize an endpoint agent name for cross-metric matching.
        Strips common local domain suffixes and lowercases so that
        'MKOWALEW-M-YQVQ' and 'MKOWALEW-M-YQVQ.local' resolve to the same key.
        """
        n = name.lower().strip()
        for suffix in (".local", ".lan", ".internal", ".home", ".localdomain"):
            if n.endswith(suffix):
                n = n[: -len(suffix)]
                break
        return n

    for ep_key, ep_resp in ep_responses.items():
        if ep_resp and isinstance(ep_resp, dict):
            hdr = (ep_resp.get("csv", "") or "").split("\n", 1)[0]
            agg_keys = list((ep_resp.get("names") or {}).get("aggregatesMap", {}).keys())
            log.debug("EP metric=%s csv_header=%s aggregatesMap_keys=%s", ep_key, hdr[:120], agg_keys)

    latency_by_name = _parse_ep_csv(ep_responses.get("latency"))
    rssi_by_name = _parse_ep_csv(ep_responses.get("rssi"))
    gateway_latency_by_name = _parse_ep_csv(ep_responses.get("gateway_latency"))
    gateway_loss_by_name = _parse_ep_csv(ep_responses.get("gateway_loss"))
    cpu_by_name = _parse_ep_csv(ep_responses.get("cpu"))
    memory_by_name = _parse_ep_csv(ep_responses.get("memory"))
    log.info(
        "EP CSV parsed: latency=%d, rssi=%d, gw_lat=%d, gw_loss=%d, cpu=%d, mem=%d",
        len(latency_by_name), len(rssi_by_name),
        len(gateway_latency_by_name), len(gateway_loss_by_name),
        len(cpu_by_name), len(memory_by_name),
    )

    # Build a normalized→canonical name index across all metric dicts so that name
    # variants (e.g. "HOST" vs "HOST.local") collapse to a single ep_perf entry.
    norm_to_canonical: dict[str, str] = {}
    all_metric_dicts = (latency_by_name, rssi_by_name, gateway_latency_by_name,
                        gateway_loss_by_name, cpu_by_name, memory_by_name)
    for d in all_metric_dicts:
        for name in d:
            norm = _ep_norm(name)
            existing = norm_to_canonical.get(norm)
            # Prefer the shorter, cleaner name (no domain suffix) as canonical
            if existing is None or len(name) < len(existing):
                norm_to_canonical[norm] = name

    def _ep_get(d: dict, norm: str):
        """Look up a value by normalized name, trying exact then normalized match."""
        canonical = norm_to_canonical.get(norm, "")
        if canonical in d:
            return d[canonical]
        # Fallback: check all keys in d for a normalized match
        for k, v in d.items():
            if _ep_norm(k) == norm:
                return v
        return None

    with _cache_lock:
        ep_agents = list(_base_cache.get("ENDPOINT_AGENTS") or [])

    ep_perf = []
    seen_norms = set()
    for norm, comp_name in norm_to_canonical.items():
        if norm in seen_norms:
            continue
        seen_norms.add(norm)
        lat_val = _ep_get(latency_by_name, norm)
        rssi_val = _ep_get(rssi_by_name, norm)
        gw_lat = _ep_get(gateway_latency_by_name, norm)
        gw_loss = _ep_get(gateway_loss_by_name, norm)
        cpu_val = _ep_get(cpu_by_name, norm)
        mem_val = _ep_get(memory_by_name, norm)
        loc = ""
        agent_id = ""
        device = ""
        display_name = comp_name
        for ep in ep_agents:
            ep_name = ep.get("name", "")
            if _ep_norm(ep_name) == norm or ep_name == comp_name:
                loc = ep.get("loc", "")
                agent_id = ep.get("id", "")
                device = ep.get("device", "")
                display_name = ep_name
                break
        if all(v is None for v in (lat_val, rssi_val, gw_lat, gw_loss, cpu_val, mem_val)):
            log.debug("EP perf: skipping %s — all metrics None", display_name)
            continue
        ep_perf.append({
                "name": display_name,
                "loc": loc,
                "id": agent_id,
                "device": device,
                "latency": lat_val,
                "rssi": rssi_val,
                "loss": None,
                "gateway_latency": gw_lat,
                "gateway_loss": gw_loss,
                "cpu": cpu_val,
                "memory": mem_val,
            })

    # Sort by test net latency (highest first = worst); then by RSSI (lowest first = worst)
    ep_perf.sort(key=lambda x: (
        x.get("latency") is None,
        -(x.get("latency") or 0),
        x.get("rssi") is None,
        x.get("rssi") or 0,
    ))
    extra["ep_worst_performers"] = ep_perf

    _set_refresh_status(message="Extra KPIs loaded")
    log.info("Extra KPIs for %dh: resp=%s loss=%s ep_worst=%d",
             hours, extra["avg_response_ms"], extra["avg_packet_loss"], len(ep_perf))
    return extra


def get_or_fetch_extra_kpis(window_key: str) -> dict:
    hours = WINDOWS.get(window_key, 24)

    with _cache_lock:
        entry = _extra_kpi_cache.get(window_key)
        if entry and (time.monotonic() - entry["ts"]) < METRICS_TTL_SECONDS:
            return entry["data"]

    try:
        data = run_async(fetch_extra_kpis_async(hours))
    except Exception as e:
        log.error("Extra KPIs fetch failed: window=%s, error=%s", window_key, e)
        data = {}

    has_data = data and (data.get("avg_response_ms") is not None or data.get("avg_packet_loss") is not None)
    if has_data:
        with _cache_lock:
            _extra_kpi_cache[window_key] = {"data": data, "ts": time.monotonic()}
        return data

    with _cache_lock:
        fallback = _extra_kpi_cache.get("24h")
        if fallback:
            log.info("Falling back to 24h extra KPIs for window %s", window_key)
            return fallback["data"]
    return data or {}


# ---------------------------------------------------------------------------
# Sync wrappers for background scheduler
# ---------------------------------------------------------------------------

def refresh_base():
    run_async(refresh_base_data_async())


def refresh_default_metrics():
    """Fetch 1h window from MCP and persist hourly rollup to DB."""
    m = get_or_fetch_metrics("1h") or {}
    e = get_or_fetch_extra_kpis("1h") or {}
    _kpi_persist_hourly(m, e)


def scheduled_refresh():
    """One scheduler tick: base first (defines test IDs), then 1h metrics + hourly persist."""
    start = time.perf_counter()
    log.info("Scheduled refresh started")
    try:
        refresh_base()
        refresh_default_metrics()
        elapsed = time.perf_counter() - start
        log.info("Scheduled refresh complete in %.2fs", elapsed)
    except Exception as e:
        elapsed = time.perf_counter() - start
        log.exception("Scheduled refresh failed after %.2fs: %s", elapsed, e)


def _initial_load_background():
    """Background startup: load 1h metrics, persist hourly bucket, then warm 24h from DB or MCP.

    Runs in a daemon thread after the bootstrap window first paint.
    Checks existing DB data first -- if the DB has sufficient recent hourly
    buckets from a previous run, the expensive 24h MCP fallback is skipped
    entirely and caches are warmed from DB instead.
    """
    try:
        # -- Check existing DB data --
        inv = _db_inventory()
        _startup_mark("db_inventory", inv)
        log.info(
            "DB inventory: %d total buckets, %d recent (24h), latest=%s (age=%.1fh), %d tests, 24h_coverage=%s",
            inv["total_hourly_buckets"], inv["recent_hourly_buckets"],
            inv["latest_bucket"] or "none",
            inv["latest_bucket_age_hours"] or -1,
            inv["test_count"],
            inv["has_24h_coverage"],
        )

        # -- Fetch 1h for the current hourly bucket --
        t0 = time.perf_counter()
        _set_refresh_status(phase="metrics", message="Loading 1h metrics for current hourly bucket...")
        m1 = get_or_fetch_metrics("1h") or {}
        _set_refresh_status(phase="extra_kpis", message="Loading 1h extra KPIs...")
        e1 = get_or_fetch_extra_kpis("1h") or {}
        _kpi_persist_hourly(m1, e1)
        hourly_sec = time.perf_counter() - t0
        _startup_mark("hourly_1h_sec", hourly_sec)
        log.info("Background 1h hourly bucket loaded in %.2fs", hourly_sec)

        # -- Warm 24h view: try DB first, fall back to MCP --
        if inv["has_24h_coverage"]:
            t_w = time.perf_counter()
            _set_refresh_status(phase="metrics", message="Warming 24h caches from existing DB data...")
            warmed = _warm_caches_from_db(inv)
            warmup_sec = time.perf_counter() - t_w
            _startup_mark("db_warmup_sec", warmup_sec)

            if warmed:
                _startup_mark("skipped_24h_fallback", True)
                total = time.perf_counter() - (_startup_t0 or t0)
                _startup_mark("total_sec", total)
                _set_refresh_status(phase="done", message="Ready (from DB)")
                log.info(
                    "Startup complete (DB warm): total=%.2fs, 1h=%.2fs, db_warmup=%.2fs — skipped 24h MCP fallback",
                    total, hourly_sec, warmup_sec,
                )
                return
            log.info("DB warmup returned no data; falling through to MCP 24h fallback")

        # -- Full MCP 24h fallback (cold start or insufficient DB data) --
        t1 = time.perf_counter()
        _set_refresh_status(phase="metrics", message="Loading 24h metrics from MCP (no DB history)...")
        get_or_fetch_metrics("24h")
        _set_refresh_status(phase="extra_kpis", message="Loading 24h extra KPIs from MCP...")
        get_or_fetch_extra_kpis("24h")
        fallback_sec = time.perf_counter() - t1
        _startup_mark("fallback_24h_sec", fallback_sec)
        log.info("Background 24h MCP cache fallback loaded in %.2fs", fallback_sec)

        total = time.perf_counter() - (_startup_t0 or t0)
        _startup_mark("total_sec", total)
        _set_refresh_status(phase="done", message="Ready")
        log.info("Startup complete: total=%.2fs (1h=%.2fs, 24h_fallback=%.2fs)", total, hourly_sec, fallback_sec)
    except Exception as e:
        total = time.perf_counter() - (_startup_t0 or time.perf_counter())
        _startup_mark("total_sec", total)
        log.exception("Background initial load failed after %.2fs: %s", total, e)
        _set_refresh_status(error=str(e))


def run_initial_load():
    """Run initial data load in background; updates _refresh_status for /api/refresh-status."""
    global _startup_t0
    _startup_t0 = time.perf_counter()
    try:
        _set_refresh_status(phase="base", message="Fetching base data (tests, agents, alerts)...")
        t_base = time.perf_counter()
        refresh_base()
        base_sec = time.perf_counter() - t_base
        _startup_mark("base_sec", base_sec)
        log.info("Base data loaded in %.2fs", base_sec)

        if INITIAL_BOOTSTRAP_DISABLED:
            _set_refresh_status(phase="base", message="Checking local database for existing hourly data...")
            inv = _db_inventory()
            _startup_mark("db_inventory", inv)
            db_msg = (
                f"DB: {inv['recent_hourly_buckets']} recent hourly buckets, "
                f"{inv['test_count']} tests — {'using DB cache' if inv['has_24h_coverage'] else 'cold start'}"
            )
            with _refresh_status_lock:
                _refresh_status["db_check"] = db_msg
            log.info("DB inventory (bootstrap disabled): buckets=%d, recent=%d, 24h_coverage=%s",
                     inv["total_hourly_buckets"], inv["recent_hourly_buckets"], inv["has_24h_coverage"])

            t_h = time.perf_counter()
            _set_refresh_status(phase="metrics", message="Fetching 1h metrics...")
            m1 = get_or_fetch_metrics("1h") or {}
            _set_refresh_status(phase="extra_kpis", message="Fetching 1h extra KPIs...")
            e1 = get_or_fetch_extra_kpis("1h") or {}
            _kpi_persist_hourly(m1, e1)
            _startup_mark("hourly_1h_sec", time.perf_counter() - t_h)

            if inv["has_24h_coverage"]:
                t_w = time.perf_counter()
                _set_refresh_status(phase="metrics", message="Warming 24h caches from DB...")
                warmed = _warm_caches_from_db(inv)
                _startup_mark("db_warmup_sec", time.perf_counter() - t_w)
                if warmed:
                    _startup_mark("skipped_24h_fallback", True)
                    total = time.perf_counter() - _startup_t0
                    _startup_mark("total_sec", total)
                    _set_refresh_status(phase="done", message="Ready (from DB)")
                    log.info("Initial load complete in %.2fs (DB warm, skipped 24h MCP)", total)
                    return

            t_f = time.perf_counter()
            _set_refresh_status(phase="metrics", message="Loading 24h from MCP (no DB history)...")
            get_or_fetch_metrics("24h")
            get_or_fetch_extra_kpis("24h")
            _startup_mark("fallback_24h_sec", time.perf_counter() - t_f)

            total = time.perf_counter() - _startup_t0
            _startup_mark("total_sec", total)
            _set_refresh_status(phase="done", message="Ready")
            log.info("Initial data load complete in %.2fs", total)
            return

        # -- Check DB for existing hourly data before hitting MCP --
        _set_refresh_status(phase="base", message="Checking local database for existing hourly data...")
        inv = _db_inventory()
        _startup_mark("db_inventory", inv)
        db_msg = (
            f"DB: {inv['recent_hourly_buckets']} recent hourly buckets, "
            f"{inv['test_count']} tests — {'using DB cache' if inv['has_24h_coverage'] else 'cold start'}"
        )
        with _refresh_status_lock:
            _refresh_status["db_check"] = db_msg
        log.info(
            "DB inventory: %d total buckets, %d recent (24h), latest=%s (age=%.1fh), %d tests, 24h_coverage=%s",
            inv["total_hourly_buckets"], inv["recent_hourly_buckets"],
            inv["latest_bucket"] or "none",
            inv["latest_bucket_age_hours"] or -1,
            inv["test_count"],
            inv["has_24h_coverage"],
        )

        if inv["has_24h_coverage"]:
            t_fast = time.perf_counter()
            _set_refresh_status(phase="metrics", message="Fetching 1h metrics (DB has 24h history)...")
            m1 = get_or_fetch_metrics("1h") or {}
            _set_refresh_status(phase="extra_kpis", message="Fetching 1h extra KPIs...")
            e1 = get_or_fetch_extra_kpis("1h") or {}
            _kpi_persist_hourly(m1, e1)
            _startup_mark("hourly_1h_sec", time.perf_counter() - t_fast)

            t_w = time.perf_counter()
            _set_refresh_status(phase="metrics", message="Warming 24h caches from DB...")
            warmed = _warm_caches_from_db(inv)
            _startup_mark("db_warmup_sec", time.perf_counter() - t_w)
            if warmed:
                _startup_mark("skipped_24h_fallback", True)
                total = time.perf_counter() - _startup_t0
                _startup_mark("total_sec", total)
                _set_refresh_status(phase="done", message="Ready (from DB)")
                log.info("Initial load complete in %.2fs (DB warm, skipped 24h MCP)", total)
                return

            log.info("DB warmup returned no data; falling through to MCP bootstrap")

        # -- No DB coverage (or warmup failed): bootstrap from MCP, then background load --
        bw = INITIAL_BOOTSTRAP_WINDOW
        _set_refresh_status(
            phase="metrics",
            message=f"Quick metrics ({bw}) for first paint; full load next...",
        )
        t_boot = time.perf_counter()
        m_boot = get_or_fetch_metrics(bw) or {}
        _set_refresh_status(phase="extra_kpis", message=f"Quick KPIs ({bw})...")
        e_boot = get_or_fetch_extra_kpis(bw) or {}
        if bw == "1h":
            _kpi_persist_hourly(m_boot, e_boot)
        boot_sec = time.perf_counter() - t_boot
        _startup_mark("bootstrap_sec", boot_sec)
        log.info("Initial bootstrap (%s) complete in %.2fs; starting background load", bw, boot_sec)

        threading.Thread(target=_initial_load_background, daemon=True, name="initial-bg-load").start()
    except Exception as e:
        total = time.perf_counter() - _startup_t0
        _startup_mark("total_sec", total)
        log.exception("Initial load failed after %.2fs: %s", total, e)
        _set_refresh_status(error=str(e))


# ---------------------------------------------------------------------------
# Flask routes
# ---------------------------------------------------------------------------

_TEST_ID_RE = re.compile(r"^(\d{1,20}|ep-[\w-]{1,64})$")


def _sha256_utf8(value: str) -> bytes:
    return hashlib.sha256(value.encode("utf-8")).digest()


def _credentials_match(user_guess: str, password_guess: str) -> bool:
    if not AUTH_ENABLED:
        return True
    return hmac.compare_digest(_sha256_utf8(user_guess), _sha256_utf8(AUTH_USERNAME)) and hmac.compare_digest(
        _sha256_utf8(password_guess), _sha256_utf8(AUTH_PASSWORD)
    )


def _session_authenticated() -> bool:
    return session.get("dash_auth") is True


def _safe_next_path(raw: str | None) -> str:
    if not raw or not isinstance(raw, str):
        return "/"
    s = raw.strip()
    if not s.startswith("/") or s.startswith("//"):
        return "/"
    return s.split("?", 1)[0].split("#", 1)[0] or "/"


@app.before_request
def _require_dashboard_auth():
    if not AUTH_ENABLED:
        return None
    p = request.path
    if p.startswith("/static/") or p == "/login":
        return None
    if p == "/api/health":
        return None
    if p == "/api/refresh-status":
        return None
    if _session_authenticated():
        return None
    if p.startswith("/api/"):
        return jsonify({"error": "Unauthorized", "login": "/login"}), 401
    return redirect(url_for("login", next=p))


@app.route("/login", methods=["GET", "POST"])
def login():
    if not AUTH_ENABLED:
        return redirect("/")
    if request.method == "GET":
        if _session_authenticated():
            return redirect(_safe_next_path(request.args.get("next")))
        return send_file("login.html")
    user = (request.form.get("username") or "").strip()
    password = request.form.get("password") or ""
    nxt = _safe_next_path(request.form.get("next"))
    if _credentials_match(user, password):
        session.clear()
        session.permanent = True
        session["dash_auth"] = True
        return redirect(nxt)
    return redirect(url_for("login", next=nxt, err="1"))


@app.route("/logout")
def logout():
    session.clear()
    if AUTH_ENABLED:
        return redirect(url_for("login"))
    return redirect("/")


@app.after_request
def _security_headers(response):
    response.headers["X-Content-Type-Options"] = "nosniff"
    if request.path.startswith("/api/"):
        response.headers["Cache-Control"] = "no-store"
    return response


@app.route("/")
def index():
    return send_file("noc_dashboard.html")


@app.route("/site-health")
def site_health():
    return send_file("site_health.html")


@app.route("/executive")
def executive():
    return send_file("executive.html")


@app.route("/api/health")
def api_health():
    """Liveness + whether MCP base cache has been populated."""
    with _cache_lock:
        ready = bool(_base_cache)
        last = (_base_cache.get("lastRefresh") or "") if _base_cache else ""
    return jsonify({
        "status": "ok",
        "base_cache_ready": ready,
        "last_base_refresh": last,
        "refresh_interval_minutes": REFRESH_MINUTES,
        "kpi_history_enabled": not KPI_HISTORY_DISABLED and bool(KPI_DB_PATH),
        "kpi_hourly_retention_days": KPI_HOURLY_RETENTION_DAYS,
        "kpi_daily_retention_days": KPI_DAILY_RETENTION_DAYS,
        "initial_bootstrap_window": None if INITIAL_BOOTSTRAP_DISABLED else INITIAL_BOOTSTRAP_WINDOW,
        "metrics_first_paint_ready": _metrics_first_paint_ready(),
        "full_metrics_ready": _full_metrics_ready(),
        "startup_timing": _startup_get(),
    })


@app.route("/api/refresh-status")
def api_refresh_status():
    """Current status of the initial/background data load for startup script and UI."""
    with _refresh_status_lock:
        out = dict(_refresh_status)
    out["initial_bootstrap_window"] = None if INITIAL_BOOTSTRAP_DISABLED else INITIAL_BOOTSTRAP_WINDOW
    out["metrics_first_paint_ready"] = _metrics_first_paint_ready()
    out["full_metrics_ready"] = _full_metrics_ready()
    out["startup_timing"] = _startup_get()
    return jsonify(out)


@app.route("/api/kpi_history")
def api_kpi_history():
    """Time series for stored KPI scalars.

    Reads from kpi_hourly (hourly resolution, up to KPI_HOURLY_RETENTION_DAYS)
    and kpi_daily (daily resolution, up to KPI_DAILY_RETENTION_DAYS).
    Auto-selects resolution based on since_hours unless overridden.

    Query params:
        kpi      - required, repeatable or comma-separated
        since_hours - default 168 (7 days), max 8760
        resolution  - 'hourly', 'daily', or 'auto' (default)
    """
    if KPI_HISTORY_DISABLED:
        return jsonify({"error": "KPI history disabled (KPI_HISTORY_DISABLED)"}), 503

    raw = request.args.getlist("kpi")
    if len(raw) == 1 and "," in raw[0]:
        kpis = [x.strip() for x in raw[0].split(",") if x.strip()]
    else:
        kpis = [x.strip() for x in raw if x.strip()]
    if not kpis:
        return jsonify({
            "error": "missing kpi parameter",
            "known_keys": sorted(_kpi_known_keys()),
        }), 400

    known = _kpi_known_keys()
    bad = [k for k in kpis if k not in known]
    if bad:
        return jsonify({"error": "unknown kpi", "unknown": bad, "known_keys": sorted(known)}), 400

    try:
        since_h = int(request.args.get("since_hours") or "168")
    except (ValueError, TypeError):
        since_h = 168
    since_h = max(1, min(8760, since_h))

    resolution = (request.args.get("resolution") or "auto").strip().lower()
    if resolution not in ("hourly", "daily", "auto"):
        resolution = "auto"

    # Auto-select: hourly for <= 14 days, daily for longer ranges
    if resolution == "auto":
        resolution = "hourly" if since_h <= KPI_HOURLY_RETENTION_DAYS * 24 else "daily"

    conn = _kpi_get_conn()
    if not conn:
        return jsonify({"error": "KPI database unavailable"}), 503

    series: dict[str, list[dict[str, float | str]]] = {k: [] for k in kpis}
    qmarks = ",".join("?" * len(kpis))

    if resolution == "hourly":
        start_s = (datetime.now(timezone.utc) - timedelta(hours=since_h)).strftime("%Y-%m-%dT%H:%M:%SZ")
        sql = (
            f"SELECT kpi_key, hour_bucket, value FROM kpi_hourly "
            f"WHERE hour_bucket >= ? AND kpi_key IN ({qmarks}) "
            f"ORDER BY kpi_key, hour_bucket"
        )
        params: list = [start_s, *kpis]
    else:
        start_s = (datetime.now(timezone.utc) - timedelta(hours=since_h)).strftime("%Y-%m-%d")
        sql = (
            f"SELECT kpi_key, day_bucket, avg_value FROM kpi_daily "
            f"WHERE day_bucket >= ? AND kpi_key IN ({qmarks}) "
            f"ORDER BY kpi_key, day_bucket"
        )
        params = [start_s, *kpis]

    with _kpi_db_lock:
        try:
            cur = conn.execute(sql, params)
            for row in cur:
                k, t, v = row[0], row[1], row[2]
                if k in series:
                    series[k].append({"t": t, "v": float(v)})
        except sqlite3.Error as e:
            log.error("kpi_history query failed: %s", e)
            return jsonify({"error": "query failed"}), 500

    # Per-series fallback: for any KPI that has no data from the primary table,
    # try the legacy kpi_point table (which stored per-refresh snapshots).
    empty_kpis = [k for k in kpis if not series[k]]
    if empty_kpis:
        legacy_window = (request.args.get("window") or "24h").strip()
        if legacy_window not in WINDOWS:
            legacy_window = "24h"
        legacy_start = (datetime.now(timezone.utc) - timedelta(hours=since_h)).strftime("%Y-%m-%dT%H:%M:%SZ")
        eq = ",".join("?" * len(empty_kpis))
        legacy_sql = (
            f"SELECT kpi_key, sampled_at, value FROM kpi_point "
            f"WHERE window_key = ? AND sampled_at >= ? AND kpi_key IN ({eq}) "
            f"ORDER BY kpi_key, sampled_at"
        )
        with _kpi_db_lock:
            try:
                cur = conn.execute(legacy_sql, [legacy_window, legacy_start, *empty_kpis])
                for row in cur:
                    k, t, v = row[0], row[1], row[2]
                    if k in series:
                        series[k].append({"t": t, "v": float(v)})
            except sqlite3.Error:
                pass

    return jsonify({
        "resolution": resolution,
        "since_hours": since_h,
        "hourly_retention_days": KPI_HOURLY_RETENTION_DAYS,
        "daily_retention_days": KPI_DAILY_RETENTION_DAYS,
        "series": series,
    })


@app.route("/api/data")
def api_data():
    window = request.args.get("window", "24h")
    if window not in WINDOWS:
        window = "24h"

    with _cache_lock:
        if not _base_cache:
            return jsonify({"error": "Data not yet loaded. Please wait..."}), 503
        base = dict(_base_cache)

    hours = WINDOWS[window]
    metrics = None
    extra_kpis = None
    served_mw = window
    served_ew = window
    data_source = "cache"

    # For multi-hour windows, try to build from stored hourly data first
    if hours > 1 and _has_enough_hourly_data(hours):
        metrics = _build_metrics_from_hourly(hours)
        extra_kpis = _build_extra_kpis_from_hourly(hours)
        if metrics and extra_kpis:
            data_source = "hourly_db"

    # Fall back to in-memory cache if DB view unavailable
    if not metrics or not extra_kpis:
        served_mw, m_entry = _resolve_metrics_cache_entry(window)
        served_ew, e_entry = _resolve_extra_cache_entry(window)
        metrics = m_entry["data"] if m_entry else {}
        extra_kpis = e_entry["data"] if e_entry else {}
        data_source = "cache"

    base["TEST_AVAILABILITY"] = metrics
    base["EXTRA_KPI"] = extra_kpis
    base["window"] = window
    base["served_metrics_window"] = served_mw
    base["served_extra_window"] = served_ew
    base["data_source"] = data_source
    base["full_metrics_ready"] = _full_metrics_ready()
    base["metrics_ready"] = bool(metrics)
    base["refresh_interval_minutes"] = REFRESH_MINUTES
    base["metrics_ttl_seconds"] = METRICS_TTL_SECONDS
    base["business_services_config"] = _business_services_config
    return jsonify(base)


@app.route("/api/fetch_window")
def api_fetch_window():
    """Fetch metrics for a specific window.

    For multi-hour windows with sufficient hourly DB data, serves from DB
    without hitting MCP. Falls back to on-demand MCP fetch otherwise.
    """
    window = request.args.get("window", "24h")
    if window not in WINDOWS:
        window = "24h"
    hours = WINDOWS[window]

    # For multi-hour windows, try DB-backed view first
    if hours > 1 and _has_enough_hourly_data(hours):
        log.info("Serving window %s from hourly DB data", window)
        return jsonify({"status": "from_hourly_db", "window": window})

    with _cache_lock:
        m_entry = _metrics_cache.get(window)
        e_entry = _extra_kpi_cache.get(window)
    if (m_entry and (time.monotonic() - m_entry["ts"]) < METRICS_TTL_SECONDS and
            e_entry and (time.monotonic() - e_entry["ts"]) < METRICS_TTL_SECONDS):
        return jsonify({"status": "cached", "window": window})

    log.info("Fetching metrics on demand for window %s (%dh)...", window, hours)
    updated_m = False
    updated_e = False
    try:
        metrics = run_async(fetch_metrics_async(hours))
        if metrics:
            with _cache_lock:
                _metrics_cache[window] = {"data": metrics, "ts": time.monotonic()}
            updated_m = True
    except Exception as e:
        log.error("On-demand metrics fetch failed: window=%s, error=%s", window, e)

    try:
        extra = run_async(fetch_extra_kpis_async(hours))
        has_data = extra and (extra.get("avg_response_ms") is not None or extra.get("avg_packet_loss") is not None)
        if has_data:
            with _cache_lock:
                _extra_kpi_cache[window] = {"data": extra, "ts": time.monotonic()}
            updated_e = True
    except Exception as e:
        log.error("On-demand extra KPIs fetch failed: window=%s, error=%s", window, e)

    if updated_m and updated_e:
        with _cache_lock:
            m_data = dict((_metrics_cache.get(window) or {}).get("data") or {})
            e_data = dict((_extra_kpi_cache.get(window) or {}).get("data") or {})
        _kpi_persist_hourly(m_data, e_data)

    return jsonify({"status": "fetched", "window": window})


METRIC_FOR_TYPE = {
    "agent-to-server": "NET_LOSS",
    "agent-to-agent": "ONE_WAY_NET_LOSS_TO_TARGET",
    "http-server": "WEB_AVAILABILITY",
    "ftp-server": "FTP_AVAILABILITY",
    "web-transactions": "WEB_TRANSACTION_COMPLETION",
    "page-load": "WEB_PAGE_LOAD_COMPLETION_RATE",
    "voice": "VOIP_MOS",
    "dns-server": "DNS_SERVER_AVAILABILITY",
    "dnstrace": "DNS_TRACE_AVAILABILITY",
    "sip-server": "SIP_AVAILABILITY",
    "bgp": "BGP_REACHABILITY",
    "api": "API_REQUEST_COMPLETION",
}

METRIC_LABEL_FOR_TYPE = {
    "agent-to-server": "Pkt Loss %",
    "agent-to-agent": "Pkt Loss %",
    "http-server": "Availability %",
    "ftp-server": "Availability %",
    "web-transactions": "Completion %",
    "page-load": "Completion %",
    "voice": "MOS",
    "dns-server": "Availability %",
    "dnstrace": "Availability %",
    "sip-server": "Availability %",
    "bgp": "Reachability %",
    "api": "Completion %",
}


def _parse_agent_csv(data: dict) -> list[dict]:
    """Parse per-agent metrics from SOURCE_AGENT grouped response."""
    csv_text = data.get("csv", "")
    agg_map = data.get("names", {}).get("aggregatesMap", {})
    names_map = agg_map.get("SOURCE", {})
    sums = defaultdict(float)
    cnts = defaultdict(int)
    reader = csv.DictReader(io.StringIO(csv_text))
    for row in reader:
        aid = row.get("SOURCE", "")
        v = row.get("v", "")
        if aid and v:
            try:
                sums[aid] += float(v)
                cnts[aid] += 1
            except ValueError:
                pass
    result = []
    for aid in sums:
        name = names_map.get(aid, aid)
        result.append({"id": aid, "name": name, "value": round(sums[aid] / cnts[aid], 4)})
    return result


# Path visualization: MCP tool "Get Full Path Visualization" (Network Path Analysis)
# https://docs.thousandeyes.com/product-documentation/integration-guides/thousandeyes-mcp-server#mcp-server-functionality-and-sample-prompts
_path_vis_cache: dict = {}
PATH_VIS_CACHE_TTL = 300  # 5 min

def _parse_path_vis_node(node_str: str) -> dict:
    """Parse pathVis node string 'ip|prefix|AS|location' into { ipAddress, rdns }."""
    if not node_str or not isinstance(node_str, str):
        return {"ipAddress": "", "rdns": ""}
    parts = node_str.split("|")
    ip = (parts[0] or "").strip()
    # Prefer location for label, then AS, then prefix
    rdns = (parts[3] if len(parts) > 3 else "") or (parts[2] if len(parts) > 2 else "") or (parts[1] if len(parts) > 1 else "") or ip
    return {"ipAddress": ip, "rdns": rdns.strip()}


def _parse_path_trace_metrics(step) -> dict:
    """Parse pathTrace step 4th element (e.g. 'L=0,E=0,S=0,T=1/L=279934,E=0,S=1,T=1') for latency and loss.
    L= latency (typically µs), E= errors; returns { latencyMs, lossPercent } (None if absent).
    """
    out = {}
    if not isinstance(step, (list, tuple)) or len(step) < 4:
        return out
    raw = step[3]
    if not isinstance(raw, str):
        return out
    # One or two segments separated by /
    segments = raw.split("/")
    latencies = []
    errors = []
    for seg in segments:
        for part in seg.split(","):
            part = part.strip()
            m = re.match(r"L=(\d+)", part, re.IGNORECASE)
            if m:
                latencies.append(int(m.group(1)))
            m = re.match(r"E=(\d+)", part, re.IGNORECASE)
            if m:
                errors.append(int(m.group(1)))
    if latencies:
        # Assume L is in µs when > 1000, else ms
        max_l = max(latencies)
        out["latencyMs"] = round(max_l / 1000.0, 2) if max_l >= 1000 else max_l
    if errors and any(e > 0 for e in errors):
        out["hasLoss"] = True
        if len(errors) == 1:
            out["lossPercent"] = min(100, errors[0])
        else:
            out["lossPercent"] = min(100, max(errors))
    return out


def _normalize_path_vis_response(raw):
    """Normalize MCP path visualization response for frontend (agents, hops, rounds).
    Supports ThousandEyes pathVis shape: pathVis.nodes (node strings), pathVis.agents (agents with runs.pathTraces as node indices).
    """
    if not raw or not isinstance(raw, dict):
        return raw
    out = {"results": [], "error": raw.get("error")}

    # Handle pathVis shape: { test, pathVis: { server, nodes[], agents[] } }
    path_vis = raw.get("pathVis") if isinstance(raw.get("pathVis"), dict) else None
    if path_vis:
        nodes_list = path_vis.get("nodes") or []
        if not isinstance(nodes_list, list):
            nodes_list = []
        server_default = path_vis.get("server") or ""
        agents_list = path_vis.get("agents") or []
        if not isinstance(agents_list, list):
            agents_list = []
        for ag in agents_list:
            if not isinstance(ag, dict):
                continue
            agent_node = {
                "agentId": ag.get("agentId") or ag.get("agent_id") or "",
                "agentName": ag.get("agentName") or ag.get("agent_name") or "Agent",
                "location": ag.get("countryId") or ag.get("location") or "",
            }
            runs = ag.get("runs") or []
            if not isinstance(runs, list):
                runs = []
            # Use first run (or latest) to get one path
            run = runs[0] if runs else {}
            if not isinstance(run, dict):
                run = {}
            server_ip = run.get("serverIp") or server_default
            path_traces = run.get("pathTraces") or []
            if not isinstance(path_traces, list):
                path_traces = []
            hops = []
            # pathTraces is list of traces; each trace is list of [nodeIndex, x, y] or [nodeIndex, x, y, "L=...,E=..."]
            if path_traces:
                first_trace = path_traces[0] if isinstance(path_traces[0], list) else []
                for step in first_trace if isinstance(first_trace, list) else []:
                    if isinstance(step, (list, tuple)) and len(step) >= 1:
                        idx = step[0]
                        if isinstance(idx, int) and 0 <= idx < len(nodes_list):
                            node_str = nodes_list[idx]
                            if isinstance(node_str, str):
                                hop = dict(_parse_path_vis_node(node_str))
                                metrics = _parse_path_trace_metrics(step)
                                if metrics.get("latencyMs") is not None:
                                    hop["latencyMs"] = metrics["latencyMs"]
                                if metrics.get("hasLoss") or metrics.get("lossPercent") is not None:
                                    hop["hasLoss"] = True
                                    if metrics.get("lossPercent") is not None:
                                        hop["lossPercent"] = metrics["lossPercent"]
                                hops.append(hop)
                    elif isinstance(step, dict) and ("ipAddress" in step or "ip" in step):
                        hops.append({
                            "ipAddress": step.get("ipAddress") or step.get("ip") or "",
                            "rdns": step.get("rdns") or step.get("hostname") or "",
                        })
            out["results"].append({
                "agent": agent_node,
                "server": server_ip if isinstance(server_ip, str) else server_default,
                "serverIp": server_ip if isinstance(server_ip, str) else "",
                "roundId": run.get("roundId") or run.get("round_id"),
                "responseTime": run.get("responseTime"),
                "numberOfHops": len(hops) if hops else None,
                "hops": hops,
            })
        return out

    # Legacy/alternate shape: results or path_results array with agent, pathTraces, hops
    results = raw.get("results") or raw.get("path_results") or raw.get("pathResults") or []
    if isinstance(results, dict):
        results = results.get("items") or results.get("results") or []
    if not isinstance(results, list):
        return out
    for r in results:
        if not isinstance(r, dict):
            continue
        agent = r.get("agent") or {}
        if isinstance(agent, dict):
            node = {
                "agentId": agent.get("agentId") or agent.get("agent_id"),
                "agentName": agent.get("agentName") or agent.get("agent_name") or "Agent",
                "location": agent.get("location") or "",
            }
        else:
            node = {"agentName": str(agent), "location": ""}
        path_traces = r.get("pathTraces") or r.get("path_traces") or []
        if not isinstance(path_traces, list):
            path_traces = []
        hops = []
        for pt in path_traces[:1]:
            h = pt.get("hops") if isinstance(pt, dict) else None
            if isinstance(h, list):
                hops = h
                break
        out["results"].append({
            "agent": node,
            "server": r.get("server") or r.get("serverIp") or "",
            "serverIp": r.get("serverIp") or r.get("server_ip") or "",
            "roundId": r.get("roundId") or r.get("round_id"),
            "responseTime": (pt or {}).get("responseTime") if path_traces else r.get("responseTime"),
            "numberOfHops": (pt or {}).get("numberOfHops") if path_traces else r.get("numberOfHops"),
            "hops": hops,
        })
    return out


@app.route("/api/path_vis/<test_id>")
def api_path_vis(test_id):
    """Return path visualization for a test via MCP Get Full Path Visualization tool."""
    if not _TEST_ID_RE.match(test_id or ""):
        return jsonify({"error": "Invalid test id"}), 400
    with _cache_lock:
        cached = _path_vis_cache.get(test_id)
        now = time.monotonic()
        if cached and (now - cached["ts"]) < PATH_VIS_CACHE_TTL:
            return jsonify(cached["data"])

    try:
        # MCP tool: "Get Full Path Visualization" – comprehensive path data for all agents and rounds.
        # Tool requires start_date and end_date (ISO format). Use a 15-minute window two periods
        # back so data is reliably available (e.g. at 22:47 use 22:00–22:15, matching MCP behavior).
        now_utc = datetime.now(timezone.utc)
        last_5min = now_utc.replace(
            minute=(now_utc.minute // 5) * 5, second=0, microsecond=0
        )
        end_utc = last_5min - timedelta(minutes=30)   # two 15-min periods back
        start_utc = end_utc - timedelta(minutes=15)
        start_date = start_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_date = end_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
        resp = run_async(call_mcp_tool("get_full_path_visualization", {
            "test_id": test_id,
            "start_date": start_date,
            "end_date": end_date,
        }))
    except Exception as e:
        log.error("path_vis MCP call failed: test_id=%s, window=%s to %s, error=%s", test_id, start_date, end_date, e)
        return jsonify({
            "test_id": test_id,
            "results": [],
            "error": "Path visualization unavailable (MCP tool failed or rate limited). Try again later.",
        })

    # call_mcp_tool returns parsed JSON
    data = _normalize_path_vis_response(resp if isinstance(resp, dict) else {"raw": resp})
    data["test_id"] = test_id
    results = data.get("results") or []
    if not results:
        data["reason"] = (
            "No path trace data for the previous 15-minute window. "
            "Try again after the next test round completes."
        )
        log.error(
            "path_vis no result: test_id=%s, window=%s to %s, result_count=0",
            test_id, start_date, end_date,
        )
    else:
        log.info(
            "path_vis got result: test_id=%s, window=%s to %s, result_count=%s",
            test_id, start_date, end_date, len(results),
        )
    with _cache_lock:
        _path_vis_cache[test_id] = {"data": data, "ts": time.monotonic()}
    return jsonify(data)


@app.route("/api/agent_perf/<test_id>")
def api_agent_perf(test_id):
    if not _TEST_ID_RE.match(test_id or ""):
        return jsonify({"error": "Invalid test id"}), 400
    with _cache_lock:
        base = dict(_base_cache) if _base_cache else {}
        cached = _agent_perf_cache.get(test_id)
        now = time.monotonic()
        if cached and (now - cached["ts"]) < METRICS_TTL_SECONDS:
            return jsonify(cached["data"])

    test_types = base.get("TEST_TYPES", {})
    test_ids = base.get("TEST_IDS", {})
    inv_ids = {v: k for k, v in test_ids.items()}
    test_name = inv_ids.get(test_id, "")
    test_type = test_types.get(test_name, "http-server").lower()

    metric_id = METRIC_FOR_TYPE.get(test_type, "WEB_AVAILABILITY")
    metric_label = METRIC_LABEL_FOR_TYPE.get(test_type, "Availability %")
    higher_is_worse = test_type in ("agent-to-server", "agent-to-agent")

    end = datetime.now(timezone.utc)
    start = end - timedelta(hours=24)
    sf = start.strftime("%Y-%m-%dT%H:%M:%SZ")
    ef = end.strftime("%Y-%m-%dT%H:%M:%SZ")

    try:
        resp = run_async(call_mcp_tool("get_network_app_synthetics_metrics", {
            "metric_id": metric_id,
            "start_date": sf, "end_date": ef,
            "aggregation_type": "MEAN",
            "group_by": "SOURCE_AGENT",
            "filter_dimension": "TEST", "filter_values": [test_id],
        }))
        agents = _parse_agent_csv(resp)
    except Exception as e:
        log.error("agent_perf fetch failed: test_id=%s, error=%s", test_id, e)
        with _cache_lock:
            stale = _agent_perf_cache.get(test_id)
        if stale:
            out = dict(stale["data"])
            out["cached"] = True
            out["cached_message"] = "Rate limited or temporary error; showing last cached data."
            return jsonify(out)
        return jsonify({
            "test_id": test_id,
            "test_name": test_name,
            "test_type": test_type,
            "metric_id": metric_id,
            "metric_label": metric_label,
            "higher_is_worse": higher_is_worse,
            "agents": [],
            "error": "Unable to load per-agent data (rate limited or connection error). Try again in a few minutes.",
        })

    agents.sort(key=lambda a: a["value"], reverse=higher_is_worse)

    payload = {
        "test_id": test_id,
        "test_name": test_name,
        "test_type": test_type,
        "metric_id": metric_id,
        "metric_label": metric_label,
        "higher_is_worse": higher_is_worse,
        "agents": agents,
    }
    with _cache_lock:
        _agent_perf_cache[test_id] = {"data": payload, "ts": time.monotonic()}
    return jsonify(payload)


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    threading.Thread(target=scheduled_refresh, daemon=True).start()
    return jsonify({"status": "refresh started"})


# ---------------------------------------------------------------------------
# Background scheduler and initial load (start once at import so gunicorn/wsgi also get refresh)
# ---------------------------------------------------------------------------

_scheduler = None
_background_tasks_started = False


def start_background_tasks():
    """Start the refresh scheduler and initial data load. Safe to call multiple times (no-op after first)."""
    global _scheduler, _background_tasks_started
    if _background_tasks_started:
        return
    if not MCP_TOKEN:
        log.warning("TE_TOKEN not set; scheduler and initial load not started")
        return
    _background_tasks_started = True
    _scheduler = BackgroundScheduler()
    _scheduler.add_job(scheduled_refresh, "interval", minutes=REFRESH_MINUTES, id="interval_refresh")
    # First refresh 1 minute after start so data updates soon; then every REFRESH_MINUTES
    _scheduler.add_job(
        scheduled_refresh,
        "date",
        run_date=datetime.now(timezone.utc) + timedelta(minutes=1),
        id="first_refresh",
    )
    _scheduler.start()
    log.info("Scheduler started: first refresh in 1 min, then every %d minutes", REFRESH_MINUTES)
    threading.Thread(target=run_initial_load, daemon=True).start()
    log.info("Initial data load thread started (status at GET /api/refresh-status)")


# Start when module is loaded so refresh works with "python server.py", gunicorn, or "flask run"
start_background_tasks()


# ---------------------------------------------------------------------------
# Main (only for direct run; scheduler already started above)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if not MCP_TOKEN:
        log.error("TE_TOKEN environment variable is not set!")
        raise SystemExit(1)

    port = int(os.getenv("PORT", "8000"))
    log.info("Listening on http://0.0.0.0:%s/", port)
    log.info("Log level=%s (set LOG_LEVEL=ERROR|WARNING|INFO|DEBUG|TRACE to change)", _LOG_LEVEL_NAME)
    app.run(host="0.0.0.0", port=port, debug=False)
