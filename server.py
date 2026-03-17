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
import io
import json
import logging
import os
import random
import re
import threading
import time

import httpx
from collections import defaultdict
from datetime import datetime, timezone, timedelta

from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv
from flask import Flask, jsonify, request, send_file

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
# Suppress Werkzeug's per-request access logs (no incoming API call logging)
logging.getLogger("werkzeug").setLevel(logging.WARNING)

# --- Config (env); no secrets logged ---
MCP_URL = os.getenv("MCP_URL", "https://api.thousandeyes.com/mcp").strip()
MCP_TOKEN = (os.getenv("TE_TOKEN") or "").strip()
REFRESH_MINUTES = max(1, min(120, int(os.getenv("REFRESH_MINUTES", "15"))))
MCP_BATCH_SIZE = max(5, min(50, int(os.getenv("MCP_BATCH_SIZE", "15"))))
# Delay between batch requests; higher reduces 429s (default 1.0s; min 0.5s when set)
_raw_delay = float(os.getenv("MCP_INTER_BATCH_DELAY_SEC", "1.0"))
MCP_INTER_BATCH_DELAY = max(0.5, _raw_delay)
# Max concurrent get_network_app_synthetics_metrics calls (1 = sequential, reduces 429s)
MCP_SYNTH_CONCURRENCY = max(1, min(10, int(os.getenv("MCP_SYNTH_CONCURRENCY", "1"))))
# Global MCP rate limit: max requests per minute (e.g. 240 rpm = 4 req/s); min interval between calls
MCP_MAX_RPM = max(10, min(500, int(os.getenv("MCP_MAX_RPM", "240"))))
_mcp_min_interval_sec = 60.0 / MCP_MAX_RPM
# Per-event-loop primitives (avoids "bound to a different event loop" when asyncio.run() is used from multiple threads)
_mcp_rate_limit_locks: dict[int, asyncio.Lock] = {}
_mcp_last_call_times: dict[int, float] = {}
_synth_semaphores: dict[int, asyncio.Semaphore] = {}


async def _mcp_rate_limit_wait() -> None:
    """Enforce global MCP request rate (MCP_MAX_RPM). Call once before each MCP request."""
    loop = asyncio.get_running_loop()
    lid = id(loop)
    if lid not in _mcp_rate_limit_locks:
        _mcp_rate_limit_locks[lid] = asyncio.Lock()
        _mcp_last_call_times[lid] = 0.0
    async with _mcp_rate_limit_locks[lid]:
        now = time.monotonic()
        wait = max(0.0, _mcp_min_interval_sec - (now - _mcp_last_call_times[lid]))
        if wait > 0:
            await asyncio.sleep(wait)
        _mcp_last_call_times[lid] = time.monotonic()


async def _get_synth_sem() -> asyncio.Semaphore:
    """Return semaphore for current event loop (created on first use in that loop)."""
    loop = asyncio.get_running_loop()
    lid = id(loop)
    if lid not in _synth_semaphores:
        _synth_semaphores[lid] = asyncio.Semaphore(MCP_SYNTH_CONCURRENCY)
    return _synth_semaphores[lid]

# Cache window metrics at least one refresh cycle (capped)
METRICS_TTL_SECONDS = max(300, min(3600, REFRESH_MINUTES * 60))

WINDOWS = {
    "1h": 1,
    "6h": 6,
    "12h": 12,
    "24h": 24,
    "2d": 48,
    "7d": 168,
}

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


async def call_mcp_tool(tool_name: str, arguments: dict | None = None, retries: int = 4):
    from mcp.client.streamable_http import streamablehttp_client
    from mcp import ClientSession

    async def _do() -> dict | list:
        headers = {"Authorization": f"Bearer {MCP_TOKEN}"}
        last_err = None
        for attempt in range(retries):
            await _mcp_rate_limit_wait()
            try:
                async with streamablehttp_client(url=MCP_URL, headers=headers) as (read, write, _):
                    async with ClientSession(read, write) as session:
                        await session.initialize()
                        result = await session.call_tool(tool_name, arguments or {})
                        if getattr(result, "isError", False):
                            texts = [c.text for c in result.content if hasattr(c, "text")]
                            err_text = "\n".join(texts)
                            if "429" in err_text or "Too Many Requests" in err_text:
                                wait = min(60, 2 ** (attempt + 2))
                                jitter = random.uniform(0, min(5, wait * 0.2))
                                log.error(
                                    "MCP 429 rate limited: tool=%s, attempt=%d/%d, retry_in=%.1fs",
                                    tool_name, attempt + 1, retries, wait + jitter,
                                )
                                await asyncio.sleep(wait + jitter)
                                continue
                            log.error("MCP tool error: tool=%s, error=%s", tool_name, err_text[:500])
                            raise RuntimeError(f"MCP tool error: {err_text[:200]}")
                        texts = [c.text for c in result.content if hasattr(c, "text")]
                        combined = "\n".join(texts)
                        try:
                            return json.loads(combined)
                        except json.JSONDecodeError:
                            return {"raw": combined}
            except RuntimeError:
                raise
            except Exception as e:
                root = _unwrap_exception(e)
                last_err = root
                err_str = (str(root) or repr(root) or type(root).__name__).strip()
                err_str = err_str or type(root).__name__
                if isinstance(root, httpx.HTTPStatusError) and getattr(root, "response", None):
                    if getattr(root.response, "status_code", None) == 503:
                        log.warning(
                            "ThousandEyes API returned 503 Service Unavailable; will retry (attempt %d/%d).",
                            attempt + 1, retries,
                        )
                elif type(root).__name__ == "BrokenResourceError":
                    log.warning(
                        "MCP connection broken (stream closed); will retry with fresh connection (attempt %d/%d).",
                        attempt + 1, retries,
                    )
                is_429 = (
                    "429" in str(e) or "Too Many Requests" in str(e)
                    or "429" in str(root) or "Too Many Requests" in str(root)
                )
                if is_429:
                    wait = min(60, 2 ** (attempt + 2))
                    jitter = random.uniform(0, min(5, wait * 0.2))
                    log.error(
                        "MCP 429 rate limited: tool=%s, attempt=%d/%d, retry_in=%.1fs",
                        tool_name, attempt + 1, retries, wait + jitter,
                    )
                else:
                    wait = min(30, 2 ** (attempt + 2))
                    jitter = random.uniform(0, min(3, wait * 0.2))
                    log.error(
                        "MCP transient error: tool=%s, attempt=%d/%d, retry_in=%.1fs, error=%s",
                        tool_name, attempt + 1, retries, wait + jitter, err_str,
                    )
                await asyncio.sleep(wait + jitter)
                continue
        root = _unwrap_exception(last_err) if last_err else last_err
        err_str = (str(root) or repr(root) or type(root).__name__).strip() or type(root).__name__
        log.error("MCP failed after %d retries: tool=%s, last_error=%s", retries, tool_name, err_str)
        raise RuntimeError(f"Failed after {retries} retries on {tool_name}: {err_str}")

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
                    dt = datetime.fromtimestamp(start_time, tz=timezone.utc)
                else:
                    dt = datetime.fromisoformat(str(start_time).replace("Z", "+00:00"))
                time_str = dt.strftime("%H:%M")
                time_iso = dt.isoformat().replace("+00:00", "Z")
            except (ValueError, TypeError, OSError):
                time_str = str(start_time)[:5]
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
        message=f"Fetching 24h availability ({len(synth_ids)} tests, {num_batches} batches)...",
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

    _set_refresh_status(message=f"24h availability loaded ({len(test_availability)} tests)")
    log.info("Metrics for %dh: %d tests with availability", hours, len(test_availability))
    return test_availability


def get_or_fetch_metrics(window_key: str) -> dict[str, float] | None:
    hours = WINDOWS.get(window_key)
    if hours is None:
        return None

    with _cache_lock:
        entry = _metrics_cache.get(window_key)
        if entry and (time.time() - entry["ts"]) < METRICS_TTL_SECONDS:
            return entry["data"]

    try:
        metrics = asyncio.run(fetch_metrics_async(hours))
    except Exception as e:
        log.error("Metrics fetch failed: window=%s, error=%s", window_key, e)
        metrics = {}

    if metrics:
        with _cache_lock:
            _metrics_cache[window_key] = {"data": metrics, "ts": time.time()}
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
    delay = MCP_INTER_BATCH_DELAY
    _set_refresh_status(
        phase="extra_kpis",
        message=f"Fetching extra KPIs (response, loss, jitter, latency) for {len(synth_ids)} tests...",
    )
    extra = {"avg_response_ms": None, "p95_response_ms": None,
             "avg_packet_loss": None, "loss_affected_paths": 0}

    resp_by_test = {}
    resp_metric_order = ["WEB_TTFB", "DNS_SERVER_TIME", "DNS_TRACE_QUERY_TIME"]
    for i in range(0, len(synth_ids), batch_size):
        batch = synth_ids[i : i + batch_size]
        resps = await _mcp_synth_metrics_batch(resp_metric_order, batch, start, end)
        merged = _merge_parsed_first_wins(resps)
        resp_by_test.update({k: v for k, v in merged.items() if k not in resp_by_test})
        await asyncio.sleep(delay + random.uniform(0, 0.4))

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
    loss_metric_order = ["NET_LOSS", "ONE_WAY_NET_LOSS_BIDIRECTIONAL", "ONE_WAY_NET_LOSS_TO_TARGET"]
    for i in range(0, len(synth_ids), batch_size):
        batch = synth_ids[i : i + batch_size]
        resps = await _mcp_synth_metrics_batch(loss_metric_order, batch, start, end)
        merged = _merge_parsed_first_wins(resps)
        loss_by_test.update({k: v for k, v in merged.items() if k not in loss_by_test})
        await asyncio.sleep(delay + random.uniform(0, 0.4))

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

    # --- Jitter by test ---
    jitter_by_test = {}
    jitter_order = ["NET_JITTER", "ONE_WAY_NET_JITTER_BIDIRECTIONAL", "ONE_WAY_NET_JITTER_TO_TARGET"]
    for i in range(0, len(synth_ids), batch_size):
        batch = synth_ids[i : i + batch_size]
        resps = await _mcp_synth_metrics_batch(jitter_order, batch, start, end)
        merged = _merge_parsed_first_wins(resps)
        jitter_by_test.update({k: v for k, v in merged.items() if k not in jitter_by_test})
        await asyncio.sleep(delay + random.uniform(0, 0.4))

    if jitter_by_test:
        jitter_detail = []
        for test_name, j_val in jitter_by_test.items():
            numeric_id = test_ids_by_name.get(test_name, "")
            jitter_detail.append({"test": test_name, "id": numeric_id, "jitter": round(j_val, 2)})
        jitter_detail.sort(key=lambda x: x["jitter"], reverse=True)
        extra["jitter_by_test"] = jitter_detail

    latency_by_test = {}
    lat_order = ["NET_LATENCY", "ONE_WAY_NET_LATENCY_BIDIRECTIONAL", "ONE_WAY_NET_LATENCY_TO_TARGET"]
    for i in range(0, len(synth_ids), batch_size):
        batch = synth_ids[i : i + batch_size]
        resps = await _mcp_synth_metrics_batch(lat_order, batch, start, end)
        merged = _merge_parsed_first_wins(resps)
        latency_by_test.update({k: v for k, v in merged.items() if k not in latency_by_test})
        await asyncio.sleep(delay + random.uniform(0, 0.4))

    if latency_by_test:
        lat_detail = []
        for test_name, lat_val in latency_by_test.items():
            numeric_id = test_ids_by_name.get(test_name, "")
            lat_detail.append({"test": test_name, "id": numeric_id, "latency": round(lat_val, 2)})
        lat_detail.sort(key=lambda x: x["latency"], reverse=True)
        extra["latency_by_test"] = lat_detail

    # --- VoIP / RTP metrics by test (MOS, latency, loss, PDV) ---
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
            await asyncio.sleep(delay + random.uniform(0, 0.4))
        if by_test:
            detail = []
            for test_name, val in by_test.items():
                numeric_id_v = test_ids_by_name.get(test_name, "")
                detail.append({"test": test_name, "id": numeric_id_v, "value": round(val, 2)})
            detail.sort(key=lambda x: x["value"], reverse=(metric_id != "VOIP_MOS"))
            extra[extra_key] = detail

    # --- Additional per-type KPI metrics (BGP, API, Transaction, Page Load) ---
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
            await asyncio.sleep(delay + random.uniform(0, 0.4))
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

    def _parse_ep_csv(resp):
        if not resp or not isinstance(resp, dict):
            return {}
        csv_text = resp.get("csv", "")
        agg_map = resp.get("names", {}).get("aggregatesMap", {})
        machine_names = agg_map.get("EYEBROW_MACHINE_ID", {})
        mid_col = "EYEBROW_MACHINE_ID"
        header = csv_text.split("\n", 1)[0] if csv_text else ""
        if mid_col not in header:
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

    with _cache_lock:
        ep_agents = list(_base_cache.get("ENDPOINT_AGENTS") or [])

    all_names = set()
    for m in (latency_by_name, rssi_by_name, gateway_latency_by_name, gateway_loss_by_name, cpu_by_name, memory_by_name):
        all_names |= set(m.keys())
    ep_perf = []
    seen_names = set()
    for comp_name in all_names:
        lat_val = latency_by_name.get(comp_name)
        rssi_val = rssi_by_name.get(comp_name)
        gw_lat = gateway_latency_by_name.get(comp_name)
        gw_loss = gateway_loss_by_name.get(comp_name)
        cpu_val = cpu_by_name.get(comp_name)
        mem_val = memory_by_name.get(comp_name)
        loc = ""
        agent_id = ""
        device = ""
        display_name = comp_name
        for ep in ep_agents:
            ep_name = ep.get("name", "")
            if ep_name == comp_name or ep_name.split(".")[0] == comp_name:
                loc = ep.get("loc", "")
                agent_id = ep.get("id", "")
                device = ep.get("device", "")
                display_name = ep_name
                break
        if display_name not in seen_names:
            seen_names.add(display_name)
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
        if entry and (time.time() - entry["ts"]) < METRICS_TTL_SECONDS:
            return entry["data"]

    try:
        data = asyncio.run(fetch_extra_kpis_async(hours))
    except Exception as e:
        log.error("Extra KPIs fetch failed: window=%s, error=%s", window_key, e)
        data = {}

    has_data = data and (data.get("avg_response_ms") is not None or data.get("avg_packet_loss") is not None)
    if has_data:
        with _cache_lock:
            _extra_kpi_cache[window_key] = {"data": data, "ts": time.time()}
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
    asyncio.run(refresh_base_data_async())


def refresh_default_metrics():
    get_or_fetch_metrics("24h")
    get_or_fetch_extra_kpis("24h")


def scheduled_refresh():
    """One scheduler tick: base first (defines test IDs), then default-window metrics."""
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


def run_initial_load():
    """Run initial data load in background; updates _refresh_status for /api/refresh-status."""
    start = time.perf_counter()
    try:
        _set_refresh_status(phase="base", message="Fetching base data (tests, agents, alerts)...")
        refresh_base()
        _set_refresh_status(phase="metrics", message="Fetching 24h metrics...")
        get_or_fetch_metrics("24h")
        _set_refresh_status(phase="extra_kpis", message="Fetching extra KPIs...")
        get_or_fetch_extra_kpis("24h")
        _set_refresh_status(phase="done", message="Ready")
        elapsed = time.perf_counter() - start
        log.info("Initial data load complete in %.2fs", elapsed)
    except Exception as e:
        elapsed = time.perf_counter() - start
        log.exception("Initial load failed after %.2fs: %s", elapsed, e)
        _set_refresh_status(error=str(e))


# ---------------------------------------------------------------------------
# Flask routes
# ---------------------------------------------------------------------------

_TEST_ID_RE = re.compile(r"^(\d{1,20}|ep-[\w-]{1,64})$")


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
    })


@app.route("/api/refresh-status")
def api_refresh_status():
    """Current status of the initial/background data load for startup script and UI."""
    with _refresh_status_lock:
        out = dict(_refresh_status)
    return jsonify(out)


@app.route("/api/data")
def api_data():
    window = request.args.get("window", "24h")
    if window not in WINDOWS:
        window = "24h"

    with _cache_lock:
        if not _base_cache:
            return jsonify({"error": "Data not yet loaded. Please wait..."}), 503
        base = dict(_base_cache)

    with _cache_lock:
        m_entry = _metrics_cache.get(window) or _metrics_cache.get("24h")
        e_entry = _extra_kpi_cache.get(window) or _extra_kpi_cache.get("24h")

    metrics = m_entry["data"] if m_entry else {}
    extra_kpis = e_entry["data"] if e_entry else {}

    base["TEST_AVAILABILITY"] = metrics
    base["EXTRA_KPI"] = extra_kpis
    base["window"] = window
    base["metrics_ready"] = bool(metrics)
    base["refresh_interval_minutes"] = REFRESH_MINUTES
    base["metrics_ttl_seconds"] = METRICS_TTL_SECONDS
    base["business_services_config"] = _business_services_config
    return jsonify(base)


@app.route("/api/fetch_window")
def api_fetch_window():
    """Synchronously fetch metrics for a specific window. Blocks until done."""
    window = request.args.get("window", "24h")
    if window not in WINDOWS:
        window = "24h"
    hours = WINDOWS[window]

    with _cache_lock:
        m_entry = _metrics_cache.get(window)
        e_entry = _extra_kpi_cache.get(window)
    if (m_entry and (time.time() - m_entry["ts"]) < METRICS_TTL_SECONDS and
            e_entry and (time.time() - e_entry["ts"]) < METRICS_TTL_SECONDS):
        return jsonify({"status": "cached", "window": window})

    log.info("Fetching metrics on demand for window %s (%dh)...", window, hours)
    try:
        metrics = asyncio.run(fetch_metrics_async(hours))
        if metrics:
            with _cache_lock:
                _metrics_cache[window] = {"data": metrics, "ts": time.time()}
    except Exception as e:
        log.error("On-demand metrics fetch failed: window=%s, error=%s", window, e)

    try:
        extra = asyncio.run(fetch_extra_kpis_async(hours))
        has_data = extra and (extra.get("avg_response_ms") is not None or extra.get("avg_packet_loss") is not None)
        if has_data:
            with _cache_lock:
                _extra_kpi_cache[window] = {"data": extra, "ts": time.time()}
    except Exception as e:
        log.error("On-demand extra KPIs fetch failed: window=%s, error=%s", window, e)

    return jsonify({"status": "fetched", "window": window})


METRIC_FOR_TYPE = {
    "agent-to-server": "NET_LOSS",
    "agent-to-agent": "NET_LOSS",
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
            # pathTraces is list of traces; each trace is list of [nodeIndex, x, y] (or similar)
            if path_traces:
                first_trace = path_traces[0] if isinstance(path_traces[0], list) else []
                for step in first_trace if isinstance(first_trace, list) else []:
                    if isinstance(step, (list, tuple)) and len(step) >= 1:
                        idx = step[0]
                        if isinstance(idx, int) and 0 <= idx < len(nodes_list):
                            node_str = nodes_list[idx]
                            if isinstance(node_str, str):
                                hops.append(_parse_path_vis_node(node_str))
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
        now = time.time()
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
        resp = asyncio.run(call_mcp_tool("get_full_path_visualization", {
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
        log.error(
            "path_vis got result: test_id=%s, window=%s to %s, result_count=%s",
            test_id, start_date, end_date, len(results),
        )
    with _cache_lock:
        _path_vis_cache[test_id] = {"data": data, "ts": time.time()}
    return jsonify(data)


@app.route("/api/agent_perf/<test_id>")
def api_agent_perf(test_id):
    if not _TEST_ID_RE.match(test_id or ""):
        return jsonify({"error": "Invalid test id"}), 400
    with _cache_lock:
        base = dict(_base_cache) if _base_cache else {}
        cached = _agent_perf_cache.get(test_id)
        now = time.time()
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
        resp = asyncio.run(call_mcp_tool("get_network_app_synthetics_metrics", {
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
        _agent_perf_cache[test_id] = {"data": payload, "ts": time.time()}
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
