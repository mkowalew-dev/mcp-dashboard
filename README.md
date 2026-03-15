# mcp-dashboard

NOC-style dashboard that pulls live ThousandEyes data over the **Model Context Protocol (MCP)** and serves a single-page UI.

**Sales engineers:** See **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)** for the technical architecture guide and diagrams (system overview, MCP collection flow, request flow, deployment).

## What `server.py` does (capabilities)

| Area | Behavior |
|------|----------|
| **Backend** | Flask app on `0.0.0.0:8000` (override with `PORT`); serves `noc_dashboard.html` at `/`. |
| **Data source** | ThousandEyes MCP over HTTPS (`https://api.thousandeyes.com/mcp`) using a Bearer token. |
| **MCP tools used** | Lists synthetics tests, endpoint tests, account groups, cloud/enterprise/endpoint agents; triggered alerts; events; outages; batch synthetics metrics (availability, loss, TTFB, VoIP, BGP/API/page metrics); per-test agent breakdown; endpoint agent metrics (latency, Wi‑Fi RSSI, loss). |
| **Caching** | In-memory base + per-window metrics; TTL defaults to one refresh cycle (min 5m, max 60m). |
| **Refresh** | Single scheduled job: **base MCP pull first**, then **24h metrics** (avoids racing on stale test IDs). Same interval for UI (`refresh_interval_minutes` in `/api/data`). |
| **API** | `GET /api/data?window=…`, `GET /api/health`, `GET /api/fetch_window`, `GET /api/agent_perf/<test_id>` (validated id), `POST /api/refresh`. |

### MCP collection (design)

- **Batched filters** — Synthetics metrics use `MCP_BATCH_SIZE` test IDs per call (default 20).
- **Parallel per batch** — Within a batch, complementary metrics (e.g. availability + DNS trace) run in parallel; first-wins merge order stays deterministic.
- **Throttle** — `MCP_INTER_BATCH_DELAY_SEC` between batches to reduce 429s; retries with backoff on rate limit.
- **Scheduler** — One interval job runs base refresh then metrics so the UI and cache stay aligned.

## Requirements

| Requirement | Details |
|-------------|---------|
| **Python** | **3.10+** recommended (code uses `dict \| None` style unions). |
| **Network** | Outbound HTTPS to `api.thousandeyes.com` for MCP. |
| **Credentials** | ThousandEyes API token with MCP access, supplied only via environment (see below). **Do not commit tokens.** |
| **Files** | `server.py`, `noc_dashboard.html`, and dependencies from `requirements.txt` in the project root. |

### Environment variables

| Variable | Required | Default | Purpose |
|----------|----------|---------|---------|
| `TE_TOKEN` | **Yes** | — | Bearer token for ThousandEyes MCP. |
| `REFRESH_MINUTES` | No | `15` | Scheduler interval (1–120); front-end polls on the same cadence via API. |
| `MCP_URL` | No | ThousandEyes MCP URL | Override only if directed by Cisco. |
| `MCP_BATCH_SIZE` | No | `20` | Test IDs per synthetics metrics call (5–50). |
| `MCP_INTER_BATCH_DELAY_SEC` | No | `0.35` | Pause between batch rounds to limit API pressure. |
| `PORT` | No | `8000` | HTTP port the server listens on. |

Optional: copy `.env` in the project root (loaded by `python-dotenv`); keep `.env` out of version control.

---

## Deploy with a Python virtual environment (venv)

### Windows (PowerShell)

```powershell
cd path\to\mcp-dashboard

# Create venv
python -m venv .venv

# Activate (PowerShell)
.\.venv\Scripts\Activate.ps1

# If execution policy blocks activation:
# Set-ExecutionPolicy -Scope CurrentUser RemoteSigned

# Install dependencies (use ci-style install when you have a lockfile; here pip install is fine)
python -m pip install --upgrade pip
pip install -r requirements.txt

# Token: set for this session or use a .env file (not committed)
$env:TE_TOKEN = "your-thousandeyes-token"
# optional:
$env:REFRESH_MINUTES = "15"

python server.py
```

Open **http://127.0.0.1:8000/** (or from another machine: `http://<host>:8000/`).

Deactivate when done: `deactivate`.

### macOS / Linux

```bash
cd /path/to/mcp-dashboard

python3 -m venv .venv
source .venv/bin/activate

python -m pip install --upgrade pip
pip install -r requirements.txt

export TE_TOKEN="your-thousandeyes-token"
# optional:
export REFRESH_MINUTES=15

python server.py
```

### `.env` example (local only; do not commit)

```env
TE_TOKEN=your-thousandeyes-token
REFRESH_MINUTES=15
```

### Production notes

- Run behind a reverse proxy (TLS termination, access control) if exposed beyond a lab; the app is **not** hardened as a public multi-tenant service.
- Prefer process managers (systemd, supervisord, Windows Service) with the venv’s `python` and env loaded from a secret store—not from the shell history.

---

## Dependencies (`requirements.txt`)

- **flask** — HTTP server and JSON API  
- **mcp[cli]** — MCP client (streamable HTTP to ThousandEyes)  
- **python-dotenv** — optional `.env` loading  
- **apscheduler** — scheduled refresh jobs  
- **httpx** — HTTP stack used by the MCP client  

---

## Repository layout (typical)

- `server.py` — Flask + MCP aggregation  
- `noc_dashboard.html` — dashboard UI  
- `requirements.txt` — Python packages  
