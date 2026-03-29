# mcp-dashboard

NOC-style dashboard that pulls live ThousandEyes data over the **Model Context Protocol (MCP)** and serves a single-page UI. Includes a **Site Health Overview** for per-site monitoring and an **Executive Dashboard** for network & application assurance.

**Sales engineers:** See **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)** for the technical architecture guide and diagrams (system overview, MCP collection flow, request flow, deployment).

## What `server.py` does (capabilities)

| Area | Behavior |
|------|----------|
| **Backend** | Flask app on `0.0.0.0:8000` (override with `PORT`); serves `noc_dashboard.html` at `/`, `site_health.html` at `/site-health`, and `executive.html` at `/executive`. |
| **Data source** | ThousandEyes MCP over HTTPS (`https://api.thousandeyes.com/mcp`) using a Bearer token. |
| **MCP tools used** | Lists synthetics tests, endpoint tests, account groups, cloud/enterprise/endpoint agents; triggered alerts; events; outages; batch synthetics metrics (availability, loss, TTFB, VoIP, BGP/API/page metrics); per-test agent breakdown; endpoint agent metrics (latency, Wi‑Fi RSSI, loss). |
| **Caching** | In-memory base + per-window metrics; TTL defaults to one refresh cycle (min 5m, max 60m). |
| **Refresh** | Single scheduled job: **base MCP pull first**, then **24h metrics** (avoids racing on stale test IDs). Same interval for UI (`refresh_interval_minutes` in `/api/data`). |
| **API** | `GET /api/data?window=…`, `GET /api/health`, `GET /api/refresh-status`, `GET /api/fetch_window`, `GET /api/agent_perf/<test_id>` (validated id), `POST /api/refresh`. |
| **Dashboards** | **NOC Performance Overview** (`/`) — global view of all tests, agents, alerts, events, outages. **Site Health Overview** (`/site-health`) — per-site view for each enterprise agent location; tests are grouped into the same **business services** as the NOC (name patterns and test-type rules from `business_services_config` in the API, overridable via env). **Executive Dashboard** (`/executive`) — C-suite network & application assurance view with assurance score gauge, business service health grid, top issues, alert/event feed, and infrastructure coverage. |

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
| **Files** | `server.py`, `noc_dashboard.html`, `site_health.html`, `executive.html`, and dependencies from `requirements.txt` in the project root. |

### Environment variables

| Variable | Required | Default | Purpose |
|----------|----------|---------|---------|
| `TE_TOKEN` | **Yes** | — | Bearer token for ThousandEyes MCP. |
| `REFRESH_MINUTES` | No | `15` | Scheduler interval (1–120); front-end polls on the same cadence via API. |
| `MCP_URL` | No | ThousandEyes MCP URL | Override only if directed by Cisco. |
| `MCP_BATCH_SIZE` | No | `20` | Test IDs per synthetics metrics call (5–50). |
| `MCP_INTER_BATCH_DELAY_SEC` | No | `0.35` | Pause between batch rounds to limit API pressure. |
| `PORT` | No | `8000` | HTTP port the server listens on. |
| `BUSINESS_SERVICES_CONFIG` | No | Built-in default | Optional JSON string: `type_rules` and `services` (display name, icon, name substring `patterns`) used to map tests to business services. |
| `BUSINESS_SERVICES_CONFIG_FILE` | No | — | Path to a JSON file with the same schema (preferred for larger configs). Applies to the NOC, Site Health, and Executive dashboards via `GET /api/data`. See `config/business_services.example.json`. |

Optional: copy `.env` in the project root (loaded by `python-dotenv`); keep `.env` out of version control.

---

## Deploy with a Python virtual environment (venv)

Configure the environment **before** starting the server. Either set variables in your shell or use a `.env` file in the project root (see below). The startup script checks that `TE_TOKEN` is set and loads `.env` when you run it.

### 1. Create venv and install dependencies

**Windows (PowerShell):**

```powershell
cd path\to\mcp-dashboard

python -m venv .venv
.\.venv\Scripts\Activate.ps1
# If execution policy blocks: Set-ExecutionPolicy -Scope CurrentUser RemoteSigned

python -m pip install --upgrade pip
pip install -r requirements.txt
```

**macOS / Linux:**

```bash
cd /path/to/mcp-dashboard

python3 -m venv .venv
source .venv/bin/activate

python -m pip install --upgrade pip
pip install -r requirements.txt
```

### 2. Configure environment (before startup)

Set **TE_TOKEN** (required) and any optional variables. Do this **before** running the startup script.

**Option A — `.env` file (recommended):** Copy `.env.example` to `.env` in the project root, edit it, and do not commit it. The startup script loads `.env` automatically.

```env
TE_TOKEN=your-thousandeyes-token
REFRESH_MINUTES=15
```

**Option B — Shell:**

- **PowerShell:** `$env:TE_TOKEN = "your-token"` (and optionally `$env:REFRESH_MINUTES = "15"`, `$env:PORT = "8000"`).
- **Bash:** `export TE_TOKEN="your-token"` (and optionally `export REFRESH_MINUTES=15`, `export PORT=8000`).

### 3. Start and stop the server

From the project root, with the venv activated and environment configured:

**Windows:**

```powershell
.\scripts\start-stop.ps1 --startup
```

**macOS / Linux:**

```bash
./scripts/start-stop.sh --startup
```

The script checks that `TE_TOKEN` is set, then starts the server in the background. Open **http://127.0.0.1:8000/** for the NOC dashboard, **http://127.0.0.1:8000/site-health** for the Site Health Overview, or **http://127.0.0.1:8000/executive** for the Executive Dashboard (or `http://<host>:8000/` from another machine; port is from `PORT` or 8000). All dashboards share a sidebar navigation.

To stop the server:

**Windows:** `.\scripts\start-stop.ps1 --shutdown`  
**macOS / Linux:** `./scripts/start-stop.sh --shutdown`

See **scripts/README.md** for more detail.

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
- `noc_dashboard.html` — NOC Performance Overview dashboard
- `site_health.html` — Site Health Overview (per-site tests grouped by business service, same definitions as the NOC)
- `executive.html` — Executive Dashboard for network & application assurance
- `requirements.txt` — Python packages  
