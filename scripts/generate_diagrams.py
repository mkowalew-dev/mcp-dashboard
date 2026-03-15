#!/usr/bin/env python3
"""
Generate docs/diagrams/*.png using Pillow. Run from project root:
  pip install -r requirements-diagrams.txt
  python scripts/generate_diagrams.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

try:
    from PIL import Image, ImageDraw, ImageFont
except ImportError:
    print("Install Pillow: pip install -r requirements-diagrams.txt", file=sys.stderr)
    sys.exit(1)

# Colors (from original SVG theme)
BG = (11, 17, 32)           # #0B1120
BOX_BG = (26, 35, 50)       # #1A2332
BOX_BORDER = (4, 159, 217)  # #049FD9
TEXT = (232, 236, 244)      # #E8ECF4
MUTED = (123, 143, 173)     # #7B8FAD
GREEN = (0, 214, 143)       # #00D68F

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = PROJECT_ROOT / "docs" / "diagrams"


def get_font(size: int = 11):
    """Load a readable font; fall back to default."""
    candidates = []
    if sys.platform == "win32":
        win = Path(os.environ.get("WINDIR", "C:/Windows"))
        candidates.extend([
            win / "Fonts" / "segoeui.ttf",
            win / "Fonts" / "arial.ttf",
            win / "Fonts" / "Arial.ttf",
        ])
    elif sys.platform == "darwin":
        candidates.append(Path("/System/Library/Fonts/Helvetica.ttc"))
    candidates.extend([
        Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"),
        Path("/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf"),
    ])
    for p in candidates:
        try:
            if p.exists():
                return ImageFont.truetype(str(p), size)
        except OSError:
            pass
    return ImageFont.load_default()


def draw_arrow(draw, x1, y1, x2, y2, color=BOX_BORDER, width=2):
    """Draw line and simple arrowhead."""
    draw.line([(x1, y1), (x2, y2)], fill=color, width=width)
    dx, dy = x2 - x1, y2 - y1
    import math
    ang = math.atan2(dy, dx)
    size = 8
    for a in (ang + 2.8, ang - 2.8):
        ax = x2 + size * math.cos(a)
        ay = y2 + size * math.sin(a)
        draw.line([(x2, y2), (ax, ay)], fill=color, width=width)


def draw_box(draw, x, y, w, h, label: str, lines: list[str], font, font_small):
    """Draw rounded rect and text."""
    draw.rectangle([x, y, x + w, y + h], outline=BOX_BORDER, fill=BOX_BG, width=2)
    draw.text((x + w // 2 - len(label) * 3, y + 8), label, fill=TEXT, font=font)
    yy = y + 28
    for line in lines[:8]:
        draw.text((x + 10, yy), line[:40], fill=MUTED, font=font_small)
        yy += 14


def diagram_01_system_overview(draw, font, font_small, w, h):
    """System overview: Browser, Flask, ThousandEyes MCP."""
    draw.rectangle([0, 0, w, h], fill=BG)
    draw.text((w // 2 - 120, 12), "MCP Dashboard - System Overview", fill=TEXT, font=font)
    # Browser
    draw_box(draw, 24, 44, 160, 120, "Browser (Customer)", [
        "noc_dashboard.html", "Leaflet map, KPIs,", "tabs, modals",
        "Poll /api/data every", "refresh_interval_min"
    ], font, font_small)
    # Flask
    draw_box(draw, 220, 44, 240, 200, "Flask Server (server.py)", [
        "Host 0.0.0.0 :8000", "In-memory caches:",
        "base_cache, metrics_cache,", "extra_kpi_cache",
        "APScheduler:", "scheduled_refresh() every", "REFRESH_MINUTES"
    ], font, font_small)
    # MCP
    draw_box(draw, 496, 44, 200, 200, "ThousandEyes MCP", [
        "api.thousandeyes.com/mcp", "Streamable HTTP", "Bearer TE_TOKEN",
        "Tools: list tests,", "agents, alerts, events,", "outages, get_*_metrics"
    ], font, font_small)
    # Arrows
    draw_arrow(draw, 184, 100, 212, 100)
    draw.text((192, 88), "GET /, /api/data", fill=MUTED, font=font_small)
    draw_arrow(draw, 460, 140, 490, 140, GREEN)
    draw.text((465, 128), "MCP tool calls", fill=MUTED, font=font_small)
    # Legend
    draw.rectangle([24, 278, w - 24, h - 12], fill=(17, 24, 39), outline=MUTED)
    draw.text((40, 292), "All data is fetched by the server via MCP; browser only talks to the server.", fill=MUTED, font=font_small)
    draw.text((40, 308), "One scheduler job: base refresh then 24h metrics. POST /api/refresh runs same.", fill=MUTED, font=font_small)


def diagram_02_mcp_flow(draw, font, font_small, w, h):
    """MCP data collection flow."""
    draw.rectangle([0, 0, w, h], fill=BG)
    draw.text((w // 2 - 100, 12), "MCP Data Collection Flow", fill=TEXT, font=font)
    y = 44
    draw.rectangle([20, y, w - 20, y + 118], fill=(17, 24, 39), outline=MUTED)
    draw.text((40, y + 20), "Phase 1 - Base refresh (async, parallel groups)", fill=TEXT, font=font)
    draw.text((40, y + 45), "asyncio.gather(3): list_network_app_synthetics_tests, list_endpoint_agent_tests, get_account_groups", fill=MUTED, font=font_small)
    draw.text((40, y + 65), "asyncio.gather(2): list_cloud_enterprise_agents, list_endpoint_agents", fill=MUTED, font=font_small)
    draw.text((40, y + 85), "asyncio.gather(3): list_alerts (TRIGGER), list_events, search_outages (24h)", fill=MUTED, font=font_small)
    y += 130
    draw.rectangle([20, y, w - 20, y + 198], fill=(17, 24, 39), outline=MUTED)
    draw.text((40, y + 20), "Phase 2 - 24h metrics (batched, parallel per batch)", fill=TEXT, font=font)
    draw.text((40, y + 45), "Availability: WEB_AVAILABILITY + DNS_TRACE_AVAILABILITY in parallel; then NET_LOSS, ONE_WAY_*", fill=MUTED, font=font_small)
    draw.text((40, y + 70), "Extra KPIs: Response (WEB_TTFB, DNS_*); Loss/Jitter/Latency trios; VoIP; BGP, API, Web Txn, Page Load", fill=MUTED, font=font_small)
    draw.text((40, y + 95), "Batches of MCP_BATCH_SIZE test IDs; delay MCP_INTER_BATCH_DELAY_SEC between batches.", fill=MUTED, font=font_small)
    y += 210
    draw.rectangle([20, y, w - 20, y + 68], fill=(17, 24, 39), outline=MUTED)
    draw.text((40, y + 20), "Phase 3 - Endpoint metrics + cache write", fill=TEXT, font=font)
    draw.text((40, y + 42), "asyncio.gather(3): get_endpoint_agent_metrics (LATENCY, WIRELESS_RSSI, LOSS). Write base_cache, metrics_cache, extra_kpi_cache.", fill=MUTED, font=font_small)


def diagram_03_request_flow(draw, font, font_small, w, h):
    """Request flow: user, browser, Flask, caches."""
    draw.rectangle([0, 0, w, h], fill=BG)
    draw.text((w // 2 - 110, 12), "Request Flow - Dashboard Load & Polling", fill=TEXT, font=font)
    draw_box(draw, 24, 44, 100, 44, "User", ["opens URL"], font, font_small)
    draw_box(draw, 24, 108, 100, 100, "Browser", [
        "1. GET /", "2. GET /api/data", "   ?window=24h", "3. Repeat after", "   refresh_interval"
    ], font, font_small)
    draw_box(draw, 148, 44, 280, 200, "Flask (server.py)", [
        "GET / -> send_file(noc_dashboard.html)",
        "GET /api/data -> validate window; read base + metrics + extra_kpi caches",
        "Return JSON with refresh_interval_minutes",
        "Optional: GET /api/fetch_window for on-demand metrics"
    ], font, font_small)
    draw_box(draw, 452, 44, 120, 100, "In-memory", ["base_cache", "metrics_cache", "extra_kpi_cache"], font, font_small)
    draw_arrow(draw, 124, 66, 140, 66)
    draw_arrow(draw, 124, 155, 140, 155)
    draw_arrow(draw, 428, 92, 444, 92, GREEN)
    draw_arrow(draw, 428, 155, 124, 155)
    draw.rectangle([24, 268, w - 24, h - 12], fill=(17, 24, 39), outline=MUTED)
    draw.text((40, 282), "Other: GET /api/health (liveness, base_cache_ready); GET /api/agent_perf/<id>; POST /api/refresh.", fill=MUTED, font=font_small)


def diagram_04_deployment(draw, font, font_small, w, h):
    """Deployment topology."""
    draw.rectangle([0, 0, w, h], fill=BG)
    draw.text((w // 2 - 130, 12), "Deployment - Sales Engineer Install", fill=TEXT, font=font)
    draw.rectangle([24, 42, w - 24, 244], fill=(17, 24, 39), outline=MUTED)
    draw.text((40, 62), "Host (Windows / macOS / Linux). Outbound HTTPS to api.thousandeyes.com required.", fill=MUTED, font=font_small)
    draw_box(draw, 40, 98, 280, 128, "Python 3.10+ venv", [
        ".venv/  |  pip install -r requirements.txt",
        "flask, mcp[cli], python-dotenv, apscheduler, httpx",
        "Run via scripts/start-stop.ps1 or start-stop.sh --startup"
    ], font, font_small)
    draw_box(draw, 340, 98, 256, 128, "Environment / .env", [
        "TE_TOKEN (required)", "REFRESH_MINUTES=15", "PORT=8000",
        "MCP_BATCH_SIZE, MCP_INTER_BATCH_DELAY_SEC"
    ], font, font_small)
    draw.rectangle([40, 250, w - 40, 332], fill=BOX_BG, outline=BOX_BORDER)
    draw.text((w // 2 - 180, 268), "Server: 0.0.0.0:8000 (set PORT to override).", fill=TEXT, font=font)
    draw.text((w // 2 - 200, 290), "Start: scripts/start-stop.ps1 --startup  or  ./scripts/start-stop.sh --startup", fill=MUTED, font=font_small)
    draw.text((w // 2 - 180, 308), "Health: GET http://<host>:8000/api/health", fill=MUTED, font=font_small)


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    font = get_font(14)
    font_small = get_font(10)

    diagrams = [
        ("01-system-overview.png", 720, 380, diagram_01_system_overview),
        ("02-mcp-collection-flow.png", 700, 520, diagram_02_mcp_flow),
        ("03-request-flow.png", 620, 400, diagram_03_request_flow),
        ("04-deployment.png", 640, 360, diagram_04_deployment),
    ]

    for name, width, height, draw_fn in diagrams:
        img = Image.new("RGB", (width, height), BG)
        draw = ImageDraw.Draw(img)
        draw_fn(draw, font, font_small, width, height)
        path = OUT_DIR / name
        img.save(path, "PNG")
        print(f"Wrote {path}")

    print("Done. Refresh docs to view the diagrams.")


if __name__ == "__main__":
    main()
