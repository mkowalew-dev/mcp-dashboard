# Scripts

## start-stop.ps1 (Windows)

Start or stop the MCP dashboard server in the background.

```powershell
# From project root
.\scripts\start-stop.ps1 --startup    # Checks TE_TOKEN, then starts server.py
.\scripts\start-stop.ps1 --shutdown   # Stops the server
```

**Startup:** Loads `.env` from the project root (if present), verifies `TE_TOKEN` is set, then runs `python server.py` in the background and writes the process ID to `.mcp-dashboard.pid`. Once running, open `http://127.0.0.1:8000/` for the NOC Performance Overview, `http://127.0.0.1:8000/site-health` for the Site Health Overview, or `http://127.0.0.1:8000/executive` for the Executive Dashboard. All dashboards share a sidebar navigation.

**Shutdown:** Reads the PID from `.mcp-dashboard.pid` and terminates that process.

If execution policy blocks the script: `Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned`

---

## start-stop.sh (macOS / Linux)

Same behavior as the PowerShell script.

```bash
# From project root; make executable once: chmod +x scripts/start-stop.sh
./scripts/start-stop.sh --startup
./scripts/start-stop.sh --shutdown
```

Startup logs are appended to `.mcp-dashboard.log` in the project root.

After the server is up, the script waits for the **initial data load** to finish and prints progress (base data → 24h metrics → extra KPIs). The server exposes `GET /api/refresh-status` with `phase`, `message`, `current`/`total`, and `error` so the UI or other tools can show the same loading state.

**If the process runs but port 8000 is not listening (Linux/macOS):**

1. Check the log: `tail -50 .mcp-dashboard.log` — look for errors (e.g. missing `TE_TOKEN`, import errors, or "Address already in use").
2. Confirm the port in use: `ss -tlnp | grep 8000` or `netstat -tlnp | grep 8000`. If you set `PORT` in `.env`, the server listens on that port instead of 8000.
3. Run from project root with the venv activated so dependencies and `server.py` are found:  
   `source .venv/bin/activate` (or your venv path), then `./scripts/start-stop.sh --startup`.
