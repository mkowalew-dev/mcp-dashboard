# Scripts

## start-stop.ps1 (Windows)

Start or stop the MCP dashboard server in the background.

```powershell
# From project root
.\scripts\start-stop.ps1 --startup    # Checks TE_TOKEN, then starts server.py
.\scripts\start-stop.ps1 --shutdown   # Stops the server
```

**Startup:** Loads `.env` from the project root (if present), verifies `TE_TOKEN` is set, then runs `python server.py` in the background and writes the process ID to `.mcp-dashboard.pid`.

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
