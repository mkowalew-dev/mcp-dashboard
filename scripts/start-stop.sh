#!/usr/bin/env bash
#
# Start or stop the MCP dashboard server.
#
#   --startup   Loads .env, checks required env vars, then starts server.py in the background.
#   --shutdown  Stops the server process recorded at last startup.
#
# Examples:
#   ./scripts/start-stop.sh --startup
#   ./scripts/start-stop.sh --shutdown
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PID_FILE="$PROJECT_ROOT/.mcp-dashboard.pid"
SERVER_SCRIPT="$PROJECT_ROOT/server.py"
ENV_FILE="$PROJECT_ROOT/.env"

load_dotenv() {
    if [[ -f "$ENV_FILE" ]]; then
        while IFS= read -r line || [[ -n "$line" ]]; do
            line="${line%%#*}"
            line="${line#"${line%%[![:space:]]*}"}"
            line="${line%"${line##*[![:space:]]}"}"
            [[ -z "$line" ]] && continue
            if [[ "$line" == *=* ]]; then
                name="${line%%=*}"
                value="${line#*=}"
                value="${value#\"}"; value="${value%\"}"
                value="${value#\'}"; value="${value%\'}"
                export "$name=$value"
            fi
        done < "$ENV_FILE"
    fi
}

check_required_env() {
    load_dotenv
    if [[ -z "${TE_TOKEN:-}" ]]; then
        echo "Error: TE_TOKEN is not set. Set it in the environment or in a .env file in the project root." >&2
        exit 1
    fi
}

start_dashboard() {
    if [[ ! -f "$SERVER_SCRIPT" ]]; then
        echo "Error: server.py not found at $SERVER_SCRIPT" >&2
        exit 1
    fi
    check_required_env
    if [[ -f "$PID_FILE" ]]; then
        old_pid=$(cat "$PID_FILE")
        if kill -0 "$old_pid" 2>/dev/null; then
            echo "Error: Dashboard may already be running (PID $old_pid). Use --shutdown first, or remove $PID_FILE" >&2
            exit 1
        fi
        rm -f "$PID_FILE"
    fi
    cd "$PROJECT_ROOT"
    # Use python3 if available (common on Linux), else python (e.g. venv)
    PYTHON=""
    if command -v python3 >/dev/null 2>&1; then
        PYTHON="python3"
    else
        PYTHON="python"
    fi
    # Run server in background and disown so we get the real server PID (not nohup's).
    # Redirect output so we can inspect .mcp-dashboard.log if port doesn't show.
    $PYTHON server.py >> .mcp-dashboard.log 2>&1 &
    pid=$!
    disown $pid
    echo $pid > "$PID_FILE"
    port="${PORT:-8000}"
    echo "MCP dashboard started in the background (PID $pid)."
    echo "URL: http://127.0.0.1:${port}/"
    echo "To stop: $0 --shutdown"

    # Wait for server to listen, then track initial load status
    base_url="http://127.0.0.1:${port}"
    wait_sec=0
    max_wait=30
    while [ $wait_sec -lt $max_wait ]; do
        if curl -s -o /dev/null -w "%{http_code}" "$base_url/" 2>/dev/null | grep -q 200; then
            break
        fi
        sleep 1
        wait_sec=$((wait_sec + 1))
    done
    if [ $wait_sec -ge $max_wait ]; then
        echo "Server did not respond within ${max_wait}s. Check: $PROJECT_ROOT/.mcp-dashboard.log"
        return 0
    fi

    echo "Waiting for initial data load..."
    last_msg=""
    while true; do
        status_json=$(curl -s "$base_url/api/refresh-status" 2>/dev/null || echo "{}")
        status_out=$(echo "$status_json" | $PYTHON -c "
import json, sys
try:
    d = json.load(sys.stdin)
    for k in ('phase', 'message', 'current', 'total', 'error'):
        v = d.get(k, '' if k != 'current' and k != 'total' else 0)
        if v is None: v = '' if k != 'current' and k != 'total' else 0
        print(str(v).replace(chr(10), ' '))
except Exception:
    print(''); print(''); print('0'); print('0'); print('')
" 2>/dev/null)
        phase=$(echo "$status_out" | sed -n '1p')
        message=$(echo "$status_out" | sed -n '2p')
        current=$(echo "$status_out" | sed -n '3p')
        total=$(echo "$status_out" | sed -n '4p')
        error=$(echo "$status_out" | sed -n '5p')

        line=""
        if [ -n "$message" ]; then
            if [ -n "$total" ] && [ "$total" -gt 0 ] 2>/dev/null && [ -n "$current" ]; then
                line="  [$phase] $message ($current/$total)"
            else
                line="  [$phase] $message"
            fi
            if [ "$line" != "$last_msg" ]; then
                echo "$line"
                last_msg="$line"
            fi
        fi
        case "$phase" in
            done) echo "Dashboard ready."; break ;;
            error) echo "Initial load failed: ${error:-unknown error}"; break ;;
        esac
        sleep 2
    done
}

stop_dashboard() {
    if [[ ! -f "$PID_FILE" ]]; then
        echo "No PID file found. Dashboard may not be running."
        return 0
    fi
    pid=$(cat "$PID_FILE")
    if kill -0 "$pid" 2>/dev/null; then
        kill "$pid"
        echo "Stopped MCP dashboard (PID $pid)."
    else
        echo "Process $pid was not running."
    fi
    rm -f "$PID_FILE"
}

case "${1:-}" in
    --startup)  start_dashboard ;;
    --shutdown) stop_dashboard ;;
    *)
        echo "Usage: $0 --startup | --shutdown"
        exit 1
        ;;
esac
