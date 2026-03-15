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
    nohup python server.py > .mcp-dashboard.log 2>&1 &
    echo $! > "$PID_FILE"
    port="${PORT:-8000}"
    echo "MCP dashboard started in the background (PID $(cat "$PID_FILE"))."
    echo "URL: http://127.0.0.1:${port}/"
    echo "To stop: $0 --shutdown"
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
