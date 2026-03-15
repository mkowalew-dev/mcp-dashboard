# Architecture diagrams

PNG diagrams for the [Architecture Guide](../ARCHITECTURE.md). They render correctly on GitHub and in most doc viewers.

| File | Description |
|------|-------------|
| `01-system-overview.png` | System overview: browser, Flask server, ThousandEyes MCP. |
| `02-mcp-collection-flow.png` | MCP data collection: base refresh (parallel), metrics (batched, parallel per batch). |
| `03-request-flow.png` | Request flow: dashboard load and polling, API usage. |
| `04-deployment.png` | Deployment: host, venv, env, port, health. |

## Regenerating the PNGs

Diagrams are generated with Python and Pillow. From the project root:

```bash
pip install -r requirements-diagrams.txt
python scripts/generate_diagrams.py
```

This overwrites the four PNGs in `docs/diagrams/`.
