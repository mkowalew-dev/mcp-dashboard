# Architecture diagrams

SVG diagrams for the [Architecture Guide](../ARCHITECTURE.md). Open any `.svg` in a browser or use in presentations.

| File | Description |
|------|-------------|
| `01-system-overview.svg` | System overview: browser, Flask server, ThousandEyes MCP. |
| `02-mcp-collection-flow.svg` | MCP data collection: base refresh (parallel), metrics (batched, parallel per batch). |
| `03-request-flow.svg` | Request flow: dashboard load and polling, API usage. |
| `04-deployment.svg` | Deployment: host, venv, env, port, health. |

## Exporting to PNG

- **Browser:** Open the SVG, right‑click → Save image / Print → Save as PDF or use a screenshot tool for PNG.
- **Command line (optional):** If you have Node.js and want batch export, you can use `npx svgexport` or similar; otherwise use the browser method above.
