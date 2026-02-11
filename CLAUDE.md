# SD Climate Action

San Diego Climate Action Plan progress tracker — solar adoption, permit expediting impact, geographic equity of clean energy, and energy consumption trends.

## Architecture

- **pipeline/**: Download + transform raw data into parquet files
- **api/queries.py**: Shared query layer (DuckDB over parquets) used by both FastAPI and MCP
- **api/main.py**: FastAPI REST endpoints
- **api/mcp_server.py**: FastMCP server for Claude tool use
- **dashboard/app.py**: Streamlit dashboard (6 tabs)

## Data

- Development permits from `seshat.datasd.org` (same source as sd-housing-permits)
- SDG&E energy consumption from `energydata.sdge.com` (quarterly by zip code)
- Processed data lives in `data/processed/` (2 main parquets)
- Aggregated data lives in `data/aggregated/` (9 parquets for dashboard/API)

## Commands

```bash
uv sync                                    # Install deps
uv run climate-build                       # Run full pipeline (download + transform)
uv run climate-build --force               # Re-download everything
uv run uvicorn api.main:app --reload       # Start API server
uv run streamlit run dashboard/app.py      # Start dashboard
```

## Conventions

- All SQL lives in `api/queries.py` — API and MCP are thin wrappers
- Query functions return `list[dict]` or `dict`
- Use `_where()`, `_q()`, `_run()`, `_pq()` helpers for filter composition
- Solar identification: `UPPER(approval_type) LIKE '%PHOTOVOLTAIC%' OR '%PV%' OR '%SOLAR%'`
- Policy eras: Pre-CAP (<2015), CAP Adopted (2015-2017), Expedited Era (2018+)
- San Diego zip codes: 920xx-921xx
