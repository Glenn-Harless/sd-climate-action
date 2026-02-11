"""FastAPI app — thin wrappers around the shared query layer."""

from __future__ import annotations

from fastapi import FastAPI, Query

from api import queries
from api.models import (
    ApprovalSpeed,
    EnergyConsumption,
    EnergyPermitTrend,
    EnergyVsSolar,
    FilterOptions,
    MonthlyTrend,
    OverviewResponse,
    PolicyEraComparison,
    SolarAdoption,
    SolarByZip,
    SolarMapPoint,
    ZipCodeEquity,
)

app = FastAPI(
    title="San Diego Climate Action API",
    description=(
        "Query San Diego's climate action progress: solar adoption, "
        "permit expediting impact, geographic equity of clean energy, "
        "and energy consumption trends. Data from city development permits "
        "and SDG&E energy consumption reports."
    ),
    version="0.1.0",
)


@app.get("/")
def root():
    return {
        "message": "San Diego Climate Action API",
        "docs": "/docs",
        "endpoints": [
            "/filters", "/overview", "/solar-adoption", "/solar-by-zip",
            "/approval-speed", "/energy-permit-trends", "/zip-equity",
            "/monthly-trends", "/policy-era-comparison", "/solar-map",
            "/energy-consumption", "/energy-vs-solar",
        ],
    }


@app.get("/health")
def health():
    """Debug endpoint — shows data path and file availability."""
    from pathlib import Path
    agg = Path(queries._AGG)
    files = sorted(p.name for p in agg.glob("*.parquet")) if agg.exists() else []
    return {"agg_path": str(agg), "exists": agg.exists(), "files": files}


@app.get("/filters", response_model=FilterOptions)
def filters():
    """Available years, zip codes, permit categories, and policy eras."""
    return queries.get_filter_options()


@app.get("/overview", response_model=OverviewResponse)
def overview(
    year_min: int = Query(2015, description="Start year"),
    year_max: int | None = Query(None, description="End year"),
):
    """KPIs: total solar permits, cumulative, solar %, median approval days."""
    return queries.get_overview(year_min, year_max)


@app.get("/solar-adoption", response_model=list[SolarAdoption])
def solar_adoption(
    year_min: int | None = Query(None, description="Start year"),
    year_max: int | None = Query(None, description="End year"),
):
    """Annual solar permit counts and cumulative S-curve."""
    return queries.get_solar_adoption_curve(year_min, year_max)


@app.get("/solar-by-zip", response_model=list[SolarByZip])
def solar_by_zip(
    year_min: int | None = Query(None, description="Start year"),
    year_max: int | None = Query(None, description="End year"),
    limit: int = Query(20, ge=1, le=200, description="Max zip codes"),
):
    """Top zip codes by solar permit count."""
    return queries.get_solar_by_zip(year_min, year_max, limit)


@app.get("/approval-speed", response_model=list[ApprovalSpeed])
def approval_speed(
    year_min: int | None = Query(None, description="Start year"),
    year_max: int | None = Query(None, description="End year"),
    permit_category: str | None = Query(None, description="Filter by permit category"),
):
    """Permit approval timeline metrics by category, year, and policy era."""
    return queries.get_approval_speed(year_min, year_max, permit_category)


@app.get("/energy-permit-trends", response_model=list[EnergyPermitTrend])
def energy_permit_trends(
    year_min: int | None = Query(None, description="Start year"),
    year_max: int | None = Query(None, description="End year"),
):
    """Annual counts for solar, electrical, and mechanical permits."""
    return queries.get_energy_permit_trends(year_min, year_max)


@app.get("/zip-equity", response_model=list[ZipCodeEquity])
def zip_equity(
    limit: int = Query(50, ge=1, le=200, description="Max zip codes"),
):
    """Zip code summary with solar adoption rates."""
    return queries.get_zip_code_equity(limit)


@app.get("/monthly-trends", response_model=list[MonthlyTrend])
def monthly_trends(
    year_min: int | None = Query(None, description="Start year"),
    year_max: int | None = Query(None, description="End year"),
    permit_category: str | None = Query(None, description="Filter by permit category"),
):
    """Monthly permit counts by category."""
    return queries.get_monthly_trends(year_min, year_max, permit_category)


@app.get("/policy-era-comparison", response_model=list[PolicyEraComparison])
def policy_era_comparison():
    """Compare solar permit speed across policy eras (Pre-CAP, CAP Adopted, Expedited Era)."""
    return queries.get_policy_era_comparison()


@app.get("/solar-map", response_model=list[SolarMapPoint])
def solar_map(
    year_min: int | None = Query(None, description="Start year"),
    year_max: int | None = Query(None, description="End year"),
    limit: int = Query(50000, ge=1, le=200000, description="Max points"),
):
    """Geo points for solar permit map visualization."""
    return queries.get_solar_map_data(year_min, year_max, limit)


@app.get("/energy-consumption", response_model=list[EnergyConsumption])
def energy_consumption(
    year_min: int | None = Query(None, description="Start year"),
    year_max: int | None = Query(None, description="End year"),
    zip_code: str | None = Query(None, description="Filter by zip code"),
):
    """Citywide electricity and gas consumption trends (SDG&E data)."""
    return queries.get_energy_consumption(year_min, year_max, zip_code)


@app.get("/energy-vs-solar", response_model=list[EnergyVsSolar])
def energy_vs_solar(
    year_min: int | None = Query(None, description="Start year"),
    year_max: int | None = Query(None, description="End year"),
    limit: int = Query(30, ge=1, le=200, description="Max zip codes"),
):
    """Solar permits vs energy consumption by zip code (correlation analysis)."""
    return queries.get_energy_vs_solar(year_min, year_max, limit)
