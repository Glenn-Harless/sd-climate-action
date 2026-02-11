"""MCP server for San Diego climate action data.

Exposes 12 tools that let Claude query climate/energy parquet files directly.
Uses FastMCP (v2) with stdio transport.
"""

from __future__ import annotations

from fastmcp import FastMCP

from api import queries

mcp = FastMCP(
    "San Diego Climate Action",
    instructions=(
        "San Diego Climate Action Plan progress data. Covers solar permit adoption, "
        "permit expediting impact, geographic equity of clean energy, and SDG&E energy "
        "consumption trends. Call get_filter_options first to see available filter values. "
        "Policy eras: Pre-CAP (<2015), CAP Adopted (2015-2017), Expedited Era (2018+). "
        "San Diego adopted its Climate Action Plan in 2015 and began expediting solar "
        "permits around 2017, targeting 100% clean electricity by 2035."
    ),
)


@mcp.tool()
def get_filter_options() -> dict:
    """Get available filter values: years, zip codes, permit categories, and policy eras.

    Call this first to see what values are valid for other tools.
    """
    return queries.get_filter_options()


@mcp.tool()
def get_overview(
    year_min: int = 2015,
    year_max: int | None = None,
) -> dict:
    """Get climate action KPIs: total solar permits, cumulative count, solar % of all permits, median approval days."""
    return queries.get_overview(year_min, year_max)


@mcp.tool()
def get_solar_adoption_curve(
    year_min: int | None = None,
    year_max: int | None = None,
) -> list[dict]:
    """Get annual solar permit counts and cumulative total (the S-curve of adoption).

    Returns year, solar_count, cumulative_solar, total_valuation, median_approval_days.
    """
    return queries.get_solar_adoption_curve(year_min, year_max)


@mcp.tool()
def get_solar_by_zip(
    year_min: int | None = None,
    year_max: int | None = None,
    limit: int = 20,
) -> list[dict]:
    """Get top zip codes ranked by solar permit count.

    Returns zip_code, solar_count, total_valuation. Use limit to control results.
    """
    return queries.get_solar_by_zip(year_min, year_max, limit)


@mcp.tool()
def get_approval_speed(
    year_min: int | None = None,
    year_max: int | None = None,
    permit_category: str | None = None,
) -> list[dict]:
    """Get permit approval timeline metrics (median/avg/p90 days) by category, year, and policy era.

    Permit categories: Solar/PV, Electrical, Mechanical/HVAC, Building, Other.
    Shows whether permit expediting policies actually sped things up.
    """
    return queries.get_approval_speed(year_min, year_max, permit_category)


@mcp.tool()
def get_energy_permit_trends(
    year_min: int | None = None,
    year_max: int | None = None,
) -> list[dict]:
    """Get annual counts for solar, electrical, and mechanical permits.

    Shows the growth trajectory of climate-relevant permit types.
    """
    return queries.get_energy_permit_trends(year_min, year_max)


@mcp.tool()
def get_zip_code_equity(limit: int = 50) -> list[dict]:
    """Get zip code summary with solar adoption rates and permit counts.

    Shows geographic equity of clean energy adoption across San Diego neighborhoods.
    """
    return queries.get_zip_code_equity(limit)


@mcp.tool()
def get_monthly_trends(
    year_min: int | None = None,
    year_max: int | None = None,
    permit_category: str | None = None,
) -> list[dict]:
    """Get monthly permit counts by category for detailed trend analysis."""
    return queries.get_monthly_trends(year_min, year_max, permit_category)


@mcp.tool()
def get_policy_era_comparison() -> list[dict]:
    """Compare solar permit approval speed across policy eras.

    Shows Pre-CAP (<2015), CAP Adopted (2015-2017), and Expedited Era (2018+).
    Key metric: did the expedited permitting policy actually reduce approval times?
    """
    return queries.get_policy_era_comparison()


@mcp.tool()
def get_solar_map_data(
    year_min: int | None = None,
    year_max: int | None = None,
    limit: int = 50000,
) -> list[dict]:
    """Get lat/lng coordinates for solar permits (for map visualization).

    Returns sampled points with year, valuation, zip_code, approval_days, policy_era.
    """
    return queries.get_solar_map_data(year_min, year_max, limit)


@mcp.tool()
def get_energy_consumption(
    year_min: int | None = None,
    year_max: int | None = None,
    zip_code: str | None = None,
) -> list[dict]:
    """Get SDG&E electricity and gas consumption trends (quarterly, citywide).

    Shows whether grid usage is declining with solar growth. Customer classes:
    R=Residential, C=Commercial, A=Agricultural, I=Industrial.
    """
    return queries.get_energy_consumption(year_min, year_max, zip_code)


@mcp.tool()
def get_energy_vs_solar(
    year_min: int | None = None,
    year_max: int | None = None,
    limit: int = 30,
) -> list[dict]:
    """Compare solar permits with energy consumption by zip code.

    Shows whether zip codes with more solar permits have lower grid electricity usage.
    Returns zip_code, solar_count, avg_kwh_per_customer, total_kwh.
    """
    return queries.get_energy_vs_solar(year_min, year_max, limit)


def main():
    mcp.run()


if __name__ == "__main__":
    main()
