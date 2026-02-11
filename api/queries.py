"""Shared query layer — all SQL lives here.

Both FastAPI endpoints and MCP tools call these functions.
Each function queries pre-aggregated parquet files via DuckDB
and returns list[dict] or dict.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

_ROOT = Path(__file__).resolve().parent.parent
_AGG = str(_ROOT / "data" / "aggregated")
_PROCESSED = str(_ROOT / "data" / "processed")


def _pq(name: str) -> str:
    """Return full path to an aggregated parquet file."""
    return f"{_AGG}/{name}.parquet"


def _q(where: str, condition: str) -> str:
    """Append a condition to a WHERE clause."""
    if not where:
        return f"WHERE {condition}"
    return f"{where} AND {condition}"


def _where(
    year_min: int | None = None,
    year_max: int | None = None,
    zip_code: str | None = None,
    permit_category: str | None = None,
    policy_era: str | None = None,
    *,
    has_zip: bool = True,
    has_category: bool = True,
    has_era: bool = True,
) -> str:
    """Build a WHERE clause from optional filter params."""
    clauses: list[str] = []
    if year_min is not None:
        clauses.append(f"year >= {int(year_min)}")
    if year_max is not None:
        clauses.append(f"year <= {int(year_max)}")
    if zip_code and has_zip:
        clauses.append(f"zip_code = '{zip_code.replace(chr(39), chr(39)*2)}'")
    if permit_category and has_category:
        clauses.append(f"permit_category = '{permit_category.replace(chr(39), chr(39)*2)}'")
    if policy_era and has_era:
        clauses.append(f"policy_era = '{policy_era.replace(chr(39), chr(39)*2)}'")
    return ("WHERE " + " AND ".join(clauses)) if clauses else ""


def _run(sql: str) -> list[dict]:
    """Execute SQL and return list of row dicts."""
    con = duckdb.connect()
    df = con.execute(sql).fetchdf()
    con.close()
    return df.to_dict(orient="records")


# ── 1. Filter options ──


def get_filter_options() -> dict:
    """Return available years, zip_codes, permit_categories, policy_eras."""
    con = duckdb.connect()
    pq = _pq("solar_annual")
    years = sorted(
        con.execute(f"SELECT DISTINCT year FROM '{pq}' ORDER BY year")
        .fetchdf()["year"].tolist()
    )

    pq_zip = _pq("zip_code_summary")
    zips = sorted(
        con.execute(f"SELECT DISTINCT zip_code FROM '{pq_zip}' WHERE zip_code IS NOT NULL ORDER BY zip_code")
        .fetchdf()["zip_code"].tolist()
    )

    pq_speed = _pq("approval_speed")
    categories = sorted(
        con.execute(f"SELECT DISTINCT permit_category FROM '{pq_speed}' ORDER BY permit_category")
        .fetchdf()["permit_category"].tolist()
    )

    eras = sorted(
        con.execute(f"SELECT DISTINCT policy_era FROM '{pq_speed}' WHERE policy_era IS NOT NULL ORDER BY policy_era")
        .fetchdf()["policy_era"].tolist()
    )

    con.close()
    return {
        "years": [int(y) for y in years],
        "zip_codes": zips,
        "permit_categories": categories,
        "policy_eras": eras,
    }


# ── 2. Overview ──


def get_overview(
    year_min: int | None = 2015,
    year_max: int | None = None,
) -> dict:
    """KPIs: total solar, cumulative, solar %, median approval days."""
    con = duckdb.connect()
    w = _where(year_min, year_max, has_zip=False, has_category=False, has_era=False)

    row = con.execute(f"""
        SELECT
            COALESCE(SUM(solar_count), 0) AS total_solar,
            MAX(cumulative_solar) AS cumulative_solar,
            COALESCE(MEDIAN(median_approval_days), 0) AS median_approval_days
        FROM '{_pq("solar_annual")}' {w}
    """).fetchone()

    # Total permits for solar % calculation
    w2 = _where(year_min, year_max, has_zip=False, has_category=False, has_era=False)
    total_permits = con.execute(f"""
        SELECT COALESCE(SUM(permit_count), 0)
        FROM '{_pq("climate_permits_monthly")}' {w2}
    """).fetchone()[0]

    con.close()
    solar_pct = (row[0] / total_permits * 100) if total_permits else 0
    return {
        "total_solar": int(row[0]),
        "cumulative_solar": int(row[1]) if row[1] else 0,
        "solar_pct": round(solar_pct, 1),
        "median_approval_days": float(row[2]) if row[2] else 0,
    }


# ── 3. Solar adoption curve ──


def get_solar_adoption_curve(
    year_min: int | None = None,
    year_max: int | None = None,
) -> list[dict]:
    """Annual solar count + cumulative (the S-curve)."""
    w = _where(year_min, year_max, has_zip=False, has_category=False, has_era=False)
    return _run(f"""
        SELECT year, solar_count, cumulative_solar, total_valuation, median_approval_days
        FROM '{_pq("solar_annual")}' {w}
        ORDER BY year
    """)


# ── 4. Solar by zip ──


def get_solar_by_zip(
    year_min: int | None = None,
    year_max: int | None = None,
    limit: int = 20,
) -> list[dict]:
    """Ranked zip codes by solar permit count."""
    w = _where(year_min, year_max, has_category=False, has_era=False)
    return _run(f"""
        SELECT zip_code, SUM(solar_count) AS solar_count,
               SUM(total_valuation) AS total_valuation
        FROM '{_pq("solar_by_zip")}' {w}
        GROUP BY zip_code
        ORDER BY solar_count DESC
        LIMIT {int(limit)}
    """)


# ── 5. Approval speed ──


def get_approval_speed(
    year_min: int | None = None,
    year_max: int | None = None,
    permit_category: str | None = None,
) -> list[dict]:
    """Timeline metrics by category/year/era."""
    w = _where(year_min, year_max, permit_category=permit_category, has_zip=False)
    return _run(f"""
        SELECT year, permit_category, policy_era, permit_count,
               median_days, avg_days, p90_days
        FROM '{_pq("approval_speed")}' {w}
        ORDER BY year, permit_category
    """)


# ── 6. Energy permit trends ──


def get_energy_permit_trends(
    year_min: int | None = None,
    year_max: int | None = None,
) -> list[dict]:
    """Annual solar/electrical/mechanical counts."""
    w = _where(year_min, year_max, has_zip=False, has_category=False, has_era=False)
    return _run(f"""
        SELECT year, solar_count, electrical_count, mechanical_count, climate_total
        FROM '{_pq("energy_permits_annual")}' {w}
        ORDER BY year
    """)


# ── 7. Zip code equity ──


def get_zip_code_equity(limit: int = 50) -> list[dict]:
    """Zip summary with solar adoption rates."""
    return _run(f"""
        SELECT zip_code, total_permits, solar_count, electrical_count,
               mechanical_count, climate_count, solar_pct, total_valuation
        FROM '{_pq("zip_code_summary")}'
        ORDER BY solar_count DESC
        LIMIT {int(limit)}
    """)


# ── 8. Monthly trends ──


def get_monthly_trends(
    year_min: int | None = None,
    year_max: int | None = None,
    permit_category: str | None = None,
) -> list[dict]:
    """Monthly detail by permit_category."""
    w = _where(year_min, year_max, permit_category=permit_category, has_zip=False, has_era=False)
    return _run(f"""
        SELECT year, month, permit_category, permit_count
        FROM '{_pq("climate_permits_monthly")}' {w}
        ORDER BY year, month, permit_category
    """)


# ── 9. Policy era comparison ──


def get_policy_era_comparison() -> list[dict]:
    """Pre-CAP vs post-CAP vs expedited era (solar only)."""
    return _run(f"""
        SELECT
            policy_era,
            SUM(permit_count) AS total_permits,
            MEDIAN(median_days) AS median_days,
            AVG(avg_days)::INTEGER AS avg_days,
            AVG(p90_days)::INTEGER AS p90_days
        FROM '{_pq("approval_speed")}'
        WHERE permit_category = 'Solar/PV' AND policy_era IS NOT NULL
        GROUP BY policy_era
        ORDER BY CASE policy_era
            WHEN 'Pre-CAP' THEN 1
            WHEN 'CAP Adopted' THEN 2
            WHEN 'Expedited Era' THEN 3
        END
    """)


# ── 10. Solar map data ──


def get_solar_map_data(
    year_min: int | None = None,
    year_max: int | None = None,
    limit: int = 50000,
) -> list[dict]:
    """Geo points for solar permit mapping."""
    w = _where(year_min, year_max, has_zip=False, has_category=False, has_era=False)
    return _run(f"""
        SELECT lat, lng, year, valuation, zip_code, approval_days, policy_era
        FROM '{_pq("solar_map_points")}' {w}
        ORDER BY RANDOM()
        LIMIT {int(limit)}
    """)


# ── 11. Energy consumption ──


def get_energy_consumption(
    year_min: int | None = None,
    year_max: int | None = None,
) -> list[dict]:
    """Electricity + gas consumption trends from SDG&E data (citywide aggregates, zip-level filtering not available)."""
    pq = _pq("energy_trends")
    if not Path(f"{_AGG}/energy_trends.parquet").exists():
        return []
    w = _where(year_min, year_max, has_category=False, has_era=False, has_zip=False)
    return _run(f"""
        SELECT year, quarter, customer_class,
               total_kwh, elec_customers,
               total_thm, gas_customers
        FROM '{pq}' {w}
        ORDER BY year, quarter, customer_class
    """)


# ── 12. Energy vs solar ──


def get_energy_vs_solar(
    year_min: int | None = None,
    year_max: int | None = None,
    limit: int = 30,
) -> list[dict]:
    """Join solar permits with energy consumption by zip to show correlation."""
    pq_energy = _pq("energy_by_zip_annual")
    pq_solar = _pq("solar_by_zip")
    if not Path(f"{_AGG}/energy_by_zip_annual.parquet").exists():
        return []

    w_solar = ""
    w_energy = ""
    if year_min is not None:
        w_solar = _q(w_solar, f"s.year >= {int(year_min)}")
        w_energy = _q(w_energy, f"e.year >= {int(year_min)}")
    if year_max is not None:
        w_solar = _q(w_solar, f"s.year <= {int(year_max)}")
        w_energy = _q(w_energy, f"e.year <= {int(year_max)}")

    return _run(f"""
        WITH solar_totals AS (
            SELECT zip_code, SUM(solar_count) AS solar_count
            FROM '{pq_solar}'
            {w_solar.replace('s.', '')}
            GROUP BY zip_code
        ),
        energy_totals AS (
            SELECT zip_code,
                   AVG(avg_kwh_per_customer)::INTEGER AS avg_kwh_per_customer,
                   SUM(total_kwh)::BIGINT AS total_kwh
            FROM '{pq_energy}'
            {w_energy.replace('e.', '')}
            GROUP BY zip_code
        )
        SELECT
            s.zip_code,
            s.solar_count,
            e.avg_kwh_per_customer,
            e.total_kwh
        FROM solar_totals s
        JOIN energy_totals e ON s.zip_code = e.zip_code
        WHERE e.avg_kwh_per_customer IS NOT NULL
        ORDER BY s.solar_count DESC
        LIMIT {int(limit)}
    """)
