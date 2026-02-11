"""Transform raw permit CSVs + SDG&E energy data into parquets."""

from __future__ import annotations

from pathlib import Path

import duckdb

_ROOT = Path(__file__).resolve().parent.parent
_RAW = _ROOT / "data" / "raw"
_SDGE = _RAW / "sdge"
_PROCESSED = _ROOT / "data" / "processed"
_AGG = _ROOT / "data" / "aggregated"

# Raw permit CSVs
_SET1_ACTIVE = str(_RAW / "set1_active.csv")
_SET1_CLOSED = str(_RAW / "set1_closed.csv")
_SET2_ACTIVE = str(_RAW / "set2_active.csv")
_SET2_CLOSED = str(_RAW / "set2_closed.csv")

_PERMITS_PARQUET = str(_PROCESSED / "climate_permits.parquet")
_ENERGY_PARQUET = str(_PROCESSED / "energy_consumption.parquet")


def transform() -> None:
    """Run the full transform pipeline."""
    _PROCESSED.mkdir(parents=True, exist_ok=True)
    _AGG.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()

    _transform_permits(con)
    _transform_energy(con)
    _build_aggregations(con)

    con.close()
    print("  Transform complete.")


def _transform_permits(con: duckdb.DuckDBPyConnection) -> None:
    """Load, normalize, and export climate-focused permit data."""

    # ── Load Set 1 (legacy system) ──
    print("  Loading Set 1 (legacy) ...")
    con.execute(f"""
        CREATE OR REPLACE TABLE set1_raw AS
        SELECT * FROM read_csv(
            ['{_SET1_ACTIVE}', '{_SET1_CLOSED}'],
            union_by_name = true,
            auto_detect = true,
            ignore_errors = true
        )
    """)
    row_count_1 = con.execute("SELECT COUNT(*) FROM set1_raw").fetchone()[0]
    print(f"    Set 1 rows: {row_count_1:,}")

    con.execute("""
        CREATE OR REPLACE TABLE set1 AS
        SELECT
            CAST(APPROVAL_ID AS VARCHAR)        AS approval_id,
            CAST(PROJECT_ID AS VARCHAR)         AS project_id,
            CAST(JOB_ID AS VARCHAR)             AS job_id,
            TRIM(ADDRESS_JOB)                   AS address,
            TRIM(CAST(JOB_APN AS VARCHAR))      AS apn,
            TRY_CAST(LAT_JOB AS DOUBLE)         AS lat,
            TRY_CAST(LNG_JOB AS DOUBLE)         AS lng,
            TRIM(APPROVAL_TYPE)                 AS approval_type,
            TRIM(APPROVAL_STATUS)               AS approval_status,
            TRY_CAST(DATE_APPROVAL_CREATE AS DATE)  AS date_approval_create,
            TRY_CAST(DATE_APPROVAL_ISSUE AS DATE)   AS date_approval_issue,
            TRY_CAST(DATE_APPROVAL_EXPIRE AS DATE)  AS date_approval_expire,
            TRY_CAST(DATE_APPROVAL_CLOSE AS DATE)   AS date_approval_close,
            TRY_CAST(APPROVAL_VALUATION AS DOUBLE)  AS valuation,
            'legacy'                            AS source_system
        FROM set1_raw
    """)

    # ── Load Set 2 (current system) ──
    print("  Loading Set 2 (current) ...")
    con.execute(f"""
        CREATE OR REPLACE TABLE set2_raw AS
        SELECT * FROM read_csv(
            ['{_SET2_ACTIVE}', '{_SET2_CLOSED}'],
            union_by_name = true,
            auto_detect = true,
            ignore_errors = true
        )
    """)
    row_count_2 = con.execute("SELECT COUNT(*) FROM set2_raw").fetchone()[0]
    print(f"    Set 2 rows: {row_count_2:,}")

    con.execute("""
        CREATE OR REPLACE TABLE set2 AS
        SELECT
            CAST(APPROVAL_ID AS VARCHAR)        AS approval_id,
            CAST(PROJECT_ID AS VARCHAR)         AS project_id,
            CAST(JOB_ID AS VARCHAR)             AS job_id,
            TRIM(ADDRESS_JOB)                   AS address,
            TRIM(CAST(JOB_APN AS VARCHAR))      AS apn,
            TRY_CAST(LAT_JOB AS DOUBLE)         AS lat,
            TRY_CAST(LNG_JOB AS DOUBLE)         AS lng,
            TRIM(APPROVAL_TYPE)                 AS approval_type,
            TRIM(APPROVAL_STATUS)               AS approval_status,
            TRY_CAST(DATE_APPROVAL_CREATE AS DATE)  AS date_approval_create,
            TRY_CAST(DATE_APPROVAL_ISSUE AS DATE)   AS date_approval_issue,
            TRY_CAST(DATE_APPROVAL_EXPIRE AS DATE)  AS date_approval_expire,
            TRY_CAST(DATE_APPROVAL_CLOSE AS DATE)   AS date_approval_close,
            TRY_CAST(APPROVAL_VALUATION AS DOUBLE)  AS valuation,
            'current'                           AS source_system
        FROM set2_raw
    """)

    # ── Union + derive climate fields ──
    print("  Unioning sets + deriving climate fields ...")
    con.execute("""
        CREATE OR REPLACE TABLE permits_union AS
        SELECT * FROM set1
        UNION ALL
        SELECT * FROM set2
    """)

    total_raw = con.execute("SELECT COUNT(*) FROM permits_union").fetchone()[0]
    print(f"    Union total: {total_raw:,}")

    con.execute("""
        CREATE OR REPLACE TABLE permits AS
        WITH derived AS (
            SELECT
                *,
                -- zip code from address (SD zips: 920xx-921xx)
                CASE
                    WHEN REGEXP_EXTRACT(address, '(9[12][0-9]{3})', 1) != ''
                    THEN REGEXP_EXTRACT(address, '(9[12][0-9]{3})', 1)
                    ELSE NULL
                END AS zip_code,

                -- approval timeline
                CASE
                    WHEN date_approval_issue IS NOT NULL
                         AND date_approval_create IS NOT NULL
                         AND DATEDIFF('day', date_approval_create, date_approval_issue) >= 0
                    THEN DATEDIFF('day', date_approval_create, date_approval_issue)
                    ELSE NULL
                END AS approval_days,

                -- year/month from issue date (fallback to create date)
                YEAR(COALESCE(date_approval_issue, date_approval_create))  AS approval_year,
                MONTH(COALESCE(date_approval_issue, date_approval_create)) AS approval_month,

                -- is_solar
                CASE
                    WHEN UPPER(TRIM(approval_type)) LIKE '%PHOTOVOLTAIC%'
                      OR UPPER(TRIM(approval_type)) LIKE '%PV%'
                      OR UPPER(TRIM(approval_type)) LIKE '%SOLAR%'
                    THEN TRUE
                    ELSE FALSE
                END AS is_solar,

                -- is_electrical (potential EV chargers, electrical upgrades)
                CASE
                    WHEN UPPER(TRIM(approval_type)) LIKE '%ELECTRICAL%'
                    THEN TRUE
                    ELSE FALSE
                END AS is_electrical,

                -- is_mechanical (HVAC upgrades)
                CASE
                    WHEN UPPER(TRIM(approval_type)) LIKE '%MECHANICAL%'
                    THEN TRUE
                    ELSE FALSE
                END AS is_mechanical,

                -- permit_category
                CASE
                    WHEN UPPER(TRIM(approval_type)) LIKE '%PHOTOVOLTAIC%'
                      OR UPPER(TRIM(approval_type)) LIKE '%PV%'
                      OR UPPER(TRIM(approval_type)) LIKE '%SOLAR%'
                    THEN 'Solar/PV'
                    WHEN UPPER(TRIM(approval_type)) LIKE '%ELECTRICAL%'
                    THEN 'Electrical'
                    WHEN UPPER(TRIM(approval_type)) LIKE '%MECHANICAL%'
                    THEN 'Mechanical/HVAC'
                    WHEN UPPER(TRIM(approval_type)) LIKE '%COMBINATION BUILDING%'
                      OR UPPER(TRIM(approval_type)) = 'BUILDING PERMIT'
                      OR UPPER(TRIM(approval_type)) LIKE 'BUILDING PERMIT%'
                    THEN 'Building'
                    ELSE 'Other'
                END AS permit_category,

                -- policy_era
                CASE
                    WHEN YEAR(COALESCE(date_approval_issue, date_approval_create)) < 2015
                    THEN 'Pre-CAP'
                    WHEN YEAR(COALESCE(date_approval_issue, date_approval_create)) BETWEEN 2015 AND 2017
                    THEN 'CAP Adopted'
                    WHEN YEAR(COALESCE(date_approval_issue, date_approval_create)) >= 2018
                    THEN 'Expedited Era'
                    ELSE NULL
                END AS policy_era
            FROM permits_union
        ),
        with_climate AS (
            SELECT
                *,
                (is_solar OR is_electrical OR is_mechanical) AS is_climate_relevant
            FROM derived
        ),
        deduped AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY approval_id
                    ORDER BY date_approval_close DESC NULLS LAST
                ) AS _rn
            FROM with_climate
        )
        SELECT * EXCLUDE (_rn)
        FROM deduped
        WHERE _rn = 1
          AND (lat IS NULL OR (lat BETWEEN 32.5 AND 33.3))
          AND (lng IS NULL OR (lng BETWEEN -117.7 AND -116.8))
    """)

    final_count = con.execute("SELECT COUNT(*) FROM permits").fetchone()[0]
    solar_count = con.execute("SELECT COUNT(*) FROM permits WHERE is_solar").fetchone()[0]
    print(f"    Final permits: {final_count:,} ({solar_count:,} solar)")

    # ── Export main parquet ──
    print(f"  Exporting {_PERMITS_PARQUET} ...")
    con.execute(f"""
        COPY permits TO '{_PERMITS_PARQUET}'
        (FORMAT PARQUET, CODEC 'ZSTD')
    """)
    size_mb = Path(_PERMITS_PARQUET).stat().st_size / (1024 * 1024)
    print(f"    climate_permits.parquet: {size_mb:.1f} MB")


def _transform_energy(con: duckdb.DuckDBPyConnection) -> None:
    """Load and process SDG&E energy consumption data."""

    sdge_files = sorted(_SDGE.glob("SDGE-*.csv"))
    if not sdge_files:
        print("  [warn] No SDG&E energy files found, skipping energy transform")
        return

    elec_files = [str(f) for f in sdge_files if "ELEC" in f.name and f.stat().st_size > 0]
    gas_files = [str(f) for f in sdge_files if "GAS" in f.name and f.stat().st_size > 0]

    # ── Electricity ──
    if elec_files:
        print(f"  Loading {len(elec_files)} electricity files ...")
        file_list = ", ".join(f"'{f}'" for f in elec_files)
        con.execute(f"""
            CREATE OR REPLACE TABLE elec_raw AS
            SELECT * FROM read_csv(
                [{file_list}],
                union_by_name = true,
                auto_detect = true,
                ignore_errors = true
            )
        """)

        con.execute("""
            CREATE OR REPLACE TABLE elec AS
            SELECT
                CAST("ZipCode" AS VARCHAR) AS zip_code,
                TRY_CAST("Month" AS INTEGER) AS month,
                TRY_CAST("Year" AS INTEGER) AS year,
                TRIM("CustomerClass") AS customer_class,
                TRY_CAST("TotalCustomers" AS INTEGER) AS total_customers,
                TRY_CAST("TotalkWh" AS DOUBLE) AS total_kwh,
                TRY_CAST("AveragekWh" AS DOUBLE) AS avg_kwh,
                'electricity' AS fuel_type
            FROM elec_raw
            WHERE CAST("ZipCode" AS VARCHAR) LIKE '92%'
        """)
        elec_rows = con.execute("SELECT COUNT(*) FROM elec").fetchone()[0]
        print(f"    Electricity rows (SD zips): {elec_rows:,}")
    else:
        con.execute("CREATE TABLE elec (zip_code VARCHAR, month INTEGER, year INTEGER, customer_class VARCHAR, total_customers INTEGER, total_kwh DOUBLE, avg_kwh DOUBLE, fuel_type VARCHAR)")

    # ── Gas ──
    if gas_files:
        print(f"  Loading {len(gas_files)} gas files ...")
        file_list = ", ".join(f"'{f}'" for f in gas_files)
        con.execute(f"""
            CREATE OR REPLACE TABLE gas_raw AS
            SELECT * FROM read_csv(
                [{file_list}],
                union_by_name = true,
                auto_detect = true,
                ignore_errors = true
            )
        """)

        con.execute("""
            CREATE OR REPLACE TABLE gas AS
            SELECT
                CAST("ZipCode" AS VARCHAR) AS zip_code,
                TRY_CAST("Month" AS INTEGER) AS month,
                TRY_CAST("Year" AS INTEGER) AS year,
                TRIM("CustomerClass") AS customer_class,
                TRY_CAST("TotalCustomers" AS INTEGER) AS total_customers,
                TRY_CAST("TotalTherms" AS DOUBLE) AS total_thm,
                TRY_CAST("AverageTherms" AS DOUBLE) AS avg_thm,
                'gas' AS fuel_type
            FROM gas_raw
            WHERE CAST("ZipCode" AS VARCHAR) LIKE '92%'
        """)
        gas_rows = con.execute("SELECT COUNT(*) FROM gas").fetchone()[0]
        print(f"    Gas rows (SD zips): {gas_rows:,}")
    else:
        con.execute("CREATE TABLE gas (zip_code VARCHAR, month INTEGER, year INTEGER, customer_class VARCHAR, total_customers INTEGER, total_thm DOUBLE, avg_thm DOUBLE, fuel_type VARCHAR)")

    # ── Combine into unified energy table ──
    con.execute("""
        CREATE OR REPLACE TABLE energy AS
        SELECT
            zip_code, month, year, customer_class,
            total_customers,
            total_kwh, avg_kwh,
            NULL::DOUBLE AS total_thm, NULL::DOUBLE AS avg_thm,
            fuel_type
        FROM elec
        UNION ALL
        SELECT
            zip_code, month, year, customer_class,
            total_customers,
            NULL::DOUBLE AS total_kwh, NULL::DOUBLE AS avg_kwh,
            total_thm, avg_thm,
            fuel_type
        FROM gas
    """)

    total_energy = con.execute("SELECT COUNT(*) FROM energy").fetchone()[0]
    print(f"    Combined energy rows: {total_energy:,}")

    # ── Export energy parquet ──
    print(f"  Exporting {_ENERGY_PARQUET} ...")
    con.execute(f"""
        COPY energy TO '{_ENERGY_PARQUET}'
        (FORMAT PARQUET, CODEC 'ZSTD')
    """)
    if Path(_ENERGY_PARQUET).exists():
        size_mb = Path(_ENERGY_PARQUET).stat().st_size / (1024 * 1024)
        print(f"    energy_consumption.parquet: {size_mb:.1f} MB")


def _build_aggregations(con: duckdb.DuckDBPyConnection) -> None:
    """Build 9 pre-aggregated parquet files for dashboard/API."""

    # 1. solar_annual — annual solar count, cumulative, valuation, median approval days
    print("  Aggregating: solar_annual ...")
    con.execute(f"""
        COPY (
            SELECT
                year,
                solar_count,
                SUM(solar_count) OVER (ORDER BY year) AS cumulative_solar,
                total_valuation,
                median_approval_days,
                median_approval_days_nonzero,
                same_day_count
            FROM (
                SELECT
                    approval_year AS year,
                    COUNT(*) AS solar_count,
                    SUM(COALESCE(valuation, 0))::BIGINT AS total_valuation,
                    MEDIAN(approval_days) AS median_approval_days,
                    MEDIAN(CASE WHEN approval_days > 0 THEN approval_days END) AS median_approval_days_nonzero,
                    SUM(CASE WHEN approval_days = 0 THEN 1 ELSE 0 END) AS same_day_count
                FROM permits
                WHERE is_solar = TRUE AND approval_year IS NOT NULL
                GROUP BY approval_year
            )
            ORDER BY year
        ) TO '{_AGG}/solar_annual.parquet'
        (FORMAT PARQUET, CODEC 'ZSTD')
    """)

    # 2. solar_by_zip — solar permits by zip + year
    print("  Aggregating: solar_by_zip ...")
    con.execute(f"""
        COPY (
            SELECT
                zip_code,
                approval_year AS year,
                COUNT(*) AS solar_count,
                SUM(COALESCE(valuation, 0))::BIGINT AS total_valuation,
                MEDIAN(approval_days) AS median_approval_days
            FROM permits
            WHERE is_solar = TRUE AND zip_code IS NOT NULL AND approval_year IS NOT NULL
            GROUP BY zip_code, approval_year
            ORDER BY zip_code, year
        ) TO '{_AGG}/solar_by_zip.parquet'
        (FORMAT PARQUET, CODEC 'ZSTD')
    """)

    # 3. approval_speed — median/avg/p90 approval days by category, year, policy_era
    print("  Aggregating: approval_speed ...")
    con.execute(f"""
        COPY (
            SELECT
                approval_year AS year,
                permit_category,
                policy_era,
                COUNT(*) AS permit_count,
                MEDIAN(approval_days) AS median_days,
                MEDIAN(CASE WHEN approval_days > 0 THEN approval_days END) AS median_days_nonzero,
                AVG(approval_days)::INTEGER AS avg_days,
                QUANTILE_CONT(approval_days, 0.9)::INTEGER AS p90_days
            FROM permits
            WHERE approval_days IS NOT NULL AND approval_year IS NOT NULL
            GROUP BY approval_year, permit_category, policy_era
            ORDER BY year, permit_category
        ) TO '{_AGG}/approval_speed.parquet'
        (FORMAT PARQUET, CODEC 'ZSTD')
    """)

    # 4. climate_permits_monthly — monthly counts by permit_category
    print("  Aggregating: climate_permits_monthly ...")
    con.execute(f"""
        COPY (
            SELECT
                approval_year AS year,
                approval_month AS month,
                permit_category,
                COUNT(*) AS permit_count
            FROM permits
            WHERE approval_year IS NOT NULL
            GROUP BY approval_year, approval_month, permit_category
            ORDER BY year, month, permit_category
        ) TO '{_AGG}/climate_permits_monthly.parquet'
        (FORMAT PARQUET, CODEC 'ZSTD')
    """)

    # 5. solar_map_points — lat/lng for solar permits only
    print("  Aggregating: solar_map_points ...")
    con.execute(f"""
        COPY (
            SELECT
                lat, lng,
                approval_year AS year,
                valuation,
                zip_code,
                approval_days,
                policy_era
            FROM permits
            WHERE is_solar = TRUE AND lat IS NOT NULL AND lng IS NOT NULL
        ) TO '{_AGG}/solar_map_points.parquet'
        (FORMAT PARQUET, CODEC 'ZSTD')
    """)

    # 6. energy_permits_annual — annual counts for solar/electrical/mechanical
    print("  Aggregating: energy_permits_annual ...")
    con.execute(f"""
        COPY (
            SELECT
                approval_year AS year,
                SUM(CASE WHEN is_solar THEN 1 ELSE 0 END) AS solar_count,
                SUM(CASE WHEN is_electrical THEN 1 ELSE 0 END) AS electrical_count,
                SUM(CASE WHEN is_mechanical THEN 1 ELSE 0 END) AS mechanical_count,
                SUM(CASE WHEN is_climate_relevant THEN 1 ELSE 0 END) AS climate_total
            FROM permits
            WHERE approval_year IS NOT NULL
            GROUP BY approval_year
            ORDER BY year
        ) TO '{_AGG}/energy_permits_annual.parquet'
        (FORMAT PARQUET, CODEC 'ZSTD')
    """)

    # 7. zip_code_summary — per-zip totals
    print("  Aggregating: zip_code_summary ...")
    con.execute(f"""
        COPY (
            SELECT
                zip_code,
                COUNT(*) AS total_permits,
                SUM(CASE WHEN is_solar THEN 1 ELSE 0 END) AS solar_count,
                SUM(CASE WHEN is_electrical THEN 1 ELSE 0 END) AS electrical_count,
                SUM(CASE WHEN is_mechanical THEN 1 ELSE 0 END) AS mechanical_count,
                SUM(CASE WHEN is_climate_relevant THEN 1 ELSE 0 END) AS climate_count,
                ROUND(SUM(CASE WHEN is_solar THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS solar_pct,
                SUM(COALESCE(valuation, 0))::BIGINT AS total_valuation
            FROM permits
            WHERE zip_code IS NOT NULL
            GROUP BY zip_code
            ORDER BY solar_count DESC
        ) TO '{_AGG}/zip_code_summary.parquet'
        (FORMAT PARQUET, CODEC 'ZSTD')
    """)

    # 8. energy_by_zip_annual — annual electricity + gas consumption by zip (residential)
    _has_energy = con.execute("SELECT COUNT(*) FROM energy").fetchone()[0] > 0
    if _has_energy:
        print("  Aggregating: energy_by_zip_annual ...")
        con.execute(f"""
            COPY (
                SELECT
                    zip_code,
                    year,
                    SUM(CASE WHEN fuel_type = 'electricity' THEN total_kwh ELSE 0 END)::BIGINT AS total_kwh,
                    SUM(CASE WHEN fuel_type = 'electricity' THEN total_customers ELSE 0 END) AS elec_customers,
                    CASE
                        WHEN SUM(CASE WHEN fuel_type = 'electricity' THEN total_customers ELSE 0 END) > 0
                        THEN (SUM(CASE WHEN fuel_type = 'electricity' THEN total_kwh ELSE 0 END)
                              / SUM(CASE WHEN fuel_type = 'electricity' THEN total_customers ELSE 0 END))::INTEGER
                        ELSE NULL
                    END AS avg_kwh_per_customer,
                    SUM(CASE WHEN fuel_type = 'gas' THEN total_thm ELSE 0 END)::BIGINT AS total_thm,
                    SUM(CASE WHEN fuel_type = 'gas' THEN total_customers ELSE 0 END) AS gas_customers
                FROM energy
                WHERE customer_class = 'R' AND year IS NOT NULL
                GROUP BY zip_code, year
                ORDER BY zip_code, year
            ) TO '{_AGG}/energy_by_zip_annual.parquet'
            (FORMAT PARQUET, CODEC 'ZSTD')
        """)

        # 9. energy_trends — citywide quarterly electricity + gas totals
        print("  Aggregating: energy_trends ...")
        con.execute(f"""
            COPY (
                SELECT
                    year,
                    ((month - 1) // 3 + 1) AS quarter,
                    customer_class,
                    SUM(CASE WHEN fuel_type = 'electricity' THEN total_kwh ELSE 0 END)::BIGINT AS total_kwh,
                    SUM(CASE WHEN fuel_type = 'electricity' THEN total_customers ELSE 0 END) AS elec_customers,
                    SUM(CASE WHEN fuel_type = 'gas' THEN total_thm ELSE 0 END)::BIGINT AS total_thm,
                    SUM(CASE WHEN fuel_type = 'gas' THEN total_customers ELSE 0 END) AS gas_customers
                FROM energy
                WHERE year IS NOT NULL
                GROUP BY year, ((month - 1) // 3 + 1), customer_class
                ORDER BY year, quarter, customer_class
            ) TO '{_AGG}/energy_trends.parquet'
            (FORMAT PARQUET, CODEC 'ZSTD')
        """)
    else:
        print("  [skip] energy aggregations (no energy data)")

    print("  All aggregations complete.")


if __name__ == "__main__":
    transform()
