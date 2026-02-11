"""Streamlit dashboard for San Diego Climate Action Plan progress."""

from __future__ import annotations

from pathlib import Path

import duckdb
import plotly.express as px
import plotly.graph_objects as go
import pydeck as pdk
import streamlit as st

# ── Parquet paths ──
_root = Path(__file__).resolve().parent.parent
_AGG = str(_root / "data" / "aggregated")
_PROCESSED = str(_root / "data" / "processed")


def _pq(name: str) -> str:
    return f"{_AGG}/{name}.parquet"


def _pq_exists(name: str) -> bool:
    return Path(f"{_AGG}/{name}.parquet").exists()


st.set_page_config(
    page_title="San Diego Climate Action",
    page_icon="\u2600\ufe0f",
    layout="wide",
)

CHART_COLOR = "#83c9ff"
SOLAR_COLOR = "#FFB800"
ELEC_COLOR = "#00B4D8"
MECH_COLOR = "#E63946"


def query(sql: str):
    """Run SQL against parquet files and return a pandas DataFrame."""
    con = duckdb.connect()
    return con.execute(sql).fetchdf()


# ── Sidebar filters ──
st.sidebar.title("Filters")


@st.cache_data(ttl=3600)
def _sidebar_options():
    years = sorted(
        query(f"SELECT DISTINCT year FROM '{_pq('solar_annual')}' ORDER BY year")
        ["year"].tolist()
    )
    categories = sorted(
        query(f"SELECT DISTINCT permit_category FROM '{_pq('approval_speed')}' ORDER BY permit_category")
        ["permit_category"].tolist()
    )
    zips = sorted(
        query(f"SELECT DISTINCT zip_code FROM '{_pq('zip_code_summary')}' WHERE zip_code IS NOT NULL ORDER BY zip_code")
        ["zip_code"].tolist()
    )
    eras = sorted(
        query(f"SELECT DISTINCT policy_era FROM '{_pq('approval_speed')}' WHERE policy_era IS NOT NULL ORDER BY policy_era")
        ["policy_era"].tolist()
    )
    return years, categories, zips, eras


all_years, all_categories, all_zips, all_eras = _sidebar_options()

if all_years:
    year_range = st.sidebar.slider(
        "Year Range",
        min_value=int(min(all_years)),
        max_value=int(max(all_years)),
        value=(2015, int(max(all_years))),
    )
else:
    year_range = (2015, 2026)

selected_categories = st.sidebar.multiselect(
    "Permit Category",
    options=all_categories,
    default=None,
    placeholder="All categories",
)

selected_zips = st.sidebar.multiselect(
    "Zip Code",
    options=all_zips,
    default=None,
    placeholder="All zip codes",
)

selected_era = st.sidebar.selectbox(
    "Policy Era",
    options=["All"] + all_eras,
    index=0,
    help="**Pre-CAP**: Before 2015 (no climate plan). "
         "**CAP Adopted**: 2015-2017 (Climate Action Plan adopted). "
         "**Expedited Era**: 2018+ (streamlined solar permitting).",
)


def _year_filter(col: str = "year") -> str:
    return f"{col} BETWEEN {year_range[0]} AND {year_range[1]}"


def _cat_filter(col: str = "permit_category") -> str:
    if not selected_categories:
        return ""
    escaped = ", ".join(f"'{c.replace(chr(39), chr(39)*2)}'" for c in selected_categories)
    return f"{col} IN ({escaped})"


def _era_filter(col: str = "policy_era") -> str:
    if selected_era == "All":
        return ""
    return f"{col} = '{selected_era}'"


def _where(*conditions: str) -> str:
    parts = [c for c in conditions if c]
    return ("WHERE " + " AND ".join(parts)) if parts else ""


# ── Header ──
st.title("San Diego Climate Action")
st.markdown(
    "Tracking progress on San Diego's **Climate Action Plan** (adopted 2015): "
    "solar adoption curves, permit expediting impact, geographic equity of clean energy, "
    "and energy consumption trends. Data from the city's "
    "[open data portal](https://data.sandiego.gov) development permits and "
    "[SDG&E](https://energydata.sdge.com) energy consumption reports."
)

# ── Tabs ──
tab_solar, tab_speed, tab_equity, tab_energy, tab_energy_solar, tab_targets = st.tabs([
    "Solar Adoption",
    "Expedited Permitting",
    "Geographic Equity",
    "Energy Consumption",
    "Energy + Solar by Zip",
    "Climate Targets",
])


# ══════════════════════════════════════════════════════════════
# TAB 1: Solar Adoption
# ══════════════════════════════════════════════════════════════
with tab_solar:
    w = _where(_year_filter())
    solar = query(f"""
        SELECT year, solar_count, cumulative_solar, total_valuation,
               median_approval_days_nonzero, same_day_count
        FROM '{_pq("solar_annual")}' {w}
        ORDER BY year
    """)

    if len(solar) > 0:
        latest = solar.iloc[-1]
        total_solar = int(solar["solar_count"].sum())
        cumulative = int(latest["cumulative_solar"])

        # Growth rate — exclude current partial year
        from datetime import date
        current_year = date.today().year
        full_years = solar[solar["year"] < current_year]
        if len(full_years) >= 2:
            prev = int(full_years.iloc[-2]["solar_count"])
            curr = int(full_years.iloc[-1]["solar_count"])
            growth_rate = ((curr - prev) / prev * 100) if prev else 0
            growth_label = f"{int(full_years.iloc[-2]['year'])}-{int(full_years.iloc[-1]['year'])}"
        else:
            growth_rate = 0
            growth_label = "YoY"

        # Median from latest full year
        _med_row = full_years.iloc[-1] if len(full_years) > 0 else latest
        _med = _med_row["median_approval_days_nonzero"]

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Solar Permits (filtered)", f"{total_solar:,}")
        col2.metric("Cumulative Total", f"{cumulative:,}")
        col3.metric(f"Growth ({growth_label})", f"{growth_rate:+.1f}%")
        col4.metric("Median Approval Days", f"{_med:.0f}" if _med is not None and _med == _med else "N/A",
                     help="Excludes same-day approvals. Based on latest full year.")

        # Cumulative S-curve
        st.subheader("Cumulative Solar Installations (S-Curve)")
        fig_cum = px.area(solar, x="year", y="cumulative_solar",
                          labels={"year": "Year", "cumulative_solar": "Cumulative Permits"})
        fig_cum.add_vline(x=2015, line_dash="dash", line_color="red",
                          annotation_text="CAP Adopted", annotation_position="top left")
        fig_cum.add_vline(x=2017, line_dash="dash", line_color="orange",
                          annotation_text="Expedited Permitting", annotation_position="top right")
        fig_cum.update_traces(fillcolor="rgba(255,184,0,0.3)", line_color=SOLAR_COLOR)
        st.plotly_chart(fig_cum, use_container_width=True)

        # Annual bar chart
        st.subheader("Annual Solar Permits")
        fig_bar = px.bar(solar, x="year", y="solar_count",
                         labels={"year": "Year", "solar_count": "Permits"})
        fig_bar.update_traces(marker_color=SOLAR_COLOR)
        fig_bar.add_vline(x=2015, line_dash="dash", line_color="red")
        fig_bar.add_vline(x=2017, line_dash="dash", line_color="orange")
        st.plotly_chart(fig_bar, use_container_width=True)
    else:
        st.info("No solar permit data for the selected year range.")


# ══════════════════════════════════════════════════════════════
# TAB 2: Expedited Permitting
# ══════════════════════════════════════════════════════════════
with tab_speed:
    # Policy era comparison
    st.subheader("Policy Era Comparison (Solar Permits)")
    era_data = query(f"""
        SELECT
            policy_era,
            SUM(permit_count) AS total_permits,
            MEDIAN(median_days_nonzero) AS median_days,
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

    if len(era_data) > 0:
        cols = st.columns(len(era_data))
        for i, row in era_data.iterrows():
            with cols[i]:
                st.metric(row["policy_era"], f"{row['median_days']:.0f} days" if row["median_days"] else "N/A",
                          help=f"Total permits: {int(row['total_permits']):,}")
                st.caption(f"Avg: {int(row['avg_days'])} days | P90: {int(row['p90_days'])} days" if row["avg_days"] else "")

    # Median approval days trend by category
    st.subheader("Median Approval Days by Category")
    w_speed = _where(_year_filter(), _cat_filter(), _era_filter())
    speed = query(f"""
        SELECT year, permit_category,
               SUM(permit_count) AS permit_count,
               MEDIAN(median_days_nonzero) AS median_days
        FROM '{_pq("approval_speed")}' {w_speed}
        GROUP BY year, permit_category
        ORDER BY year
    """)

    if len(speed) > 0:
        fig_speed = px.line(speed, x="year", y="median_days", color="permit_category",
                            labels={"year": "Year", "median_days": "Median Days", "permit_category": "Category"})
        fig_speed.add_vline(x=2015, line_dash="dash", line_color="red",
                            annotation_text="CAP", annotation_position="top left")
        fig_speed.add_vline(x=2017, line_dash="dash", line_color="orange",
                            annotation_text="Expedited", annotation_position="top right")
        st.plotly_chart(fig_speed, use_container_width=True)

    # P90 trend
    st.subheader("P90 Approval Days (Solar/PV)")
    p90 = query(f"""
        SELECT year, MEDIAN(p90_days) AS p90_days
        FROM '{_pq("approval_speed")}'
        WHERE permit_category = 'Solar/PV' AND {_year_filter()}
        GROUP BY year
        ORDER BY year
    """)
    if len(p90) > 0:
        fig_p90 = px.bar(p90, x="year", y="p90_days",
                         labels={"year": "Year", "p90_days": "P90 Days"})
        fig_p90.update_traces(marker_color=SOLAR_COLOR)
        st.plotly_chart(fig_p90, use_container_width=True)


# ══════════════════════════════════════════════════════════════
# TAB 3: Geographic Equity
# ══════════════════════════════════════════════════════════════
with tab_equity:
    w_zip = _where(_year_filter())

    # Solar by zip (top 20)
    st.subheader("Top 20 Zip Codes by Solar Permits")
    zip_solar = query(f"""
        SELECT zip_code, SUM(solar_count) AS solar_count, SUM(total_valuation) AS total_valuation
        FROM '{_pq("solar_by_zip")}' {w_zip}
        GROUP BY zip_code
        ORDER BY solar_count DESC
        LIMIT 20
    """)
    if len(zip_solar) > 0:
        zip_solar["zip_code"] = zip_solar["zip_code"].astype(str)
        fig_zip = px.bar(zip_solar, x="solar_count", y="zip_code", orientation="h",
                         labels={"solar_count": "Solar Permits", "zip_code": "Zip Code"})
        fig_zip.update_traces(marker_color=SOLAR_COLOR)
        fig_zip.update_layout(yaxis={"categoryorder": "total ascending", "type": "category"})
        st.plotly_chart(fig_zip, use_container_width=True)

    # Solar % by zip
    st.subheader("Solar as % of All Permits by Zip")
    zip_pct = query(f"""
        SELECT zip_code, solar_pct, solar_count, total_permits
        FROM '{_pq("zip_code_summary")}'
        WHERE solar_count > 0
        ORDER BY solar_pct DESC
        LIMIT 20
    """)
    if len(zip_pct) > 0:
        zip_pct["zip_code"] = zip_pct["zip_code"].astype(str)
        fig_pct = px.bar(zip_pct, x="solar_pct", y="zip_code", orientation="h",
                         labels={"solar_pct": "Solar %", "zip_code": "Zip Code"},
                         hover_data=["solar_count", "total_permits"])
        fig_pct.update_traces(marker_color=ELEC_COLOR)
        fig_pct.update_layout(yaxis={"categoryorder": "total ascending", "type": "category"})
        st.plotly_chart(fig_pct, use_container_width=True)

    # Map
    st.subheader("Solar Permit Locations")
    map_df = query(f"""
        SELECT lat, lng, valuation
        FROM '{_pq("solar_map_points")}'
        WHERE {_year_filter()}
        ORDER BY RANDOM()
        LIMIT 50000
    """)
    if len(map_df) > 0:
        st.caption(f"{len(map_df):,} solar permits visualized")
        layer = pdk.Layer(
            "ScatterplotLayer",
            data=map_df,
            get_position=["lng", "lat"],
            get_radius=60,
            get_fill_color=[255, 184, 0, 140],
            pickable=True,
        )
        view = pdk.ViewState(latitude=32.7157, longitude=-117.1611, zoom=10.5, pitch=0)
        st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view, map_style="light"))


# ══════════════════════════════════════════════════════════════
# TAB 4: Energy Consumption
# ══════════════════════════════════════════════════════════════
with tab_energy:
    if _pq_exists("energy_trends"):
        w_e = _where(_year_filter())

        # Citywide electricity trends (residential vs commercial)
        st.subheader("Citywide Electricity Consumption (Quarterly)")
        elec_trend = query(f"""
            SELECT year, quarter, customer_class,
                   total_kwh, elec_customers
            FROM '{_pq("energy_trends")}' {w_e}
            AND customer_class IN ('R', 'C') AND total_kwh > 0
            ORDER BY year, quarter
        """)
        if len(elec_trend) > 0:
            elec_trend["period"] = elec_trend["year"].astype(str) + "-Q" + elec_trend["quarter"].astype(str)
            elec_trend["class_label"] = elec_trend["customer_class"].map({"R": "Residential", "C": "Commercial"})
            fig_elec = px.line(elec_trend, x="period", y="total_kwh", color="class_label",
                               labels={"period": "", "total_kwh": "Total kWh", "class_label": "Class"})
            st.plotly_chart(fig_elec, use_container_width=True)

        # Gas trends
        st.subheader("Citywide Gas Consumption (Quarterly)")
        gas_trend = query(f"""
            SELECT year, quarter, customer_class,
                   total_thm, gas_customers
            FROM '{_pq("energy_trends")}' {w_e}
            AND customer_class IN ('R', 'C') AND total_thm > 0
            ORDER BY year, quarter
        """)
        if len(gas_trend) > 0:
            gas_trend["period"] = gas_trend["year"].astype(str) + "-Q" + gas_trend["quarter"].astype(str)
            gas_trend["class_label"] = gas_trend["customer_class"].map({"R": "Residential", "C": "Commercial"})
            fig_gas = px.line(gas_trend, x="period", y="total_thm", color="class_label",
                              labels={"period": "", "total_thm": "Total Therms", "class_label": "Class"})
            st.plotly_chart(fig_gas, use_container_width=True)

        # Energy per customer
        st.subheader("Average kWh per Residential Customer (Quarterly)")
        per_cust = query(f"""
            SELECT year, quarter,
                   (total_kwh / NULLIF(elec_customers, 0))::INTEGER AS kwh_per_customer
            FROM '{_pq("energy_trends")}' {w_e}
            AND customer_class = 'R' AND total_kwh > 0
            ORDER BY year, quarter
        """)
        if len(per_cust) > 0:
            per_cust["period"] = per_cust["year"].astype(str) + "-Q" + per_cust["quarter"].astype(str)
            fig_pc = px.line(per_cust, x="period", y="kwh_per_customer",
                             labels={"period": "", "kwh_per_customer": "kWh per Customer"})
            fig_pc.update_traces(line_color=ELEC_COLOR)
            st.plotly_chart(fig_pc, use_container_width=True)

        # Solar permits overlaid on energy chart
        st.subheader("Solar Permits vs Residential Electricity")
        solar_annual = query(f"""
            SELECT year, solar_count FROM '{_pq("solar_annual")}' {w_e}
            ORDER BY year
        """)
        res_annual = query(f"""
            SELECT year, SUM(total_kwh)::BIGINT AS total_kwh
            FROM '{_pq("energy_trends")}' {w_e}
            AND customer_class = 'R'
            GROUP BY year
            ORDER BY year
        """)
        if len(solar_annual) > 0 and len(res_annual) > 0:
            fig_dual = go.Figure()
            fig_dual.add_trace(go.Bar(
                x=res_annual["year"], y=res_annual["total_kwh"],
                name="Residential kWh", marker_color=ELEC_COLOR, opacity=0.4, yaxis="y"
            ))
            fig_dual.add_trace(go.Scatter(
                x=solar_annual["year"], y=solar_annual["solar_count"],
                name="Solar Permits", line=dict(color=SOLAR_COLOR, width=3), yaxis="y2"
            ))
            fig_dual.update_layout(
                yaxis=dict(title="Total kWh"),
                yaxis2=dict(title="Solar Permits", overlaying="y", side="right"),
                legend=dict(x=0, y=1.1, orientation="h"),
            )
            st.plotly_chart(fig_dual, use_container_width=True)
    else:
        st.info("Energy consumption data not yet available. Run `climate-build` to download SDG&E data.")


# ══════════════════════════════════════════════════════════════
# TAB 5: Energy + Solar by Zip
# ══════════════════════════════════════════════════════════════
with tab_energy_solar:
    if _pq_exists("energy_by_zip_annual"):
        st.subheader("Solar Permits vs Average Electricity per Customer by Zip")
        st.caption("Do zip codes with more solar permits use less grid electricity?")

        # Join solar with energy by zip
        scatter = query(f"""
            WITH solar_totals AS (
                SELECT zip_code, SUM(solar_count) AS solar_count
                FROM '{_pq("solar_by_zip")}'
                WHERE {_year_filter()}
                GROUP BY zip_code
            ),
            energy_totals AS (
                SELECT zip_code,
                       AVG(avg_kwh_per_customer)::INTEGER AS avg_kwh_per_customer,
                       SUM(total_kwh)::BIGINT AS total_kwh
                FROM '{_pq("energy_by_zip_annual")}'
                WHERE {_year_filter()}
                GROUP BY zip_code
            )
            SELECT s.zip_code, s.solar_count,
                   e.avg_kwh_per_customer, e.total_kwh
            FROM solar_totals s
            JOIN energy_totals e ON s.zip_code = e.zip_code
            WHERE e.avg_kwh_per_customer IS NOT NULL AND e.avg_kwh_per_customer > 0
            ORDER BY s.solar_count DESC
        """)

        if len(scatter) > 0:
            scatter["zip_code"] = scatter["zip_code"].astype(str)
            fig_scatter = px.scatter(
                scatter, x="solar_count", y="avg_kwh_per_customer",
                hover_name="zip_code", size="total_kwh",
                labels={"solar_count": "Solar Permits", "avg_kwh_per_customer": "Avg kWh/Customer"},
                size_max=40,
            )
            fig_scatter.update_traces(marker_color=SOLAR_COLOR, marker_opacity=0.7)
            st.plotly_chart(fig_scatter, use_container_width=True)

            # Top vs bottom comparison
            col_left, col_right = st.columns(2)
            with col_left:
                st.markdown("**Top 10 Solar Zips**")
                st.dataframe(
                    scatter.head(10)[["zip_code", "solar_count", "avg_kwh_per_customer"]],
                    hide_index=True,
                    column_config={
                        "zip_code": "Zip",
                        "solar_count": st.column_config.NumberColumn("Solar Permits", format="%d"),
                        "avg_kwh_per_customer": st.column_config.NumberColumn("Avg kWh/Customer", format="%d"),
                    },
                )
            with col_right:
                st.markdown("**Bottom 10 Solar Zips**")
                st.dataframe(
                    scatter.tail(10)[["zip_code", "solar_count", "avg_kwh_per_customer"]],
                    hide_index=True,
                    column_config={
                        "zip_code": "Zip",
                        "solar_count": st.column_config.NumberColumn("Solar Permits", format="%d"),
                        "avg_kwh_per_customer": st.column_config.NumberColumn("Avg kWh/Customer", format="%d"),
                    },
                )

            # Full table
            with st.expander("Full Solar + Energy Table"):
                st.dataframe(
                    scatter,
                    hide_index=True,
                    use_container_width=True,
                    column_config={
                        "zip_code": "Zip Code",
                        "solar_count": st.column_config.NumberColumn("Solar Permits", format="%d"),
                        "avg_kwh_per_customer": st.column_config.NumberColumn("Avg kWh/Customer", format="%d"),
                        "total_kwh": st.column_config.NumberColumn("Total kWh", format="%d"),
                    },
                )
        else:
            st.info("No matching data for the selected filters.")
    else:
        st.info("Energy consumption data not yet available. Run `climate-build` to download SDG&E data.")


# ══════════════════════════════════════════════════════════════
# TAB 6: Climate Targets
# ══════════════════════════════════════════════════════════════
with tab_targets:
    st.subheader("Climate Action Plan Progress")
    st.caption("San Diego's CAP targets 100% clean/renewable electricity by 2035.")

    # Solar milestone tracking
    solar_all = query(f"""
        SELECT year, solar_count, cumulative_solar
        FROM '{_pq("solar_annual")}'
        ORDER BY year
    """)

    if len(solar_all) > 0:
        latest_year = int(solar_all.iloc[-1]["year"])
        total_cum = int(solar_all.iloc[-1]["cumulative_solar"])

        # Growth rate analysis
        recent = solar_all[solar_all["year"] >= 2018]
        if len(recent) >= 2:
            avg_annual = int(recent["solar_count"].mean())
            last_year_count = int(recent.iloc[-1]["solar_count"])
        else:
            avg_annual = 0
            last_year_count = 0

        col1, col2, col3 = st.columns(3)
        col1.metric("Total Solar Installations", f"{total_cum:,}")
        col2.metric("Avg Annual (Expedited Era)", f"{avg_annual:,}")
        col3.metric(f"Latest Year ({latest_year})", f"{last_year_count:,}")

        # Solar trajectory
        st.subheader("Solar Installation Trajectory")
        fig_traj = px.bar(solar_all, x="year", y="solar_count",
                          labels={"year": "Year", "solar_count": "Annual Permits"})
        fig_traj.update_traces(marker_color=SOLAR_COLOR)
        # Target line (if we need X per year to hit target)
        fig_traj.add_hline(y=avg_annual, line_dash="dot", line_color="green",
                           annotation_text=f"Avg ({avg_annual:,}/yr)")
        st.plotly_chart(fig_traj, use_container_width=True)

    # Energy permits breakdown
    st.subheader("Climate-Relevant Permits by Type")
    energy_permits = query(f"""
        SELECT year, solar_count, electrical_count, mechanical_count
        FROM '{_pq("energy_permits_annual")}'
        ORDER BY year
    """)
    if len(energy_permits) > 0:
        fig_ep = go.Figure()
        fig_ep.add_trace(go.Bar(x=energy_permits["year"], y=energy_permits["solar_count"],
                                name="Solar/PV", marker_color=SOLAR_COLOR))
        fig_ep.add_trace(go.Bar(x=energy_permits["year"], y=energy_permits["electrical_count"],
                                name="Electrical", marker_color=ELEC_COLOR))
        fig_ep.add_trace(go.Bar(x=energy_permits["year"], y=energy_permits["mechanical_count"],
                                name="Mechanical/HVAC", marker_color=MECH_COLOR))
        fig_ep.update_layout(barmode="stack", legend=dict(x=0, y=1.1, orientation="h"))
        st.plotly_chart(fig_ep, use_container_width=True)

    # Energy consumption trajectory
    if _pq_exists("energy_trends"):
        st.subheader("Residential Electricity Consumption Trajectory")
        res_trend = query(f"""
            SELECT year, SUM(total_kwh)::BIGINT AS total_kwh
            FROM '{_pq("energy_trends")}'
            WHERE customer_class = 'R'
            GROUP BY year
            ORDER BY year
        """)
        if len(res_trend) > 0:
            fig_res = px.line(res_trend, x="year", y="total_kwh",
                              labels={"year": "Year", "total_kwh": "Total Residential kWh"})
            fig_res.update_traces(line_color=ELEC_COLOR, line_width=3)
            st.plotly_chart(fig_res, use_container_width=True)

    # By-the-numbers narrative
    st.subheader("By the Numbers")
    era_compare = query(f"""
        SELECT policy_era,
               SUM(permit_count) AS total,
               MEDIAN(median_days_nonzero) AS median_days
        FROM '{_pq("approval_speed")}'
        WHERE permit_category = 'Solar/PV' AND policy_era IS NOT NULL
        GROUP BY policy_era
    """)
    if len(era_compare) > 0:
        pre_cap = era_compare[era_compare["policy_era"] == "Pre-CAP"]
        expedited = era_compare[era_compare["policy_era"] == "Expedited Era"]

        if len(pre_cap) > 0 and len(expedited) > 0:
            pre_days = float(pre_cap.iloc[0]["median_days"]) if pre_cap.iloc[0]["median_days"] else 0
            exp_days = float(expedited.iloc[0]["median_days"]) if expedited.iloc[0]["median_days"] else 0
            pre_count = int(pre_cap.iloc[0]["total"])
            exp_count = int(expedited.iloc[0]["total"])

            if pre_days > 0 and exp_days > 0:
                speed_improvement = ((pre_days - exp_days) / pre_days * 100)
                st.markdown(f"""
                - **{total_cum:,}** total solar installations across San Diego
                - **{speed_improvement:.0f}%** faster solar permit approval in the Expedited Era
                  ({exp_days:.0f} days vs {pre_days:.0f} days median)
                - **{exp_count:,}** solar permits in the Expedited Era vs **{pre_count:,}** Pre-CAP
                - Solar permits represent **{float(query(f"SELECT ROUND(SUM(CASE WHEN is_solar THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct FROM '{_PROCESSED}/climate_permits.parquet'").iloc[0][0]):.1f}%** of all development permits
                """)
