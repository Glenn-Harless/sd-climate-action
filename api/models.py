"""Pydantic response models for FastAPI's auto-generated OpenAPI docs."""

from __future__ import annotations

from pydantic import BaseModel


class FilterOptions(BaseModel):
    years: list[int]
    zip_codes: list[str]
    permit_categories: list[str]
    policy_eras: list[str]


class OverviewResponse(BaseModel):
    total_solar: int
    cumulative_solar: int
    solar_pct: float
    median_approval_days: float


class SolarAdoption(BaseModel):
    year: int
    solar_count: int
    cumulative_solar: int
    total_valuation: int
    median_approval_days: float | None


class SolarByZip(BaseModel):
    zip_code: str
    solar_count: int
    total_valuation: int


class ApprovalSpeed(BaseModel):
    year: int
    permit_category: str
    policy_era: str | None
    permit_count: int
    median_days: float | None
    avg_days: int | None
    p90_days: int | None


class EnergyPermitTrend(BaseModel):
    year: int
    solar_count: int
    electrical_count: int
    mechanical_count: int
    climate_total: int


class ZipCodeEquity(BaseModel):
    zip_code: str
    total_permits: int
    solar_count: int
    electrical_count: int
    mechanical_count: int
    climate_count: int
    solar_pct: float
    total_valuation: int


class MonthlyTrend(BaseModel):
    year: int
    month: int
    permit_category: str
    permit_count: int


class PolicyEraComparison(BaseModel):
    policy_era: str
    total_permits: int
    median_days: float | None
    avg_days: int | None
    p90_days: int | None


class SolarMapPoint(BaseModel):
    lat: float
    lng: float
    year: int | None
    valuation: float | None
    zip_code: str | None
    approval_days: int | None
    policy_era: str | None


class EnergyConsumption(BaseModel):
    year: int
    quarter: int
    customer_class: str
    total_kwh: int
    elec_customers: int
    total_thm: int
    gas_customers: int


class EnergyVsSolar(BaseModel):
    zip_code: str
    solar_count: int
    avg_kwh_per_customer: int | None
    total_kwh: int
