"""Download development permit CSVs and SDG&E energy data."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import httpx

_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = _ROOT / "data" / "raw"
SDGE_DIR = RAW_DIR / "sdge"

# ── Permit sources (same as sd-housing-permits) ──

PERMIT_SOURCES: dict[str, str] = {
    "set1_active": "https://seshat.datasd.org/development_permits_set1/permits_set1_active_datasd.csv",
    "set1_closed": "https://seshat.datasd.org/development_permits_set1/permits_set1_closed_datasd.csv",
    "set2_active": "https://seshat.datasd.org/development_permits_set2/permits_set2_active_datasd.csv",
    "set2_closed": "https://seshat.datasd.org/development_permits_set2/permits_set2_closed_datasd.csv",
    "tags": "https://seshat.datasd.org/development_permits_tags/permits_project_tags_datasd.csv",
}

# ── SDG&E energy data ──

SDGE_BASE = "https://energydata.sdge.com/downloadEnergyUsageFile?name="
SDGE_START_YEAR = 2012
SDGE_START_QUARTER = 1


def _sdge_urls() -> list[tuple[str, str]]:
    """Generate (filename, url) pairs for all SDG&E quarterly files."""
    today = date.today()
    current_year = today.year
    current_quarter = (today.month - 1) // 3 + 1

    pairs: list[tuple[str, str]] = []
    for year in range(SDGE_START_YEAR, current_year + 1):
        max_q = current_quarter if year == current_year else 4
        for q in range(1, max_q + 1):
            for fuel in ("ELEC", "GAS"):
                name = f"SDGE-{fuel}-{year}-Q{q}"
                url = f"{SDGE_BASE}{name}.csv"
                pairs.append((name, url))
    return pairs


def _download(name: str, url: str, dest: Path, *, force: bool = False) -> Path | None:
    """Download a single file. Skips if exists and force=False."""
    if dest.exists() and not force:
        print(f"  [skip] {name} ({dest.stat().st_size:,} bytes)")
        return dest

    print(f"  [download] {name} ...")
    try:
        with httpx.stream("GET", url, follow_redirects=True, timeout=300) as r:
            r.raise_for_status()
            with open(dest, "wb") as f:
                for chunk in r.iter_bytes(chunk_size=1 << 20):
                    f.write(chunk)
        print(f"  [done] {name} -> {dest.stat().st_size:,} bytes")
        return dest
    except httpx.HTTPStatusError as e:
        if e.response.status_code in (403, 404):
            print(f"  [warn] {name}: {e.response.status_code}, skipping")
            return None
        raise


def ingest(*, force: bool = False) -> None:
    """Download all source data: permits + SDG&E energy files."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    SDGE_DIR.mkdir(parents=True, exist_ok=True)

    # Permits
    print("Downloading permit CSVs ...")
    for name, url in PERMIT_SOURCES.items():
        _download(name, url, RAW_DIR / f"{name}.csv", force=force)

    # SDG&E energy
    print("Downloading SDG&E energy data ...")
    pairs = _sdge_urls()
    print(f"  {len(pairs)} quarterly files to check ...")
    downloaded = 0
    for name, url in pairs:
        result = _download(name, url, SDGE_DIR / f"{name}.csv", force=force)
        if result:
            downloaded += 1
    print(f"  SDG&E: {downloaded}/{len(pairs)} files available")


if __name__ == "__main__":
    ingest()
