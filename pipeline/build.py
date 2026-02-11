"""Orchestrator: ingest then transform."""

from __future__ import annotations

import argparse
import time

from pipeline.ingest import ingest
from pipeline.transform import transform


def main() -> None:
    parser = argparse.ArgumentParser(description="Build climate action data pipeline")
    parser.add_argument(
        "--force", action="store_true", help="Re-download all source files"
    )
    args = parser.parse_args()

    t0 = time.perf_counter()

    print("=== Climate Action Pipeline ===")
    print()
    print("Step 1/2: Ingest")
    ingest(force=args.force)

    print()
    print("Step 2/2: Transform")
    transform()

    elapsed = time.perf_counter() - t0
    print(f"\nPipeline complete in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
