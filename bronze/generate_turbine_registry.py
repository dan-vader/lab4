"""Generates synthetic turbine-registry snapshots for bronze ingestion."""
import argparse
import json
import random
import uuid
from datetime import datetime, timezone
from pathlib import Path

from common.logging_utils import get_logger

logger = get_logger(__name__)

MODELS = ["V90-2.0", "V112-3.3", "N100-2.5", "SG 6.0-155"]
LOCATIONS = ["Lviv", "Kyiv", "Odesa", "Kharkiv"]
STATUSES = ["active", "maintenance", "decommissioned"]


def build_snapshot(turbine_ids, overrides=None):
    overrides = overrides or {}
    rows = []
    for tid in turbine_ids:
        row = {
            "turbine_id": tid,
            "model": random.choice(MODELS),
            "rated_power_kw": random.choice([2000, 2500, 3300, 6000]),
            "location": random.choice(LOCATIONS),
            "status": random.choice(STATUSES),
            "source_ts": datetime.now(timezone.utc).isoformat(),
        }
        row.update(overrides.get(tid, {}))
        rows.append(row)
    return rows


def write_snapshot(rows, out_dir: Path, filename: str):
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / filename
    with open(path, "w") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")
    logger.info("Wrote %d rows to %s", len(rows), path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default="bronze/out_files")
    parser.add_argument("--turbines", type=int, default=5)
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    turbine_ids = [str(uuid.uuid4()) for _ in range(args.turbines)]

    base_rows = build_snapshot(turbine_ids)
    write_snapshot(base_rows, out_dir, "snapshot_v1.json")

    changed_id = turbine_ids[0]
    updated_rows = [dict(row) for row in base_rows]
    for row in updated_rows:
        if row["turbine_id"] == changed_id:
            row["status"] = "maintenance"
        row["source_ts"] = datetime.now(timezone.utc).isoformat()

    write_snapshot(updated_rows, out_dir, "snapshot_v2.json")

    logger.info("Turbine %s changed status between snapshots", changed_id)


if __name__ == "__main__":
    main()
