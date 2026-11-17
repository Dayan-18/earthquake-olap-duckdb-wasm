"""
Data quality gate: the deploy job fails if any check fails,
so a broken extraction never reaches the public dashboard.
"""
from __future__ import annotations

import pathlib
import sys

import duckdb

DATA = pathlib.Path(__file__).resolve().parents[1] / "web" / "data"

con = duckdb.connect()
for name in ["fact_earthquake", "dim_date", "dim_region", "dim_magnitude", "dim_depth"]:
    con.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{(DATA / (name + '.parquet')).as_posix()}')")

CHECKS = {
    "fact table is not empty":
        "SELECT count(*) = 0 FROM fact_earthquake",
    "event_id is unique":
        "SELECT count(*) - count(DISTINCT event_id) FROM fact_earthquake",
    "no NULL foreign keys":
        "SELECT count(*) FROM fact_earthquake WHERE date_key IS NULL OR region_key IS NULL "
        "OR magnitude_key IS NULL OR depth_key IS NULL",
    "every date_key exists in dim_date":
        "SELECT count(*) FROM fact_earthquake f ANTI JOIN dim_date d USING (date_key)",
    "every region_key exists in dim_region":
        "SELECT count(*) FROM fact_earthquake f ANTI JOIN dim_region r USING (region_key)",
    "every magnitude_key exists in dim_magnitude":
        "SELECT count(*) FROM fact_earthquake f ANTI JOIN dim_magnitude m USING (magnitude_key)",
    "every depth_key exists in dim_depth":
        "SELECT count(*) FROM fact_earthquake f ANTI JOIN dim_depth d USING (depth_key)",
    "magnitude within physical range [-2, 10]":
        "SELECT count(*) FROM fact_earthquake WHERE magnitude NOT BETWEEN -2 AND 10",
    "latitude/longitude within valid range":
        "SELECT count(*) FROM fact_earthquake WHERE latitude NOT BETWEEN -90 AND 90 "
        "OR longitude NOT BETWEEN -180 AND 180",
    "dim_date has no gaps":
        "SELECT (max(full_date) - min(full_date) + 1) - count(*) FROM dim_date",
}

failed = 0
for label, sql in CHECKS.items():
    bad = con.execute(sql).fetchone()[0]
    ok = not bad
    failed += not ok
    print(f"[{'PASS' if ok else 'FAIL'}] {label}" + ("" if ok else f" -> {bad}"))

print(f"\n{len(CHECKS) - failed}/{len(CHECKS)} checks passed")
sys.exit(1 if failed else 0)
