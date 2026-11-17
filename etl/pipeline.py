"""
Earthquake OLAP pipeline.

Extract : USGS FDSN Event API (public, no API key) -> raw CSV files, one per month.
Transform: DuckDB SQL -> star schema (1 fact table + 4 dimensions).
Load    : Parquet files (ZSTD) inside web/data/, queried in the browser by DuckDB-WASM.

Usage:
    python etl/pipeline.py --days 730 --min-mag 2.5
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import pathlib
import time

import duckdb
import requests

USGS_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
ROOT = pathlib.Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
OUT_DIR = ROOT / "web" / "data"

# USGS writes US locations as "..., CA" or "..., Alaska". Both map to a state + "United States".
US_STATES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California",
    "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware", "FL": "Florida", "GA": "Georgia",
    "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa",
    "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada", "NH": "New Hampshire",
    "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York", "NC": "North Carolina",
    "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania",
    "RI": "Rhode Island", "SC": "South Carolina", "SD": "South Dakota", "TN": "Tennessee",
    "TX": "Texas", "UT": "Utah", "VT": "Vermont", "VA": "Virginia", "WA": "Washington",
    "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming",
}
OTHER_CODES = {"MX": ("Mexico", "Mexico")}

TABLES = ["dim_date", "dim_region", "dim_magnitude", "dim_depth", "fact_earthquake"]


def month_windows(start: dt.date, end: dt.date):
    """Yield [start, end) windows aligned to calendar months (USGS caps a query at 20k rows)."""
    cur = start
    while cur < end:
        nxt = (cur.replace(day=1) + dt.timedelta(days=32)).replace(day=1)
        yield cur, min(nxt, end)
        cur = nxt


def fetch_csv(params: dict) -> str:
    for attempt in range(4):
        try:
            resp = requests.get(USGS_URL, params=params, timeout=120)
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as exc:
            if attempt == 3:
                raise
            wait = 5 * (attempt + 1)
            print(f"  retry in {wait}s ({exc})")
            time.sleep(wait)
    raise RuntimeError("unreachable")


def extract(days: int, min_mag: float) -> tuple[dt.date, dt.date]:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    for old in RAW_DIR.glob("*.csv"):
        old.unlink()

    end = dt.date.today() + dt.timedelta(days=1)
    start = end - dt.timedelta(days=days)
    total = 0
    for s, e in month_windows(start, end):
        text = fetch_csv({
            "format": "csv",
            "starttime": s.isoformat(),
            "endtime": e.isoformat(),
            "minmagnitude": min_mag,
            "eventtype": "earthquake",
            "orderby": "time-asc",
        })
        rows = max(text.count("\n") - 1, 0)
        total += rows
        (RAW_DIR / f"usgs_{s:%Y_%m_%d}.csv").write_text(text, encoding="utf-8")
        print(f"  extracted {s} -> {e}: {rows:>6} events")
    print(f"Extract done: {total} raw rows")
    return start, end


def transform_and_load(start: dt.date, end: dt.date, min_mag: float) -> dict:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    raw_glob = (RAW_DIR / "*.csv").as_posix()

    # Lookup table for "..., CA" / "..., Alaska" style place suffixes.
    lookup = [(code, name, "United States") for code, name in US_STATES.items()]
    lookup += [(name, name, "United States") for name in US_STATES.values()]
    lookup += [(code, region, country) for code, (region, country) in OTHER_CODES.items()]
    con.execute("CREATE TABLE area_lookup(area VARCHAR, region_name VARCHAR, country VARCHAR)")
    con.executemany("INSERT INTO area_lookup VALUES (?, ?, ?)", lookup)

    # ---------- staging: typed, deduplicated, cleaned ----------
    con.execute(f"""
        CREATE TABLE stg_earthquake AS
        WITH src AS (
            SELECT * FROM read_csv('{raw_glob}', header = true, all_varchar = true, union_by_name = true)
        ),
        typed AS (
            SELECT
                id                                              AS event_id,
                CAST(replace(time, 'Z', '') AS TIMESTAMP)       AS event_ts,
                CAST(replace(updated, 'Z', '') AS TIMESTAMP)    AS updated_ts,
                CAST(latitude AS DOUBLE)                        AS latitude,
                CAST(longitude AS DOUBLE)                       AS longitude,
                TRY_CAST(depth AS DOUBLE)                       AS depth_km,
                TRY_CAST(mag AS DOUBLE)                         AS magnitude,
                magType                                         AS mag_type,
                net                                             AS network,
                TRY_CAST(nst AS INTEGER)                        AS stations,
                TRY_CAST(gap AS DOUBLE)                         AS azimuthal_gap,
                status,
                COALESCE(NULLIF(trim(place), ''), 'Unknown')    AS place
            FROM src
            WHERE id IS NOT NULL
        )
        SELECT DISTINCT ON (event_id) *,
            -- area = text after the last comma ("10 km SW of X, Japan" -> "Japan"),
            -- or the whole place without the distance prefix ("Mid-Atlantic Ridge").
            trim(CASE
                WHEN place LIKE '%,%' THEN regexp_extract(place, ',\\s*([^,]+)$', 1)
                ELSE regexp_replace(place, '^\\d+(\\.\\d+)?\\s*km\\s+[NSEW]{{1,3}}\\s+of\\s+', '')
            END) AS area
        FROM typed
        WHERE magnitude IS NOT NULL AND depth_km IS NOT NULL AND magnitude >= {min_mag}
        ORDER BY event_id, updated_ts DESC
    """)

    # ---------- dimensions ----------
    con.execute(f"""
        CREATE TABLE dim_date AS
        SELECT
            CAST(strftime(d, '%Y%m%d') AS INTEGER) AS date_key,
            CAST(d AS DATE)                        AS full_date,
            year(d)                                AS year,
            quarter(d)                             AS quarter,
            month(d)                               AS month,
            strftime(d, '%B')                      AS month_name,
            weekofyear(d)                          AS iso_week,
            strftime(d, '%A')                      AS day_name,
            isodow(d) >= 6                         AS is_weekend
        FROM range(DATE '{start}', DATE '{end}', INTERVAL 1 DAY) t(d)
    """)

    con.execute("""
        CREATE TABLE dim_region AS
        WITH areas AS (
            SELECT DISTINCT
                COALESCE(l.region_name, s.area) AS region_name,
                COALESCE(l.country, regexp_replace(s.area, '\\s+region$', '')) AS country
            FROM stg_earthquake s
            LEFT JOIN area_lookup l ON l.area = s.area
        )
        SELECT row_number() OVER (ORDER BY country, region_name)::INTEGER AS region_key,
               region_name, country
        FROM areas
    """)

    con.execute("""
        CREATE TABLE dim_magnitude AS
        SELECT * FROM (VALUES
            (1, 'Minor',    -10.0, 4.0, 'Often felt, rarely causes damage'),
            (2, 'Light',      4.0, 5.0, 'Noticeable shaking, little damage'),
            (3, 'Moderate',   5.0, 6.0, 'Can damage poorly built structures'),
            (4, 'Strong',     6.0, 7.0, 'Destructive in populated areas'),
            (5, 'Major',      7.0, 8.0, 'Serious damage over large areas'),
            (6, 'Great',      8.0, 99.0, 'Can destroy communities near the epicenter')
        ) t(magnitude_key, magnitude_class, min_mag, max_mag, description)
    """)
    con.execute("ALTER TABLE dim_magnitude ALTER min_mag TYPE DOUBLE; ALTER TABLE dim_magnitude ALTER max_mag TYPE DOUBLE")

    con.execute("""
        CREATE TABLE dim_depth AS
        SELECT * FROM (VALUES
            (1, 'Shallow',      -100.0,   70.0),
            (2, 'Intermediate',   70.0,  300.0),
            (3, 'Deep',          300.0, 1000.0)
        ) t(depth_key, depth_class, min_km, max_km)
    """)
    con.execute("ALTER TABLE dim_depth ALTER min_km TYPE DOUBLE; ALTER TABLE dim_depth ALTER max_km TYPE DOUBLE")

    # ---------- fact ----------
    con.execute("""
        CREATE TABLE fact_earthquake AS
        SELECT
            s.event_id,
            CAST(strftime(s.event_ts, '%Y%m%d') AS INTEGER) AS date_key,
            r.region_key,
            m.magnitude_key,
            dp.depth_key,
            s.event_ts,
            hour(s.event_ts)          AS hour_utc,
            round(s.magnitude, 2)     AS magnitude,
            round(s.depth_km, 2)      AS depth_km,
            round(s.latitude, 4)      AS latitude,
            round(s.longitude, 4)     AS longitude,
            s.mag_type,
            s.network,
            s.stations,
            s.azimuthal_gap,
            s.status,
            s.place
        FROM stg_earthquake s
        LEFT JOIN area_lookup l ON l.area = s.area
        JOIN dim_region r
          ON r.region_name = COALESCE(l.region_name, s.area)
         AND r.country     = COALESCE(l.country, regexp_replace(s.area, '\\s+region$', ''))
        JOIN dim_magnitude m ON s.magnitude >= m.min_mag AND s.magnitude < m.max_mag
        JOIN dim_depth dp    ON s.depth_km  >= dp.min_km AND s.depth_km  < dp.max_km
        ORDER BY s.event_ts
    """)

    counts = {}
    for table in TABLES:
        out = (OUT_DIR / f"{table}.parquet").as_posix()
        con.execute(f"COPY {table} TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        counts[table] = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        print(f"  loaded {table:<16} {counts[table]:>7} rows")

    metadata = {
        "generated_at_utc": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": "USGS Earthquake Catalog (FDSN Event API)",
        "source_url": "https://earthquake.usgs.gov/fdsnws/event/1/",
        "start_date": start.isoformat(),
        "end_date": (end - dt.timedelta(days=1)).isoformat(),
        "min_magnitude": min_mag,
        "row_counts": counts,
    }
    (OUT_DIR / "metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    return metadata


def main() -> None:
    parser = argparse.ArgumentParser(description="USGS earthquakes -> star schema Parquet")
    parser.add_argument("--days", type=int, default=730, help="days of history to extract")
    parser.add_argument("--min-mag", type=float, default=2.5, help="minimum magnitude")
    args = parser.parse_args()

    print(f"Extracting last {args.days} days of M{args.min_mag}+ earthquakes from USGS...")
    start, end = extract(args.days, args.min_mag)
    print("Transforming with DuckDB and loading Parquet...")
    meta = transform_and_load(start, end, args.min_mag)
    print(json.dumps(meta, indent=2))


if __name__ == "__main__":
    main()
