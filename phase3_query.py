"""Read the seed and the live file together and show whether the job is alive.

A quake counts as pipeline output only when it has ingested_at. That stamp
is written by phase3_ingest.py. The historical rows do not have one.
"""

import duckdb
from pathlib import Path

from phase1_source import SEED_END

ROOT = Path(__file__).resolve().parent
SEED = ROOT / "data" / "seed" / "earthquakes.parquet"
LIVE = ROOT / "data" / "live" / "earthquakes.jsonl"


def main():
    if not LIVE.exists():
        raise SystemExit("no live file yet; run phase3_ingest.py first")
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.execute(
        f"""
        CREATE VIEW combined AS
        WITH seed AS (
            SELECT
                event_id, event_time, updated, mag, place, lon, lat, depth_km, event_type,
                NULL::TIMESTAMP AS ingested_at
            FROM read_parquet('{SEED}')
        ),
        live AS (
            SELECT
                event_id,
                event_time::TIMESTAMP AS event_time,
                updated::TIMESTAMP AS updated,
                mag, place, lon, lat, depth_km, event_type,
                ingested_at::TIMESTAMP AS ingested_at
            FROM (
                SELECT *,
                       row_number() OVER (PARTITION BY event_id ORDER BY updated DESC) AS rn
                FROM read_json_auto('{LIVE}')
            )
            WHERE rn = 1
        ),
        unioned AS (
            SELECT * FROM seed
            UNION ALL BY NAME
            SELECT * FROM live
        )
        SELECT * EXCLUDE (rn)
        FROM (
            SELECT *,
                   row_number() OVER (
                       PARTITION BY event_id
                       ORDER BY ingested_at DESC NULLS LAST
                   ) AS rn
            FROM unioned
        )
        WHERE rn = 1
        """
    )
    cutoff = SEED_END + " 00:00:00"
    total, oldest, newest = con.execute(
        "SELECT count(*), min(event_time), max(event_time) FROM combined"
    ).fetchone()
    live_rows, first_ingest, last_ingest = con.execute(
        """
        SELECT count(*), min(ingested_at), max(ingested_at)
        FROM combined
        WHERE ingested_at IS NOT NULL
        """
    ).fetchone()
    today = con.execute(
        f"""
        SELECT count(*)
        FROM combined
        WHERE event_time >= TIMESTAMP '{cutoff}'
        """
    ).fetchone()[0]
    typical = con.execute(
        f"""
        SELECT round(avg(n), 1)
        FROM (
            SELECT count(*) AS n
            FROM combined
            WHERE event_time < TIMESTAMP '{cutoff}'
            GROUP BY CAST(event_time AS DATE)
        )
        """
    ).fetchone()[0]
    print(f"combined rows={total}")
    print(f"event_time {oldest} .. {newest}")
    print(f"rows this job stored={live_rows}  ingested_at {first_ingest} .. {last_ingest}")
    print(f"quakes since the seed cutoff={today}")
    print(f"typical day in the seed={typical}")
    print("ingest batches:")
    for when, count in con.execute(
        """
        SELECT ingested_at, count(*)
        FROM combined
        WHERE ingested_at IS NOT NULL
        GROUP BY ingested_at
        ORDER BY ingested_at
        """
    ).fetchall():
        print(f"  {when}  {count}")


if __name__ == "__main__":
    main()
