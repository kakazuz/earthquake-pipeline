# Earthquake pipeline

Magnitude 2.5+ earthquakes from the USGS catalog.

The historical seed is a one-time download on this machine: 2015-01-01 through 2026-11-12 00:00 UTC, stored in `data/seed/earthquakes.parquet`. Those files are gitignored. The live job only adds quakes at or after that cutoff.

## What runs on a timer

GitHub Actions runs `phase3_ingest.py` every 30 minutes. The script asks USGS for quakes from six hours before the newest one already stored, through now. Each quake is kept or dropped by its event id:

- A new id is appended to `data/live/earthquakes.jsonl`, with `ingested_at` set to the time of that run.
- The same id with a newer USGS `updated` time replaces the stored row. `ingested_at` stays at the first time it was stored.
- The same id with the same or an older `updated` time is dropped.

If the file changed, the Action commits it. A run that finds nothing new is a success.

The six-hour overlap is the backfill. If a run fails or GitHub starts late, the next run still asks for the gap after the last stored quake, plus six hours behind it. The seed and the live file meet at 2026-11-12 00:00 UTC. Quakes before that cutoff are left in the seed snapshot even if USGS revises them later.

## Query

On a machine that has the seed file:

```bash
python3 phase3_query.py
```

That reads the seed and the live file together and keeps one row per event id.

## Cost

$0. USGS is a public feed with no key. GitHub-hosted runners on a public repo are inside the free tier for a job this small. Nothing here is a paid cluster, so there is nothing to tear down except disabling the Action after the one-week check.
