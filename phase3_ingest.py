"""Phase 3: keep the pipeline alive after the historical seed.

The seed stops at 2026-11-12 00:00 UTC. Each run asks USGS for quakes
from six hours before the newest one already stored, through now. That
overlap is how a missed run gets filled in. Rows already saved are
skipped by event id. ingested_at is the time this job first stored the row,
which is separate from when the quake happened.
"""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from phase1_source import (
    CATALOG,
    PAGE_LIMIT,
    SEED_END,
    curl_bytes,
    iso_from_ms,
    query_url,
)

ROOT = Path(__file__).resolve().parent
LIVE_PATH = ROOT / "data" / "live" / "earthquakes.jsonl"
LOOKBACK = timedelta(hours=6)
CUTOFF = datetime.strptime(SEED_END, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def stamp(when):
    return when.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_stamp(text):
    return datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def load_live():
    rows = {}
    if not LIVE_PATH.exists():
        return rows
    for line in LIVE_PATH.read_text().splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        current = rows.get(row["event_id"])
        if current is None or row["updated"] > current["updated"]:
            if current is not None:
                row["ingested_at"] = current["ingested_at"]
            rows[row["event_id"]] = row
    return rows


def write_live(rows):
    LIVE_PATH.parent.mkdir(parents=True, exist_ok=True)
    ordered = sorted(rows.values(), key=lambda row: (row["event_time"], row["event_id"]))
    payload = "".join(json.dumps(row, separators=(",", ":")) + "\n" for row in ordered)
    partial = LIVE_PATH.with_suffix(".jsonl.partial")
    partial.write_text(payload)
    partial.replace(LIVE_PATH)


def row_from_feature(feature, ingested_at):
    props = feature["properties"]
    coords = (feature.get("geometry") or {}).get("coordinates") or [None, None, None]
    lon, lat, depth = (list(coords) + [None, None, None])[:3]
    return {
        "event_id": feature["id"],
        "event_time": iso_from_ms(props["time"]),
        "updated": iso_from_ms(props["updated"]),
        "mag": props.get("mag"),
        "place": props.get("place"),
        "lon": lon,
        "lat": lat,
        "depth_km": depth,
        "event_type": props.get("type"),
        "ingested_at": ingested_at,
    }


def fetch(start, end):
    url = query_url(
        CATALOG,
        stamp(start),
        stamp(end),
        {"orderby": "time-asc", "limit": str(PAGE_LIMIT)},
    )
    page = json.loads(curl_bytes(url, timeout=120))
    features = page["features"]
    if len(features) >= PAGE_LIMIT:
        raise SystemExit(f"live window returned {PAGE_LIMIT} events; it needs a shorter range")
    return features


def main():
    rows = load_live()
    now = datetime.now(timezone.utc)
    if rows:
        newest = max(parse_stamp(row["event_time"]) for row in rows.values())
        start = newest - LOOKBACK
    else:
        start = CUTOFF
    features = fetch(start, now)
    ingested_at = stamp(now)
    added = 0
    revised = 0
    skipped = 0
    for feature in features:
        row = row_from_feature(feature, ingested_at)
        if parse_stamp(row["event_time"]) < CUTOFF:
            skipped += 1
            continue
        current = rows.get(row["event_id"])
        if current is None:
            rows[row["event_id"]] = row
            added += 1
        elif row["updated"] > current["updated"]:
            row["ingested_at"] = current["ingested_at"]
            rows[row["event_id"]] = row
            revised += 1
        else:
            skipped += 1
    write_live(rows)
    print(f"window {stamp(start)} .. {stamp(now)}")
    print(f"fetched={len(features)} added={added} revised={revised} skipped={skipped}")
    print(f"live rows={len(rows)}  file={LIVE_PATH.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
