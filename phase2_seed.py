"""Phase 2: download the rest of the historical seed once and pack it.

Page 1 is already on disk. Each later page is a time window that stays at
or under USGS's 20,000-event cap. Raw GeoJSON stays in data/raw. One
parquet file is the table the later queries read. Re-running this script
skips files that are already saved.
"""

import json
from datetime import timedelta
from pathlib import Path

import duckdb

from phase1_source import (
    CATALOG,
    PAGE_LIMIT,
    RAW,
    SEED_END,
    count_window,
    curl_bytes,
    format_day,
    iso_from_ms,
    parse_day,
    query_url,
)

SEED = Path(__file__).resolve().parent / "data" / "seed"
MANIFEST_PATH = RAW / "seed_manifest.json"
PARQUET_PATH = SEED / "earthquakes.parquet"
TARGET_ROWS = 18000


def load_manifest():
    if MANIFEST_PATH.exists():
        return json.loads(MANIFEST_PATH.read_text())
    report = json.loads((RAW / "source_report.json").read_text())
    first = report["first_page"]
    return {
        "slice_end": SEED_END,
        "pages": [
            {
                "start": first["start"],
                "end": first["end"],
                "count": first["count"],
                "path": first["path"],
                "bytes": first["bytes"],
            }
        ],
    }


def save_manifest(manifest):
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2) + "\n")


def choose_end(start, hard_end, rate):
    """Pick the far end of the next page. start and hard_end are dates."""
    span_days = max(14, int(TARGET_ROWS / max(rate, 1)))
    end = min(start + timedelta(days=span_days), hard_end)
    total = count_window(format_day(start), format_day(end))

    while total > PAGE_LIMIT and (end - start).days > 1:
        span_days = int((end - start).days * TARGET_ROWS / total)
        span_days = max(1, min(span_days, (end - start).days - 1))
        end = start + timedelta(days=span_days)
        total = count_window(format_day(start), format_day(end))

    if total > PAGE_LIMIT:
        raise SystemExit(f"{start} .. {end} still has {total} events, over {PAGE_LIMIT}")

    if total < 14000 and end < hard_end and total > 0:
        per_day = total / max((end - start).days, 1)
        extra = int((TARGET_ROWS - total) / per_day)
        if extra >= 1:
            stretched = min(end + timedelta(days=extra), hard_end)
            stretched_total = count_window(format_day(start), format_day(stretched))
            if stretched_total <= PAGE_LIMIT:
                end, total = stretched, stretched_total

    return end, total


def download_page(start, end, total):
    path = RAW / f"catalog_{format_day(start)}_{format_day(end)}.geojson"
    if path.exists():
        print(f"using saved {path.name}")
        return path

    url = query_url(
        CATALOG,
        format_day(start),
        format_day(end),
        {"orderby": "time-asc", "limit": str(PAGE_LIMIT)},
    )
    print(f"downloading {path.name}")
    payload = curl_bytes(url, timeout=180)
    partial = path.with_suffix(".geojson.partial")
    partial.write_bytes(payload)
    page = json.loads(payload)
    got = len(page["features"])
    if got != total:
        partial.unlink(missing_ok=True)
        raise SystemExit(f"{path.name}: count said {total}, file has {got}")
    partial.replace(path)
    return path


def rows_from_page(path):
    page = json.loads(path.read_text())
    for feature in page["features"]:
        props = feature["properties"]
        coords = (feature.get("geometry") or {}).get("coordinates") or [None, None, None]
        lon, lat, depth = (list(coords) + [None, None, None])[:3]
        yield (
            feature["id"],
            iso_from_ms(props["time"]).replace("Z", ""),
            iso_from_ms(props["updated"]).replace("Z", ""),
            props.get("mag"),
            props.get("place"),
            lon,
            lat,
            depth,
            props.get("type"),
        )


def write_parquet(pages):
    SEED.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(
        """
        CREATE TABLE events (
            event_id VARCHAR,
            event_time TIMESTAMP,
            updated TIMESTAMP,
            mag DOUBLE,
            place VARCHAR,
            lon DOUBLE,
            lat DOUBLE,
            depth_km DOUBLE,
            event_type VARCHAR
        )
        """
    )
    for page in pages:
        path = Path(__file__).resolve().parent / page["path"]
        rows = list(rows_from_page(path))
        con.executemany("INSERT INTO events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
        print(f"loaded {page['path']} ({len(rows)} rows)")

    con.execute(
        f"""
        COPY (
            SELECT event_id, event_time, updated, mag, place, lon, lat, depth_km, event_type
            FROM (
                SELECT *,
                       row_number() OVER (
                           PARTITION BY event_id
                           ORDER BY updated DESC
                       ) AS rn
                FROM events
            )
            WHERE rn = 1
            ORDER BY event_time, event_id
        ) TO '{PARQUET_PATH}' (FORMAT PARQUET)
        """
    )
    summary = con.execute(
        f"""
        SELECT
            (SELECT count(*) FROM events) AS loaded_rows,
            (SELECT count(*) FROM read_parquet('{PARQUET_PATH}')) AS kept_rows,
            (SELECT count(DISTINCT event_id) FROM events) AS distinct_ids,
            (SELECT min(event_time) FROM read_parquet('{PARQUET_PATH}')) AS oldest,
            (SELECT max(event_time) FROM read_parquet('{PARQUET_PATH}')) AS newest
        """
    ).fetchone()
    con.close()
    return {
        "loaded_rows": summary[0],
        "kept_rows": summary[1],
        "distinct_ids": summary[2],
        "boundary_duplicates": summary[0] - summary[2],
        "oldest": str(summary[3]),
        "newest": str(summary[4]),
        "parquet": str(PARQUET_PATH.relative_to(Path(__file__).resolve().parent)),
        "parquet_bytes": PARQUET_PATH.stat().st_size,
    }


def main():
    RAW.mkdir(parents=True, exist_ok=True)
    manifest = load_manifest()
    hard_end = parse_day(SEED_END)
    start = parse_day(manifest["pages"][-1]["end"])
    previous = manifest["pages"][-1]
    span_days = max((parse_day(previous["end"]) - parse_day(previous["start"])).days, 1)
    rate = previous["count"] / span_days

    while start < hard_end:
        end, total = choose_end(start, hard_end, rate)
        if end <= start:
            raise SystemExit(f"window did not advance from {start}")
        path = download_page(start, end, total)
        manifest["pages"].append(
            {
                "start": format_day(start),
                "end": format_day(end),
                "count": total,
                "path": str(path.relative_to(Path(__file__).resolve().parent)),
                "bytes": path.stat().st_size,
            }
        )
        save_manifest(manifest)
        rate = total / max((end - start).days, 1)
        print(f"page {len(manifest['pages'])}: {format_day(start)} .. {format_day(end)} = {total}")
        start = end

    page_rows = sum(page["count"] for page in manifest["pages"])
    print(f"raw pages={len(manifest['pages'])} counted rows={page_rows}")
    summary = write_parquet(manifest["pages"])
    summary["pages"] = len(manifest["pages"])
    summary["counted_rows"] = page_rows
    manifest["parquet"] = summary
    save_manifest(manifest)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
