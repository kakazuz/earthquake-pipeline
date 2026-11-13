"""Phase 1: confirm the USGS source and save the first catalog page.

Historical seed: magnitude 2.5+ earthquakes from 2015-01-01 until the start
of 2026-11-12 (UTC). That slice is about a third of a million events.
USGS returns HTTP 400 above 20,000 events per call, and a limit on an
11-year window times out, so the first page is the longest window that
starts at 2015-01-01 and still fits in one response.

The live path is the past-hour feed for magnitude 2.5+, a separate URL
that USGS rebuilds every minute.

Python on this Mac does not ship a CA bundle that urllib trusts, so HTTPS
goes through curl.
"""

import json
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
RAW = ROOT / "data" / "raw"

CATALOG = "https://earthquake.usgs.gov/fdsnws/event/1/query"
COUNT = "https://earthquake.usgs.gov/fdsnws/event/1/count"
FEED = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_hour.geojson"
DAY_FEED = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_day.geojson"

MIN_MAGNITUDE = 2.5
EVENT_TYPE = "earthquake"
SEED_START = "2015-01-01"
SEED_END = "2026-11-12"
PAGE_LIMIT = 20000

PAGE_PATH = RAW / "catalog_page_0001.geojson"
FEED_PATH = RAW / "feed_2.5_hour.geojson"
DAY_FEED_PATH = RAW / "feed_2.5_day.geojson"
REPORT_PATH = RAW / "source_report.json"


def curl_bytes(url, timeout, attempts=4):
    last_error = ""
    for attempt in range(attempts):
        result = subprocess.run(
            ["curl", "-fsS", "--compressed", "--max-time", str(timeout), url],
            capture_output=True,
        )
        if result.returncode == 0:
            return result.stdout
        last_error = result.stderr.decode(errors="replace")
        retry = any(code in last_error for code in ("429", "502", "503", "504"))
        if retry and attempt + 1 < attempts:
            time.sleep(5 * (attempt + 1))
            continue
        raise subprocess.CalledProcessError(
            result.returncode, result.args, result.stdout, result.stderr
        )
    raise RuntimeError(last_error)


def curl_text(url, timeout):
    return curl_bytes(url, timeout).decode().strip()


def query_url(base, start, end, extra=None):
    params = {
        "format": "geojson",
        "eventtype": EVENT_TYPE,
        "minmagnitude": str(MIN_MAGNITUDE),
        "starttime": start,
        "endtime": end,
    }
    if extra:
        params.update(extra)
    if base == COUNT:
        params["format"] = "text"
    pieces = [f"{key}={value}" for key, value in params.items()]
    return base + "?" + "&".join(pieces)


def count_window(start, end):
    url = query_url(COUNT, start, end)
    total = int(curl_text(url, timeout=120))
    print(f"  count {start} .. {end} = {total}")
    return total


def parse_day(text):
    return datetime.strptime(text, "%Y-%m-%d").date()


def format_day(day):
    return day.strftime("%Y-%m-%d")


def first_page_end(seed_start, seed_end):
    """Latest end date whose [seed_start, end) window has at most PAGE_LIMIT rows."""
    start = parse_day(seed_start)
    hard_end = parse_day(seed_end)
    span_days = 30
    under_end = None
    under_count = None
    over_end = None

    while True:
        candidate = min(start + timedelta(days=span_days), hard_end)
        total = count_window(seed_start, format_day(candidate))
        if total <= PAGE_LIMIT:
            under_end = candidate
            under_count = total
            if candidate == hard_end or total == PAGE_LIMIT:
                return format_day(candidate), total
            span_days *= 2
            continue
        over_end = candidate
        break

    if under_end is None:
        raise SystemExit(f"even {span_days} days from {seed_start} exceeds {PAGE_LIMIT} events")

    lo = under_end
    hi = over_end
    lo_count = under_count
    while (hi - lo).days > 1:
        mid = lo + timedelta(days=(hi - lo).days // 2)
        total = count_window(seed_start, format_day(mid))
        if total <= PAGE_LIMIT:
            lo = mid
            lo_count = total
        else:
            hi = mid
    return format_day(lo), lo_count


def iso_from_ms(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def summarize_feature(feature):
    props = feature["properties"]
    coords = (feature.get("geometry") or {}).get("coordinates") or [None, None, None]
    lon, lat, depth = (list(coords) + [None, None, None])[:3]
    return {
        "event_id": feature.get("id"),
        "event_time": iso_from_ms(props["time"]),
        "updated": iso_from_ms(props["updated"]),
        "mag": props.get("mag"),
        "place": props.get("place"),
        "lon": lon,
        "lat": lat,
        "depth_km": depth,
        "type": props.get("type"),
    }


def load_or_download(path, url, timeout, label):
    if path.exists():
        print(f"using saved {label} {path}")
        return json.loads(path.read_text())
    print(f"downloading {label}")
    path.write_bytes(curl_bytes(url, timeout))
    return json.loads(path.read_text())


def main():
    RAW.mkdir(parents=True, exist_ok=True)

    print(f"catalog slice {SEED_START} .. {SEED_END}  M{MIN_MAGNITUDE}+ {EVENT_TYPE}")
    slice_count = count_window(SEED_START, SEED_END)
    pages = (slice_count + PAGE_LIMIT - 1) // PAGE_LIMIT
    print(f"pages at {PAGE_LIMIT}/page = {pages}")

    page_end, page_count = first_page_end(SEED_START, SEED_END)
    page_url = query_url(
        CATALOG,
        SEED_START,
        page_end,
        {"orderby": "time-asc", "limit": str(PAGE_LIMIT)},
    )
    page = load_or_download(PAGE_PATH, page_url, 180, "catalog page")
    features = page["features"]
    if len(features) != page_count:
        raise SystemExit(f"count said {page_count} events, file has {len(features)}")
    if len(features) > PAGE_LIMIT:
        raise SystemExit(f"page has {len(features)} events, over the {PAGE_LIMIT} cap")

    feed = load_or_download(FEED_PATH, FEED, 60, "hour feed")
    feed_features = feed["features"]
    quakes = [f for f in feed_features if f["properties"].get("type") == EVENT_TYPE]
    catalog_keys = set(features[0]["properties"].keys())

    # A quiet hour has no features, so the day feed is what we compare fields against.
    schema_features = feed_features
    schema_source = FEED
    if not schema_features:
        day = load_or_download(DAY_FEED_PATH, DAY_FEED, 60, "day feed")
        schema_features = day["features"]
        schema_source = DAY_FEED
    schema_keys = set(schema_features[0]["properties"].keys()) if schema_features else set()

    report = {
        "slice": {
            "min_magnitude": MIN_MAGNITUDE,
            "event_type": EVENT_TYPE,
            "start": SEED_START,
            "end": SEED_END,
            "count": slice_count,
            "page_limit": PAGE_LIMIT,
            "pages_required": pages,
        },
        "first_page": {
            "start": SEED_START,
            "end": page_end,
            "count": page_count,
            "url": page_url,
            "path": str(PAGE_PATH.relative_to(ROOT)),
            "bytes": PAGE_PATH.stat().st_size,
            "features": len(features),
            "first": summarize_feature(features[0]),
            "last": summarize_feature(features[-1]),
            "property_keys": sorted(catalog_keys),
        },
        "hour_feed": {
            "url": FEED,
            "path": str(FEED_PATH.relative_to(ROOT)),
            "bytes": FEED_PATH.stat().st_size,
            "metadata_count": feed.get("metadata", {}).get("count"),
            "features": len(feed_features),
            "earthquakes": len(quakes),
            "generated": iso_from_ms(feed["metadata"]["generated"]),
            "sample": summarize_feature(feed_features[0]) if feed_features else None,
            "schema_checked_against": schema_source,
            "property_keys_match_catalog": catalog_keys == schema_keys,
        },
    }
    REPORT_PATH.write_text(json.dumps(report, indent=2) + "\n")

    first = report["first_page"]["first"]
    last = report["first_page"]["last"]
    print(f"first page {SEED_START} .. {page_end}  {len(features)} events  {PAGE_PATH.stat().st_size} bytes")
    print(f"  oldest {first['event_time']}  M{first['mag']}  {first['event_id']}")
    print(f"  newest {last['event_time']}  M{last['mag']}  {last['event_id']}")
    print(
        f"hour feed features={len(feed_features)} earthquakes={len(quakes)} "
        f"generated={report['hour_feed']['generated']}"
    )
    print(f"property keys match = {catalog_keys == schema_keys}  (checked against {schema_source})")
    print(f"wrote {REPORT_PATH.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
