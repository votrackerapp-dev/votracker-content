#!/usr/bin/env python3
import argparse
import datetime as dt
import hashlib
import json
import re
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser
from dateutil import tz as datetz


# -----------------------------
# Helpers
# -----------------------------

def canonicalize_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    try:
        u = url.strip()
        p = urlparse(u)
        scheme = p.scheme or "https"
        netloc = p.netloc.lower()
        path = p.path.rstrip("/") or "/"

        # Drop common tracking params
        q = [(k, v) for (k, v) in parse_qsl(p.query, keep_blank_values=True)
             if not k.lower().startswith("utm_") and k.lower() not in {"fbclid", "gclid"}]
        query = urlencode(q, doseq=True)

        return urlunparse((scheme, netloc, path, "", query, ""))
    except Exception:
        return url


def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def isoformat_with_tz(d: dt.datetime) -> str:
    # Always include offset
    if d.tzinfo is None:
        d = d.replace(tzinfo=datetz.UTC)
    return d.isoformat()


def parse_date(value: Any, default_tz: str) -> Optional[dt.datetime]:
    """
    Parses many date formats into a timezone-aware datetime.
    If the parsed datetime has no tz, we apply default_tz.
    """
    if not value:
        return None
    try:
        d = dateparser.parse(str(value))
        if d is None:
            return None
        if d.tzinfo is None:
            d = d.replace(tzinfo=datetz.gettz(default_tz))
        return d
    except Exception:
        return None


def safe_text(s: Optional[str], max_len: int = 6000) -> Optional[str]:
    if s is None:
        return None
    t = re.sub(r"\s+", " ", s).strip()
    if not t:
        return None
    return t[:max_len]


# -----------------------------
# Extractors
# -----------------------------

def fetch_html(url: str) -> str:
    r = requests.get(url, timeout=25, headers={"User-Agent": "VOTrackerWorkshopBot/1.0"})
    r.raise_for_status()
    return r.text


def extract_jsonld_events(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """
    Looks for schema.org Event JSON-LD blocks and extracts events.
    Works on many sites that publish structured Event data.
    """
    html = fetch_html(source["url"])
    soup = BeautifulSoup(html, "html.parser")
    scripts = soup.find_all("script", attrs={"type": "application/ld+json"})
    events: List[Dict[str, Any]] = []

    def handle_obj(obj: Any):
        if isinstance(obj, list):
            for it in obj:
                handle_obj(it)
            return
        if not isinstance(obj, dict):
            return

        # Sometimes Event is inside @graph
        if "@graph" in obj and isinstance(obj["@graph"], list):
            for g in obj["@graph"]:
                handle_obj(g)
            return

        t = obj.get("@type")
        if isinstance(t, list):
            if "Event" not in t:
                return
        elif t != "Event":
            return

        name = obj.get("name")
        start = parse_date(obj.get("startDate"), default_tz)
        end = parse_date(obj.get("endDate"), default_tz) or start
        url = obj.get("url") or source["url"]
        desc = obj.get("description")
        image = obj.get("image")

        # Location can be a string or object
        venue = None
        city = None
        state = None
        loc = obj.get("location")
        if isinstance(loc, dict):
            venue = loc.get("name")
            addr = loc.get("address")
            if isinstance(addr, dict):
                city = addr.get("addressLocality")
                state = addr.get("addressRegion")

        event = {
            "title": safe_text(name) or "Workshop",
            "host": source.get("name"),
            "city": safe_text(city),
            "state": safe_text(state),
            "venue": safe_text(venue),
            "startAt": isoformat_with_tz(start) if start else None,
            "endAt": isoformat_with_tz(end) if end else None,
            "registrationURL": canonicalize_url(url),
            "imageURL": image if isinstance(image, str) else None,
            "detail": safe_text(desc),
            "links": [{"title": "Event Page", "url": canonicalize_url(url)}] if url else None,
            "_source": source["id"]
        }
        if event["startAt"]:
            events.append(event)

    for s in scripts:
        raw = s.get_text(strip=True)
        if not raw:
            continue
        try:
            obj = json.loads(raw)
            handle_obj(obj)
        except Exception:
            continue

    return events


def extract_soundonstudio_classsignup(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """
    SoundOnStudio's class signup page shows items like:
      "2.23.26 - Making Promos Pop! with Darius Marquis Johnson ..."
    We'll parse those. Times may not exist; we default to 6:00pm local if missing.
    """
    html = fetch_html(source["url"])
    text = BeautifulSoup(html, "html.parser").get_text("\n")

    # date format like 2.23.26
    pattern = re.compile(r"\b(\d{1,2})\.(\d{1,2})\.(\d{2})\s*-\s*([^\n$]+)")
    events: List[Dict[str, Any]] = []

    for m in pattern.finditer(text):
        mm, dd, yy = int(m.group(1)), int(m.group(2)), int(m.group(3))
        year = 2000 + yy
        title = safe_text(m.group(4))
        if not title:
            continue

        # Default time: 6:00pm PT (reasonable placeholder)
        start = dt.datetime(year, mm, dd, 18, 0, tzinfo=datetz.gettz(default_tz))
        end = start + dt.timedelta(hours=2)

        events.append({
            "title": title,
            "host": source.get("name"),
            "city": None,
            "state": None,
            "venue": "Online / See listing",
            "startAt": isoformat_with_tz(start),
            "endAt": isoformat_with_tz(end),
            "registrationURL": canonicalize_url(source["url"]),
            "imageURL": None,
            "detail": None,
            "links": [{"title": "Class Signup", "url": canonicalize_url(source["url"])}],
            "_source": source["id"]
        })

    return events


def extract_html_fallback(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """
    Fallback extractor:
    - Try JSON-LD first (many sites have it)
    - If none found, do nothing (safe)
    """
    events = extract_jsonld_events(source, default_tz)
    return events


EXTRACTORS = {
    "jsonld_events": extract_jsonld_events,
    "soundonstudio_classsignup": extract_soundonstudio_classsignup,
    "html_fallback": extract_html_fallback
}


# -----------------------------
# ID + Merge (NO DUPES)
# -----------------------------

def compute_event_id(e: Dict[str, Any]) -> str:
    """
    Stable ID:
      - If registrationURL exists, use source + canonical URL
      - else use source + (title + startAt)
    """
    source_id = e.get("_source") or "unknown"
    url = canonicalize_url(e.get("registrationURL")) or canonicalize_url(
        (e.get("links") or [{}])[0].get("url") if e.get("links") else None
    )

    if url:
        key = f"{source_id}|url|{url}"
    else:
        key = f"{source_id}|ts|{(e.get('title') or '').strip().lower()}|{e.get('startAt') or ''}"

    # Nice readable prefix, but still stable
    return f"{source_id}-{sha1(key)[:16]}"


def normalize_workshop(e: Dict[str, Any], default_tz: str) -> Optional[Dict[str, Any]]:
    # Ensure required
    start = parse_date(e.get("startAt"), default_tz)
    if not start:
        return None
    end = parse_date(e.get("endAt"), default_tz) or start

    # Normalize urls
    reg = canonicalize_url(e.get("registrationURL"))
    links = e.get("links") or None
    if links:
        cleaned_links = []
        for l in links:
            if not isinstance(l, dict):
                continue
            t = safe_text(l.get("title"), 80)
            u = canonicalize_url(l.get("url"))
            if t and u:
                cleaned_links.append({"title": t, "url": u})
        links = cleaned_links or None

    out = {
        "id": None,  # computed below
        "title": safe_text(e.get("title"), 200) or "Workshop",
        "host": safe_text(e.get("host"), 120),
        "city": safe_text(e.get("city"), 80),
        "state": safe_text(e.get("state"), 40),
        "venue": safe_text(e.get("venue"), 160),
        "startAt": isoformat_with_tz(start),
        "endAt": isoformat_with_tz(end),
        "registrationURL": reg,
        "imageURL": e.get("imageURL"),
        "detail": safe_text(e.get("detail")),
        "links": links
    }

    # Keep source internally to compute stable id, then remove
    out["_source"] = e.get("_source")
    out["id"] = compute_event_id(out)
    out.pop("_source", None)

    return out


def dedupe_by_id(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = {}
    for it in items:
        i = it.get("id")
        if not i:
            continue
        # Prefer the one that has a registrationURL
        if i not in seen:
            seen[i] = it
        else:
            prev = seen[i]
            if (not prev.get("registrationURL")) and it.get("registrationURL"):
                seen[i] = it
    return list(seen.values())


def merge_workshops(existing: List[Dict[str, Any]], incoming: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_id = {w.get("id"): w for w in existing if w.get("id")}
    for w in incoming:
        wid = w.get("id")
        if not wid:
            continue
        if wid in by_id:
            # Update in place, but don't wipe good old fields with empty new ones
            cur = by_id[wid]
            for k, v in w.items():
                if v is not None and v != "":
                    cur[k] = v
        else:
            by_id[wid] = w

    merged = list(by_id.values())
    merged = dedupe_by_id(merged)

    # Sort by start date
    def keyfn(x):
        try:
            return dateparser.parse(x.get("startAt")).timestamp()
        except Exception:
            return float("inf")

    merged.sort(key=keyfn)
    return merged


def prune_events(items: List[Dict[str, Any]], prune_days_past: int, keep_days_future: int, default_tz: str) -> List[Dict[str, Any]]:
    now = dt.datetime.now(tz=datetz.gettz(default_tz))
    past_cut = now - dt.timedelta(days=prune_days_past)
    future_cut = now + dt.timedelta(days=keep_days_future)

    kept = []
    for w in items:
        start = parse_date(w.get("startAt"), default_tz)
        end = parse_date(w.get("endAt"), default_tz) or start
        if not start:
            continue
        # Keep if it's not too far past, and not too far in the future
        if end >= past_cut and start <= future_cut:
            kept.append(w)
    return kept


# -----------------------------
# Main
# -----------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--resources", required=True)
    ap.add_argument("--sources", required=True)
    ap.add_argument("--default-tz", default="America/Los_Angeles")
    args = ap.parse_args()

    with open(args.sources, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    sources = cfg.get("sources", [])
    settings = cfg.get("settings", {})
    max_events_per_source = int(settings.get("max_events_per_source", 50))
    prune_days_past = int(settings.get("prune_days_past", 7))
    keep_days_future = int(settings.get("keep_days_future", 365))

    with open(args.resources, "r", encoding="utf-8") as f:
        resources = json.load(f)

    existing = resources.get("workshops", []) or []

    incoming_all: List[Dict[str, Any]] = []

    for s in sources:
        extractor_name = s.get("extractor", "jsonld_events")
        fn = EXTRACTORS.get(extractor_name)
        if not fn:
            print(f"[WARN] Unknown extractor '{extractor_name}' for {s.get('id')}, skipping.")
            continue

        try:
            raw_events = fn(s, args.default_tz)
            raw_events = raw_events[:max_events_per_source]
            normalized = []
            for e in raw_events:
                e["_source"] = s.get("id")
                nw = normalize_workshop(e, args.default_tz)
                if nw:
                    normalized.append(nw)
            print(f"[OK] {s.get('id')}: {len(normalized)} events")
            incoming_all.extend(normalized)
        except Exception as ex:
            print(f"[ERROR] {s.get('id')}: {ex}")

    # Merge + prune + final dedupe
    merged = merge_workshops(existing, incoming_all)
    merged = prune_events(merged, prune_days_past, keep_days_future, args.default_tz)
    merged = dedupe_by_id(merged)

    resources["workshops"] = merged

    # bump lastUpdated if changed
    resources["lastUpdated"] = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")

    with open(args.resources, "w", encoding="utf-8") as f:
        json.dump(resources, f, ensure_ascii=False, indent=2)
        f.write("\n")


if __name__ == "__main__":
    main()
