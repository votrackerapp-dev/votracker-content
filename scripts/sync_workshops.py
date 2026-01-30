#!/usr/bin/env python3
"""
sync_workshops.py

Scrape workshop/class listings from multiple websites and merge them into resources.json
WITHOUT duplicating entries across runs.

Key goals:
- Stable IDs across runs (so the same workshop updates instead of duplicating)
- Normalize URLs (strip tracking params, normalize host/scheme, remove fragments)
- Merge behavior: incoming data improves existing entries, but never overwrites good
  existing fields with empty/None.
- Prune old events + cap future horizon (configurable)

Expected config JSON format (default: config/workshop_sources.json):

{
  "settings": {
    "max_events_per_source": 50,
    "prune_days_past": 7,
    "keep_days_future": 365
  },
  "sources": [
    { "id": "van", "name": "Voice Actors Network", "url": "https://voiceactorsnetwork.com/", "extractor": "jsonld_events" },
    { "id": "soundon", "name": "Sound On Studio", "url": "https://www.soundonstudio.com/classsignup", "extractor": "soundonstudio_classsignup" }
  ]
}
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode, urlparse, urlunparse, parse_qsl

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser
from dateutil import tz as datetz


# -----------------------------
# URL + ID Normalization
# -----------------------------

TRACKING_PARAMS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "fbclid", "gclid", "mc_cid", "mc_eid",
}


def sha1_hex(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def normalize_url(url: Optional[str]) -> Optional[str]:
    """
    Canonicalize a URL enough to make de-duping stable:
    - lowercase scheme/host
    - remove fragments
    - remove common tracking params
    - normalize trailing slash
    """
    if not url:
        return None
    u = url.strip()
    if not u:
        return None

    try:
        p = urlparse(u)

        scheme = (p.scheme or "https").lower()
        netloc = (p.netloc or "").lower()

        # If someone pasted a schemeless URL like //example.com/...
        if not netloc and p.path.startswith("//"):
            p2 = urlparse("https:" + p.path)
            scheme = "https"
            netloc = (p2.netloc or "").lower()
            path = p2.path
            query_raw = p2.query
        else:
            path = p.path or ""
            query_raw = p.query or ""

        # remove fragments
        fragment = ""

        # normalize path trailing slash (keep root)
        if path != "/" and path.endswith("/"):
            path = path[:-1]

        # strip tracking params
        q = []
        for k, v in parse_qsl(query_raw, keep_blank_values=True):
            if k in TRACKING_PARAMS:
                continue
            if k.lower().startswith("utm_"):
                continue
            q.append((k, v))
        query = urlencode(q, doseq=True)

        return urlunparse((scheme, netloc, path, "", query, fragment))
    except Exception:
        return u


def workshop_fingerprint(w: Dict[str, Any]) -> str:
    """
    Strong fingerprint for de-duping across runs.
    Priority:
      1) registrationURL (normalized)
      2) first links[].url (normalized)
      3) fallback hash of title|host|startAt
    """
    reg = normalize_url(w.get("registrationURL"))
    if reg:
        return f"url:{reg}"

    links = w.get("links") or []
    if isinstance(links, list) and links:
        first_url = normalize_url((links[0] or {}).get("url"))
        if first_url:
            return f"link:{first_url}"

    title = (w.get("title") or "").strip().lower()
    host = (w.get("host") or "").strip().lower()
    start = (w.get("startAt") or "").strip()
    raw = f"{title}|{host}|{start}"
    return f"fallback:{sha1_hex(raw)[:16]}"


def stable_workshop_id(source_id: str, fingerprint: str) -> str:
    """
    Stable ID that does NOT change across runs for the same source+event.
    """
    digest = sha1_hex(f"{source_id}|{fingerprint}")[:16]
    return f"{source_id}-{digest}"


# -----------------------------
# Parsing helpers
# -----------------------------

def safe_text(s: Optional[str], max_len: int = 6000) -> Optional[str]:
    if s is None:
        return None
    t = re.sub(r"\s+", " ", str(s)).strip()
    if not t:
        return None
    return t[:max_len]


def parse_dt(value: Any, default_tz: str) -> Optional[dt.datetime]:
    """
    Parses many date formats into timezone-aware datetime.
    If parsed datetime has no tz, applies default_tz.
    """
    if value is None:
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


def to_iso(d: dt.datetime) -> str:
    if d.tzinfo is None:
        d = d.replace(tzinfo=datetz.UTC)
    return d.isoformat()


def fetch_html(url: str) -> str:
    r = requests.get(
        url,
        timeout=30,
        headers={"User-Agent": "VOTrackerWorkshopBot/1.0 (+https://github.com/votrackerapp-dev)"},
    )
    r.raise_for_status()
    return r.text


# -----------------------------
# Extractors
# -----------------------------

def extract_jsonld_events(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """
    Extract schema.org Event items from JSON-LD.
    Handles:
      - single dict Event
      - list of Events
      - @graph containing Events
      - Event @type may be "Event" or list containing "Event"
    """
    html = fetch_html(source["url"])
    soup = BeautifulSoup(html, "html.parser")

    scripts = soup.find_all("script", attrs={"type": "application/ld+json"})
    out: List[Dict[str, Any]] = []

    def handle(obj: Any):
        if isinstance(obj, list):
            for it in obj:
                handle(it)
            return
        if not isinstance(obj, dict):
            return

        # Graph wrapper
        if "@graph" in obj and isinstance(obj["@graph"], list):
            for g in obj["@graph"]:
                handle(g)
            return

        t = obj.get("@type")
        is_event = False
        if isinstance(t, list):
            is_event = "Event" in t
        else:
            is_event = t == "Event"
        if not is_event:
            return

        name = safe_text(obj.get("name"), 240) or "Workshop"
        start = parse_dt(obj.get("startDate"), default_tz)
        if not start:
            return
        end = parse_dt(obj.get("endDate"), default_tz) or start

        page_url = obj.get("url") or source.get("url")
        page_url = normalize_url(page_url)

        desc = safe_text(obj.get("description"))
        image = obj.get("image")
        image_url = image if isinstance(image, str) else None

        venue = None
        city = None
        state = None
        loc = obj.get("location")
        if isinstance(loc, dict):
            venue = safe_text(loc.get("name"), 160)
            addr = loc.get("address")
            if isinstance(addr, dict):
                city = safe_text(addr.get("addressLocality"), 80)
                state = safe_text(addr.get("addressRegion"), 40)

        out.append(
            {
                "title": name,
                "host": source.get("name"),
                "city": city,
                "state": state,
                "venue": venue,
                "startAt": to_iso(start),
                "endAt": to_iso(end),
                "registrationURL": page_url,
                "imageURL": image_url,
                "detail": desc,
                "links": [{"title": "Event Page", "url": page_url}] if page_url else None,
            }
        )

    for s in scripts:
        raw = s.get_text(strip=True)
        if not raw:
            continue
        try:
            obj = json.loads(raw)
            handle(obj)
        except Exception:
            continue

    return out


def extract_soundonstudio_classsignup(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """
    SoundOnStudio class signup tends to contain lines like:
      "2.23.26 - Making Promos Pop! with Darius Marquis Johnson ..."
    We'll parse date + title. Times are often missing; we default to 6pm local, 2-hour duration.
    """
    html = fetch_html(source["url"])
    text = BeautifulSoup(html, "html.parser").get_text("\n")

    # date format like 2.23.26
    pattern = re.compile(r"\b(\d{1,2})\.(\d{1,2})\.(\d{2})\s*-\s*([^\n$]+)")
    events: List[Dict[str, Any]] = []

    for m in pattern.finditer(text):
        mm = int(m.group(1))
        dd = int(m.group(2))
        yy = int(m.group(3))
        year = 2000 + yy

        title = safe_text(m.group(4), 240)
        if not title:
            continue

        start = dt.datetime(year, mm, dd, 18, 0, tzinfo=datetz.gettz(default_tz))
        end = start + dt.timedelta(hours=2)

        page = normalize_url(source["url"])

        events.append(
            {
                "title": title,
                "host": source.get("name"),
                "city": None,
                "state": None,
                "venue": "See listing",
                "startAt": to_iso(start),
                "endAt": to_iso(end),
                "registrationURL": page,
                "imageURL": None,
                "detail": None,
                "links": [{"title": "Class Signup", "url": page}] if page else None,
            }
        )

    return events


def extract_html_fallback(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """
    Safe fallback: try JSON-LD only (do not attempt brittle scraping).
    """
    return extract_jsonld_events(source, default_tz)


EXTRACTORS = {
    "jsonld_events": extract_jsonld_events,
    "soundonstudio_classsignup": extract_soundonstudio_classsignup,
    "html_fallback": extract_html_fallback,
}


# -----------------------------
# Normalize / Merge / Prune
# -----------------------------

def normalize_workshop(source_id: str, raw: Dict[str, Any], default_tz: str) -> Optional[Dict[str, Any]]:
    """
    Convert raw scraped dict into the canonical resources.json workshop object.
    Must have startAt.
    """
    start = parse_dt(raw.get("startAt"), default_tz)
    if not start:
        return None
    end = parse_dt(raw.get("endAt"), default_tz) or start

    reg = normalize_url(raw.get("registrationURL"))

    # Clean links
    links_in = raw.get("links")
    links: Optional[List[Dict[str, str]]] = None
    if isinstance(links_in, list):
        cleaned: List[Dict[str, str]] = []
        for l in links_in:
            if not isinstance(l, dict):
                continue
            t = safe_text(l.get("title"), 80)
            u = normalize_url(l.get("url"))
            if t and u:
                cleaned.append({"title": t, "url": u})
        links = cleaned or None

    normalized = {
        "id": "",  # filled below
        "title": safe_text(raw.get("title"), 240) or "Workshop",
        "host": safe_text(raw.get("host"), 140),
        "city": safe_text(raw.get("city"), 80),
        "state": safe_text(raw.get("state"), 40),
        "venue": safe_text(raw.get("venue"), 160),
        "startAt": to_iso(start),
        "endAt": to_iso(end),
        "registrationURL": reg,
        "imageURL": raw.get("imageURL") if isinstance(raw.get("imageURL"), str) else None,
        "detail": safe_text(raw.get("detail")),
        "links": links,
    }

    fp = workshop_fingerprint(normalized)
    normalized["id"] = stable_workshop_id(source_id, fp)
    return normalized


def merge_workshops(existing: List[Dict[str, Any]], incoming: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Merge WITHOUT duplicates:
    - Primary identity is workshop["id"] (stable)
    - If existing JSON already has duplicates (same id), collapse them.
    - Merge rule: incoming overwrites only when it provides a non-empty value.
    """
    existing = existing or []
    incoming = incoming or []

    # collapse duplicates in existing by id
    by_id: Dict[str, Dict[str, Any]] = {}
    for w in existing:
        wid = w.get("id")
        if not wid:
            continue
        if wid not in by_id:
            by_id[wid] = dict(w)
        else:
            # keep the richer one
            cur = by_id[wid]
            def score(x: Dict[str, Any]) -> int:
                return (
                    int(bool(x.get("registrationURL")))
                    + int(bool(x.get("detail")))
                    + int(bool(x.get("links")))
                    + int(bool(x.get("imageURL")))
                )
            if score(w) > score(cur):
                by_id[wid] = dict(w)

    # merge incoming
    for nw in incoming:
        wid = nw.get("id")
        if not wid:
            continue
        if wid in by_id:
            cur = by_id[wid]
            merged = dict(cur)
            for k, v in nw.items():
                if v is None:
                    continue
                if isinstance(v, str) and v.strip() == "":
                    continue
                if isinstance(v, list) and len(v) == 0:
                    continue
                merged[k] = v
            by_id[wid] = merged
        else:
            by_id[wid] = dict(nw)

    merged_list = list(by_id.values())

    # sort by startAt
    def sort_key(w: Dict[str, Any]) -> float:
        try:
            d = dateparser.parse(w.get("startAt") or "")
            if d is None:
                return float("inf")
            return d.timestamp()
        except Exception:
            return float("inf")

    merged_list.sort(key=sort_key)
    return merged_list


def prune_workshops(items: List[Dict[str, Any]], prune_days_past: int, keep_days_future: int, default_tz: str) -> List[Dict[str, Any]]:
    now = dt.datetime.now(tz=datetz.gettz(default_tz))
    past_cut = now - dt.timedelta(days=prune_days_past)
    future_cut = now + dt.timedelta(days=keep_days_future)

    kept: List[Dict[str, Any]] = []
    for w in items:
        start = parse_dt(w.get("startAt"), default_tz)
        end = parse_dt(w.get("endAt"), default_tz) or start
        if not start or not end:
            continue
        if end >= past_cut and start <= future_cut:
            kept.append(w)
    return kept


# -----------------------------
# Main
# -----------------------------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--resources", required=True, help="Path to resources.json")
    ap.add_argument("--sources", required=True, help="Path to workshop_sources.json")
    ap.add_argument("--default-tz", default="America/Los_Angeles")
    args = ap.parse_args()

    with open(args.sources, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    settings = cfg.get("settings", {}) or {}
    sources = cfg.get("sources", []) or []

    max_events_per_source = int(settings.get("max_events_per_source", 50))
    prune_days_past = int(settings.get("prune_days_past", 7))
    keep_days_future = int(settings.get("keep_days_future", 365))

    with open(args.resources, "r", encoding="utf-8") as f:
        resources = json.load(f)

    existing = resources.get("workshops", []) or []
    incoming_all: List[Dict[str, Any]] = []

    any_success = False

    for src in sources:
        src_id = src.get("id")
        src_url = src.get("url")
        if not src_id or not src_url:
            print(f"[WARN] Skipping source missing id/url: {src}")
            continue

        extractor_name = src.get("extractor", "jsonld_events")
        extractor = EXTRACTORS.get(extractor_name)
        if not extractor:
            print(f"[WARN] Unknown extractor '{extractor_name}' for {src_id}, skipping.")
            continue

        try:
            raw_events = extractor(src, args.default_tz) or []
            raw_events = raw_events[:max_events_per_source]

            normalized = []
            for e in raw_events:
                nw = normalize_workshop(src_id, e, args.default_tz)
                if nw:
                    normalized.append(nw)

            print(f"[OK] {src_id}: {len(normalized)} workshops")
            incoming_all.extend(normalized)
            any_success = True
        except Exception as ex:
            print(f"[ERROR] {src_id}: {ex}")

    # Merge + prune
    merged = merge_workshops(existing, incoming_all)
    merged = prune_workshops(merged, prune_days_past, keep_days_future, args.default_tz)

    resources["workshops"] = merged
    resources["lastUpdated"] = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")

    with open(args.resources, "w", encoding="utf-8") as f:
        json.dump(resources, f, ensure_ascii=False, indent=2)
        f.write("\n")

    # If literally every source failed, return non-zero so Actions flags it.
    return 0 if any_success else 2


if __name__ == "__main__":
    raise SystemExit(main())
