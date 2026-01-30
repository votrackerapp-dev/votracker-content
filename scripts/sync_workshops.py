#!/usr/bin/env python3
"""
Sync workshops from multiple sites into resources.json

Features:
- Extracts events primarily from schema.org JSON-LD Event blocks (very common)
- Optional extractor for SoundOnStudio class signup page (date-like "2.23.26 - Title...")
- Stable dedupe (by normalized registrationURL; fallback by source+title+startAt)
- Stable ids derived from the dedupe key
- Merges incoming updates without duplicating
- Prunes events outside a window
- Only updates resources.json + lastUpdated if the workshop list actually changed
"""

import argparse
import copy
import datetime as dt
import hashlib
import json
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser
from dateutil import tz as datetz


TRACKING_PARAMS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "fbclid", "gclid", "mc_cid", "mc_eid"
}


# -----------------------------
# URL + ID helpers
# -----------------------------

def normalize_url(url: Optional[str]) -> str:
    if not url:
        return ""
    u = url.strip()
    try:
        p = urlparse(u)
        scheme = (p.scheme or "https").lower()
        netloc = (p.netloc or "").lower()
        path = p.path or ""
        if path != "/" and path.endswith("/"):
            path = path[:-1]

        q = [(k, v) for (k, v) in parse_qsl(p.query, keep_blank_values=True) if k not in TRACKING_PARAMS]
        query = urlencode(q, doseq=True)

        # Drop fragments
        return urlunparse((scheme, netloc, path, "", query, ""))
    except Exception:
        return u


def sha1_hex(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def stable_id(key: str, prefix: str = "ws") -> str:
    return f"{prefix}-{sha1_hex(key)[:16]}"


# -----------------------------
# Date helpers
# -----------------------------

def safe_text(s: Any, max_len: int = 6000) -> Optional[str]:
    if s is None:
        return None
    t = re.sub(r"\s+", " ", str(s)).strip()
    if not t:
        return None
    return t[:max_len]


def parse_date_any(value: Any, default_tz: str) -> Optional[dt.datetime]:
    """
    Parses many date formats into a timezone-aware datetime.
    If the parsed datetime has no tz, apply default_tz.
    If the parsed value looks like date-only, we set a reasonable default time (7pm local).
    """
    if not value:
        return None
    try:
        raw = str(value).strip()
        d = dateparser.parse(raw)
        if d is None:
            return None

        tzinfo = datetz.gettz(default_tz)

        # If no tz provided, attach default tz
        if d.tzinfo is None:
            d = d.replace(tzinfo=tzinfo)

        # If the string was date-only-ish, dateutil often returns midnight.
        # We push to 7pm local to keep it useful on a calendar UI.
        # (Only do this when the input doesn't obviously include a time.)
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw) or re.fullmatch(r"\d{1,2}/\d{1,2}/\d{2,4}", raw):
            d = d.replace(hour=19, minute=0, second=0, microsecond=0)

        return d
    except Exception:
        return None


def iso_with_offset(d: dt.datetime) -> str:
    if d.tzinfo is None:
        d = d.replace(tzinfo=datetz.UTC)
    # Keep offset; your Swift code already handles offsets well.
    return d.isoformat()


# -----------------------------
# HTTP helpers
# -----------------------------

def fetch_html(url: str) -> str:
    r = requests.get(
        url,
        timeout=30,
        headers={
            "User-Agent": "VOTrackerWorkshopBot/1.0 (+https://github.com/votrackerapp-dev/votracker-content)"
        },
    )
    r.raise_for_status()
    return r.text


# -----------------------------
# Extractors
# -----------------------------

def _jsonld_walk(obj: Any) -> List[Dict[str, Any]]:
    """Flatten JSON-LD into a list of dict nodes."""
    out: List[Dict[str, Any]] = []
    if isinstance(obj, dict):
        out.append(obj)
        if "@graph" in obj and isinstance(obj["@graph"], list):
            for g in obj["@graph"]:
                out.extend(_jsonld_walk(g))
    elif isinstance(obj, list):
        for it in obj:
            out.extend(_jsonld_walk(it))
    return out


def extract_jsonld_events(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """
    Extract schema.org Event blocks from application/ld+json.
    """
    html = fetch_html(source["url"])
    soup = BeautifulSoup(html, "html.parser")
    scripts = soup.find_all("script", attrs={"type": "application/ld+json"})

    events: List[Dict[str, Any]] = []

    for s in scripts:
        raw = s.get_text(strip=True)
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue

        for node in _jsonld_walk(data):
            t = node.get("@type")
            is_event = False
            if isinstance(t, str) and t == "Event":
                is_event = True
            elif isinstance(t, list) and "Event" in t:
                is_event = True
            if not is_event:
                continue

            title = safe_text(node.get("name"), 240) or "Workshop"
            start = parse_date_any(node.get("startDate"), default_tz)
            end = parse_date_any(node.get("endDate"), default_tz) or start
            if not start:
                continue

            # URL can be string or object
            url = node.get("url") or source.get("url")
            if isinstance(url, dict):
                url = url.get("@id") or url.get("url") or source.get("url")
            reg_url = normalize_url(url) if url else ""

            desc = safe_text(node.get("description"))
            image = node.get("image")
            image_url = image if isinstance(image, str) else None

            venue = None
            city = None
            state = None

            loc = node.get("location")
            if isinstance(loc, dict):
                venue = safe_text(loc.get("name"), 200)
                addr = loc.get("address")
                if isinstance(addr, dict):
                    city = safe_text(addr.get("addressLocality"), 80)
                    state = safe_text(addr.get("addressRegion"), 40)

            events.append({
                "title": title,
                "host": source.get("name"),
                "city": city,
                "state": state,
                "venue": venue,
                "startAt": iso_with_offset(start),
                "endAt": iso_with_offset(end) if end else iso_with_offset(start),
                "registrationURL": reg_url or normalize_url(source.get("url")),
                "imageURL": image_url,
                "detail": desc,
                "links": [{"title": "Event Page", "url": reg_url}] if reg_url else None,
                "_source": source.get("id"),
            })

    return events


def extract_soundonstudio_classsignup(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """
    SoundOnStudio class signup page sometimes contains lines like:
      "2.23.26 - Making Promos Pop! with Darius Marquis Johnson ..."
    We parse those into date-only events with a default time window (6–8pm local).
    """
    html = fetch_html(source["url"])
    text = BeautifulSoup(html, "html.parser").get_text("\n")

    # e.g. 2.23.26 - Title
    pattern = re.compile(r"\b(\d{1,2})\.(\d{1,2})\.(\d{2})\s*-\s*([^\n]+)")
    tzinfo = datetz.gettz(default_tz)
    events: List[Dict[str, Any]] = []

    for m in pattern.finditer(text):
        mm, dd, yy = int(m.group(1)), int(m.group(2)), int(m.group(3))
        year = 2000 + yy
        title = safe_text(m.group(4), 240)
        if not title:
            continue

        start = dt.datetime(year, mm, dd, 18, 0, tzinfo=tzinfo)
        end = start + dt.timedelta(hours=2)

        events.append({
            "title": title,
            "host": source.get("name"),
            "city": None,
            "state": None,
            "venue": "See listing",
            "startAt": iso_with_offset(start),
            "endAt": iso_with_offset(end),
            "registrationURL": normalize_url(source["url"]),
            "imageURL": None,
            "detail": None,
            "links": [{"title": "Class Signup", "url": normalize_url(source["url"])}],
            "_source": source.get("id"),
        })

    return events


def extract_html_fallback(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """
    Safe fallback: try JSON-LD (many sites have it), otherwise return nothing.
    You can add custom extractors per-site as needed.
    """
    return extract_jsonld_events(source, default_tz)


EXTRACTORS = {
    "jsonld_events": extract_jsonld_events,
    "soundonstudio_classsignup": extract_soundonstudio_classsignup,
    "html_fallback": extract_html_fallback,
}


# -----------------------------
# Normalization + keys
# -----------------------------

def workshop_key(w: Dict[str, Any]) -> str:
    """
    Stable dedupe key:
    - prefer normalized registrationURL
    - fallback to source + title + startAt (+ host as extra stability)
    """
    reg = normalize_url(w.get("registrationURL"))
    source_id = (w.get("_source") or "").strip()
    if reg:
        return f"{source_id}|url|{reg}"

    title = (w.get("title") or "").strip().lower()
    host = (w.get("host") or "").strip().lower()
    start = (w.get("startAt") or "").strip()
    return f"{source_id}|fallback|{title}|{host}|{start}"


def normalize_workshop(raw: Dict[str, Any], default_tz: str) -> Optional[Dict[str, Any]]:
    start = parse_date_any(raw.get("startAt"), default_tz)
    if not start:
        return None
    end = parse_date_any(raw.get("endAt"), default_tz) or start

    links = raw.get("links")
    cleaned_links = None
    if isinstance(links, list):
        tmp = []
        for l in links:
            if not isinstance(l, dict):
                continue
            t = safe_text(l.get("title"), 80)
            u = normalize_url(l.get("url"))
            if t and u:
                tmp.append({"title": t, "url": u})
        cleaned_links = tmp or None

    out = {
        "id": None,  # set after key
        "title": safe_text(raw.get("title"), 240) or "Workshop",
        "host": safe_text(raw.get("host"), 120),
        "city": safe_text(raw.get("city"), 80),
        "state": safe_text(raw.get("state"), 40),
        "venue": safe_text(raw.get("venue"), 180),
        "startAt": iso_with_offset(start),
        "endAt": iso_with_offset(end),
        "registrationURL": normalize_url(raw.get("registrationURL")) or None,
        "imageURL": raw.get("imageURL") if isinstance(raw.get("imageURL"), str) else None,
        "detail": safe_text(raw.get("detail")),
        "links": cleaned_links,
        "_source": raw.get("_source"),
    }

    k = workshop_key(out)
    out["id"] = stable_id(k, prefix="ws")
    out.pop("_source", None)
    return out


# -----------------------------
# Merge (NO DUPES)
# -----------------------------

def merge_workshops(existing: List[Dict[str, Any]], incoming: List[Dict[str, Any]], source_id: str) -> List[Dict[str, Any]]:
    """
    Merge incoming into existing with NO duplicates.
    Matching uses workshop_key logic (url preferred).
    Preserve existing ids; stable ids applied otherwise.
    """
    existing = existing or []
    incoming = incoming or []

    def key_for_existing(w: Dict[str, Any]) -> str:
        # existing items won't have _source; emulate for matching:
        ww = dict(w)
        ww["_source"] = source_id  # not perfect, but URL match dominates
        return workshop_key(ww)

    # Index by normalized URL first (cross-source stable), then fallback key
    by_url: Dict[str, Dict[str, Any]] = {}
    by_fallback: Dict[str, Dict[str, Any]] = {}

    merged_list: List[Dict[str, Any]] = []

    # Seed with existing
    for w in existing:
        reg = normalize_url(w.get("registrationURL"))
        if reg:
            by_url[reg] = dict(w)
        else:
            # Use fallback key without source
            title = (w.get("title") or "").strip().lower()
            host = (w.get("host") or "").strip().lower()
            start = (w.get("startAt") or "").strip()
            fb = f"{title}|{host}|{start}"
            by_fallback[fb] = dict(w)

    # Merge incoming
    for w in incoming:
        reg = normalize_url(w.get("registrationURL"))
        if reg and reg in by_url:
            cur = by_url[reg]
            # preserve id if present
            if cur.get("id"):
                w["id"] = cur["id"]
            # merge: don't overwrite good old fields with empty new ones
            merged = dict(cur)
            for k, v in w.items():
                if v is None:
                    continue
                if isinstance(v, str) and v.strip() == "":
                    continue
                merged[k] = v
            by_url[reg] = merged
        elif not reg:
            title = (w.get("title") or "").strip().lower()
            host = (w.get("host") or "").strip().lower()
            start = (w.get("startAt") or "").strip()
            fb = f"{title}|{host}|{start}"
            if fb in by_fallback:
                cur = by_fallback[fb]
                if cur.get("id"):
                    w["id"] = cur["id"]
                merged = dict(cur)
                for k, v in w.items():
                    if v is None:
                        continue
                    if isinstance(v, str) and v.strip() == "":
                        continue
                    merged[k] = v
                by_fallback[fb] = merged
            else:
                by_fallback[fb] = dict(w)
        else:
            by_url[reg] = dict(w)

    # Combine
    merged_list.extend(by_url.values())
    merged_list.extend(by_fallback.values())

    # Deduplicate by id as a final safety net
    seen: Dict[str, Dict[str, Any]] = {}
    for w in merged_list:
        wid = w.get("id")
        if not wid:
            continue
        if wid not in seen:
            seen[wid] = w
        else:
            # prefer one with registrationURL/detail
            a = seen[wid]
            def score(x):
                return int(bool(x.get("registrationURL"))) + int(bool(x.get("detail"))) + int(bool(x.get("links")))
            if score(w) > score(a):
                seen[wid] = w

    out = list(seen.values())

    # Stable sort: by startAt, then title
    out.sort(key=lambda x: ((x.get("startAt") or "9999"), (x.get("title") or "").lower()))
    return out


def prune_events(items: List[Dict[str, Any]], prune_days_past: int, keep_days_future: int, default_tz: str) -> List[Dict[str, Any]]:
    tzinfo = datetz.gettz(default_tz)
    now = dt.datetime.now(tz=tzinfo)
    past_cut = now - dt.timedelta(days=prune_days_past)
    future_cut = now + dt.timedelta(days=keep_days_future)

    kept = []
    for w in items:
        start = parse_date_any(w.get("startAt"), default_tz)
        end = parse_date_any(w.get("endAt"), default_tz) or start
        if not start:
            continue
        if end >= past_cut and start <= future_cut:
            kept.append(w)

    kept.sort(key=lambda x: ((x.get("startAt") or "9999"), (x.get("title") or "").lower()))
    return kept


def workshops_semantically_equal(a: List[Dict[str, Any]], b: List[Dict[str, Any]]) -> bool:
    """
    Compare two workshop lists in a stable way.
    """
    def normalize_list(lst: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        # Remove any accidental ordering differences; keep only relevant fields
        keep_fields = {
            "id", "title", "host", "city", "state", "venue",
            "startAt", "endAt", "registrationURL", "imageURL", "detail", "links"
        }
        out = []
        for w in lst:
            ww = {k: w.get(k) for k in keep_fields}
            out.append(ww)
        out.sort(key=lambda x: ((x.get("startAt") or "9999"), (x.get("id") or "")))
        return out

    return normalize_list(a) == normalize_list(b)


# -----------------------------
# Main
# -----------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--resources", required=True, help="Path to resources.json")
    ap.add_argument("--sources", required=True, help="Path to config/workshop_sources.json")
    ap.add_argument("--default-tz", default="America/Los_Angeles")
    args = ap.parse_args()

    with open(args.sources, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    sources = cfg.get("sources", [])
    settings = cfg.get("settings", {}) or {}

    max_events_per_source = int(settings.get("max_events_per_source", 50))
    prune_days_past = int(settings.get("prune_days_past", 7))
    keep_days_future = int(settings.get("keep_days_future", 365))

    with open(args.resources, "r", encoding="utf-8") as f:
        resources = json.load(f)

    existing_workshops = resources.get("workshops", []) or []
    original_workshops = copy.deepcopy(existing_workshops)

    incoming_all: List[Dict[str, Any]] = []

    for s in sources:
        sid = s.get("id") or "source"
        extractor_name = s.get("extractor", "jsonld_events")
        fn = EXTRACTORS.get(extractor_name)
        if not fn:
            print(f"[WARN] Unknown extractor '{extractor_name}' for {sid}, skipping.")
            continue

        try:
            raw = fn(s, args.default_tz)[:max_events_per_source]
            for e in raw:
                e["_source"] = sid
                nw = normalize_workshop(e, args.default_tz)
                if nw:
                    incoming_all.append(nw)
            print(f"[OK] {sid}: {len(raw)} raw, {len([x for x in raw if x])} parsed-ish")
        except Exception as ex:
            print(f"[ERROR] {sid}: {ex}")

    # Merge incoming into existing without dupes.
    # Note: We don't track source per item in final JSON, so URL matching is the big win.
    merged = merge_workshops(existing_workshops, incoming_all, source_id="mixed")
    merged = prune_events(merged, prune_days_past, keep_days_future, args.default_tz)

    changed = not workshops_semantically_equal(original_workshops, merged)
    if changed:
        resources["workshops"] = merged
        resources["lastUpdated"] = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
        with open(args.resources, "w", encoding="utf-8") as f:
            json.dump(resources, f, ensure_ascii=False, indent=2)
            f.write("\n")
        print(f"[DONE] Updated workshops: {len(merged)} (changes detected)")
    else:
        print(f"[DONE] No workshop changes detected. Leaving resources.json untouched.")


if __name__ == "__main__":
    main()
