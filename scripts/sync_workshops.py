#!/usr/bin/env python3
"""
sync_workshops.py

Pull workshops/classes/events from multiple sources and merge into resources.json
without duplicates.

Key features:
- Stable de-dupe (by normalized registrationURL; fallback to title+host+startAt)
- Stable IDs (sha1-based)
- Multiple extractors:
    - jsonld_events: schema.org Event blocks
    - shopify_products_json: Shopify stores exposing products.json
    - soundonstudio_classsignup: parses Sound On Studio class signup text
    - html_links_with_dates: very light fallback parser for pages that list dated links
"""

import argparse
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


# -----------------------------
# URL normalization / keys / IDs
# -----------------------------

TRACKING_PARAMS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "fbclid", "gclid", "mc_cid", "mc_eid"
}

def normalize_url(url: str) -> str:
    """Normalize URL to maximize match stability for de-duping."""
    if not url:
        return ""
    url = url.strip()
    try:
        p = urlparse(url)
        scheme = (p.scheme or "https").lower()
        netloc = p.netloc.lower()

        q = [(k, v) for (k, v) in parse_qsl(p.query, keep_blank_values=True)
             if k not in TRACKING_PARAMS]
        query = urlencode(q, doseq=True)

        path = p.path or ""
        if path != "/" and path.endswith("/"):
            path = path[:-1]

        return urlunparse((scheme, netloc, path, "", query, ""))
    except Exception:
        return url.strip()


def sha1_hex(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def stable_id(prefix: str, key: str) -> str:
    return f"{prefix}-{sha1_hex(key)[:16]}"


def workshop_fingerprint(w: Dict[str, Any]) -> str:
    """
    Primary key:
      - normalized registrationURL if present
    Fallback:
      - title + host + startAt
    """
    reg = normalize_url(w.get("registrationURL") or "")
    if reg:
        return f"url:{reg}"

    title = (w.get("title") or "").strip().lower()
    host = (w.get("host") or "").strip().lower()
    start = (w.get("startAt") or "").strip()
    raw = f"{title}|{host}|{start}"
    return f"fallback:{sha1_hex(raw)[:16]}"


# -----------------------------
# Date helpers
# -----------------------------

def parse_date(value: Any, default_tz: str) -> Optional[dt.datetime]:
    """Parse many date formats into a tz-aware datetime."""
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


def isoformat_with_tz(d: dt.datetime) -> str:
    if d.tzinfo is None:
        d = d.replace(tzinfo=datetz.UTC)
    return d.isoformat()


def safe_text(s: Optional[str], max_len: int = 4000) -> Optional[str]:
    if s is None:
        return None
    t = re.sub(r"\s+", " ", str(s)).strip()
    if not t:
        return None
    return t[:max_len]


# -----------------------------
# Fetch helper
# -----------------------------

def fetch_html(url: str) -> str:
    r = requests.get(
        url,
        timeout=30,
        headers={"User-Agent": "VOTrackerWorkshopBot/1.0 (+https://votracker.app)"}
    )
    r.raise_for_status()
    return r.text


def fetch_json(url: str) -> Any:
    r = requests.get(
        url,
        timeout=30,
        headers={"User-Agent": "VOTrackerWorkshopBot/1.0 (+https://votracker.app)"}
    )
    r.raise_for_status()
    return r.json()


# -----------------------------
# Extractor: JSON-LD schema.org Event
# -----------------------------

def extract_jsonld_events(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    html = fetch_html(source["url"])
    soup = BeautifulSoup(html, "html.parser")
    scripts = soup.find_all("script", attrs={"type": "application/ld+json"})
    out: List[Dict[str, Any]] = []

    def handle_obj(obj: Any):
        if isinstance(obj, list):
            for it in obj:
                handle_obj(it)
            return
        if not isinstance(obj, dict):
            return

        # Sometimes in @graph
        if "@graph" in obj and isinstance(obj["@graph"], list):
            for g in obj["@graph"]:
                handle_obj(g)
            return

        t = obj.get("@type")
        if isinstance(t, list):
            if "Event" not in t:
                return
        else:
            if t != "Event":
                return

        name = safe_text(obj.get("name"), 200) or "Workshop"
        start = parse_date(obj.get("startDate"), default_tz)
        end = parse_date(obj.get("endDate"), default_tz) or start
        url = obj.get("url") or source["url"]
        desc = safe_text(obj.get("description"))
        image = obj.get("image")

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

        if not start:
            return

        out.append({
            "title": name,
            "host": source.get("name"),
            "city": safe_text(city, 80),
            "state": safe_text(state, 40),
            "venue": safe_text(venue, 160),
            "startAt": isoformat_with_tz(start),
            "endAt": isoformat_with_tz(end) if end else isoformat_with_tz(start),
            "registrationURL": normalize_url(url),
            "imageURL": image if isinstance(image, str) else None,
            "detail": desc,
            "links": [{"title": "Event Page", "url": normalize_url(url)}] if url else None,
            "_source": source["id"],
        })

    for s in scripts:
        raw = s.get_text(strip=True)
        if not raw:
            continue
        try:
            obj = json.loads(raw)
            handle_obj(obj)
        except Exception:
            continue

    return out


# -----------------------------
# Extractor: Shopify products.json
# -----------------------------

MONTHS = r"(jan|january|feb|february|mar|march|apr|april|may|jun|june|jul|july|aug|august|sep|sept|september|oct|october|nov|november|dec|december)"

TIME_RANGE_RE = re.compile(
    r"(?P<h1>\d{1,2})(?::(?P<m1>\d{2}))?\s*(?P<ap1>am|pm)?\s*[-–]\s*"
    r"(?P<h2>\d{1,2})(?::(?P<m2>\d{2}))?\s*(?P<ap2>am|pm)?",
    re.IGNORECASE
)

DATE_RE_1 = re.compile(rf"\b{MONTHS}\s+(\d{{1,2}})(?:st|nd|rd|th)?(?:,?\s*(\d{{4}}))?\b", re.IGNORECASE)
DATE_RE_2 = re.compile(r"\b(\d{1,2})[./-](\d{1,2})[./-](\d{2,4})\b")

def _infer_ampm(h: int, ap: Optional[str]) -> str:
    if ap:
        return ap.lower()
    # If no am/pm: default evening for typical VO workshops
    return "pm" if h <= 11 else "pm"

def _parse_time_range(text: str) -> Optional[Tuple[int,int,int,int]]:
    """
    Returns (h1,m1,h2,m2) in 24h form if found.
    """
    m = TIME_RANGE_RE.search(text or "")
    if not m:
        return None

    h1 = int(m.group("h1"))
    m1 = int(m.group("m1") or 0)
    h2 = int(m.group("h2"))
    m2 = int(m.group("m2") or 0)

    ap1 = _infer_ampm(h1, m.group("ap1"))
    ap2 = _infer_ampm(h2, m.group("ap2") or m.group("ap1"))

    def to24(h, ap):
        if ap == "am":
            return 0 if h == 12 else h
        # pm
        return 12 if h == 12 else h + 12

    return (to24(h1, ap1), m1, to24(h2, ap2), m2)

def _parse_date_from_text(text: str) -> Optional[dt.date]:
    t = text or ""
    m = DATE_RE_1.search(t)
    if m:
        month_name = m.group(1)
        day = int(m.group(2))
        year = int(m.group(3)) if m.group(3) else None

        month_map = {
            "jan":1,"january":1, "feb":2,"february":2, "mar":3,"march":3,
            "apr":4,"april":4, "may":5, "jun":6,"june":6, "jul":7,"july":7,
            "aug":8,"august":8, "sep":9,"sept":9,"september":9,
            "oct":10,"october":10, "nov":11,"november":11, "dec":12,"december":12
        }
        month = month_map[month_name.lower()]
        if year is None:
            # Guess: use current year or next year if date already passed
            now = dt.datetime.now(dt.timezone.utc).date()
            y = now.year
            try_date = dt.date(y, month, day)
            if try_date < now:
                y += 1
            year = y
        return dt.date(year, month, day)

    m2 = DATE_RE_2.search(t)
    if m2:
        mm = int(m2.group(1))
        dd = int(m2.group(2))
        yy = int(m2.group(3))
        if yy < 100:
            yy += 2000
        try:
            return dt.date(yy, mm, dd)
        except Exception:
            return None

    return None

def extract_shopify_products_json(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """
    Tries to fetch product listings from a Shopify store.
    Works if the site exposes:
      - /products.json?limit=250&page=N
    or a collection endpoint you provide:
      - /collections/<handle>/products.json?limit=250&page=N
    """

    base = source["url"].rstrip("/")
    products_json_url = source.get("products_json_url")

    if not products_json_url:
        # Default attempt: /products.json
        p = urlparse(base)
        products_json_url = urlunparse((p.scheme or "https", p.netloc, "/products.json", "", "", ""))

    all_products: List[Dict[str, Any]] = []
    for page in range(1, 6):  # up to 5 pages * 250 = 1250 products
        url = f"{products_json_url}?limit=250&page={page}"
        try:
            data = fetch_json(url)
        except Exception:
            break

        products = data.get("products") if isinstance(data, dict) else None
        if not products:
            break

        all_products.extend(products)
        if len(products) < 250:
            break

    events: List[Dict[str, Any]] = []
    tzinfo = datetz.gettz(default_tz)

    for pr in all_products:
        title = safe_text(pr.get("title"), 220) or "Workshop"
        handle = pr.get("handle")
        vendor = safe_text(pr.get("vendor"), 120)
        body_html = pr.get("body_html") or ""
        soup = BeautifulSoup(body_html, "html.parser")
        body_text = soup.get_text(" ", strip=True)
        candidate_text = f"{title} {body_text}"

        d = _parse_date_from_text(candidate_text)
        if not d:
            continue  # can't use it if no date

        tr = _parse_time_range(candidate_text)
        if tr:
            h1, m1, h2, m2 = tr
        else:
            # Default times if none found
            h1, m1, h2, m2 = (18, 0, 20, 0)

        start = dt.datetime(d.year, d.month, d.day, h1, m1, tzinfo=tzinfo)
        end = dt.datetime(d.year, d.month, d.day, h2, m2, tzinfo=tzinfo)
        if end <= start:
            end = start + dt.timedelta(hours=2)

        # Build a product URL
        reg_url = None
        try:
            p = urlparse(base)
            if handle:
                reg_url = urlunparse((p.scheme or "https", p.netloc, f"/products/{handle}", "", "", ""))
        except Exception:
            reg_url = base

        host_name = source.get("name") or vendor or source.get("id")

        events.append({
            "title": title,
            "host": host_name,
            "city": None,
            "state": None,
            "venue": safe_text(source.get("venue")) or None,
            "startAt": isoformat_with_tz(start),
            "endAt": isoformat_with_tz(end),
            "registrationURL": normalize_url(reg_url or base),
            "imageURL": None,
            "detail": safe_text(body_text),
            "links": [{"title": "Listing", "url": normalize_url(reg_url or base)}],
            "_source": source["id"],
        })

    return events


# -----------------------------
# Extractor: SoundOnStudio classsignup
# -----------------------------

def extract_soundonstudio_classsignup(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """
    Parses lines like:
      "2.23.26 - Making Promos Pop! with Darius Marquis Johnson"
    """
    html = fetch_html(source["url"])
    text = BeautifulSoup(html, "html.parser").get_text("\n")

    pattern = re.compile(r"\b(\d{1,2})\.(\d{1,2})\.(\d{2})\s*-\s*([^\n$]+)")
    events: List[Dict[str, Any]] = []
    tzinfo = datetz.gettz(default_tz)

    for m in pattern.finditer(text):
        mm, dd, yy = int(m.group(1)), int(m.group(2)), int(m.group(3))
        year = 2000 + yy
        title = safe_text(m.group(4), 220)
        if not title:
            continue

        # Default: 6–8pm local
        start = dt.datetime(year, mm, dd, 18, 0, tzinfo=tzinfo)
        end = start + dt.timedelta(hours=2)

        events.append({
            "title": title,
            "host": source.get("name"),
            "city": None,
            "state": None,
            "venue": safe_text(source.get("venue")) or "See listing",
            "startAt": isoformat_with_tz(start),
            "endAt": isoformat_with_tz(end),
            "registrationURL": normalize_url(source["url"]),
            "imageURL": None,
            "detail": None,
            "links": [{"title": "Class Signup", "url": normalize_url(source["url"])}],
            "_source": source["id"],
        })

    return events


# -----------------------------
# Extractor: HTML links with dates (light fallback)
# -----------------------------

def extract_html_links_with_dates(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """
    Very lightweight fallback:
    - collects links
    - tries to detect a date near the link text
    This won't work for everything, but can catch simple "Upcoming Classes" pages.
    """
    html = fetch_html(source["url"])
    soup = BeautifulSoup(html, "html.parser")

    tzinfo = datetz.gettz(default_tz)
    events: List[Dict[str, Any]] = []

    for a in soup.find_all("a"):
        text = safe_text(a.get_text(" ", strip=True), 240)
        href = a.get("href")
        if not text or not href:
            continue

        # Make href absolute if relative
        try:
            base = urlparse(source["url"])
            if href.startswith("/"):
                href = urlunparse((base.scheme or "https", base.netloc, href, "", "", ""))
        except Exception:
            pass

        # Parse date from link text
        d = _parse_date_from_text(text)
        if not d:
            continue

        # Default time window
        start = dt.datetime(d.year, d.month, d.day, 18, 0, tzinfo=tzinfo)
        end = start + dt.timedelta(hours=2)

        events.append({
            "title": text,
            "host": source.get("name"),
            "city": None,
            "state": None,
            "venue": safe_text(source.get("venue")) or None,
            "startAt": isoformat_with_tz(start),
            "endAt": isoformat_with_tz(end),
            "registrationURL": normalize_url(href),
            "imageURL": None,
            "detail": None,
            "links": [{"title": "Listing", "url": normalize_url(href)}],
            "_source": source["id"],
        })

    return events


EXTRACTORS = {
    "jsonld_events": extract_jsonld_events,
    "shopify_products_json": extract_shopify_products_json,
    "soundonstudio_classsignup": extract_soundonstudio_classsignup,
    "html_links_with_dates": extract_html_links_with_dates,
}


# -----------------------------
# Normalize / Merge / Prune
# -----------------------------

def normalize_workshop(e: Dict[str, Any], default_tz: str) -> Optional[Dict[str, Any]]:
    start = parse_date(e.get("startAt"), default_tz)
    if not start:
        return None
    end = parse_date(e.get("endAt"), default_tz) or start

    reg = normalize_url(e.get("registrationURL") or "")

    links = e.get("links") or None
    cleaned_links = None
    if links and isinstance(links, list):
        tmp = []
        for l in links:
            if not isinstance(l, dict):
                continue
            t = safe_text(l.get("title"), 80)
            u = normalize_url(l.get("url") or "")
            if t and u:
                tmp.append({"title": t, "url": u})
        cleaned_links = tmp or None

    out = {
        "id": None,  # set below
        "title": safe_text(e.get("title"), 220) or "Workshop",
        "host": safe_text(e.get("host"), 120),
        "city": safe_text(e.get("city"), 80),
        "state": safe_text(e.get("state"), 40),
        "venue": safe_text(e.get("venue"), 160),
        "startAt": isoformat_with_tz(start),
        "endAt": isoformat_with_tz(end),
        "registrationURL": reg or None,
        "imageURL": e.get("imageURL"),
        "detail": safe_text(e.get("detail")),
        "links": cleaned_links,
    }

    fp = workshop_fingerprint(out)
    out["id"] = stable_id(e.get("_source") or "ws", fp)
    return out


def merge_workshops(existing: List[Dict[str, Any]], incoming: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Merge without duplicates:
    - use workshop_fingerprint as the merge key
    - incoming fills blanks but doesn't wipe existing good fields
    """
    existing = existing or []
    incoming = incoming or []

    by_fp: Dict[str, Dict[str, Any]] = {}
    order: List[str] = []

    def score(w: Dict[str, Any]) -> int:
        return (
            int(bool(w.get("registrationURL"))) +
            int(bool(w.get("detail"))) +
            int(bool(w.get("links"))) +
            int(bool(w.get("venue")))
        )

    # Seed existing
    for w in existing:
        fp = workshop_fingerprint(w)
        if fp not in by_fp:
            by_fp[fp] = dict(w)
            order.append(fp)
        else:
            if score(w) > score(by_fp[fp]):
                by_fp[fp] = dict(w)

    # Merge incoming
    for w in incoming:
        fp = workshop_fingerprint(w)
        if fp in by_fp:
            cur = by_fp[fp]
            merged = dict(cur)
            # preserve existing id if present
            if cur.get("id"):
                w["id"] = cur["id"]
            for k, v in w.items():
                if v is None:
                    continue
                if isinstance(v, str) and v.strip() == "":
                    continue
                merged[k] = v
            by_fp[fp] = merged
        else:
            by_fp[fp] = dict(w)
            order.append(fp)

    merged_list = [by_fp[fp] for fp in order]

    def sort_key(w: Dict[str, Any]) -> float:
        try:
            d = dateparser.parse(w.get("startAt"))
            return d.timestamp() if d else 1e18
        except Exception:
            return 1e18

    merged_list.sort(key=sort_key)
    return merged_list


def prune_events(items: List[Dict[str, Any]], default_tz: str, prune_days_past: int, keep_days_future: int) -> List[Dict[str, Any]]:
    now = dt.datetime.now(tz=datetz.gettz(default_tz))
    past_cut = now - dt.timedelta(days=prune_days_past)
    future_cut = now + dt.timedelta(days=keep_days_future)

    kept = []
    for w in items:
        start = parse_date(w.get("startAt"), default_tz)
        end = parse_date(w.get("endAt"), default_tz) or start
        if not start:
            continue
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

    sources = cfg.get("sources", []) or []
    settings = cfg.get("settings", {}) or {}
    max_events_per_source = int(settings.get("max_events_per_source", 75))
    prune_days_past = int(settings.get("prune_days_past", 7))
    keep_days_future = int(settings.get("keep_days_future", 365))

    with open(args.resources, "r", encoding="utf-8") as f:
        resources = json.load(f)

    existing = resources.get("workshops", []) or []
    incoming_all: List[Dict[str, Any]] = []

    for s in sources:
        sid = s.get("id")
        url = s.get("url")
        if not sid or not url:
            print("[WARN] Source missing id/url, skipping.")
            continue

        extractor_name = s.get("extractor", "jsonld_events")
        fn = EXTRACTORS.get(extractor_name)
        if not fn:
            print(f"[WARN] Unknown extractor '{extractor_name}' for {sid}, skipping.")
            continue

        try:
            raw_events = fn(s, args.default_tz) or []
            raw_events = raw_events[:max_events_per_source]
            normalized: List[Dict[str, Any]] = []

            for e in raw_events:
                if not isinstance(e, dict):
                    continue
                e["_source"] = sid
                nw = normalize_workshop(e, args.default_tz)
                if nw:
                    normalized.append(nw)

            print(f"[OK] {sid}: {len(normalized)} events via {extractor_name}")
            incoming_all.extend(normalized)

        except Exception as ex:
            print(f"[ERROR] {sid}: {ex}")

    merged = merge_workshops(existing, incoming_all)
    merged = prune_events(merged, args.default_tz, prune_days_past, keep_days_future)
    resources["workshops"] = merged

    # bump lastUpdated (UTC date)
    resources["lastUpdated"] = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")

    with open(args.resources, "w", encoding="utf-8") as f:
        json.dump(resources, f, ensure_ascii=False, indent=2)
        f.write("\n")


if __name__ == "__main__":
    main()
