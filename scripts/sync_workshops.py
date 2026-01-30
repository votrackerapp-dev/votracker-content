#!/usr/bin/env python3
import argparse
import datetime as dt
import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode, urljoin

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser
from dateutil import tz as datetz


# -----------------------------
# Config / Constants
# -----------------------------

USER_AGENT = "VOTrackerWorkshopBot/1.1 (+https://github.com/votrackerapp-dev/votracker-content)"
TRACKING_PARAMS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "fbclid", "gclid", "mc_cid", "mc_eid"
}

MONTHS = (
    "january february march april may june july august september october november december"
).split()


@dataclass
class Settings:
    max_events_per_source: int = 50
    prune_days_past: int = 7
    keep_days_future: int = 365
    crawl_max_pages: int = 30          # for crawl_jsonld
    crawl_same_host_only: bool = True


# -----------------------------
# URL helpers
# -----------------------------

def normalize_url(url: str) -> str:
    if not url:
        return ""
    url = url.strip()
    try:
        p = urlparse(url)
        scheme = (p.scheme or "https").lower()
        netloc = (p.netloc or "").lower()

        q = [(k, v) for (k, v) in parse_qsl(p.query, keep_blank_values=True) if k not in TRACKING_PARAMS]
        query = urlencode(q, doseq=True)

        path = p.path or ""
        if path != "/" and path.endswith("/"):
            path = path[:-1]

        # Drop fragment always
        return urlunparse((scheme, netloc, path, "", query, ""))
    except Exception:
        return url


def same_host(a: str, b: str) -> bool:
    try:
        return (urlparse(a).netloc or "").lower() == (urlparse(b).netloc or "").lower()
    except Exception:
        return False


def slugify(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s[:80] or "workshop"


# -----------------------------
# Date parsing helpers
# -----------------------------

def tzinfo(default_tz: str):
    return datetz.gettz(default_tz) or datetz.UTC


def parse_date_any(value: Any, default_tz: str) -> Optional[dt.datetime]:
    if not value:
        return None
    try:
        d = dateparser.parse(str(value))
        if not d:
            return None
        if d.tzinfo is None:
            d = d.replace(tzinfo=tzinfo(default_tz))
        return d
    except Exception:
        return None


def isoformat_with_tz(d: dt.datetime, default_tz: str) -> str:
    if d.tzinfo is None:
        d = d.replace(tzinfo=tzinfo(default_tz))
    return d.isoformat()


def safe_text(s: Optional[str], max_len: int = 6000) -> Optional[str]:
    if s is None:
        return None
    t = re.sub(r"\s+", " ", str(s)).strip()
    if not t:
        return None
    return t[:max_len]


# Parse "4-7pm", "7:30-10pm", "4pm-7pm PT", etc.
_TIME_RANGE_RE = re.compile(
    r"(?P<s>\d{1,2}(?::\d{2})?)\s*(?P<samp>am|pm)\s*[-–]\s*(?P<e>\d{1,2}(?::\d{2})?)\s*(?P<eamp>am|pm)",
    re.IGNORECASE
)

def _to_24h(hm: str, ampm: str) -> Tuple[int, int]:
    if ":" in hm:
        h, m = hm.split(":", 1)
        h, m = int(h), int(m)
    else:
        h, m = int(hm), 0
    ampm = ampm.lower()
    if ampm == "pm" and h != 12:
        h += 12
    if ampm == "am" and h == 12:
        h = 0
    return h, m


# Parse from Shopify-ish slugs like:
# "...-tuesday-february-3rd-2026-4-7pm-pt-zoom"
def parse_datetime_from_slug(text: str, default_tz: str) -> Optional[Tuple[dt.datetime, Optional[dt.datetime]]]:
    if not text:
        return None
    t = text.lower().replace("_", "-").replace("/", "-")

    # Find "month day year"
    # day can be "3rd", "11th", "1", etc.
    month_pat = "(" + "|".join(MONTHS) + ")"
    m = re.search(rf"{month_pat}-(\d{{1,2}})(?:st|nd|rd|th)?-(20\d{{2}})", t)
    if not m:
        # also allow spaces
        m = re.search(rf"{month_pat}\s+(\d{{1,2}})(?:st|nd|rd|th)?\s+(20\d{{2}})", t)
    if not m:
        return None

    month_name = m.group(1)
    day = int(m.group(2))
    year = int(m.group(3))
    month = MONTHS.index(month_name) + 1

    # Try to find time range (4-7pm etc.)
    tr = _TIME_RANGE_RE.search(t.replace("-", " "))
    if tr:
        sh, sm = _to_24h(tr.group("s"), tr.group("samp"))
        eh, em = _to_24h(tr.group("e"), tr.group("eamp"))
        start = dt.datetime(year, month, day, sh, sm, tzinfo=tzinfo(default_tz))
        end = dt.datetime(year, month, day, eh, em, tzinfo=tzinfo(default_tz))
        # Handle end past midnight (rare)
        if end <= start:
            end = end + dt.timedelta(days=1)
        return start, end

    # If date but no time, set a reasonable default
    start = dt.datetime(year, month, day, 18, 0, tzinfo=tzinfo(default_tz))
    end = start + dt.timedelta(hours=2)
    return start, end


# -----------------------------
# HTTP / scraping helpers
# -----------------------------

def fetch_html(url: str) -> str:
    r = requests.get(url, timeout=30, headers={"User-Agent": USER_AGENT})
    r.raise_for_status()
    return r.text


def looks_like_shopify(html: str) -> bool:
    h = html.lower()
    return ("cdn.shopify.com" in h) or ("shopify" in h and "/products/" in h)


def extract_links(html: str, base_url: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    urls = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href:
            continue
        absu = urljoin(base_url, href)
        absu = normalize_url(absu)
        if absu:
            urls.append(absu)
    # unique preserving order
    seen = set()
    out = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


# -----------------------------
# Event extraction: JSON-LD
# -----------------------------

def extract_jsonld_events_from_html(html: str, page_url: str, source_name: str, source_id: str, default_tz: str) -> List[Dict[str, Any]]:
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
        start = parse_date_any(obj.get("startDate"), default_tz)
        end = parse_date_any(obj.get("endDate"), default_tz) or start
        url = obj.get("url") or page_url
        desc = obj.get("description")
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

        events.append({
            "title": safe_text(name, 200) or "Workshop",
            "host": safe_text(source_name, 120),
            "city": safe_text(city, 80),
            "state": safe_text(state, 40),
            "venue": safe_text(venue, 160),
            "startAt": isoformat_with_tz(start, default_tz),
            "endAt": isoformat_with_tz(end, default_tz) if end else isoformat_with_tz(start, default_tz),
            "registrationURL": normalize_url(url),
            "imageURL": image if isinstance(image, str) else None,
            "detail": safe_text(desc),
            "links": [{"title": "Event Page", "url": normalize_url(url)}],
            "_source": source_id
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

    return events


def extract_jsonld_events(source: Dict[str, Any], default_tz: str, settings: Settings) -> List[Dict[str, Any]]:
    html = fetch_html(source["url"])
    return extract_jsonld_events_from_html(html, source["url"], source.get("name") or source["id"], source["id"], default_tz)


# -----------------------------
# Extractor: SoundOnStudio classsignup
# -----------------------------

def extract_soundonstudio_classsignup(source: Dict[str, Any], default_tz: str, settings: Settings) -> List[Dict[str, Any]]:
    html = fetch_html(source["url"])
    text = BeautifulSoup(html, "html.parser").get_text("\n")

    # Example: "2.23.26 - Making Promos Pop! with Darius Marquis Johnson"
    pattern = re.compile(r"\b(\d{1,2})\.(\d{1,2})\.(\d{2})\s*-\s*([^\n]+)")
    out: List[Dict[str, Any]] = []

    for m in pattern.finditer(text):
        mm, dd, yy = int(m.group(1)), int(m.group(2)), int(m.group(3))
        year = 2000 + yy
        title = safe_text(m.group(4), 200)
        if not title:
            continue

        # Default time if not specified on page: 6–8pm local
        start = dt.datetime(year, mm, dd, 18, 0, tzinfo=tzinfo(default_tz))
        end = start + dt.timedelta(hours=2)

        # IMPORTANT:
        # SoundOn uses one signup URL for many items.
        # We keep registrationURL as the page, BUT we also create a unique link
        # fragment so ID + links remain stable per event.
        frag = f"{year:04d}{mm:02d}{dd:02d}-{slugify(title)}"
        unique_url = normalize_url(source["url"]) + "#" + frag

        out.append({
            "title": title,
            "host": safe_text(source.get("name"), 120),
            "city": None,
            "state": None,
            "venue": "See listing",
            "startAt": isoformat_with_tz(start, default_tz),
            "endAt": isoformat_with_tz(end, default_tz),
            "registrationURL": normalize_url(source["url"]),  # keep actual clickable page
            "imageURL": None,
            "detail": None,
            "links": [{"title": "Class Signup", "url": unique_url}],
            "_source": source["id"],
            "_uniqueKeyUrl": unique_url,  # used for stable IDs
        })

    return out


# -----------------------------
# Extractor: Shopify products listing
# (helps for VAN / VO Pros if their workshops are products)
# -----------------------------

def extract_shopify_products(source: Dict[str, Any], default_tz: str, settings: Settings) -> List[Dict[str, Any]]:
    base = source["url"]
    html = fetch_html(base)
    links = extract_links(html, base)

    product_links = [u for u in links if "/products/" in u]
    # cap
    product_links = product_links[: settings.max_events_per_source]

    events: List[Dict[str, Any]] = []
    for pl in product_links:
        try:
            ph = fetch_html(pl)
            soup = BeautifulSoup(ph, "html.parser")
            title = safe_text(soup.title.get_text(" ", strip=True) if soup.title else None, 200)
            # Shopify titles often: "Commercial Clinic - ... – Voice Actors Network"
            if title and "–" in title:
                title = title.split("–")[0].strip()

            # Parse date/time from URL slug if possible
            parsed = parse_datetime_from_slug(pl, default_tz) or parse_datetime_from_slug(ph.lower(), default_tz)
            if not parsed:
                # Try to find month/day/year in visible text quickly
                text = soup.get_text(" ", strip=True).lower()
                parsed = parse_datetime_from_slug(text, default_tz)

            if not parsed:
                continue

            start, end = parsed
            venue = "Zoom" if "zoom" in pl.lower() else None
            detail = None

            events.append({
                "title": title or "Workshop",
                "host": safe_text(source.get("name"), 120),
                "city": None,
                "state": None,
                "venue": venue or "See listing",
                "startAt": isoformat_with_tz(start, default_tz),
                "endAt": isoformat_with_tz(end, default_tz) if end else isoformat_with_tz(start, default_tz),
                "registrationURL": normalize_url(pl),
                "imageURL": None,
                "detail": detail,
                "links": [{"title": "Listing", "url": normalize_url(pl)}],
                "_source": source["id"]
            })
        except Exception:
            continue

    return events


# -----------------------------
# Extractor: Crawl for JSON-LD events
# (useful if a homepage doesn’t contain Event JSON-LD but links to event pages)
# -----------------------------

KEYWORD_HINTS = (
    "event", "events", "class", "classes", "workshop", "workshops", "clinic", "clinics",
    "calendar", "signup", "register", "ticket", "tickets", "guest-instructor"
)

def extract_crawl_jsonld(source: Dict[str, Any], default_tz: str, settings: Settings) -> List[Dict[str, Any]]:
    start_url = source["url"]
    start_html = fetch_html(start_url)
    queue = extract_links(start_html, start_url)

    # prioritize likely pages
    def score(u: str) -> int:
        ul = u.lower()
        return sum(1 for k in KEYWORD_HINTS if k in ul)

    queue.sort(key=score, reverse=True)

    seen = set()
    pages = []
    for u in [start_url] + queue:
        if u in seen:
            continue
        if settings.crawl_same_host_only and not same_host(u, start_url):
            continue
        if score(u) == 0 and u != start_url:
            continue
        seen.add(u)
        pages.append(u)
        if len(pages) >= settings.crawl_max_pages:
            break

    found: List[Dict[str, Any]] = []
    for u in pages:
        try:
            html = fetch_html(u)
            found.extend(extract_jsonld_events_from_html(
                html, u, source.get("name") or source["id"], source["id"], default_tz
            ))
        except Exception:
            continue

    return found


# -----------------------------
# Normalization + Stable IDs + Merge (NO DUPES)
# -----------------------------

def compute_stable_id(e: Dict[str, Any], source_url: str) -> str:
    """
    Stable ID strategy that DOES NOT collapse same-page multi-events:
    - Always include (source + title + startAt)
    - Include normalized registrationURL when it's unique (not equal to the base source_url)
    - If extractor provides _uniqueKeyUrl (like SoundOn), use that too
    """
    src = (e.get("_source") or "unknown").strip().lower()
    title = (e.get("title") or "").strip().lower()
    start = (e.get("startAt") or "").strip()

    reg = normalize_url(e.get("registrationURL") or "")
    base = normalize_url(source_url or "")
    unique_key_url = normalize_url(e.get("_uniqueKeyUrl") or "")

    parts = [src, title, start]

    # Only treat reg URL as unique if it's not the same as the source landing page
    if reg and base and reg != base:
        parts.append(reg)

    # If provided, always include unique key url
    if unique_key_url:
        parts.append(unique_key_url)

    raw = "|".join(parts)
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]
    return f"{src}-{digest}"


def normalize_workshop(e: Dict[str, Any], default_tz: str, source_url: str) -> Optional[Dict[str, Any]]:
    start = parse_date_any(e.get("startAt"), default_tz)
    if not start:
        return None
    end = parse_date_any(e.get("endAt"), default_tz) or start

    links = e.get("links") or None
    if links:
        cleaned = []
        for l in links:
            if not isinstance(l, dict):
                continue
            t = safe_text(l.get("title"), 80)
            u = normalize_url(l.get("url") or "")
            if t and u:
                cleaned.append({"title": t, "url": u})
        links = cleaned or None

    out = {
        "id": None,
        "title": safe_text(e.get("title"), 200) or "Workshop",
        "host": safe_text(e.get("host"), 120),
        "city": safe_text(e.get("city"), 80),
        "state": safe_text(e.get("state"), 40),
        "venue": safe_text(e.get("venue"), 160),
        "startAt": isoformat_with_tz(start, default_tz),
        "endAt": isoformat_with_tz(end, default_tz),
        "registrationURL": normalize_url(e.get("registrationURL") or ""),
        "imageURL": e.get("imageURL"),
        "detail": safe_text(e.get("detail")),
        "links": links
    }

    # compute stable id using source + title + start (+ unique url if appropriate)
    out["id"] = compute_stable_id(e, source_url)
    return out


def merge_workshops(existing: List[Dict[str, Any]], incoming: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Merge by stable id.
    - Keeps existing fields if new values are blank/None
    - Adds new workshops
    """
    by_id: Dict[str, Dict[str, Any]] = {w["id"]: dict(w) for w in existing if w.get("id")}

    for w in incoming:
        wid = w.get("id")
        if not wid:
            continue
        if wid in by_id:
            cur = by_id[wid]
            for k, v in w.items():
                if v is None:
                    continue
                if isinstance(v, str) and v.strip() == "":
                    continue
                # Don't overwrite existing good data with empty arrays
                if isinstance(v, list) and len(v) == 0:
                    continue
                cur[k] = v
        else:
            by_id[wid] = dict(w)

    merged = list(by_id.values())

    # sort by startAt
    def keyfn(x):
        try:
            return dateparser.parse(x.get("startAt")).timestamp()
        except Exception:
            return float("inf")

    merged.sort(key=keyfn)
    return merged


def prune_events(items: List[Dict[str, Any]], prune_days_past: int, keep_days_future: int, default_tz: str) -> List[Dict[str, Any]]:
    now = dt.datetime.now(tz=tzinfo(default_tz))
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
    return kept


# -----------------------------
# Extractor registry
# -----------------------------

def extract_html_fallback(source: Dict[str, Any], default_tz: str, settings: Settings) -> List[Dict[str, Any]]:
    """
    Safe fallback:
    - Try JSON-LD events from the page
    - If it's Shopify-like, try product extraction
    """
    html = fetch_html(source["url"])
    ev = extract_jsonld_events_from_html(html, source["url"], source.get("name") or source["id"], source["id"], default_tz)
    if ev:
        return ev
    if looks_like_shopify(html):
        return extract_shopify_products(source, default_tz, settings)
    return []


EXTRACTORS = {
    "jsonld_events": extract_jsonld_events,
    "soundonstudio_classsignup": extract_soundonstudio_classsignup,
    "shopify_products": extract_shopify_products,
    "crawl_jsonld": extract_crawl_jsonld,
    "html_fallback": extract_html_fallback,
}


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

    srcs = cfg.get("sources", [])
    s = cfg.get("settings", {}) or {}
    settings = Settings(
        max_events_per_source=int(s.get("max_events_per_source", 50)),
        prune_days_past=int(s.get("prune_days_past", 7)),
        keep_days_future=int(s.get("keep_days_future", 365)),
        crawl_max_pages=int(s.get("crawl_max_pages", 30)),
        crawl_same_host_only=bool(s.get("crawl_same_host_only", True)),
    )

    with open(args.resources, "r", encoding="utf-8") as f:
        resources = json.load(f)

    existing = resources.get("workshops", []) or []

    incoming_all: List[Dict[str, Any]] = []
    for src in srcs:
        sid = src.get("id")
        name = src.get("name") or sid
        url = src.get("url")
        extractor_name = src.get("extractor", "jsonld_events")

        fn = EXTRACTORS.get(extractor_name)
        if not fn:
            print(f"[WARN] Unknown extractor '{extractor_name}' for {sid}, skipping.")
            continue

        try:
            raw_events = fn(src, args.default_tz, settings) or []
            raw_events = raw_events[: settings.max_events_per_source]

            normalized: List[Dict[str, Any]] = []
            for e in raw_events:
                # ensure source id present for stable IDs
                e["_source"] = sid
                nw = normalize_workshop(e, args.default_tz, url)
                if nw:
                    normalized.append(nw)

            print(f"[OK] {sid}: {len(normalized)} events ({extractor_name})")
            incoming_all.extend(normalized)

        except Exception as ex:
            print(f"[ERROR] {sid}: {ex}")

    merged = merge_workshops(existing, incoming_all)
    merged = prune_events(merged, settings.prune_days_past, settings.keep_days_future, args.default_tz)

    resources["workshops"] = merged
    resources["lastUpdated"] = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")

    with open(args.resources, "w", encoding="utf-8") as f:
        json.dump(resources, f, ensure_ascii=False, indent=2)
        f.write("\n")

    print(f"[DONE] workshops: {len(existing)} -> {len(merged)}")


if __name__ == "__main__":
    main()
