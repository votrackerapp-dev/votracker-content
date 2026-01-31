#!/usr/bin/env python3
"""
sync_workshops.py - VO Workshop Scraper for VOTracker
IMPROVED: Better per-event time extraction
"""
import argparse
import datetime as dt
import hashlib
import json
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode, urljoin

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser
from dateutil import tz as datetz

DEFAULT_EVENT_DURATION_HOURS = 2
DEFAULT_TIMEZONE = "America/Los_Angeles"

TRACKING_PARAMS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "fbclid", "gclid", "mc_cid", "mc_eid"
}

# Enable verbose logging for debugging
VERBOSE = True

def log(msg: str):
    if VERBOSE:
        print(msg)

def safe_text(s: Optional[str], max_len: int = 6000) -> Optional[str]:
    if s is None:
        return None
    t = re.sub(r"\s+", " ", s).strip()
    if not t:
        return None
    return t[:max_len]

def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def normalize_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    u = url.strip()
    if not u:
        return None
    try:
        p = urlparse(u)
        scheme = (p.scheme or "https").lower()
        netloc = (p.netloc or "").lower()
        path = p.path or ""
        if path != "/" and path.endswith("/"):
            path = path[:-1]
        q = [(k, v) for (k, v) in parse_qsl(p.query, keep_blank_values=True) if k not in TRACKING_PARAMS]
        query = urlencode(q, doseq=True)
        return urlunparse((scheme, netloc, path, "", query, ""))
    except Exception:
        return u

def fetch_html(url: str, headers: Optional[Dict[str, str]] = None) -> str:
    """Fetch HTML with a stable UA, optional headers, and light retries."""
    base_headers = {"User-Agent": "VOTrackerWorkshopBot/1.3 (+https://votracker.app)"}
    if headers:
        base_headers.update({k: str(v) for k, v in headers.items() if v is not None})
    last_err: Optional[Exception] = None
    for attempt in range(3):
        try:
            r = requests.get(url, timeout=30, headers=base_headers)
            if r.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"HTTP {r.status_code}")
            r.raise_for_status()
            return r.text
        except Exception as e:
            last_err = e
            try:
                import time
                time.sleep(1.0 + attempt * 1.25)
            except Exception:
                pass
    raise last_err or RuntimeError("fetch_html failed")


def iter_paginated_pages(start_url: str, max_pages: int, headers: Optional[Dict[str, str]] = None) -> List[Tuple[str, str]]:
    """Return a list of (url, html) for a paginated sequence starting at start_url."""
    pages: List[Tuple[str, str]] = []
    visited: set = set()
    url = start_url
    for _ in range(max(1, int(max_pages or 1))):
        if not url or url in visited:
            break
        visited.add(url)
        html = fetch_html(url, headers=headers)
        pages.append((url, html))

        soup = BeautifulSoup(html, "html.parser")

        next_href = None
        link_next = soup.find("link", rel=lambda v: v and "next" in v)
        if link_next and link_next.get("href"):
            next_href = link_next["href"]

        if not next_href:
            a_next = soup.find("a", rel=lambda v: v and "next" in v, href=True)
            if a_next:
                next_href = a_next["href"]

        if not next_href:
            a_next = soup.select_one("a.next, a.page-numbers.next, li.next a, a[aria-label='Next']")
            if a_next and a_next.get("href"):
                next_href = a_next["href"]

        if not next_href:
            break

        url = urljoin(url, next_href)

    return pages

def parse_date_any(value: Any, default_tz: str) -> Optional[dt.datetime]:
    if not value:
        return None
    try:
        s = str(value).strip()
        if not s:
            return None
        tzinfo = datetz.gettz(default_tz)
        now = dt.datetime.now(tz=tzinfo)

        d = dateparser.parse(s, fuzzy=True, default=now)
        if not d:
            return None
        if d.tzinfo is None:
            d = d.replace(tzinfo=tzinfo)

        has_year = bool(re.search(r"\b\d{4}\b", s))
        if not has_year:
            if d < (now - dt.timedelta(days=60)):
                try:
                    d = d.replace(year=d.year + 1)
                except Exception:
                    pass
        return d
    except Exception:
        return None

def isoformat_with_tz(d: dt.datetime, default_tz: str) -> str:
    if d.tzinfo is None:
        d = d.replace(tzinfo=datetz.gettz(default_tz))
    return d.isoformat()

def compute_event_id(source_id: str, title: str, start_at: str, reg_url: Optional[str]) -> str:
    if reg_url:
        key = f"{source_id}|url|{reg_url}"
    else:
        key = f"{source_id}|ts|{title.strip().lower()}|{start_at}"
    return f"{source_id}-{sha1(key)[:16]}"


def clean_whitespace(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def stable_event_id(source_id: str, url: str, title: str, start: dt.datetime) -> str:
    reg = normalize_url(url) or ""
    start_s = isoformat_with_tz(start, DEFAULT_TIMEZONE)
    return compute_event_id(source_id or "src", title or "Workshop", start_s, reg)

def parse_month_day_year(date_str: str, default_tz: str) -> Optional[dt.datetime]:
    return parse_date_any(date_str, default_tz)

def parse_fuzzy_date(date_str: str, default_tz: str) -> Optional[dt.datetime]:
    return parse_date_any(date_str, default_tz)

def ensure_workshop_schema(e: Dict[str, Any], source: Dict[str, Any], default_tz: str) -> Dict[str, Any]:
    """Normalize an event dict from any extractor into the canonical resources.json schema."""
    src_id = (source.get("id") or "unknown").strip()
    src_name = (source.get("name") or src_id).strip()

    title = safe_text(e.get("title") or e.get("name") or "Workshop", 200) or "Workshop"

    reg_url = normalize_url(
        e.get("registrationURL")
        or e.get("url")
        or e.get("link")
        or e.get("registrationUrl")
        or source.get("url")
    )

    raw_start = e.get("startAt") or e.get("start") or e.get("start_date") or e.get("startDate")
    raw_end = e.get("endAt") or e.get("end") or e.get("end_date") or e.get("endDate")

    start_dt = parse_date_any(raw_start, default_tz) if raw_start else None
    end_dt = parse_date_any(raw_end, default_tz) if raw_end else None

    if start_dt and not end_dt:
        end_dt = start_dt + dt.timedelta(hours=DEFAULT_EVENT_DURATION_HOURS)
    if start_dt and end_dt and end_dt <= start_dt:
        end_dt = end_dt + dt.timedelta(days=1)

    start_s = isoformat_with_tz(start_dt, default_tz) if start_dt else None
    end_s = isoformat_with_tz(end_dt, default_tz) if end_dt else None

    host = safe_text(e.get("host") or e.get("instructor"), 160) or src_name
    detail = safe_text(e.get("detail") or e.get("details") or e.get("description"))

    links = e.get("links")
    if isinstance(links, list):
        cleaned_links = []
        for it in links:
            if isinstance(it, dict) and it.get("url"):
                cleaned_links.append({
                    "title": safe_text(it.get("title") or "Link", 80) or "Link",
                    "url": normalize_url(it.get("url")),
                })
        links = cleaned_links or None
    else:
        links = None

    if not links and reg_url:
        links = [{"title": "Event Page", "url": reg_url}]

    provider = src_id
    provider_label = src_name

    out = {
        "id": e.get("id"),
        "title": title,
        "host": host,
        "city": safe_text(e.get("city"), 80),
        "state": safe_text(e.get("state"), 40),
        "venue": safe_text(e.get("venue"), 160),
        "startAt": start_s or (raw_start if isinstance(raw_start, str) else None),
        "endAt": end_s or (raw_end if isinstance(raw_end, str) else None),
        "registrationURL": reg_url,
        "imageURL": normalize_url(e.get("imageURL") or e.get("imageUrl") or e.get("image")),
        "detail": detail,
        "provider": provider,
        "providerLabel": provider_label,
        "links": links,
    }

    if not out["id"] and out.get("startAt"):
        out["id"] = compute_event_id(src_id, title, out["startAt"], reg_url)

    return out

def rebuild_workshops(
    existing: List[Dict[str, Any]], 
    incoming: List[Dict[str, Any]],
    successfully_scraped_sources: set
) -> List[Dict[str, Any]]:
    """
    Rebuild workshop list preferring NEW data.
    
    Logic:
    - For sources that were successfully scraped: USE ONLY NEW DATA (removes stale events)
    - For sources that failed to scrape: KEEP OLD DATA (don't lose everything if site is down)
    - This ensures removed/changed events don't linger
    """
    result: List[Dict[str, Any]] = []
    
    # Step 1: Keep existing events ONLY from sources that failed to scrape
    # (so we don't lose data if a website is temporarily down)
    for w in (existing or []):
        event_id = w.get("id", "")
        # Extract source_id from event ID (format: "sourceid-hash")
        source_id = event_id.split("-")[0] if "-" in event_id else None
        
        # Only keep if this source WASN'T successfully scraped
        # (meaning we couldn't get fresh data, so keep the old)
        if source_id and source_id not in successfully_scraped_sources:
            result.append(dict(w))
            log(f"  [KEEP] Keeping old event from failed source '{source_id}': {w.get('title', '')[:40]}")
    
    # Step 2: Add ALL incoming events (fresh data from successful scrapes)
    for w in (incoming or []):
        if w.get("id"):
            result.append(dict(w))
    
    # Step 3: Sort by start date
    def keyfn(x):
        try:
            return dateparser.parse(x.get("startAt")).timestamp()
        except Exception:
            return float("inf")
    
    result.sort(key=keyfn)
    return result

def is_ongoing_class(title: str, detail: str = None) -> bool:
    """
    Detect multi-session courses vs single workshops.
    Returns True if this appears to be an ongoing course rather than a single workshop.
    """
    combined = (title + " " + (detail or "")).lower()
    
    ongoing_patterns = [
        r'\b\d+\s*-?\s*(week|session|class|month)\s+(course|pack|series|program)',
        r'(pack|package|bundle|series)\b',
        r'(monthly|weekly|ongoing|continuous)\s+(class|workshop)',
        r'\bcurriculum\b',
        r'\bsemester\b',
        r'\bprogram\b.*\b\d+\s+weeks?',
        r'\d+\s*-?\s*session',
        r'class\s+pack',
        r'multi-week',
        r'series\s+of\s+\d+',
    ]
    
    for pattern in ongoing_patterns:
        if re.search(pattern, combined, re.IGNORECASE):
            log(f"  [FILTER] Detected ongoing class: '{title[:50]}...' (pattern: {pattern})")
            return True
    
    return False

def prune_events(items: List[Dict[str, Any]], prune_days_past: int, keep_days_future: int, default_tz: str) -> List[Dict[str, Any]]:
    now = dt.datetime.now(tz=datetz.gettz(default_tz))
    past_cut = now - dt.timedelta(days=prune_days_past)
    future_cut = now + dt.timedelta(days=keep_days_future)
    kept: List[Dict[str, Any]] = []
    for w in items:
        start = parse_date_any(w.get("startAt"), default_tz)
        end = parse_date_any(w.get("endAt"), default_tz) or start
        if not start:
            continue
        if end and end >= past_cut and start <= future_cut:
            kept.append(w)
    return kept

# -----------------------------
# IMPROVED Time parsing helpers
# -----------------------------

# Matches: "7pm-10pm", "7:00pm - 10:00pm", "7 pm to 10 pm" (both have AM/PM)
TIME_RANGE_FULL_RE = re.compile(
    r"\b(\d{1,2})(?::(\d{2}))?\s*(am|pm)\s*(?:[-–—to]+)\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm)\b",
    re.IGNORECASE,
)

# Matches: "7-10pm", "7:00-10:00pm" (only END has AM/PM - very common!)
TIME_RANGE_END_ONLY_RE = re.compile(
    r"\b(\d{1,2})(?::(\d{2}))?\s*(?:[-–—to]+)\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm)\b",
    re.IGNORECASE,
)

# Matches: "7pm", "7:00 PM", "at 7pm"
SINGLE_TIME_RE = re.compile(r"\b(?:at\s+)?(\d{1,2})(?::(\d{2}))?\s*(am|pm)\b", re.IGNORECASE)

# Matches times in parentheses or after pipe: "(7-10pm PT)" or "| 7-10pm PT"
TIME_IN_CONTEXT_RE = re.compile(
    r"[\(|\|]\s*(\d{1,2})(?::(\d{2}))?\s*(?:[-–—to]+)\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm)\s*(?:PT|PST|PDT|ET|EST|EDT|CT|CST|CDT|MT|MST|MDT)?\s*[\)]?",
    re.IGNORECASE
)

def _to_24h(h: int, m: int, ap: Optional[str]) -> Tuple[int, int]:
    """Convert 12-hour time to 24-hour."""
    if ap is None:
        # No AM/PM - assume PM for typical evening workshop hours (5-11)
        if h >= 1 and h <= 6:
            h += 12  # 1-6 -> 13-18 (1pm-6pm)
        elif h >= 7 and h <= 11:
            h += 12  # 7-11 -> 19-23 (7pm-11pm)
        return h, m
    
    ap = ap.lower()
    if ap == "am":
        if h == 12:
            h = 0
    else:  # pm
        if h != 12:
            h += 12
    return h, m

def extract_time_from_text(text: str) -> Tuple[Optional[int], Optional[int], Optional[int], Optional[int]]:
    """
    Extract start hour/minute and end hour/minute from text.
    Returns (start_hour_24, start_min, end_hour_24, end_min) or (None, None, None, None)
    
    IMPROVED: Handles multiple formats including "7-10pm" where only end has AM/PM
    """
    if not text:
        return None, None, None, None
    
    # 1. Try contextual format first (in parens or after pipe) - most specific
    m = TIME_IN_CONTEXT_RE.search(text)
    if m:
        sh, sm = int(m.group(1)), int(m.group(2) or 0)
        eh, em = int(m.group(3)), int(m.group(4) or 0)
        eap = m.group(5)
        sap = eap if (sh <= eh or sh == 12) else eap
        sh24, sm = _to_24h(sh, sm, sap)
        eh24, em = _to_24h(eh, em, eap)
        log(f"    [TIME] Found contextual: {sh}:{sm:02d}-{eh}:{em:02d} {eap} -> {sh24}:{sm:02d}-{eh24}:{em:02d}")
        return sh24, sm, eh24, em
    
    # 2. Try full range with AM/PM on both
    m = TIME_RANGE_FULL_RE.search(text)
    if m:
        sh, sm = int(m.group(1)), int(m.group(2) or 0)
        sap = m.group(3)
        eh, em = int(m.group(4)), int(m.group(5) or 0)
        eap = m.group(6)
        sh24, sm = _to_24h(sh, sm, sap)
        eh24, em = _to_24h(eh, em, eap)
        log(f"    [TIME] Found full range: {sh}:{sm:02d}{sap}-{eh}:{em:02d}{eap} -> {sh24}:{sm:02d}-{eh24}:{em:02d}")
        return sh24, sm, eh24, em
    
    # 3. Try range with AM/PM only on end (e.g., "7-10pm")
    m = TIME_RANGE_END_ONLY_RE.search(text)
    if m:
        sh, sm = int(m.group(1)), int(m.group(2) or 0)
        eh, em = int(m.group(3)), int(m.group(4) or 0)
        eap = m.group(5)
        # Infer: if "7-10pm", both are PM. If "10-1pm", 10 is AM, 1 is PM
        if sh > eh and sh >= 10:
            sap = "am"
        else:
            sap = eap
        sh24, sm = _to_24h(sh, sm, sap)
        eh24, em = _to_24h(eh, em, eap)
        log(f"    [TIME] Found end-only range: {sh}:{sm:02d}-{eh}:{em:02d}{eap} -> {sh24}:{sm:02d}-{eh24}:{em:02d}")
        return sh24, sm, eh24, em
    
    # 4. Try single time
    m = SINGLE_TIME_RE.search(text)
    if m:
        sh, sm = int(m.group(1)), int(m.group(2) or 0)
        sap = m.group(3)
        sh24, sm = _to_24h(sh, sm, sap)
        eh24 = sh24 + DEFAULT_EVENT_DURATION_HOURS
        if eh24 >= 24:
            eh24 = 23
        log(f"    [TIME] Found single time: {sh}:{sm:02d}{sap} -> {sh24}:{sm:02d} + {DEFAULT_EVENT_DURATION_HOURS}h")
        return sh24, sm, eh24, sm
    
    log(f"    [TIME] No time found in: {text[:80]}...")
    return None, None, None, None

def apply_time_to_date(base_date: dt.datetime, text: str, default_tz: str) -> Tuple[dt.datetime, dt.datetime]:
    """
    Apply extracted time to a base date.
    Returns (start_datetime, end_datetime)
    """
    tzinfo = datetz.gettz(default_tz)
    base_date = base_date.replace(tzinfo=tzinfo, hour=0, minute=0, second=0, microsecond=0)
    
    sh, sm, eh, em = extract_time_from_text(text)
    
    if sh is not None:
        start = base_date.replace(hour=sh, minute=sm)
        end = base_date.replace(hour=eh, minute=em)
        if end <= start:
            end = end + dt.timedelta(days=1)
        return start, end
    
    # No time found - default to 6pm + 2 hours
    log(f"    [TIME] Using default 6pm")
    start = base_date.replace(hour=18, minute=0)
    end = start + dt.timedelta(hours=DEFAULT_EVENT_DURATION_HOURS)
    return start, end

def infer_year_for_month_day(month_day_str: str, default_tz: str) -> dt.datetime:
    now = dt.datetime.now(tz=datetz.gettz(default_tz))
    d = dateparser.parse(f"{month_day_str} {now.year}", fuzzy=True)
    if not d:
        return now
    d = d.replace(tzinfo=datetz.gettz(default_tz))
    if d < (now - dt.timedelta(days=45)):
        d = d.replace(year=now.year + 1)
    return d


def infer_year_for_mm_dd(mm_dd_str: str, default_tz: str) -> Optional[dt.datetime]:
    """Infer a datetime for dates shown as M.D / MM.DD without a year.

    Example: "1.31" -> Jan 31 this year (or next year if it's already far in the past).
    """
    m = re.search(r"\b(\d{1,2})\.(\d{1,2})\b", mm_dd_str)
    if not m:
        return None
    month = int(m.group(1))
    day = int(m.group(2))

    tzinfo = datetz.gettz(default_tz)
    now = dt.datetime.now(tz=tzinfo)
    try:
        candidate = dt.datetime(now.year, month, day, tzinfo=tzinfo)
    except ValueError:
        return None

    # If it's more than ~90 days in the past, it's probably next year.
    if candidate < (now - dt.timedelta(days=90)):
        try:
            candidate = dt.datetime(now.year + 1, month, day, tzinfo=tzinfo)
        except ValueError:
            return None
    return candidate

# -----------------------------
# Extractors
# -----------------------------

def extract_jsonld_events(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """
    Extract events from JSON-LD structured data - most reliable source!
    Falls back to HTML parsing if no JSON-LD found.
    """
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

        name = safe_text(obj.get("name"), 200) or "Workshop"
        start = parse_date_any(obj.get("startDate"), default_tz)
        end = parse_date_any(obj.get("endDate"), default_tz) or start
        url = normalize_url(obj.get("url") or source["url"])
        desc = safe_text(obj.get("description"))

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

        if not start:
            return

        start_s = isoformat_with_tz(start, default_tz)
        end_s = isoformat_with_tz(end, default_tz) if end else None

        log(f"  [JSON-LD] {name[:50]}... @ {start_s}")

        reg = url
        wid = compute_event_id(source["id"], name, start_s, reg)

        out.append({
            "id": wid,
            "title": name,
            "host": source.get("name"),
            "city": city,
            "state": state,
            "venue": venue,
            "startAt": start_s,
            "endAt": end_s,
            "registrationURL": reg,
            "imageURL": obj.get("image") if isinstance(obj.get("image"), str) else None,
            "detail": desc,
            "links": [{"title": "Event Page", "url": reg}] if reg else None
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

    # If no JSON-LD events found, try HTML parsing as fallback
    if not out:
        log(f"  [JSON-LD] No JSON-LD found for {source.get('name')}, trying HTML fallback...")
        out = extract_html_events_fallback(source, soup, default_tz)

    return out


def extract_tidycal_index(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """Extract public class listings from a TidyCal organizer page.

    Designed for pages like https://tidycal.com/redscythestudio/ where
    each class card contains a title like:
        "1.31 | JOE HERNANDEZ | video game characters"

    What we keep:
    - Entries that look like a dated class with an instructor.
    What we skip:
    - Coaching / consultations / marketing / generic downloads.
    """

    url = source["url"]
    html = fetch_html(url, source.get("headers"))
    soup = BeautifulSoup(html, "html.parser")

    # Find all "View details" links; each points to a TidyCal booking page.
    detail_links: List[str] = []
    for a in soup.select('a[href^="https://tidycal.com/"]'):
        href = a.get("href") or ""
        if not href:
            continue
        if "tidycal.com/" not in href:
            continue
        # Prefer the explicit buttons.
        a_text = (a.get_text(" ", strip=True) or "").lower()
        if "view details" in a_text:
            detail_links.append(href)

    # De-dup while preserving order.
    seen = set()
    detail_links = [u for u in detail_links if not (u in seen or seen.add(u))]

    # The card title is usually present as a heading element.
    # We’ll also fall back to scraping the whole page text and matching patterns.
    page_text = soup.get_text("\n", strip=True)

    # Match: MM.DD | INSTRUCTOR | TOPIC
    title_re = re.compile(r"\b(\d{1,2}\.\d{1,2})\s*\|\s*([^|]{2,80}?)\s*\|\s*([^\n|]{2,200})", re.IGNORECASE)

    # Keywords to drop (you can also reinforce these with source["filters"]).
    drop_kw = {
        "coaching", "coach", "consult", "consultation", "1:1", "one-on-one",
        "marketing", "freelance", "download", "video", "demo review", "demo", "reel",
    }

    events: List[Dict[str, Any]] = []

    # Primary pass: walk card containers that have a "View details" button.
    for href in detail_links:
        a = soup.select_one(f'a[href="{href}"]')
        card = a.find_parent(["article", "section", "li", "div"]) if a else None
        card_text = ""
        if card:
            card_text = card.get_text(" ", strip=True)
        else:
            card_text = page_text

        m = title_re.search(card_text)
        if not m:
            continue

        mm_dd = m.group(1).strip()
        instructor = clean_whitespace(m.group(2))
        topic = clean_whitespace(m.group(3))

        combined = f"{mm_dd} {instructor} {topic}".lower()
        if any(k in combined for k in drop_kw):
            continue
        # Basic sanity: instructor should look like a name (at least 2 words).
        if len(instructor.split()) < 2:
            continue

        base_dt = infer_year_for_mm_dd(mm_dd, default_tz)
        if not base_dt:
            continue

        # TidyCal index usually doesn't show a start time; default 10:00am.
        start = base_dt.replace(hour=10, minute=0, second=0, microsecond=0)

        # Duration is often shown as "4 hours".
        duration_hours = None
        dur_m = re.search(r"(\d+)\s*hours?", card_text, flags=re.IGNORECASE)
        if dur_m:
            try:
                duration_hours = int(dur_m.group(1))
            except Exception:
                duration_hours = None
        end = start + dt.timedelta(hours=duration_hours or 3)

        price = None
        price_m = re.search(r"\$(\d+(?:\.\d{1,2})?)", card_text)
        if price_m:
            price = f"${price_m.group(1)}"

        title = f"{instructor} — {topic}"
        detail_parts = [source.get("name", "TidyCal")]  # context
        if duration_hours:
            detail_parts.append(f"Duration: {duration_hours} hours")
        if price:
            detail_parts.append(f"Price: {price}")
        detail_parts.append(f"Registration: {href}")

        events.append(
            {
                "id": stable_event_id(source.get("id", url), href, title, start),
                "sourceId": source.get("id"),
                "sourceName": source.get("name"),
                "title": title,
                "start": start.isoformat(),
                "end": end.isoformat(),
                "timezone": default_tz,
                "url": href,
                "details": "\n".join(detail_parts),
                "instructor": instructor,
                "category": "Workshop",
            }
        )

    # Secondary pass: if nothing found, try matching the whole page text.
    if not events:
        for m in title_re.finditer(page_text):
            mm_dd = m.group(1).strip()
            instructor = clean_whitespace(m.group(2))
            topic = clean_whitespace(m.group(3))
            combined = f"{mm_dd} {instructor} {topic}".lower()
            if any(k in combined for k in drop_kw):
                continue
            if len(instructor.split()) < 2:
                continue
            base_dt = infer_year_for_mm_dd(mm_dd, default_tz)
            if not base_dt:
                continue
            start = base_dt.replace(hour=10, minute=0, second=0, microsecond=0)
            end = start + dt.timedelta(hours=3)
            href = source["url"]
            title = f"{instructor} — {topic}"
            events.append(
                {
                    "id": stable_event_id(source.get("id", url), href, title, start),
                    "sourceId": source.get("id"),
                    "sourceName": source.get("name"),
                    "title": title,
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                    "timezone": default_tz,
                    "url": href,
                    "details": source.get("name", "TidyCal"),
                    "instructor": instructor,
                    "category": "Workshop",
                }
            )

    log(f"  [TidyCal] Parsed {len(events)} class entries from {url}")
    return events


def extract_html_events_fallback(source: Dict[str, Any], soup: BeautifulSoup, default_tz: str) -> List[Dict[str, Any]]:
    """
    Fallback HTML parser for sites without JSON-LD.
    Looks for common event patterns in HTML.
    """
    events: List[Dict[str, Any]] = []
    txt = soup.get_text("\n")
    
    # Look for event-like elements
    event_containers = soup.find_all(['article', 'div', 'li'], class_=lambda x: x and any(
        kw in str(x).lower() for kw in ['event', 'workshop', 'class', 'session', 'seminar', 'course']
    ))
    
    # Date pattern
    date_re = re.compile(r"([A-Za-z]{3,9}\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4})")
    
    # Process containers
    for container in event_containers[:50]:
        try:
            container_text = container.get_text(" ", strip=True)
            
            # Find date
            date_match = date_re.search(container_text)
            if not date_match:
                continue
            
            base = parse_date_any(date_match.group(1), default_tz)
            if not base:
                continue
            
            # Find title
            title_el = container.find(['h1', 'h2', 'h3', 'h4', 'a', 'strong'])
            title = safe_text(title_el.get_text(" ", strip=True), 150) if title_el else "Workshop"
            
            # Skip junk
            if len(title) < 5 or any(junk in title.lower() for junk in ['sign up', 'subscribe', 'contact']):
                continue
            
            # Extract time
            start, end = apply_time_to_date(base, container_text, default_tz)
            start_s = isoformat_with_tz(start, default_tz)
            end_s = isoformat_with_tz(end, default_tz)
            
            # Find link
            link_el = container.find('a', href=True)
            reg_url = None
            if link_el:
                href = link_el.get('href', '')
                reg_url = normalize_url(urljoin(source["url"], href))
            
            wid = compute_event_id(source["id"], title, start_s, reg_url)
            
            log(f"  [HTML-Fallback] {title[:50]}... @ {start_s}")
            
            events.append({
                "id": wid,
                "title": title,
                "host": source.get("name"),
                "city": None,
                "state": None,
                "venue": "See listing",
                "startAt": start_s,
                "endAt": end_s,
                "registrationURL": reg_url,
                "imageURL": None,
                "detail": None,
                "links": [{"title": "Event Page", "url": reg_url or source["url"]}]
            })
        except Exception as e:
            log(f"  [HTML-Fallback] Error: {e}")
            continue
    
    # If still no events, try scanning the full page text
    if not events:
        dates = date_re.findall(txt)
        for date_str in dates[:20]:
            base = parse_date_any(date_str, default_tz)
            if not base:
                continue
            
            # Find context around the date
            date_pos = txt.find(date_str)
            context_start = max(0, date_pos - 100)
            context_end = min(len(txt), date_pos + 200)
            context = txt[context_start:context_end]
            
            # Try to extract a title from context
            lines = context.split('\n')
            title = None
            for line in lines:
                line = line.strip()
                if len(line) > 10 and line != date_str and not re.fullmatch(r"[\d\s:apm-]+", line, re.IGNORECASE):
                    title = safe_text(line, 120)
                    break
            
            if not title:
                continue
            
            start, end = apply_time_to_date(base, context, default_tz)
            start_s = isoformat_with_tz(start, default_tz)
            end_s = isoformat_with_tz(end, default_tz)
            
            wid = compute_event_id(source["id"], title, start_s, None)
            
            # Avoid duplicates
            if any(e.get("startAt", "")[:16] == start_s[:16] for e in events):
                continue
            
            log(f"  [HTML-Scan] {title[:50]}... @ {start_s}")
            
            events.append({
                "id": wid,
                "title": title,
                "host": source.get("name"),
                "city": None,
                "state": None,
                "venue": "See listing",
                "startAt": start_s,
                "endAt": end_s,
                "registrationURL": source["url"],
                "imageURL": None,
                "detail": None,
                "links": [{"title": "Event Page", "url": source["url"]}]
            })
    
    return events

def extract_soundonstudio_classsignup(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """Sound On Studio class signup page."""
    html = fetch_html(source["url"])
    soup = BeautifulSoup(html, "html.parser")
    full_text = soup.get_text("\n")

    pattern = re.compile(r"\b(\d{1,2})\.(\d{1,2})\.(\d{2})\s*-\s*([^\n]+)")
    events: List[Dict[str, Any]] = []

    for m in pattern.finditer(full_text):
        mm, dd, yy = int(m.group(1)), int(m.group(2)), int(m.group(3))
        year = 2000 + yy
        title_raw = m.group(4).strip()
        title = safe_text(title_raw, 200)
        if not title:
            continue

        base = dt.datetime(year, mm, dd, 0, 0, tzinfo=datetz.gettz(default_tz))
        
        match_pos = m.start()
        context_start = max(0, match_pos - 100)
        context_end = min(len(full_text), m.end() + 200)
        context = full_text[context_start:context_end]
        
        log(f"  [SoundOn] Processing: {title[:50]}...")
        start, end = apply_time_to_date(base, title_raw + " " + context, default_tz)

        start_s = isoformat_with_tz(start, default_tz)
        end_s = isoformat_with_tz(end, default_tz)

        wid = compute_event_id(source["id"], title, start_s, None)

        events.append({
            "id": wid,
            "title": title,
            "host": source.get("name"),
            "city": None,
            "state": None,
            "venue": "See listing",
            "startAt": start_s,
            "endAt": end_s,
            "registrationURL": normalize_url(source["url"]),
            "imageURL": None,
            "detail": None,
            "links": [{"title": "Class Signup", "url": normalize_url(source["url"])}]
        })

    return events

def extract_thevopros_events_index(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """
    Scrape The VO Pros events pages.
    COMPLETELY REWRITTEN: Now extracts from URL slug as last resort
    """
    base_url = source["url"]
    max_pages = int(source.get("max_event_pages") or 2)
    pages = iter_paginated_pages(base_url, max_pages, headers=source.get("headers"))

    links: List[str] = []
    for page_url, index_html in pages:
        soup = BeautifulSoup(index_html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/events/" in href and not href.endswith("/events/") and not href.endswith("/events"):
                full = href if href.startswith("http") else urljoin(page_url, href)
                full = normalize_url(full)
                if full and full not in links:
                    links.append(full)

    log(f"  [VOPros] Found {len(links)} event links")
    
    events: List[Dict[str, Any]] = []
    for ev_url in links[:120]:
        try:
            html = fetch_html(ev_url, headers=source.get("headers"))
            ps = BeautifulSoup(html, "html.parser")
            txt = ps.get_text("\n")

            # REWRITTEN: More aggressive title extraction
            title = None
            
            # Try 1: Look for ANY h1 that's meaningful (>10 chars)
            h1_tags = ps.find_all("h1")
            for h1 in h1_tags:
                potential = safe_text(h1.get_text(" ", strip=True), 200)
                if potential and len(potential) > 10 and potential.upper() not in ["THE VO PROS", "THEVOPROS", "VO PROS", "EVENTS"]:
                    title = potential
                    log(f"  [VOPros] Found h1: '{title[:40]}...'")
                    break
            
            # Try 2: Look for h2/h3 with meaningful content
            if not title or title.upper() in ["THE VO PROS", "THEVOPROS", "EVENTS"]:
                for tag in ["h2", "h3"]:
                    headers = ps.find_all(tag)
                    for h in headers:
                        potential = safe_text(h.get_text(" ", strip=True), 200)
                        if potential and len(potential) > 10 and potential.upper() not in ["THE VO PROS", "THEVOPROS", "VO PROS", "EVENTS", "SHOP", "UPCOMING"]:
                            title = potential
                            log(f"  [VOPros] Found {tag}: '{title[:40]}...'")
                            break
                    if title:
                        break
            
            # Try 3: Page title tag - aggressively strip site name
            if not title or title.upper() in ["THE VO PROS", "THEVOPROS", "EVENTS"]:
                if ps.title:
                    page_title = ps.title.get_text(" ", strip=True)
                    # Remove ALL variations of site name
                    cleaned = re.sub(r'^(The\s+)?VO\s*Pros\s*[|:\-–—\s]*', '', page_title, flags=re.IGNORECASE)
                    cleaned = re.sub(r'[|:\-–—\s]*(The\s+)?VO\s*Pros$', '', cleaned, flags=re.IGNORECASE)
                    cleaned = cleaned.strip()
                    if cleaned and len(cleaned) > 10:
                        title = safe_text(cleaned, 200)
                        log(f"  [VOPros] From page title: '{title[:40]}...'")
            
            # Try 4: Meta tags (og:title, description)
            if not title or title.upper() in ["THE VO PROS", "THEVOPROS"]:
                for meta in ps.find_all("meta"):
                    if meta.get("property") == "og:title" or meta.get("name") == "description":
                        content = meta.get("content", "")
                        cleaned = re.sub(r'^(The\s+)?VO\s*Pros\s*[|:\-–—\s]*', '', content, flags=re.IGNORECASE)
                        cleaned = re.sub(r'[|:\-–—\s]*(The\s+)?VO\s*Pros$', '', cleaned, flags=re.IGNORECASE)
                        cleaned = cleaned.strip()
                        if cleaned and len(cleaned) > 10:
                            title = safe_text(cleaned, 200)
                            log(f"  [VOPros] From meta: '{title[:40]}...'")
                            break
            
            # Try 5: LAST RESORT - Parse URL slug and make it readable
            if not title or title.upper() in ["THE VO PROS", "THEVOPROS", "VO PROS"] or len(title) < 5:
                # Extract from URL: /events/agent-night-john-smith → "Agent Night John Smith"
                url_parts = ev_url.split("/events/")
                if len(url_parts) > 1:
                    slug = url_parts[1].split("?")[0].rstrip("/")
                    # Convert slug to title: replace dashes/underscores with spaces, title case
                    title = slug.replace("-", " ").replace("_", " ").strip()
                    title = " ".join(word.capitalize() for word in title.split())
                    if title and len(title) > 5:
                        log(f"  [VOPros] From URL slug: '{title[:40]}...'")
                    else:
                        title = None
            
            # If STILL no good title after all attempts, skip
            if not title or title.upper() in ["THE VO PROS", "THEVOPROS", "VO PROS"] or len(title) < 5:
                log(f"  [VOPros] SKIPPING - exhausted all title extraction methods for {ev_url}")
                continue

            # Try multiple date patterns
            date_patterns = [
                r"Event Date:\s*([A-Za-z]+\s+\d{1,2},?\s*\d{4})",
                r"Date:\s*([A-Za-z]+\s+\d{1,2},?\s*\d{4})",
                r"([A-Za-z]+\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4})",
            ]
            
            base_date = None
            for pattern in date_patterns:
                date_m = re.search(pattern, txt, re.IGNORECASE)
                if date_m:
                    base_date = parse_date_any(date_m.group(1), default_tz)
                    if base_date:
                        break

            if not base_date:
                log(f"  [VOPros] No date found, skipping")
                continue

            # Time extraction - check title AND text
            sh, sm, eh, em = extract_time_from_text(title + " " + txt[:500])
            if sh is not None:
                start = base_date.replace(hour=sh, minute=sm, second=0, microsecond=0)
                end = base_date.replace(hour=eh, minute=em, second=0, microsecond=0)
                tzinfo = datetz.gettz(default_tz)
                start = start.replace(tzinfo=tzinfo)
                end = end.replace(tzinfo=tzinfo)
            else:
                start, end = apply_time_to_date(base_date, txt, default_tz)

            start_s = isoformat_with_tz(start, default_tz)
            end_s = isoformat_with_tz(end, default_tz)

            # Location
            loc_patterns = [
                r"Event Location:\s*([^\n]+)",
                r"Location:\s*([^\n]+)",
                r"Venue:\s*([^\n]+)",
            ]
            venue = None
            for pattern in loc_patterns:
                loc_m = re.search(pattern, txt, re.IGNORECASE)
                if loc_m:
                    venue = safe_text(loc_m.group(1), 160)
                    break

            reg = normalize_url(ev_url)
            wid = compute_event_id(source["id"], title, start_s, reg)

            events.append({
                "id": wid,
                "title": title,
                "host": source.get("name"),
                "city": None,
                "state": None,
                "venue": venue,
                "startAt": start_s,
                "endAt": end_s,
                "registrationURL": reg,
                "imageURL": None,
                "detail": None,
                "links": [{"title": "Event Page", "url": reg}] if reg else None
            })
            
            log(f"  [VOPros] ✓ Added: '{title[:40]}...' on {base_date.strftime('%m/%d')}")
            
        except Exception as e:
            log(f"  [VOPros] Error on {ev_url}: {e}")
            continue

    # Deduplicate by URL (same event may be linked multiple times from shop page)
    seen_urls = {}
    deduped_events = []
    for event in events:
        url = event.get('registrationURL')
        if not url:
            deduped_events.append(event)
            continue
            
        if url in seen_urls:
            # Keep the event with the better title (not "THE VO PROS")
            existing_title = seen_urls[url].get('title', '').upper()
            current_title = event.get('title', '').upper()
            
            if current_title not in ["THE VO PROS", "THEVOPROS", "VO PROS"] and existing_title in ["THE VO PROS", "THEVOPROS", "VO PROS"]:
                # Replace generic title with real one
                idx = deduped_events.index(seen_urls[url])
                deduped_events[idx] = event
                seen_urls[url] = event
                log(f"  [VOPros] Replaced generic title with: '{event.get('title', '')[:40]}'")
        else:
            seen_urls[url] = event
            deduped_events.append(event)
    
    log(f"  [VOPros] Deduplicated {len(events)} → {len(deduped_events)} events")
    return deduped_events


def extract_halp_events_search(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """Scrape HALP Academy workshops/classes from listing pages + detail pages."""
    base_url = source["url"]
    max_pages = int(source.get("max_event_pages") or 2)
    pages = iter_paginated_pages(base_url, max_pages, headers=source.get("headers"))

    # Collect candidate workshop links.
    candidate_urls: List[str] = []
    for page_url, page_html in pages:
        soup = BeautifulSoup(page_html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if not href:
                continue
            full = href if href.startswith("http") else urljoin(page_url, href)
            full = normalize_url(full)
            if not full:
                continue
            # Heuristic: keep links that look like workshop detail pages.
            if any(k in full.lower() for k in ("/workshop", "/workshops", "/class", "/classes", "/event")):
                if full not in candidate_urls:
                    candidate_urls.append(full)

    events: List[Dict[str, Any]] = []
    seen_ids: set = set()

    # Parse each detail page.
    for url in candidate_urls[: int(source.get("max_links") or 60)]:
        try:
            prod_html = fetch_html(url, headers=source.get("headers"))
        except Exception:
            continue

        ps = BeautifulSoup(prod_html, "html.parser")
        text = ps.get_text(" ", strip=True)

        h1 = ps.find(["h1", "h2"])
        title = safe_text(h1.get_text(" ", strip=True), 200) if h1 else ""
        if not title:
            title = safe_text(text, 200)

        # Find a date + time range anywhere in the text.
        date_m = re.search(r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}(?:,\s*\d{4})?\b", text, re.I)
        time_m = re.search(r"(\d{1,2}:\d{2}\s*(?:AM|PM))\s*(?:-|–|to)\s*(\d{1,2}:\d{2}\s*(?:AM|PM))", text, re.I)

        if not date_m:
            continue

        date_str = date_m.group(0)
        start_dt = None
        end_dt = None
        if time_m:
            start_dt = parse_date_any(f"{date_str} {time_m.group(1)}", default_tz)
            end_dt = parse_date_any(f"{date_str} {time_m.group(2)}", default_tz)
        else:
            # If no time range, still include as an all-day-ish placeholder.
            start_dt = parse_date_any(date_str, default_tz)

        if not start_dt:
            continue
        if not end_dt:
            end_dt = start_dt + dt.timedelta(hours=DEFAULT_EVENT_DURATION_HOURS)

        host = None
        host_m = re.search(r"\bwith\s+([A-Z][A-Za-z\.'’\-]+(?:\s+[A-Z][A-Za-z\.'’\-]+){0,3})\b", text)
        if host_m:
            host = host_m.group(1).strip()

        evt = {
            "id": stable_event_id(source.get("id", "halp"), url, title, start_dt),
            "title": title,
            "host": host or source.get("name") or source.get("id"),
            "startAt": isoformat_with_tz(start_dt, default_tz),
            "endAt": isoformat_with_tz(end_dt, default_tz),
            "registrationURL": url,
            "detail": safe_text(text, 600),
        }
        if evt["id"] not in seen_ids:
            seen_ids.add(evt["id"])
            events.append(evt)

    return events



def extract_van_shopify_products(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """Voice Actors Network (Shopify collection) events."""
    base_url = source["url"]
    max_pages = int(source.get("max_event_pages") or 3)
    pages = iter_paginated_pages(base_url, max_pages, headers=source.get("headers"))

    product_urls: List[str] = []
    for page_url, page_html in pages:
        soup = BeautifulSoup(page_html, "html.parser")
        for a in soup.select("a[href*='/products/']"):
            href = a.get("href", "").strip()
            if not href:
                continue
            full = href if href.startswith("http") else urljoin(page_url, href)
            full = normalize_url(full)
            if full and full not in product_urls:
                product_urls.append(full)

    events: List[Dict[str, Any]] = []
    seen_urls: set = set()

    for purl in product_urls[: int(source.get("max_links") or 80)]:
        if not purl or purl in seen_urls:
            continue
        seen_urls.add(purl)

        try:
            ph = fetch_html(purl, headers=source.get("headers"))
        except Exception:
            continue

        ps = BeautifulSoup(ph, "html.parser")
        text = ps.get_text(" ", strip=True)
        lower = text.lower()

        # Skip non-events
        if any(x in lower for x in ["gift card", "donation", "membership", "merch"]):
            continue

        h1 = ps.find(["h1", "h2"])
        title = safe_text(h1.get_text(" ", strip=True), 200) if h1 else safe_text(text, 200)
        if not title:
            continue

        # Find a date
        base_date = None
        date_patterns = [
            r"(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s+([A-Za-z]+\s+\d{1,2}(?:st|nd|rd|th)?,?\s*\d{4})",
            r"\b([A-Za-z]+\s+\d{1,2}(?:st|nd|rd|th)?)\b",
        ]
        for pat in date_patterns:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                date_str = m.group(1) if m.lastindex else m.group(0)
                base_date = parse_date_any(date_str, default_tz)
                if base_date:
                    break

        if not base_date:
            continue

        start_dt, end_dt = apply_time_to_date(base_date, text, default_tz)

        start_s = isoformat_with_tz(start_dt, default_tz)
        end_s = isoformat_with_tz(end_dt, default_tz)

        host = None
        hm = re.search(r"\bwith\s+([A-Z][A-Za-z\.'’\- ]{2,60})\b", text)
        if hm:
            host = safe_text(hm.group(1), 120)

        venue = "See listing"
        if "zoom" in lower:
            venue = "Zoom"
        if "in person" in lower or "in-person" in lower:
            venue = "In Person"

        reg = normalize_url(purl)
        wid = compute_event_id(source["id"], title, start_s, reg)

        events.append({
            "id": wid,
            "title": title,
            "host": host,
            "city": None,
            "state": None,
            "venue": venue,
            "startAt": start_s,
            "endAt": end_s,
            "registrationURL": reg,
            "imageURL": None,
            "detail": None,
            "links": [{"title": "VAN Listing", "url": reg}]
        })

    return events


def extract_wix_service_list(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """
    Wix service pages (Real Voice LA, Adventures in Voice Acting, etc.)
    IMPROVED: Better time extraction patterns for Wix sites
    """
    html = fetch_html(source["url"])
    soup = BeautifulSoup(html, "html.parser")

    service_urls: List[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "service-page" in href or "book-online" in href:
            full = href if href.startswith("http") else urljoin(source["url"], href)
            full = normalize_url(full)
            if full and full not in service_urls:
                service_urls.append(full)

    events: List[Dict[str, Any]] = []
    
    # Multiple date patterns to try
    date_patterns = [
        (r"\b(Start(?:s|ed))\s+([A-Za-z]{3,9}\s+\d{1,2})", 2),  # "Starts Feb 15"
        (r"\b([A-Za-z]{3,9}\s+\d{1,2}(?:st|nd|rd|th)?),?\s+(\d{4})", 0),  # "February 15th, 2026"
        (r"\b([A-Za-z]{3,9}\s+\d{1,2})", 1),  # Just "Feb 15"
    ]
    
    # Time patterns specific to Wix sites
    time_patterns = [
        r"(\d{1,2}(?::\d{2})?\s*(?:am|pm)\s*[-–to]+\s*\d{1,2}(?::\d{2})?\s*(?:am|pm))",  # "2pm - 5pm"
        r"(\d{1,2}(?::\d{2})?\s*(?:am|pm)\s*(?:PT|PST|PDT|ET|EST)?)",  # "2pm PT"
        r"Duration:\s*(\d+)\s*hr",  # "Duration: 3 hr"
    ]

    for surl in service_urls[:120]:
        try:
            sh = fetch_html(surl)
            ss = BeautifulSoup(sh, "html.parser")
            txt = ss.get_text("\n")

            h1 = ss.find(["h1","h2"])
            title = safe_text(h1.get_text(" ", strip=True), 200) if h1 else None
            if not title:
                title = "Workshop"
            
            # Skip non-class pages
            lower_title = title.lower()
            if any(skip in lower_title for skip in ["contact", "about", "policy", "terms", "faq"]):
                continue

            # Try to find a date
            base_date = None
            for pattern, group_idx in date_patterns:
                m = re.search(pattern, txt, re.IGNORECASE)
                if m:
                    if group_idx == 0:
                        # Full date with year
                        date_str = m.group(1) + " " + m.group(2)
                    else:
                        date_str = m.group(group_idx)
                    base_date = parse_date_any(date_str, default_tz)
                    if base_date is None:
                        base_date = infer_year_for_month_day(date_str, default_tz)
                    if base_date:
                        break
            
            if not base_date:
                continue

            # Try to find time - look at the whole page but prioritize specific patterns
            time_found = None
            for time_pat in time_patterns:
                m = re.search(time_pat, txt, re.IGNORECASE)
                if m:
                    time_found = m.group(1) if m.lastindex >= 1 else m.group(0)
                    break
            
            log(f"  [Wix] {title[:40]}... time_found='{time_found}'")
            
            if time_found:
                start, end = apply_time_to_date(base_date, time_found, default_tz)
            else:
                # Fall back to context around date
                txt_lower = txt.lower()
                date_pos = txt_lower.find(str(base_date.day))
                context_start = max(0, date_pos - 200)
                context_end = min(len(txt), date_pos + 300)
                time_context = txt[context_start:context_end]
                start, end = apply_time_to_date(base_date, time_context, default_tz)

            start_s = isoformat_with_tz(start, default_tz)
            end_s = isoformat_with_tz(end, default_tz)

            reg = normalize_url(surl)
            wid = compute_event_id(source["id"], title, start_s, reg)

            events.append({
                "id": wid,
                "title": title,
                "host": source.get("name"),
                "city": None,
                "state": None,
                "venue": "See listing",
                "startAt": start_s,
                "endAt": end_s,
                "registrationURL": reg,
                "imageURL": None,
                "detail": None,
                "links": [{"title": "Listing", "url": reg}]
            })
        except Exception as e:
            log(f"  [Wix] Error on {surl}: {e}")
            continue

    return events

def extract_vodojo_upcoming(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """
    The VO Dojo

    The /upcoming-events-nav page is messy and has promo copy mixed in. The /new-events page tends to be
    more structured and includes the actual time. We will prefer /new-events automatically (without requiring
    a config change), and we will aggressively filter out promo/non-class items.

    Rule: keep ONLY listings that look like a real class, i.e. contain "with <Name>" in the title.
    """
    base_url = source["url"].strip()
    preferred_url = base_url
    if "thevodojo.com" in base_url and "new-events" not in base_url:
        preferred_url = "https://www.thevodojo.com/new-events"

    def _parse_from_url(url: str) -> List[Dict[str, Any]]:
        html = fetch_html(url)
        if not html:
            return []
        soup = BeautifulSoup(html, "html.parser")

        # Find containers that look like individual event cards/rows.
        # We'll try a few strategies and merge unique results.
        candidates: List[str] = []

        # Strategy A: any element with a date like "Feb 4, 2026"
        date_re = re.compile(r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\s+\d{1,2},\s*\d{4}\b", re.IGNORECASE)
        for el in soup.find_all(string=date_re):
            parent = el.parent
            # climb a bit to capture the full card text
            for _ in range(4):
                if parent and parent.parent:
                    parent = parent.parent
            if parent:
                txt = " ".join(parent.stripped_strings)
                if txt and len(txt) > 30:
                    candidates.append(txt)

        # Strategy B: fall back to big body text chunks
        if not candidates:
            body_text = " ".join(soup.get_text("\n", strip=True).splitlines())
            if body_text:
                candidates.append(body_text)

        events: List[Dict[str, Any]] = []
        seen: set = set()

        # Parse each candidate blob.
        # We look for a date, a title (must include "with <Name>"), and time.
        for blob in candidates:
            # Split roughly on occurrences of month names to create chunks.
            chunks = re.split(r"(?=(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\s+\d{1,2},\s*\d{4})", blob)
            for chunk in chunks:
                chunk = chunk.strip()
                if len(chunk) < 40:
                    continue

                dm = date_re.search(chunk)
                if not dm:
                    continue
                date_str = dm.group(0)

                # Title heuristics: prefer "with <Name>" line, else take first reasonable title-like segment
                # that still contains "with <Name>".
                title = None
                title_candidates = re.split(r"\s{2,}|\s*\|\s*|\s*•\s*", chunk)
                for tc in title_candidates:
                    tc = tc.strip()
                    if not tc:
                        continue
                    if re.search(r"\b(?:with|featuring|feat\.?)\s+[A-Z][A-Za-z'\-]+", tc):
                        title = tc
                        break

                if not title:
                    continue  # promo or non-class

                # Extra promo filters (paranoid)
                if re.search(r"(missed your chance|refer a friend|enroll|discount|coupon|freebie|giveaway|sale)", title, re.IGNORECASE):
                    continue

                # Parse date
                d = parse_month_day_year(date_str, default_tz)
                if not d:
                    continue

                # Parse time from the same chunk (this is the critical part)
                sh, sm, eh, em = extract_time_from_text(chunk)
                if sh is None:
                    # no time => skip (we don't want to default to 6pm and be wrong)
                    continue

                start_dt = d.replace(hour=sh, minute=sm, second=0, microsecond=0)
                end_dt = d.replace(hour=eh or min(sh + DEFAULT_EVENT_DURATION_HOURS, 23),
                                   minute=em if em is not None else sm,
                                   second=0, microsecond=0)

                # Unique key (title + start)
                key = (title.lower().strip(), start_dt.isoformat())
                if key in seen:
                    continue
                seen.add(key)

                events.append({
                    "title": title,
                    "startAt": start_dt.isoformat(),
                    "endAt": end_dt.isoformat(),
                    "registrationURL": base_url,
                    "venue": "See listing",
                })

        return events

    # Prefer new-events
    events = _parse_from_url(preferred_url)
    if events:
        log(f"  [VODojo] Parsed {len(events)} events from {preferred_url}")
        return events

    # Fall back to the legacy parser (keeps us resilient if /new-events changes).
    # (Original behavior, but we'll be stricter about keeping only items with "with <Name>".)
    html = fetch_html(base_url)
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    date_line_re = re.compile(r"\b(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)?\s*(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\s+\d{1,2}(?:st|nd|rd|th)?,?\s*\d{4}\b", re.IGNORECASE)

    events_by_datetime: Dict[str, Dict[str, Any]] = {}
    i = 0
    while i < len(lines):
        ln = lines[i]
        if date_line_re.search(ln):
            # Gather block until next date line
            block = [ln]
            j = i + 1
            while j < len(lines) and not date_line_re.search(lines[j]):
                block.append(lines[j])
                j += 1
            block_text = " ".join(block)

            # Title must include "with <Name>"
            title = None
            for candidate in block:
                if re.search(r"\b(?:with|featuring|feat\.?)\s+[A-Z][A-Za-z'\-]+", candidate):
                    title = candidate
                    break
            if not title:
                i = j
                continue

            # Parse date
            d = parse_fuzzy_date(ln, default_tz)
            if not d:
                i = j
                continue

            sh, sm, eh, em = extract_time_from_text(block_text)
            if sh is None:
                # skip if no time (don't default)
                i = j
                continue

            start_dt = d.replace(hour=sh, minute=sm, second=0, microsecond=0)
            end_dt = d.replace(hour=eh or min(sh + DEFAULT_EVENT_DURATION_HOURS, 23),
                               minute=em if em is not None else sm,
                               second=0, microsecond=0)

            key = start_dt.isoformat()
            if key not in events_by_datetime:
                events_by_datetime[key] = {
                    "title": title,
                    "startAt": start_dt.isoformat(),
                    "endAt": end_dt.isoformat(),
                    "registrationURL": base_url,
                    "venue": "See listing",
                }

            i = j
            continue

        i += 1

    events = list(events_by_datetime.values())
    log(f"  [VODojo] Fallback parsed {len(events)} events")
    return events



def extract_html_fallback(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """Generic HTML fallback - tries JSON-LD first, then HTML parsing"""
    return extract_jsonld_events(source, default_tz)




def apply_event_filters(events: List[Dict[str, Any]], filters: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Optional per-source filters (configured in workshop_sources.json):

      - exclude_title_regex: [ ... ]
      - require_title_regex: "..."
      - exclude_text_regex: [ ... ]  (applies to title + detail + venue)
      - require_any_substrings: [ ... ] (case-insensitive, applies to title)

    If a filter key is absent, it is ignored.
    
    IMPROVED: Now also filters out ongoing/multi-session courses automatically.
    """
    if not filters:
        events = [e for e in events if not is_ongoing_class(e.get("title", ""), e.get("detail", ""))]
        return events

    excl_title_res = [re.compile(p, re.IGNORECASE) for p in filters.get("exclude_title_regex", []) if p]
    req_title_re = filters.get("require_title_regex")
    req_title_re_c = re.compile(req_title_re, re.IGNORECASE) if req_title_re else None

    excl_text_res = [re.compile(p, re.IGNORECASE) for p in filters.get("exclude_text_regex", []) if p]
    req_any = [s.lower() for s in filters.get("require_any_substrings", []) if isinstance(s, str) and s.strip()]

    # Legacy/alternate config keys
    excl_title_contains = [s.lower() for s in filters.get("exclude_title_contains", []) if isinstance(s, str) and s.strip()]
    req_title_contains_any = [s.lower() for s in filters.get("require_title_contains_any", []) if isinstance(s, str) and s.strip()]

    out: List[Dict[str, Any]] = []
    for e in events:
        title = (e.get("title") or "").strip()
        detail = (e.get("detail") or "").strip()
        venue = (e.get("venue") or "").strip()
        blob = f"{title}\n{detail}\n{venue}"

        if not title:
            continue

        # IMPROVED: Filter out ongoing courses
        if is_ongoing_class(title, detail):
            continue

        t_low = title.lower()

        if excl_title_contains and any(s in t_low for s in excl_title_contains):
            continue
        if any(rx.search(title) for rx in excl_title_res):
            continue

        if req_title_re_c and not req_title_re_c.search(title):
            continue

        if any(rx.search(blob) for rx in excl_text_res):
            continue

        if req_any and not any(s in t_low for s in req_any):
            continue

        if req_title_contains_any and not any(s in t_low for s in req_title_contains_any):
            continue

        out.append(e)

    return out

def extract_voicetraxwest_guest_instructors(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """
    VoiceTrax West - Guest Instructor Classes page.

    The page contains fixed class dates/times like:
      "TUESDAY, Feb 10 - 6:30pm PST"
    plus a booking link (as.me) per class.

    We parse each card-like block that contains both a date and a time.
    """
    html = fetch_html(source["url"])
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")

    # Regex for "TUESDAY, Feb 10 - 6:30pm PST"
    sched_re = re.compile(
        r"\b(?:Mon|Tues|Tue|Wed|Thu|Thur|Fri|Sat|Sun|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\b[,\s]*"
        r"(?P<mon>Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\s+"
        r"(?P<day>\d{1,2})\s*[-–—]\s*"
        r"(?P<h>\d{1,2})(?::(?P<m>\d{2}))?\s*(?P<ampm>am|pm)\s*(?:PST|PDT|PT)\b",
        re.IGNORECASE
    )

    events: List[Dict[str, Any]] = []
    seen: set = set()

    # Find booking links and parse their surrounding text
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if "as.me" not in href:
            continue
        container = a
        for _ in range(5):
            if container.parent:
                container = container.parent
        block_text = " ".join(container.stripped_strings)

        sm = sched_re.search(block_text)
        if not sm:
            continue

        mon = sm.group("mon")
        day = int(sm.group("day"))
        d = infer_year_for_month_day(f"{mon} {day}", default_tz)
        if not d:
            continue

        hour = int(sm.group("h"))
        minute = int(sm.group("m") or 0)
        ampm = sm.group("ampm").lower()
        if ampm == "pm" and hour != 12:
            hour += 12
        if ampm == "am" and hour == 12:
            hour = 0

        start_dt = d.replace(hour=hour, minute=minute, second=0, microsecond=0)
        end_dt = start_dt + dt.timedelta(hours=DEFAULT_EVENT_DURATION_HOURS)

        # Title heuristics: nearest strong/heading text in the block
        title = None
        for tag in ["h2", "h3", "h4", "strong"]:
            t = container.find(tag)
            if t and t.get_text(strip=True):
                title = t.get_text(" ", strip=True)
                break
        if not title:
            # fallback: link text
            title = a.get_text(" ", strip=True) or "Guest Instructor Class"

        key = (title.lower().strip(), start_dt.isoformat())
        if key in seen:
            continue
        seen.add(key)

        events.append({
            "title": title,
            "startAt": start_dt.isoformat(),
            "endAt": end_dt.isoformat(),
            "registrationURL": href,
            "venue": "See listing",
        })

    return events


def extract_aiva_upcoming_schedule(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """
    Adventures in Voice Acting (AIVA)

    The homepage often lists courses with "Started/Starts <Mon> <day>" but not a precise time.
    We include these as "start-date" calendar items with a conservative default time,
    and we ALWAYS point the user to the registration page for details.
    """
    html = fetch_html(source["url"])
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)

    # Find course cards by anchors that look like course links.
    # Many AIVA links include /courses/ or /products/.
    course_links = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if "/courses/" in href or "/products/" in href:
            course_links.append((a.get_text(" ", strip=True), urljoin(source["url"], href), a))

    # Date token like "Started Jan 14" or "Starts Feb 3"
    date_re = re.compile(r"\b(Start(?:ed|s))\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\s+(\d{1,2})\b", re.IGNORECASE)

    events: List[Dict[str, Any]] = []
    seen: set = set()

    for title_text, href, a in course_links:
        # Search nearby text (parent container)
        container = a
        for _ in range(5):
            if container.parent:
                container = container.parent
        block_text = " ".join(container.stripped_strings)

        dm = date_re.search(block_text)
        if not dm:
            continue

        mon = dm.group(2)
        day = int(dm.group(3))
        d = infer_year_for_month_day(f"{mon} {day}", default_tz)
        if not d:
            continue

        # Default time (AIVA doesn't reliably expose it in static HTML)
        start_dt = d.replace(hour=10, minute=0, second=0, microsecond=0)
        end_dt = start_dt + dt.timedelta(hours=2)

        title = title_text.strip() or "AIVA Course"
        key = (title.lower(), start_dt.isoformat())
        if key in seen:
            continue
        seen.add(key)

        events.append({
            "title": title,
            "startAt": start_dt.isoformat(),
            "endAt": end_dt.isoformat(),
            "registrationURL": href,
            "venue": "See listing",
            "detail": "Time may not be listed on the homepage. Tap to view the registration page for full details.",
        })

    return events

EXTRACTORS = {
    "jsonld_events": extract_jsonld_events,
    "tidycal_index": extract_tidycal_index,
    "soundonstudio_classsignup": extract_soundonstudio_classsignup,

    # The VO Pros
    "thevopros_events_index": extract_thevopros_events_index,
    "thevopros_events_from_shop": extract_thevopros_events_index,  # alias used in config

    # HALP / VAN / Wix
    "halp_events_search": extract_halp_events_search,
    "van_shopify_products": extract_van_shopify_products,
    "wix_service_list": extract_wix_service_list,

    # The VO Dojo
    "vodojo_upcoming": extract_vodojo_upcoming,

    # VoiceTrax West
    "voicetraxwest_guest_instructors": extract_voicetraxwest_guest_instructors,

    # Adventures in Voice Acting (AIVA)
    "aiva_upcoming_schedule": extract_aiva_upcoming_schedule,
    "adventuresinvoiceacting_upcoming_schedule": extract_aiva_upcoming_schedule,  # alias

    "html_fallback": extract_html_fallback
}

# -----------------------------
# Main
# -----------------------------


def normalize_existing_providers(existing: List[Dict[str, Any]], sources: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Map legacy provider values (providerLabel / source name) to stable source IDs to avoid duplicates."""
    id_set = {str(src.get("id")) for src in sources if src.get("id")}
    label_to_id: Dict[str, str] = {}
    for src in sources:
        sid = str(src.get("id") or "").strip()
        name = str(src.get("name") or "").strip()
        if sid and name:
            label_to_id[name.lower()] = sid

    out: List[Dict[str, Any]] = []
    for w in existing:
        if not isinstance(w, dict):
            continue
        provider = str(w.get("provider") or "").strip()
        provider_label = str(w.get("providerLabel") or "").strip()

        if provider and provider not in id_set:
            mapped = label_to_id.get(provider.lower())
            if mapped:
                w["provider"] = mapped
                if not provider_label:
                    w["providerLabel"] = provider
        elif (not provider) and provider_label:
            mapped = label_to_id.get(provider_label.lower())
            if mapped:
                w["provider"] = mapped

        out.append(w)
    return out


def main():
    ap = argparse.ArgumentParser(description="Sync VO workshop data")
    ap.add_argument("--resources", required=True)
    ap.add_argument("--sources", required=True)
    ap.add_argument("--default-tz", default="America/Los_Angeles")
    ap.add_argument("--verbose", "-v", action="store_true")
    ap.add_argument("--quiet", "-q", action="store_true")
    args = ap.parse_args()
    
    global VERBOSE
    VERBOSE = args.verbose and not args.quiet

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
    existing = normalize_existing_providers(existing, sources)
    incoming_all: List[Dict[str, Any]] = []
    successfully_scraped_sources: set = set()  # Track which sources succeeded

    for s in sources:
        source_id = s.get("id")
        extractor_name = s.get("extractor", "jsonld_events")
        fn = EXTRACTORS.get(extractor_name)
        if not fn:
            print(f"[WARN] Unknown extractor '{extractor_name}' for {source_id}, skipping.")
            continue

        print(f"[INFO] Processing {s.get('name', source_id)}...")
        
        try:
            events = fn(s, args.default_tz)
            events = apply_event_filters(events, s.get("filters"))

            # Normalize all extractor outputs to the canonical schema.
            events = [ensure_workshop_schema(e, s, args.default_tz) for e in events if isinstance(e, dict)]

            events = events[:max_events_per_source]
            print(f"[OK] {source_id}: {len(events)} events")
            incoming_all.extend(events)
            # Mark this source as successfully scraped.
            # Important: if a source suddenly returns 0 events, we treat it as a scrape failure by default
            # (to avoid wiping previously-known workshops due to transient site changes / parsing breakage).
            allow_zero = bool(s.get("allow_zero_events")) or bool(s.get("filters", {}).get("allow_zero_events"))
            if events or allow_zero:
                successfully_scraped_sources.add(source_id)
            else:
                log(f"  [WARN] {s.get('name', source_id)}: 0 events; treating as failure to preserve prior entries")
        except Exception as ex:
            # Source FAILED - we'll keep old data for this source
            print(f"[ERROR] {source_id}: {ex}")
            print(f"  -> Will keep existing events for this source")

    # Rebuild using new data, keeping old only for failed sources
    print(f"\n[INFO] Successfully scraped {len(successfully_scraped_sources)}/{len(sources)} sources")
    rebuilt = rebuild_workshops(existing, incoming_all, successfully_scraped_sources)
    
    # Prune old/future events
    rebuilt = prune_events(rebuilt, prune_days_past, keep_days_future, args.default_tz)

    # Final deduplication - Two passes:
    # Pass 1: Dedupe by URL (same event, same URL = duplicate)
    seen_urls = {}
    url_deduped = []
    for w in rebuilt:
        url = w.get('registrationURL')
        if not url:
            url_deduped.append(w)
            continue
            
        if url in seen_urls:
            # Keep the one with better title
            existing_title = seen_urls[url].get('title', '').upper()
            current_title = w.get('title', '').upper()
            generic_titles = ["THE VO PROS", "THEVOPROS", "VO PROS", "WORKSHOP", "CLASS", "EVENT"]
            
            if current_title not in generic_titles and existing_title in generic_titles:
                # Replace generic with real
                idx = url_deduped.index(seen_urls[url])
                url_deduped[idx] = w
                seen_urls[url] = w
        else:
            seen_urls[url] = w
            url_deduped.append(w)
    
    # Pass 2: Dedupe by title+date (fallback for events without URLs)
    seen_keys = set()
    deduped = []
    for w in url_deduped:
        title_norm = re.sub(r'[^a-z0-9]', '', (w.get('title') or '').lower())[:30]
        date_part = (w.get('startAt') or '')[:10]
        key = f"{title_norm}|{date_part}"
        if key not in seen_keys:
            seen_keys.add(key)
            deduped.append(w)
    
    resources["workshops"] = deduped
    resources["lastUpdated"] = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")

    with open(args.resources, "w", encoding="utf-8") as f:
        json.dump(resources, f, ensure_ascii=False, indent=2)
        f.write("\n")

    print(f"\n[DONE] Total workshops: {len(deduped)}")
    if len(existing) > 0:
        diff = len(deduped) - len(existing)
        if diff > 0:
            print(f"  (+{diff} new events)")
        elif diff < 0:
            print(f"  ({diff} events removed - likely expired or no longer listed)")

if __name__ == "__main__":
    main()
