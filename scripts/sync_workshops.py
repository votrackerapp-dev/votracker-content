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

def fetch_html(url: str, headers: Optional[Dict[str, str]] = None, prefer_reader: bool = False) -> str:
    """Fetch page content.

    - Tries a few UA/header profiles for basic bot-busting.
    - If blocked (401/403/406/429) or `prefer_reader=True`, falls back to Jina Reader:
      https://r.jina.ai/https://example.com

    Reader can help with:
      * basic anti-bot that blocks vanilla requests
      * JS-heavy pages where the useful content is rendered client-side

    NOTE: Reader returns Markdown-ish text, not raw HTML.
    """

    def _direct_fetch() -> Optional[str]:
        profiles = [
            {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "Upgrade-Insecure-Requests": "1",
            },
            {
                "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            },
            {
                "User-Agent": "VOTrackerWorkshopBot/1.5",
                "Accept": "text/html,*/*;q=0.8",
            },
        ]
        if headers:
            profiles.insert(0, headers)

        last_err: Optional[Exception] = None
        for h in profiles:
            try:
                r = requests.get(url, timeout=30, headers=h)
                if r.status_code in (401, 403, 406, 429):
                    last_err = requests.HTTPError(f"{r.status_code} blocked")
                    continue
                r.raise_for_status()
                return r.text
            except Exception as e:
                last_err = e
                continue
        return None

    def _reader_fetch() -> Optional[str]:
        try:
            reader_url = f"https://r.jina.ai/{url}"
            # Reader likes simple headers.
            h = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
                "Accept": "text/plain,text/markdown,text/html;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            }
            r = requests.get(reader_url, timeout=45, headers=h)
            if r.status_code in (401, 403, 406, 429):
                return None
            r.raise_for_status()
            return r.text
        except Exception:
            return None

    if not prefer_reader:
        direct = _direct_fetch()
        if direct is not None:
            return direct

    reader = _reader_fetch()
    if reader is not None:
        return reader

    # Last attempt: direct fetch again (in case prefer_reader=True but reader failed)
    direct = _direct_fetch()
    if direct is not None:
        return direct

    raise RuntimeError("fetch failed")

def apply_source_filters(source: Dict[str, Any], events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Optional per-source filtering rules from workshop_sources.json.

    Example:
      "filters": {
        "exclude_title_regex": ["refer-a-friend", "get to know vo"],
        "exclude_title_contains": ["refer-a-friend"],
        "require_title_contains_any": ["workshop", "fight club"]
      }
    """
    f = source.get("filters") or {}
    if not f:
        return events

    ex_re = [re.compile(pat, re.IGNORECASE) for pat in (f.get("exclude_title_regex") or []) if pat]
    ex_contains = [s.lower() for s in (f.get("exclude_title_contains") or []) if isinstance(s, str) and s.strip()]
    req_any = [s.lower() for s in (f.get("require_title_contains_any") or []) if isinstance(s, str) and s.strip()]

    filtered: List[Dict[str, Any]] = []
    for e in events:
        title = (e.get("title") or "").strip()
        t = title.lower()

        if not title:
            continue

        if req_any and not any(k in t for k in req_any):
            continue

        if ex_contains and any(k in t for k in ex_contains):
            continue

        if ex_re and any(rx.search(title) for rx in ex_re):
            continue

        filtered.append(e)

    if VERBOSE and len(filtered) != len(events):
        log(f"  [Filter] {source.get('id')}: {len(events)} -> {len(filtered)} after filters")
    return filtered

def parse_date_any(value: Any, default_tz: str) -> Optional[dt.datetime]:
    if not value:
        return None
    try:
        d = dateparser.parse(str(value), fuzzy=True)
        if not d:
            return None
        if d.tzinfo is None:
            d = d.replace(tzinfo=datetz.gettz(default_tz))
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

# -----------------------------
# Extractors
# -----------------------------

def extract_jsonld_events(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """
    Extract events from JSON-LD structured data - most reliable source!
    Falls back to HTML parsing if no JSON-LD found.
    """
    html = fetch_html(source["url"], source.get("headers"))
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

def extract_html_events_fallback(source: Dict[str, Any], soup: BeautifulSoup, default_tz: str) -> List[Dict[str, Any]]:
    """
    Fallback HTML parser for sites without JSON-LD.
    Looks for common event patterns in HTML.
    """
    # Cap how many event pages we fetch (keeps within Reader rate limits)
    max_pages = int(source.get('max_event_pages', 25))
    event_urls = event_urls[:max_pages]

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
    html = fetch_html(source["url"], source.get("headers"))
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
    IMPROVED: More flexible date/time patterns
    """
    index_html = fetch_html(source["url"], source.get("headers"))
    soup = BeautifulSoup(index_html, "html.parser")

    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/events/" in href and not href.endswith("/events/") and not href.endswith("/events"):
            full = href if href.startswith("http") else urljoin(source["url"], href)
            full = normalize_url(full)
            if full and full not in links:
                links.append(full)

    log(f"  [VOPros] Found {len(links)} event links")
    
    events: List[Dict[str, Any]] = []
    for ev_url in links[:120]:
        try:
            html = fetch_html(ev_url, source.get("headers"))
            ps = BeautifulSoup(html, "html.parser")
            txt = ps.get_text("\n")

            h1 = ps.find(["h1","h2"])
            title = safe_text(h1.get_text(" ", strip=True), 200) if h1 else None
            if not title:
                title = safe_text(ps.title.get_text(" ", strip=True), 200) if ps.title else "Workshop"

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
                log(f"  [VOPros] No date found in {ev_url}")
                continue

            # Try multiple time patterns
            time_patterns = [
                r"Event Time:\s*([0-9:]+\s*(?:am|pm)?(?:\s*[-–to]+\s*[0-9:]+\s*(?:am|pm))?)",
                r"Time:\s*([0-9:]+\s*(?:am|pm)?(?:\s*[-–to]+\s*[0-9:]+\s*(?:am|pm))?)",
                r"(\d{1,2}(?::\d{2})?\s*(?:am|pm)\s*[-–to]+\s*\d{1,2}(?::\d{2})?\s*(?:am|pm))",
                r"(\d{1,2}(?::\d{2})?\s*(?:am|pm))",
            ]
            
            time_str = ""
            for pattern in time_patterns:
                time_m = re.search(pattern, txt, re.IGNORECASE)
                if time_m:
                    time_str = time_m.group(1)
                    break
            
            log(f"  [VOPros] {title[:40]}... time_str='{time_str}'")
            start, end = apply_time_to_date(base_date, time_str if time_str else txt, default_tz)

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
            wid = compute_event_id(source["id"], title or "Workshop", start_s, reg)

            events.append({
                "id": wid,
                "title": title or "Workshop",
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
        except Exception as e:
            log(f"  [VOPros] Error on {ev_url}: {e}")
            continue

    return events

def extract_halp_events_search(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """Scrape HALP Academy events."""
    html = fetch_html(source["url"], source.get("headers"))
    soup = BeautifulSoup(html, "html.parser")
    events: List[Dict[str, Any]] = []
    
    seen_urls = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/product/" not in href:
            continue
        full_url = href if href.startswith("http") else urljoin(source["url"], href)
        full_url = normalize_url(full_url)
        if not full_url or full_url in seen_urls:
            continue
        seen_urls.add(full_url)
            
        try:
            prod_html = fetch_html(full_url, source.get("headers"))
            prod_soup = BeautifulSoup(prod_html, "html.parser")
            prod_text = prod_soup.get_text("\n")
            
            h1 = prod_soup.find(["h1", "h2"])
            title = safe_text(h1.get_text(" ", strip=True), 200) if h1 else "HALP Workshop"
            
            date_patterns = [
                r"([A-Za-z]+\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4})",
                r"(\d{1,2}/\d{1,2}/\d{2,4})",
            ]
            
            base_date = None
            for pattern in date_patterns:
                m = re.search(pattern, prod_text)
                if m:
                    base_date = parse_date_any(m.group(1), default_tz)
                    if base_date:
                        break
            
            if not base_date:
                continue
            
            log(f"  [HALP] {title[:40]}...")
            start, end = apply_time_to_date(base_date, prod_text, default_tz)
            
            start_s = isoformat_with_tz(start, default_tz)
            end_s = isoformat_with_tz(end, default_tz)
            
            venue = "Zoom" if "zoom" in prod_text.lower() else "See listing"
            
            wid = compute_event_id(source["id"], title, start_s, full_url)
            
            events.append({
                "id": wid,
                "title": title,
                "host": source.get("name"),
                "city": None,
                "state": None,
                "venue": venue,
                "startAt": start_s,
                "endAt": end_s,
                "registrationURL": full_url,
                "imageURL": None,
                "detail": None,
                "links": [{"title": "Event Page", "url": full_url}]
            })
        except Exception as e:
            log(f"  [HALP] Error: {e}")
            continue
    
    return events

def extract_van_shopify_products(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """Voice Actors Network Shopify products."""
    html = fetch_html(source["url"], source.get("headers"))
    soup = BeautifulSoup(html, "html.parser")

    products = soup.select("a[href*='/products/']")
    seen_urls = set()
    events: List[Dict[str, Any]] = []

    for a in products:
        href = a.get("href", "")
        purl = urljoin(source["url"], href)
        purl = normalize_url(purl)
        if not purl or purl in seen_urls:
            continue
        seen_urls.add(purl)

        txt = a.get_text(" ", strip=True)
        if not txt:
            continue

        title = safe_text(txt, 200)
        if not title:
            continue

        lower = txt.lower()

        if any(x in lower for x in ["gift card", "donation", "membership", "merch"]):
            continue

        date_patterns = [
            r"(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s+([A-Za-z]+\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4})",
            r"([A-Za-z]+\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4})",
        ]
        
        base_date = None
        for pattern in date_patterns:
            m = re.search(pattern, txt, re.IGNORECASE)
            if m:
                date_str = m.group(1) if m.lastindex >= 1 else m.group(0)
                base_date = parse_date_any(date_str, default_tz)
                if base_date:
                    break

        if not base_date:
            continue

        log(f"  [VAN] {title[:60]}...")
        start, end = apply_time_to_date(base_date, txt, default_tz)

        start_s = isoformat_with_tz(start, default_tz)
        end_s = isoformat_with_tz(end, default_tz)

        host = None
        hm = re.search(r"\bwith\s+([A-Z][A-Za-z.\- ]{2,40})", txt)
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
    html = fetch_html(source["url"], source.get("headers"))
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
            sh = fetch_html(surl, source.get("headers"))
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
    """The VO Dojo Upcoming Events.

    Their page contains many non-class promo entries (blog-ish / referral / "missed your chance" etc).
    We try to:
      1) parse event blocks more robustly
      2) pick the *most likely* start time when multiple times are present
      3) merge obvious near-duplicates (same event, slightly different time/title extraction)

    You can still override/extend filtering via workshop_sources.json: source["filters"].
    """
    html = fetch_html(source["url"], source.get("headers"))
    soup = BeautifulSoup(html, "html.parser")

    txt = soup.get_text("\n")
    lines = [ln.strip() for ln in txt.splitlines() if ln.strip()]

    month_re = re.compile(r"^(January|February|March|April|May|June|July|August|September|October|November|December)\b", re.IGNORECASE)
    date_re = re.compile(r"^(\d{1,2})(?:st|nd|rd|th)?\b")
    time_any_re = re.compile(r"\b(\d{1,2})(?::(\d{2}))?\s*(am|pm)\b", re.IGNORECASE)

    def _pick_best_time_line(block_lines: List[str]) -> Optional[str]:
        candidates = []
        for ln in block_lines:
            m = time_any_re.search(ln)
            if not m:
                continue
            h = int(m.group(1)); mi = int(m.group(2) or 0); ap = m.group(3).lower()
            h24, _ = _to_24h(h, mi, ap)
            score = 0
            low = ln.lower()
            if any(k in low for k in ["start", "starts", "begin", "begins"]):
                score += 50
            if "doors" in low or "check-in" in low or "check in" in low:
                score -= 10
            candidates.append((score, h24, mi, ln))

        if not candidates:
            return None
        candidates.sort(key=lambda t: (-t[0], t[1], t[2], len(t[3])))
        return candidates[0][3]

    def _default_vodojo_filters() -> Dict[str, Any]:
        return {
            "exclude_title_regex": [
                r"\brefer[- ]?a[- ]?friend\b",
                r"\bmissed your chance\b",
                r"\bget to know\b",
                r"\bblog\b",
                r"\bnewsletter\b",
                r"\bpolicy\b",
                r"\bterms\b",
                r"\bprivacy\b",
                r"\bwhat is vo\b",
            ]
        }

    if not source.get("filters"):
        source = dict(source)
        source["filters"] = _default_vodojo_filters()

    events_raw: List[Tuple[str, Dict[str, Any]]] = []
    current_month = None
    current_year = dt.datetime.now(tz=datetz.gettz(default_tz)).year

    i = 0
    while i < len(lines):
        ln = lines[i]

        mm = month_re.match(ln)
        if mm:
            current_month = mm.group(1)
            i += 1
            continue

        dm = date_re.match(ln)
        if current_month and dm:
            block = [ln]
            j = i + 1
            while j < len(lines):
                nxt = lines[j]
                if month_re.match(nxt) or date_re.match(nxt):
                    break
                block.append(nxt)
                j += 1

            day = dm.group(1)
            base_date = parse_date_any(f"{current_month} {day} {current_year}", default_tz)
            if not base_date:
                i = j
                continue

            title = None
            for cand in block[1:10]:
                if is_junk_title(cand):
                    continue
                low = cand.lower()
                if low.startswith(("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")):
                    continue
                if time_any_re.fullmatch(cand.strip()):
                    continue
                if any(k in low for k in ["upcoming events", "calendar", "view all"]):
                    continue
                if len(cand.strip()) >= 6:
                    title = safe_text(cand, 160)
                    break
            if not title:
                title = "VO Dojo Event"

            time_line = _pick_best_time_line(block) or ""
            start, end = apply_time_to_date(base_date, time_line, default_tz)

            start_s = isoformat_with_tz(start, default_tz)
            end_s = isoformat_with_tz(end, default_tz)
            reg = normalize_url(source.get("url"))

            detail = safe_text("\n".join(block[1:]), 800)

            ev = {
                "id": compute_event_id(source["id"], title, start_s, reg),
                "title": title,
                "host": source.get("name"),
                "city": None,
                "state": None,
                "venue": "Online",
                "startAt": start_s,
                "endAt": end_s,
                "registrationURL": reg,
                "imageURL": None,
                "detail": detail,
                "links": [{"title": "Listing", "url": reg}] if reg else None,
            }
            events_raw.append((start_s, ev))

            i = j
            continue

        i += 1

    events = apply_source_filters(source, [e for _, e in events_raw])

    def _norm_title(t: str) -> str:
        t = (t or "").lower()
        t = re.sub(r"\b\d{1,2}(?::\d{2})?\s*(am|pm)\b", "", t)
        t = re.sub(r"\s+", " ", t).strip()
        return t

    def _date_only(iso: str) -> str:
        return (iso or "")[:10]

    merged: List[Dict[str, Any]] = []
    for ev in sorted(events, key=lambda e: e.get("startAt") or ""):
        if not merged:
            merged.append(ev)
            continue

        prev = merged[-1]
        if _date_only(prev.get("startAt")) != _date_only(ev.get("startAt")):
            merged.append(ev)
            continue

        a = _norm_title(prev.get("title", ""))
        b = _norm_title(ev.get("title", ""))
        sim = difflib.SequenceMatcher(None, a, b).ratio()

        try:
            t1 = parse_date_any(prev.get("startAt"), default_tz)
            t2 = parse_date_any(ev.get("startAt"), default_tz)
            mins = abs((t2 - t1).total_seconds()) / 60.0 if (t1 and t2) else 999
        except Exception:
            mins = 999

        if (sim >= 0.92 or a in b or b in a) and mins <= 75:
            # Merge
            keep = prev
            drop = ev
            if t2 and t1 and t2 < t1:
                keep, drop = ev, prev

            merged_title = keep.get("title")
            if len((drop.get("title") or "")) > len((merged_title or "")):
                merged_title = drop.get("title")
            keep["title"] = merged_title

            if len((drop.get("detail") or "")) > len((keep.get("detail") or "")):
                keep["detail"] = drop.get("detail")

            links = []
            seen_u = set()
            for lst in (keep.get("links") or []) + (drop.get("links") or []):
                u = lst.get("url")
                if u and u not in seen_u:
                    seen_u.add(u)
                    links.append(lst)
            keep["links"] = links or None

            merged[-1] = keep
        else:
            merged.append(ev)

    return merged

def _discover_tidycal_booking_page_url(html: str) -> Optional[str]:
    """Try to discover a TidyCal booking page URL from arbitrary HTML.

    Many sites embed TidyCal via:
      <script src="https://asset-tidycal.b-cdn.net//js/embed.js"></script>
      <div id="tidycal-embed" data-path="username/booking-type"></div>

    We try a few heuristics and return a fully-qualified URL like:
      https://tidycal.com/username/booking-type
    """
    if not html:
        return None

    # 1) Look for data-path="..." on the tidycal embed
    m = re.search(r"data-path\s*=\s*\"([^\"]+)\"", html, re.IGNORECASE)
    if m:
        path = m.group(1).strip().lstrip("/")
        if path:
            return f"https://tidycal.com/{path}"

    # 2) Direct tidycal.com links
    m2 = re.search(r"https?://(?:www\.)?tidycal\.com/([A-Za-z0-9_\-./]+)", html, re.IGNORECASE)
    if m2:
        return normalize_url(m2.group(0))

    # 3) Sometimes the embed is referenced as a path inside JS
    m3 = re.search(r"tidycal\.com/([A-Za-z0-9_\-./]+)", html, re.IGNORECASE)
    if m3:
        return normalize_url(f"https://tidycal.com/{m3.group(1).lstrip('/')}")

    return None


def extract_redscythe_tidycal(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """Red Scythe - uses an embedded TidyCal booking page.

    Problem:
      RedScythe's homepage is mostly JS + embedded TidyCal, so our normal HTML
      event parsing often finds *zero* dated events.

    Approach:
      1) Fetch redscythe.com (raw HTML) and discover a tidycal booking-page URL.
      2) Fetch that tidycal.com page (it is server-rendered enough to list booking types).
      3) Parse each booking type card and extract date/time from title/description.

    If we can't confidently extract a date, we skip that booking type (to avoid
    polluting the calendar with evergreen booking links).
    """
    home_html = fetch_html(source["url"], source.get("headers"), prefer_reader=False)
    # If you know the booking page URL already, you can set it in workshop_sources.json as "tidycal_url".
    tidy_url = normalize_url(source.get("tidycal_url")) if source.get("tidycal_url") else None
    if not tidy_url:
        tidy_url = _discover_tidycal_booking_page_url(home_html)
    if not tidy_url:
        log("  [RedScythe] Could not discover a TidyCal booking page URL from homepage HTML")
        return []

    log(f"  [RedScythe] Discovered TidyCal booking page: {tidy_url}")
    tidy_html = fetch_html(tidy_url, source.get("headers"), prefer_reader=False)
    soup = BeautifulSoup(tidy_html, "html.parser")

    # Booking types typically appear as repeated blocks with a title, description,
    # and a "View details" link.
    events: List[Dict[str, Any]] = []

    # Heuristic: each booking type has an h2/h3 followed by a paragraph and a link.
    headings = soup.find_all(["h2", "h3"])
    for h in headings:
        title = safe_text(h.get_text(" ", strip=True), 180)
        if not title:
            continue

        # Pull some nearby text (description) to help date/time parsing.
        container = h.parent
        container_text = safe_text(container.get_text("\n", strip=True), 2000) if container else None
        combined = "\n".join([t for t in [title, container_text] if t])

        # Find a registration/details link nearby.
        reg_url = None
        if container:
            a = container.find("a", href=True)
            if a:
                href = a.get("href")
                if href:
                    reg_url = normalize_url(href if href.startswith("http") else urljoin(tidy_url, href))

        # Extract a date.
        # Common patterns: "Feb 10", "February 10", "Feb 10, 2026".
        dm = re.search(r"\b([A-Za-z]{3,9}\s+\d{1,2}(?:st|nd|rd|th)?(?:,?\s+\d{4})?)\b", combined)
        if not dm:
            continue

        date_str = dm.group(1)
        base_date = parse_date_any(date_str, default_tz) or infer_year_for_month_day(date_str, default_tz)
        if not base_date:
            continue

        # Extract time if present; otherwise default to 10:00am local.
        sh, sm, eh, em = extract_time_from_text(combined)
        if sh is None:
            sh, sm = 10, 0
            eh, em = 12, 0

        start = base_date.replace(hour=sh, minute=sm, second=0, microsecond=0)
        if start.tzinfo is None:
            start = start.replace(tzinfo=datetz.gettz(default_tz))
        if eh is None:
            end = start + dt.timedelta(hours=DEFAULT_EVENT_DURATION_HOURS)
        else:
            end = base_date.replace(hour=eh, minute=em or 0, second=0, microsecond=0)
            if end.tzinfo is None:
                end = end.replace(tzinfo=datetz.gettz(default_tz))
            if end <= start:
                end = start + dt.timedelta(hours=DEFAULT_EVENT_DURATION_HOURS)

        start_s = isoformat_with_tz(start, default_tz)
        end_s = isoformat_with_tz(end, default_tz)

        wid = compute_event_id(source["id"], title, start_s, reg_url)
        events.append({
            "id": wid,
            "title": title,
            "host": source.get("name"),
            "city": None,
            "state": None,
            "venue": "See listing",
            "startAt": start_s,
            "endAt": end_s,
            "registrationURL": reg_url or tidy_url,
            "imageURL": None,
            "detail": "",
            "links": [{"title": "Book", "url": reg_url or tidy_url}],
        })

    log(f"  [RedScythe] Parsed {len(events)} events from TidyCal")
    return events


def extract_aiva_upcoming_schedule(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """Adventures in Voice Acting - parses the "UPCOMING SCHEDULE" blocks.

    Their home page lists courses with "Starts/Started <Mon> <day>" but often loads
    exact times dynamically ("Loading availability...").

    We still create a calendar entry on the correct *date* (defaulting time to 10am PT)
    and link to the booking page so the user can see the exact time.
    """
    html = fetch_html(source["url"], source.get("headers"))
    soup = BeautifulSoup(html, "html.parser")

    events: List[Dict[str, Any]] = []

    # Find service/course links (usually service-page URLs)
    course_links: List[Tuple[str, str]] = []  # (title, url)
    for a in soup.find_all("a", href=True):
        title = safe_text(a.get_text(" ", strip=True), 180)
        href = a.get("href")
        if not title or not href:
            continue
        if "service-page" in href and len(title) > 3:
            full = normalize_url(href if href.startswith("http") else urljoin(source["url"], href))
            if full:
                course_links.append((title, full))

    # De-dupe by URL
    seen = set()
    course_links = [(t, u) for (t, u) in course_links if not (u in seen or seen.add(u))]

    page_text = soup.get_text("\n")

    # For each course title, look for a nearby "Starts <Mon> <day>" string.
    for title, url in course_links[:80]:
        # Search within a small window around the title occurrence in the page text.
        idx = page_text.lower().find(title.lower())
        window = page_text[max(0, idx - 300): idx + 600] if idx != -1 else page_text

        m = re.search(r"\bStart(?:s|ed)\s+([A-Za-z]{3,9}\s+\d{1,2})\b", window, re.IGNORECASE)
        if not m:
            continue

        date_str = m.group(1)
        base_date = parse_date_any(date_str, default_tz) or infer_year_for_month_day(date_str, default_tz)
        if not base_date:
            continue

        # Default time for display (since Wix loads true availability dynamically)
        start = base_date.replace(hour=10, minute=0, second=0, microsecond=0)
        if start.tzinfo is None:
            start = start.replace(tzinfo=datetz.gettz(default_tz))
        end = start + dt.timedelta(hours=DEFAULT_EVENT_DURATION_HOURS)

        start_s = isoformat_with_tz(start, default_tz)
        end_s = isoformat_with_tz(end, default_tz)

        reg = normalize_url(url)
        wid = compute_event_id(source["id"], title, start_s, reg)

        events.append({
            "id": wid,
            "title": title,
            "host": source.get("name"),
            "city": None,
            "state": None,
            "venue": "Online",
            "startAt": start_s,
            "endAt": end_s,
            "registrationURL": reg,
            "imageURL": None,
            "detail": "Time may vary; see booking page for exact availability.",
            "links": [{"title": "Book", "url": reg}],
        })

    log(f"  [AiVA] Parsed {len(events)} schedule entries")
    return events

def extract_html_fallback(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """Generic HTML fallback - tries JSON-LD first, then HTML parsing"""
    return extract_jsonld_events(source, default_tz)

def extract_voicetraxwest_guest_instructors(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """VoiceTraxWest Guest Instructors page.

    Fixes a common issue: using the page URL as `registrationURL` makes every event
    get the same computed id, causing events to collapse/dedupe into one.

    Strategy:
      - find each date/time line node
      - walk up to a reasonable container
      - grab the nearest as.me registration link inside that container (if present)
      - pick a reasonable title from the container text

    The page often omits the year, so we infer it.
    """
    html = fetch_html(source["url"], source.get("headers"), prefer_reader=bool(source.get("prefer_reader")))
    soup = BeautifulSoup(html, "html.parser")

    dt_re = re.compile(
        r"\b(?:MONDAY|TUESDAY|WEDNESDAY|THURSDAY|FRIDAY|SATURDAY|SUNDAY)\b\s*,?\s*"
        r"([A-Za-z]{3,9}\s+\d{1,2})\s*[-–]\s*"
        r"(\d{1,2}(?::\d{2})?\s*(?:am|pm))",
        re.IGNORECASE,
    )

    # Find text nodes that look like "TUESDAY, Feb 10 - 6:30pm"
    nodes = soup.find_all(string=lambda s: bool(s) and bool(dt_re.search(str(s))))
    events: List[Dict[str, Any]] = []
    seen: set = set()

    for n in nodes:
        m = dt_re.search(str(n))
        if not m:
            continue

        month_day = m.group(1).strip()
        time_part = m.group(2).strip()
        base_date = infer_year_for_month_day(month_day, default_tz)
        start, end = apply_time_to_date(base_date, time_part, default_tz)
        start_s = isoformat_with_tz(start, default_tz)
        end_s = isoformat_with_tz(end, default_tz)

        # Find a container up the tree that includes a booking link (or at least multiple lines)
        container = n.parent
        for _ in range(8):
            if container is None:
                break
            # Prefer a container that has an as.me link
            if hasattr(container, "find") and container.find("a", href=re.compile(r"as\.me/voicetraxwest", re.I)):
                break
            # Otherwise stop at a reasonably sized block
            if container.name in {"section", "article"}:
                break
            container = container.parent

        container = container if container is not None else n.parent

        # Extract registration URLs (prefer as.me)
        reg_url = None
        links = []
        if hasattr(container, "find_all"):
            for a in container.find_all("a", href=True):
                href = normalize_url(a.get("href"))
                if not href:
                    continue
                if "as.me/voicetraxwest" in href.lower():
                    reg_url = reg_url or href
                # Keep a couple useful links
                if len(links) < 3 and href.startswith("http"):
                    links.append({"title": safe_text(a.get_text(strip=True) or "Link", 40), "url": href})

        # Title: first decent line in container text
        raw = container.get_text("\n") if container is not None else str(n)
        lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
        title = None
        for ln in lines:
            low = ln.lower()
            if "voicetrax" in low or low.startswith(("tuesday", "wednesday", "thursday", "friday", "saturday", "sunday", "monday")):
                continue
            if is_junk_title(ln):
                continue
            # Avoid pure time/location lines
            if re.fullmatch(r"\d{1,2}(?::\d{2})?\s*(am|pm)", low):
                continue
            if len(ln) >= 6:
                title = safe_text(ln, 150)
                break
        if not title:
            title = "VoiceTraxWest Workshop"

        # Ensure unique id per event (include reg_url when available)
        wid = compute_event_id(source["id"], title, start_s, reg_url)
        if wid in seen:
            continue
        seen.add(wid)

        # If we didn't find any links in the container, at least add the listing page
        if not links:
            listing = normalize_url(source.get("url"))
            if listing:
                links = [{"title": "Listing", "url": listing}]

        events.append({
            "id": wid,
            "title": title,
            "host": source.get("name"),
            "city": None,
            "state": None,
            "venue": "VoiceTraxWest",
            "startAt": start_s,
            "endAt": end_s,
            "registrationURL": reg_url,
            "imageURL": None,
            "detail": None,
            "links": links or None,
        })

    return events


def extract_thevopros_events_from_shop(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """TheVOPros blocks /events/ index for bots (often 403). We:
      1) load a safer index (default: /shop/) with direct fetch first
      2) collect /events/ links (from HTML anchors or raw text)
      3) fetch each event page (Reader fallback allowed) and parse Event Date/Time

    IMPORTANT: Jina Reader returns Markdown-ish text (not HTML), so link extraction must
    work even when there are no <a> tags.
    """
    html = fetch_html(source['url'], source.get('headers'), prefer_reader=bool(source.get('prefer_reader')))
    soup = BeautifulSoup(html, 'html.parser')

    event_urls: List[str] = []
    seen: set = set()

    for a in soup.find_all('a', href=True):
        href = normalize_url(a.get('href'))
        if not href:
            continue
        if 'thevopros.com/events/' in href and href not in seen:
            seen.add(href)
            event_urls.append(href)

    if not event_urls:
        for m in re.finditer(r"https?://(?:www\.)?thevopros\.com/events/[^\s\)\]]+", html, re.IGNORECASE):
            href = normalize_url(m.group(0))
            if href and href not in seen:
                seen.add(href)
                event_urls.append(href)

    if not event_urls:
        # Fallback: try sitemap (often exposes event URLs even when pages are protected)
        for sm_url in ["https://www.thevopros.com/sitemap.xml", "https://www.thevopros.com/sitemap_index.xml"]:
            try:
                sm = fetch_html(sm_url, source.get('headers'), prefer_reader=True)
            except Exception:
                continue
            for m in re.finditer(r"https?://(?:www\.)?thevopros\.com/events/[^\s\)\]]+", sm, re.IGNORECASE):
                href = normalize_url(m.group(0))
                if href and href not in seen:
                    seen.add(href)
                    event_urls.append(href)
            if event_urls:
                break

    events: List[Dict[str, Any]] = []
    for u in event_urls[:50]:
        try:
            page = fetch_html(u, source.get('headers'), prefer_reader=True)
        except Exception:
            continue

        s2 = BeautifulSoup(page, 'html.parser')
        txt = s2.get_text('\n')
        if not txt.strip():
            txt = page

        title = None
        h1 = s2.find(['h1', 'title'])
        if h1:
            title = safe_text(h1.get_text(' ', strip=True), 160)
        if not title:
            for ln in (l.strip() for l in txt.splitlines()):
                if ln and not is_junk_title(ln):
                    title = safe_text(ln, 160)
                    break
        if not title:
            continue

        date_m = re.search(r'Event\s*Date\s*:\s*([A-Za-z]+\s+\d{1,2},\s*\d{4})', txt, re.IGNORECASE)
        time_m = re.search(r'Event\s*Time\s*:\s*(\d{1,2}:\d{2}\s*(?:am|pm))', txt, re.IGNORECASE)

        date_str = date_m.group(1).strip() if date_m else None
        time_str = time_m.group(1).strip() if time_m else None

        if not date_str:
            dm2 = re.search(r'\b([A-Za-z]+\s+\d{1,2},\s*\d{4})\b', txt)
            if dm2:
                date_str = dm2.group(1)
        if not time_str:
            tm2 = re.search(r'\b(\d{1,2}:\d{2}\s*(?:am|pm))\b', txt, re.IGNORECASE)
            if tm2:
                time_str = tm2.group(1)

        if not date_str:
            continue

        base = parse_date_any(date_str, default_tz)
        if not base:
            continue

        start, end = apply_time_to_date(base, time_str or '', default_tz)
        start_s = isoformat_with_tz(start, default_tz)
        end_s = isoformat_with_tz(end, default_tz)

        reg = normalize_url(u)
        wid = compute_event_id(source['id'], title, start_s, reg)

        events.append({
            'id': wid,
            'title': title,
            'host': source.get('name'),
            'city': None,
            'state': None,
            'venue': 'Online',
            'startAt': start_s,
            'endAt': end_s,
            'registrationURL': reg,
            'imageURL': None,
            'detail': None,
            'links': [{'title': 'Event Page', 'url': reg}] if reg else None,
        })

    return events

def extract_reader_text_events(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """Generic 'Reader rendered' extractor.

    Useful for JS-driven booking widgets (e.g. TidyCal embeds) where the direct HTML
    doesn't contain usable event data.

    Heuristic parsing:
      - scan lines for a date (month day, optional year)
      - try to capture a time on the same or next line
      - title is the nearest prior non-junk line

    This won't be perfect, but it usually recovers the key upcoming sessions.
    """
    html = fetch_html(source['url'], source.get('headers'), prefer_reader=True)
    soup = BeautifulSoup(html, 'html.parser')
    lines = [ln.strip() for ln in soup.get_text('\n').splitlines() if ln.strip()]

    date_re = re.compile(r'\\b([A-Za-z]+\s+\d{1,2})(?:,\s*(\d{4}))?\\b')
    time_re = re.compile(r'\\b(\d{1,2}:\d{2}\s*(?:am|pm)|\d{1,2}\s*(?:am|pm))\\b', re.IGNORECASE)

    events: List[Dict[str, Any]] = []
    used = set()

    for i, ln in enumerate(lines):
        dm = date_re.search(ln)
        if not dm:
            continue

        month_day = dm.group(1)
        year = dm.group(2)

        # Determine base date
        if year:
            base = parse_date_any(f"{month_day}, {year}")
        else:
            base = infer_year_for_month_day(month_day, default_tz)

        if not base:
            continue

        # Determine time
        tm = time_re.search(ln)
        time_part = tm.group(1) if tm else None
        if not time_part and i + 1 < len(lines):
            tm2 = time_re.search(lines[i + 1])
            if tm2:
                time_part = tm2.group(1)

        start_dt, end_dt = apply_time_to_date(base, time_part or '', default_tz)
        start_s = isoformat_with_tz(start_dt, default_tz)
        end_s = isoformat_with_tz(end_dt, default_tz)

        # Title: nearest prior useful line
        title = None
        for j in range(i - 1, max(-1, i - 6), -1):
            cand = lines[j]
            if is_junk_title(cand):
                continue
            # avoid duplicate date/time lines
            if date_re.search(cand) and (time_re.search(cand) or cand.upper().startswith(('MONDAY', 'TUESDAY', 'WEDNESDAY', 'THURSDAY', 'FRIDAY', 'SATURDAY', 'SUNDAY'))):
                continue
            if len(cand) < 5:
                continue
            title = safe_text(cand, 150)
            break

        if not title:
            title = safe_text(source.get('name', 'Workshop'), 150)

        key = (title.lower(), start_s[:16])
        if key in used:
            continue
        used.add(key)

        wid = compute_event_id(source['id'], title, start_s, None)
        events.append({
            'id': wid,
            'title': title,
            'host': source.get('name'),
            'city': None,
            'state': None,
            'venue': 'See listing',
            'startAt': start_s,
            'endAt': end_s,
            'registrationURL': normalize_url(source.get('url')),
            'imageURL': None,
            'detail': None,
            'links': [{'title': 'Listing', 'url': normalize_url(source.get('url'))}],
        })

    return events


EXTRACTORS = {
    "jsonld_events": extract_jsonld_events,
    "soundonstudio_classsignup": extract_soundonstudio_classsignup,
    "thevopros_events_index": extract_thevopros_events_index,
    "halp_events_search": extract_halp_events_search,
    "van_shopify_products": extract_van_shopify_products,
    "wix_service_list": extract_wix_service_list,
    "aiva_upcoming_schedule": extract_aiva_upcoming_schedule,
    "vodojo_upcoming": extract_vodojo_upcoming,
    "redscythe_tidycal": extract_redscythe_tidycal,
    "voicetraxwest_guest_instructors": extract_voicetraxwest_guest_instructors,
    "html_fallback": extract_html_fallback,
    "thevopros_events_from_shop": extract_thevopros_events_from_shop,
    "reader_text_events": extract_reader_text_events,
}

# -----------------------------
# Main
# -----------------------------

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
            events = apply_source_filters(s, events)
            for e in events:
                if isinstance(e, dict):
                    e.setdefault("provider", s.get("name"))
            events = events[:max_events_per_source]
            print(f"[OK] {source_id}: {len(events)} events")
            incoming_all.extend(events)
            
            # Mark this source as successfully scraped
            # (even if 0 events - that means the site has no current events)
            successfully_scraped_sources.add(source_id)
            
        except Exception as ex:
            # Source FAILED - we'll keep old data for this source
            print(f"[ERROR] {source_id}: {ex}")
            print(f"  -> Will keep existing events for this source")

    # Rebuild using new data, keeping old only for failed sources
    print(f"\n[INFO] Successfully scraped {len(successfully_scraped_sources)}/{len(sources)} sources")
    rebuilt = rebuild_workshops(existing, incoming_all, successfully_scraped_sources)
    
    # Prune old/future events
    rebuilt = prune_events(rebuilt, prune_days_past, keep_days_future, args.default_tz)

    # Final deduplication (same title + date)
    seen_keys = set()
    deduped = []
    for w in rebuilt:
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
