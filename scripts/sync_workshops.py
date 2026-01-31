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

def clean_whitespace(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def stable_event_id(source_id: str, url: str, title: str, start: dt.datetime) -> str:
    """Generate stable IDs like 'sourceid-<16hex>' so updates don't churn."""
    base = f"{source_id}|{canonicalize_url(url)}|{title.strip().lower()}|{start.isoformat()}"
    return f"{source_id}-{sha1(base)[:16]}"

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
    """Fetch HTML with sane defaults and optional per-source headers.

    Many of these sites will 403 if you use a bot-like UA. We default to a
    realistic desktop UA and allow sources to override.
    """
    default_headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/122.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    if headers:
        # Source headers win.
        default_headers.update({k: v for k, v in headers.items() if v})

    # Some hosts are picky about the Referer.
    try:
        p = urlparse(url)
        default_headers.setdefault("Referer", f"{p.scheme}://{p.netloc}/")
    except Exception:
        pass

    # Single request + one fallback retry if blocked.
    r = requests.get(url, timeout=30, headers=default_headers, allow_redirects=True)
    if r.status_code in (403, 429):
        log(f"[WARN] {r.status_code} for {url} — retrying with alternate UA")
        alt = dict(default_headers)
        alt["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "                             "AppleWebKit/537.36 (KHTML, like Gecko) "                             "Chrome/122.0.0.0 Safari/537.36"
        r = requests.get(url, timeout=30, headers=alt, allow_redirects=True)

    r.raise_for_status()
    return r.text

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

    Example: https://tidycal.com/redscythestudio/
    Page shows multiple items with titles like:
        "1.31 | JOE HERNANDEZ | video game characters"

    We treat the MM.DD as the event date in the next-occurring year.
    TidyCal's index often doesn't expose the exact start time without JS,
    so we default to 10:00am local time.
    """
    url = source["url"]
    html = fetch_html(url, source.get("headers"))
    soup = BeautifulSoup(html, "html.parser")

    # Pull all H2 headings; TidyCal uses them for listing titles.
    headings = [h.get_text(" ", strip=True) for h in soup.find_all(["h2", "h3"])]

    page_text = soup.get_text("\n", strip=True)
    # Match: MM.DD | INSTRUCTOR | TOPIC
    title_re = re.compile(r"\b(\d{1,2}\.\d{1,2})\s*\|\s*([^|]{2,80}?)\s*\|\s*([^\n|]{2,200})", re.IGNORECASE)

    # Build a simple list of 'detail' links if present (nice-to-have)
    detail_links: List[str] = []
    for a in soup.find_all("a", href=True):
        t = (a.get_text(" ", strip=True) or "").lower()
        href = a["href"]
        if "tidycal.com/" in href and "view details" in t:
            detail_links.append(href)
    seen = set()
    detail_links = [u for u in detail_links if not (u in seen or seen.add(u))]

    filters = source.get("filters", {}) or {}
    drop_contains = [x.lower() for x in filters.get("exclude_title_contains", [])]
    require_any = [x.lower() for x in filters.get("require_title_contains_any", [])]

    def keep_title(t: str) -> bool:
        lt = t.lower()
        if drop_contains and any(x in lt for x in drop_contains):
            return False
        if require_any and not any(x in lt for x in require_any):
            return False
        return True

    # Find matching titles from headings first (cleaner), else from full page text.
    matches: List[Tuple[str, str, str]] = []
    for h in headings:
        m = title_re.search(h)
        if m:
            matches.append((m.group(1), clean_whitespace(m.group(2)), clean_whitespace(m.group(3))))
    if not matches:
        for m in title_re.finditer(page_text):
            matches.append((m.group(1), clean_whitespace(m.group(2)), clean_whitespace(m.group(3))))

    # Attempt to map each match to a details link (best-effort, same ordering).
    while len(detail_links) < len(matches):
        detail_links.append(url)

    events: List[Dict[str, Any]] = []
    for (mm_dd, instructor, topic), href in zip(matches, detail_links):
        raw_title = f"{instructor} — {topic}"
        if not keep_title(raw_title):
            continue
        if len(instructor.split()) < 2:
            continue

        base_dt = infer_year_for_mm_dd(mm_dd, default_tz)
        if not base_dt:
            continue

        start = base_dt.replace(hour=10, minute=0, second=0, microsecond=0)
        # Duration and price are visible on the index page as plain text.
        # We'll search around the title in page_text as a rough heuristic.
        duration_hours = None
        price = None
        # Look for lines near the heading in the full page text.
        window = page_text
        dur_m = re.search(r"\b(\d+)\s*hours?\b", window, flags=re.IGNORECASE)
        if dur_m:
            try:
                duration_hours = int(dur_m.group(1))
            except Exception:
                duration_hours = None
        price_m = re.search(r"\$(\d+(?:\.\d{1,2})?)", window)
        if price_m:
            price = f"${price_m.group(1)}"

        end = start + dt.timedelta(hours=duration_hours or DEFAULT_EVENT_DURATION_HOURS)

        detail_lines = []
        if duration_hours:
            detail_lines.append(f"Duration: {duration_hours} hours")
        if price:
            detail_lines.append(f"Price: {price}")
        detail_lines.append(f"Listing: {href}")

        events.append({
            "id": stable_event_id(source.get("id", "tidycal"), href, raw_title, start),
            "title": raw_title,
            "host": source.get("name") or "TidyCal",
            "city": None,
            "state": None,
            "venue": "Online / See listing",
            "startAt": start.isoformat(),
            "endAt": end.isoformat(),
            "registrationURL": href,
            "imageURL": None,
            "detail": "\n".join(detail_lines) if detail_lines else None,
            "links": [{"title": "Listing", "url": href}],
            "provider": source.get("name"),
        })

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
    IMPROVED: More flexible date/time patterns
    """
    index_html = fetch_html(source["url"])
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
            html = fetch_html(ev_url)
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
    html = fetch_html(source["url"])
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
            prod_html = fetch_html(full_url)
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
    html = fetch_html(source["url"])
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
    VO Dojo - IMPROVED to filter junk and avoid duplicates
    """
    html = fetch_html(source["url"])
    soup = BeautifulSoup(html, "html.parser")
    full_text = soup.get_text("\n")
    
    # Junk titles to skip
    JUNK_PATTERNS = [
        r"^vo\s*dojo\s*event$",
        r"^join",
        r"mailing\s*list",
        r"newsletter",
        r"subscribe",
        r"sign\s*up\s*for",
        r"follow\s*us",
        r"contact",
        r"about\s*us",
        r"home$",
        r"^menu$",
        r"privacy",
        r"terms",
        r"copyright",
        r"^\d+$",  # Just numbers
        r"^[A-Za-z]{1,3}$",  # Very short strings
    ]
    
    def is_junk_title(title: str) -> bool:
        lower = title.lower().strip()
        if len(lower) < 5:
            return True
        for pattern in JUNK_PATTERNS:
            if re.search(pattern, lower, re.IGNORECASE):
                return True
        return False
    
    events: List[Dict[str, Any]] = []
    date_re = re.compile(r"([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})")
    
    # Track events by date+time to avoid duplicates
    events_by_datetime: Dict[str, Dict[str, Any]] = {}
    
    lines = full_text.split('\n')
    
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        date_match = date_re.search(line)
        
        if date_match:
            date_str = date_match.group(1)
            base = parse_date_any(date_str, default_tz)
            
            if base:
                event_lines = [line]
                j = i + 1
                lines_collected = 0
                
                while j < len(lines) and lines_collected < 10:
                    next_line = lines[j].strip()
                    if not next_line:
                        j += 1
                        continue
                    if date_re.search(next_line):
                        break
                    event_lines.append(next_line)
                    lines_collected += 1
                    j += 1
                
                event_text = '\n'.join(event_lines)
                
                # Extract time
                start, end = apply_time_to_date(base, event_text, default_tz)
                start_s = isoformat_with_tz(start, default_tz)
                end_s = isoformat_with_tz(end, default_tz)
                
                # Find best title from event lines
                title = None
                for el in event_lines:
                    el_clean = el.strip()
                    # Skip date-only lines
                    if date_re.fullmatch(el_clean):
                        continue
                    # Skip very short or junk
                    if is_junk_title(el_clean):
                        continue
                    # Skip lines that look like times only
                    if re.fullmatch(r"[\d:]+\s*(am|pm)?(\s*[-–]\s*[\d:]+\s*(am|pm)?)?", el_clean, re.IGNORECASE):
                        continue
                    # Found a good title
                    title = safe_text(el_clean, 150)
                    break
                
                # Skip if no good title found
                if not title:
                    log(f"  [VODojo] Skipping event at {date_str} - no good title found")
                    i = j
                    continue
                
                # Use date+time as key for deduplication
                datetime_key = start_s[:16]  # "2026-01-30T19:00"
                
                # If we already have an event at this datetime, prefer the better title
                if datetime_key in events_by_datetime:
                    existing = events_by_datetime[datetime_key]
                    existing_title = existing.get("title", "")
                    # Keep the more descriptive title (longer, not generic)
                    if len(title) > len(existing_title) or "event" in existing_title.lower():
                        log(f"  [VODojo] Replacing '{existing_title[:30]}' with '{title[:30]}' at {datetime_key}")
                        existing["title"] = title
                        existing["id"] = compute_event_id(source["id"], title, start_s, None)
                else:
                    # New event
                    log(f"  [VODojo] Found: {title[:50]}... @ {start_s}")
                    events_by_datetime[datetime_key] = {
                        "id": compute_event_id(source["id"], title, start_s, None),
                        "title": title,
                        "host": source.get("name"),
                        "city": None,
                        "state": None,
                        "venue": "See listing",
                        "startAt": start_s,
                        "endAt": end_s,
                        "registrationURL": None,
                        "imageURL": None,
                        "detail": None,
                        "links": [{"title": "Upcoming Events", "url": normalize_url(source["url"])}]
                    }
                
                i = j
                continue
        
        i += 1
    
    events = list(events_by_datetime.values())
    log(f"  [VODojo] Found {len(events)} unique events")
    return events

def extract_html_fallback(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """Generic HTML fallback - tries JSON-LD first, then HTML parsing"""
    return extract_jsonld_events(source, default_tz)


def extract_voicetraxwest_guest_instructors(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """Scrape https://www.voicetraxwest.com/guest-instructors

    The page is plain HTML with repeating blocks:
        Title line
        Description
        'DAY, Mon DD - HH:MMpm PST'
        Links: in person / online (Acuity 'as.me')
    """
    url = source["url"]
    html = fetch_html(url, source.get("headers"))
    soup = BeautifulSoup(html, "html.parser")

    text_lines = [ln.strip() for ln in soup.get_text("\n").splitlines()]
    text_lines = [ln for ln in text_lines if ln]

    # Example: "TUESDAY, Feb 10 - 6:30pm PST"
    date_line_re = re.compile(
        r"^(?:MONDAY|TUESDAY|WEDNESDAY|THURSDAY|FRIDAY|SATURDAY|SUNDAY),?\s+"
        r"([A-Za-z]{3,})\s+(\d{1,2})\s*-\s*(\d{1,2}:\d{2})\s*(am|pm)\s*(?:PST|PT)?$",
        re.IGNORECASE,
    )

    # Collect all relevant registration links (prefer Acuity scheduling).
    anchors = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        label = (a.get_text(" ", strip=True) or "").lower()
        anchors.append((label, href))
    def pick_reg_link(near_title: str) -> Optional[str]:
        # If the page is small, just prefer an 'online' as.me link overall.
        for label, href in anchors:
            if "as.me" in href and "online" in label:
                return href
        for label, href in anchors:
            if "as.me" in href and ("in person" in label or "in-person" in label):
                return href
        return url

    events: List[Dict[str, Any]] = []
    for i, line in enumerate(text_lines):
        m = date_line_re.match(line)
        if not m:
            continue
        mon, day, hm, ampm = m.group(1), m.group(2), m.group(3), m.group(4)
        # Title is the previous non-empty line that isn't an image placeholder
        title = None
        j = i - 1
        while j >= 0:
            cand = text_lines[j].strip()
            if cand and not cand.lower().startswith("image"):
                title = cand
                break
            j -= 1
        if not title:
            continue

        # Best-effort: description is the line(s) between title and date line.
        desc_parts = []
        k = j + 1
        while k < i:
            desc_parts.append(text_lines[k])
            k += 1
        desc = safe_text(" ".join(desc_parts), 1200)

        # Infer year: pick the next occurrence of that month/day in local tz.
        # Use dateutil on a string without year, then adjust.
        base = infer_year_for_month_day(mon, int(day), default_tz)
        if not base:
            continue
        try:
            t = dateparser.parse(f"{hm} {ampm}")
            hour = t.hour
            minute = t.minute
        except Exception:
            hour, minute = 18, 30

        start = base.replace(hour=hour, minute=minute, second=0, microsecond=0)
        end = start + dt.timedelta(hours=DEFAULT_EVENT_DURATION_HOURS)

        reg = pick_reg_link(title)
        events.append({
            "id": stable_event_id(source.get("id", "voicetraxwest"), reg or url, title, start),
            "title": title,
            "host": source.get("name") or "Voice Trax West",
            "city": "Studio City",
            "state": "CA",
            "venue": "Voice Trax West (or Online)",
            "startAt": start.isoformat(),
            "endAt": end.isoformat(),
            "registrationURL": reg,
            "imageURL": None,
            "detail": desc,
            "links": [{"title": "Listing", "url": url}] + ([{"title": "Register", "url": reg}] if reg else []),
            "provider": source.get("name"),
        })

    return events


def extract_aiva_upcoming_schedule(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """Scrape AIVA instructor pages for courses with explicit 'Starts <Mon> <DD>' lines.

    NOTE: The deeper booking widget is JS-driven and often renders as
    'Loading availability...' in raw HTML, so we only capture entries that
    expose a start date in the static text.
    """
    base_url = source["url"]
    html = fetch_html(base_url, source.get("headers"))
    soup = BeautifulSoup(html, "html.parser")

    # Find 'Book <Instructor>' links in nav.
    instructor_urls: List[str] = []
    for a in soup.find_all("a", href=True):
        txt = (a.get_text(" ", strip=True) or "").lower()
        href = a["href"]
        if "book" in txt and "http" not in href:
            full = urljoin(base_url, href)
            instructor_urls.append(full)
        elif "book" in txt and href.startswith("http") and "adventuresinvoiceacting.com" in href:
            instructor_urls.append(href)

    # De-dup
    seen = set()
    instructor_urls = [u for u in instructor_urls if not (u in seen or seen.add(u))]

    # If the nav didn't include them, fall back to the two known instructor pages.
    if not instructor_urls:
        instructor_urls = [
            urljoin(base_url, "/dorahfine"),
            urljoin(base_url, "/tony-oliver"),
        ]

    # 'Starts Nov 15' / 'Started Jan 14'
    starts_re = re.compile(r"\b(Start(?:s|ed))\s+([A-Za-z]{3,})\s+(\d{1,2})\b")

    events: List[Dict[str, Any]] = []
    for inst_url in instructor_urls[:8]:
        try:
            inst_html = fetch_html(inst_url, source.get("headers"))
        except Exception as ex:
            log(f"[WARN] AIVA instructor page failed: {inst_url} ({ex})")
            continue

        inst_soup = BeautifulSoup(inst_html, "html.parser")
        lines = [ln.strip() for ln in inst_soup.get_text("\n").splitlines()]
        lines = [ln for ln in lines if ln]

        # Titles show up as clickable links in the page text; we take the line above 'Starts ...'
        for idx, ln in enumerate(lines):
            m = starts_re.search(ln)
            if not m:
                continue
            mon = m.group(2)
            day = int(m.group(3))

            # Find title as the closest previous non-empty line that isn't generic.
            title = None
            j = idx - 1
            while j >= 0:
                cand = lines[j]
                if cand and not cand.lower().startswith("image") and "service information" not in cand.lower():
                    title = cand
                    break
                j -= 1
            if not title:
                continue

            # Skip obvious non-live entries.
            if re.search(r"(?i)video\s+download|download\b", title):
                continue

            base = infer_year_for_month_day(mon, day, default_tz)
            if not base:
                continue
            # AIVA often doesn't expose start time; default 12:00pm PT.
            start = base.replace(hour=12, minute=0, second=0, microsecond=0)
            end = start + dt.timedelta(hours=DEFAULT_EVENT_DURATION_HOURS)

            events.append({
                "id": stable_event_id(source.get("id", "aiva"), inst_url, title, start),
                "title": title,
                "host": source.get("name") or "Adventures in Voice Acting",
                "city": "Burbank" if "in-studio" in " ".join(lines).lower() else None,
                "state": "CA" if "in-studio" in " ".join(lines).lower() else None,
                "venue": "See listing",
                "startAt": start.isoformat(),
                "endAt": end.isoformat(),
                "registrationURL": inst_url,
                "imageURL": None,
                "detail": safe_text(" ".join(lines[max(0, j+1):min(len(lines), idx+4)]), 1200),
                "links": [{"title": "Listing", "url": inst_url}],
                "provider": source.get("name"),
            })

    # De-dupe (same title+date)
    seen2 = set()
    out = []
    for e in events:
        key = (e.get("title"), (e.get("startAt") or "")[:10])
        if key in seen2:
            continue
        seen2.add(key)
        out.append(e)

    return out


def extract_thevopros_events_from_shop(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """Scrape The VO Pros events from /shop/ (Squarespace).

    The site is often protected; this relies on realistic headers from
    workshop_sources.json and the improved fetch_html().

    Strategy:
    1) Load /shop/ and collect links that look like /events/...
    2) Visit each event page and parse:
        - Event Date: <Month DD, YYYY>
        - Event Time: <h:mm am/pm> OR a range "4:00 pm - 6:00 pm"
    """
    base_url = source["url"]
    headers = source.get("headers")
    html = fetch_html(base_url, headers)
    soup = BeautifulSoup(html, "html.parser")

    # Collect candidate event links from the shop page.
    links: List[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/events/" in href:
            links.append(urljoin(base_url, href))
        elif href.startswith("https://www.thevopros.com/events/"):
            links.append(href)

    # De-dup + cap
    seen = set()
    links = [u for u in links if not (u in seen or seen.add(u))]
    links = links[: int(source.get("max_event_pages", 25))]

    # Regexes based on what VO Pros exposes in their HTML.
    date_re = re.compile(r"\bEvent Date\s*:\s*([A-Za-z]+\s+\d{1,2},\s+\d{4})\b")
    time_re = re.compile(r"\bEvent Time\s*:\s*([0-9]{1,2}:[0-9]{2}\s*(?:am|pm))\b", re.IGNORECASE)
    range_re = re.compile(r"\b([0-9]{1,2}:[0-9]{2}\s*(?:am|pm))\s*-\s*([0-9]{1,2}:[0-9]{2}\s*(?:am|pm))\b", re.IGNORECASE)

    events: List[Dict[str, Any]] = []
    for event_url in links:
        try:
            ev_html = fetch_html(event_url, headers)
        except Exception as ex:
            log(f"[WARN] VO Pros event fetch failed: {event_url} ({ex})")
            continue

        ev_soup = BeautifulSoup(ev_html, "html.parser")
        page_text = ev_soup.get_text("\n", strip=True)

        title = None
        h1 = ev_soup.find(["h1"])
        if h1:
            title = clean_whitespace(h1.get_text(" ", strip=True))
        if not title:
            title = clean_whitespace(ev_soup.title.get_text(" ", strip=True) if ev_soup.title else "")
            title = re.sub(r"\s*\|\s*The VO PROS.*$", "", title).strip() or None
        if not title:
            continue

        dm = date_re.search(page_text)
        if not dm:
            # Fallback: look for 'January 21, 2026' anywhere
            dm = re.search(r"\b([A-Za-z]+\s+\d{1,2},\s+\d{4})\b", page_text)
        if not dm:
            continue

        date_str = dm.group(1)
        base_dt = parse_date_any(date_str, default_tz)
        if not base_dt:
            continue

        # Parse time (prefer explicit 'Event Time', else a range).
        start_time = None
        tm = time_re.search(page_text)
        if tm:
            start_time = tm.group(1)
        rm = range_re.search(page_text)
        end_time = rm.group(2) if rm else None
        if rm and not start_time:
            start_time = rm.group(1)

        if start_time:
            t = dateparser.parse(start_time)
            start = base_dt.replace(hour=t.hour, minute=t.minute, second=0, microsecond=0)
        else:
            start = base_dt.replace(hour=10, minute=0, second=0, microsecond=0)

        if end_time:
            t2 = dateparser.parse(end_time)
            end = base_dt.replace(hour=t2.hour, minute=t2.minute, second=0, microsecond=0)
            if end <= start:
                end = start + dt.timedelta(hours=DEFAULT_EVENT_DURATION_HOURS)
        else:
            end = start + dt.timedelta(hours=DEFAULT_EVENT_DURATION_HOURS)

        events.append({
            "id": stable_event_id(source.get("id", "thevopros"), event_url, title, start),
            "title": title,
            "host": source.get("name") or "The VO Pros",
            "city": "Los Angeles",
            "state": "CA",
            "venue": "See event page",
            "startAt": start.isoformat(),
            "endAt": end.isoformat(),
            "registrationURL": event_url,
            "imageURL": None,
            "detail": None,
            "links": [{"title": "Event Page", "url": event_url}],
            "provider": source.get("name"),
        })

    # If the shop page doesn't surface /events/ links, last-resort parse the shop text directly.
    if not events:
        page_text = soup.get_text("\n", strip=True)
        # "Title · February 12, 2026 4:00 pm - 6:00 pm"
        pat = re.compile(r"\n([^\n]{10,140}?)\s*·\s*([A-Za-z]+\s+\d{1,2},\s+\d{4})\s+([0-9]{1,2}:[0-9]{2}\s*(?:am|pm))\s*-\s*([0-9]{1,2}:[0-9]{2}\s*(?:am|pm))", re.IGNORECASE)
        for m in pat.finditer("\n" + page_text + "\n"):
            title = clean_whitespace(m.group(1))
            base_dt = parse_date_any(m.group(2), default_tz)
            if not base_dt:
                continue
            s_t = dateparser.parse(m.group(3))
            e_t = dateparser.parse(m.group(4))
            start = base_dt.replace(hour=s_t.hour, minute=s_t.minute, second=0, microsecond=0)
            end = base_dt.replace(hour=e_t.hour, minute=e_t.minute, second=0, microsecond=0)
            if end <= start:
                end = start + dt.timedelta(hours=DEFAULT_EVENT_DURATION_HOURS)
            events.append({
                "id": stable_event_id(source.get("id", "thevopros"), base_url, title, start),
                "title": title,
                "host": source.get("name") or "The VO Pros",
                "city": "Los Angeles",
                "state": "CA",
                "venue": "See listing",
                "startAt": start.isoformat(),
                "endAt": end.isoformat(),
                "registrationURL": base_url,
                "imageURL": None,
                "detail": None,
                "links": [{"title": "Shop", "url": base_url}],
                "provider": source.get("name"),
            })

    # Dedup by (title,date)
    seen3 = set()
    out = []
    for e in events:
        key = (e.get("title"), (e.get("startAt") or "")[:10])
        if key in seen3:
            continue
        seen3.add(key)
        out.append(e)
    return out


EXTRACTORS = {
    "jsonld_events": extract_jsonld_events,
    "tidycal_index": extract_tidycal_index,
    "soundonstudio_classsignup": extract_soundonstudio_classsignup,
    "thevopros_events_index": extract_thevopros_events_index,
    "thevopros_events_from_shop": extract_thevopros_events_from_shop,
    "halp_events_search": extract_halp_events_search,
    "van_shopify_products": extract_van_shopify_products,
    "wix_service_list": extract_wix_service_list,
    "aiva_upcoming_schedule": extract_aiva_upcoming_schedule,
    "voicetraxwest_guest_instructors": extract_voicetraxwest_guest_instructors,
    "vodojo_upcoming": extract_vodojo_upcoming,
    "html_fallback": extract_html_fallback
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
