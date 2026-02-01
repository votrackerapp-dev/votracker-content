#!/usr/bin/env python3
"""
sync_workshops.py - VO Workshop Scraper for VOTracker
IMPROVED: Fixed Red Scythe, VO Dojo, and VoiceTrax West extractors

Key improvements:
- TidyCal: Now extracts ALL classes from the page (not just the first one)
- VO Dojo: Properly parses event cards and filters out non-class items
- VoiceTrax West: Better section parsing with schedule pattern matching
- All extractors: Mark time as TBD when not 100% certain
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
    """Fetch HTML with retries."""
    base_headers = {"User-Agent": "VOTrackerWorkshopBot/1.4 (+https://votracker.app)"}
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

def clean_title(title_raw: str) -> str:
    title = safe_text(title_raw, 200) or ""
    return title.strip()

def infer_year_for_month_day(month_day_str: str, default_tz: str) -> Optional[dt.datetime]:
    """Parse month+day, bump year if in past."""
    tzinfo = datetz.gettz(default_tz)
    now = dt.datetime.now(tz=tzinfo)
    d = dateparser.parse(month_day_str, fuzzy=True, default=now)
    if not d:
        return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=tzinfo)

    if d < (now - dt.timedelta(days=60)):
        try:
            d = d.replace(year=d.year + 1)
        except Exception:
            pass
    return d

class TimeRange:
    def __init__(self, start_hour: int, start_minute: int, end_hour: Optional[int] = None, end_minute: Optional[int] = None):
        self.start_hour = start_hour
        self.start_minute = start_minute
        self.end_hour = end_hour
        self.end_minute = end_minute

def extract_time_from_text(text: str) -> Optional[TimeRange]:
    """Parse time ranges like '6:30pm-9:00pm' or '10am-2pm'."""
    time_range_re = re.compile(
        r"(?P<sh>\d{1,2})(?::(?P<sm>\d{2}))?\s*(?P<sam>am|pm)?\s*[\-–—to]+\s*(?P<eh>\d{1,2})(?::(?P<em>\d{2}))?\s*(?P<eam>am|pm)?",
        re.I | re.X,
    )

    single_time_re = re.compile(
        r"(?P<sh>\d{1,2})(?::(?P<sm>\d{2}))?\s*(?P<sam>am|pm)",
        re.I | re.X,
    )

    m = time_range_re.search(text)
    if m:
        sh = int(m.group("sh"))
        sm = int(m.group("sm")) if m.group("sm") else 0
        sam = (m.group("sam") or "").lower()

        eh = int(m.group("eh"))
        em = int(m.group("em")) if m.group("em") else 0
        eam = (m.group("eam") or "").lower()

        if not eam and sam:
            eam = sam

        if sam == "pm" and sh < 12:
            sh += 12
        if sam == "am" and sh == 12:
            sh = 0

        if eam == "pm" and eh < 12:
            eh += 12
        if eam == "am" and eh == 12:
            eh = 0

        return TimeRange(sh, sm, eh, em)

    m = single_time_re.search(text)
    if m:
        sh = int(m.group("sh"))
        sm = int(m.group("sm")) if m.group("sm") else 0
        sam = (m.group("sam") or "").lower()

        if sam == "pm" and sh < 12:
            sh += 12
        if sam == "am" and sh == 12:
            sh = 0

        return TimeRange(sh, sm, None, None)

    return None

def is_ongoing_class(title: str, detail: str) -> bool:
    """Detect multi-session/ongoing courses."""
    combined = f"{title} {detail}".lower()
    patterns = [
        r'\b\d+\s*-?\s*(week|session|class|month)\s+(course|pack|series|program)',
        r'(pack|package|bundle|series)\b',
        r'(monthly|weekly|ongoing)',
        r'curriculum',
        r'semester',
    ]
    for pattern in patterns:
        if re.search(pattern, combined, re.I):
            return True
    return False

def ensure_workshop_schema(e: Dict[str, Any], source: Dict[str, Any], default_tz: str) -> Dict[str, Any]:
    """Normalize event to canonical schema."""
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

    out = {
        "id": compute_event_id(src_id, title, start_s or "", reg_url),
        "title": title,
        "startAt": start_s,
        "endAt": end_s,
        "registrationURL": reg_url,
        "host": host,
        "provider": src_id,
    }

    if detail:
        out["detail"] = detail
    if links:
        out["links"] = links

    for k in ("city", "state", "venue", "imageURL", "timeTBD"):
        v = e.get(k)
        if v is not None:
            out[k] = v

    return out

# ===================================================================================================
# TIDYCAL EXTRACTOR (Red Scythe)
# ===================================================================================================

def extract_tidycal_index(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """
    ✅ IMPROVED: Extracts ALL class listings from TidyCal pages.
    
    TidyCal pages list classes with headings like:
    - "2.7 | BRITTANY COX | strong reads that book the room"
    - "2.14 | CHRIS HACKNEY | potluck clinic workshop"
    
    Each class has a "View details" link.
    
    This extractor:
    1. Finds ALL "View details" links
    2. Parses the date from heading (2.7 = Feb 7)
    3. Extracts title and instructor
    4. Filters out coaching/consultation/downloads per config
    5. Marks time as TBD when not found
    """
    base_url = source["url"]
    html = fetch_html(base_url, headers=source.get("headers"))
    soup = BeautifulSoup(html, "html.parser")

    filters = source.get("filters", {})
    deny_phrases = [s.lower() for s in filters.get("exclude_title_contains", [])]
    deny_regexes = [re.compile(p, re.I) for p in filters.get("exclude_title_regex", [])]

    events: List[Dict[str, Any]] = []
    seen_keys = set()

    # Find ALL links that might be detail links (more flexible approach)
    all_links = soup.find_all("a", href=True)
    detail_links = []
    
    for link in all_links:
        href = link.get("href", "").strip()
        link_text = link.get_text(" ", strip=True).lower()
        
        # Accept "View details" or links that go to specific class pages
        if "view" in link_text and "detail" in link_text:
            detail_links.append(link)
        elif href and ("redscythestudio" in href or "tidycal.com" in href) and "/" in href.rstrip("/").split("/")[-1]:
            # Link goes to a specific page (not just the root)
            detail_links.append(link)
    
    log(f"  [TidyCal] Found {len(detail_links)} potential detail links")

    for link in detail_links:
        try:
            href = link.get("href", "").strip()
            if not href:
                continue
            
            if href.startswith("/"):
                href = "https://tidycal.com" + href
            elif not href.startswith("http"):
                href = "https://tidycal.com/" + href

            container = link.find_parent(["div", "section", "article"])
            if not container:
                continue

            container_text = container.get_text(" ", strip=True)
            
            # Check deny filters
            low = container_text.lower()
            if any(p in low for p in deny_phrases):
                log(f"  [TidyCal] Skipping (deny phrase): {container_text[:60]}")
                continue
            
            if any(rx.search(container_text) for rx in deny_regexes):
                log(f"  [TidyCal] Skipping (deny regex): {container_text[:60]}")
                continue

            # Find heading with date pattern
            heading = container.find(re.compile("^h[1-6]$"))
            if not heading:
                log(f"  [TidyCal] No heading found")
                continue

            heading_text = heading.get_text(" ", strip=True)
            
            # Parse date: "2.7" = Feb 7, "2.14" = Feb 14
            date_match = re.match(r"^(\d{1,2})\.(\d{1,2})\s*\|", heading_text)
            if not date_match:
                log(f"  [TidyCal] Could not parse date from: {heading_text}")
                continue

            month = int(date_match.group(1))
            day = int(date_match.group(2))
            
            # Infer year
            tzinfo = datetz.gettz(default_tz)
            now = dt.datetime.now(tz=tzinfo)
            try:
                base_dt = dt.datetime(now.year, month, day, tzinfo=tzinfo)
                if base_dt < (now - dt.timedelta(days=60)):
                    base_dt = base_dt.replace(year=base_dt.year + 1)
            except ValueError:
                log(f"  [TidyCal] Invalid date: {month}/{day}")
                continue

            # Extract title and instructor
            # Format: "2.7 | BRITTANY COX | strong reads that book the room"
            parts = heading_text.split("|")
            if len(parts) >= 3:
                instructor = parts[1].strip()
                class_title = parts[2].strip()
                title = f"{class_title} with {instructor}"
            elif len(parts) == 2:
                title = parts[1].strip()
            else:
                title = heading_text
            
            title = clean_title(title)
            
            # Try to extract time
            time_tbd = False
            tr = extract_time_from_text(container_text)
            if tr:
                start_dt = base_dt.replace(hour=tr.start_hour, minute=tr.start_minute, second=0, microsecond=0)
                if tr.end_hour is not None and tr.end_minute is not None:
                    end_dt = base_dt.replace(hour=tr.end_hour, minute=tr.end_minute, second=0, microsecond=0)
                else:
                    end_dt = start_dt + dt.timedelta(hours=2)
            else:
                time_tbd = True
                start_dt = base_dt.replace(hour=12, minute=0, second=0, microsecond=0)
                end_dt = start_dt + dt.timedelta(hours=2)

            key = (title.lower(), start_dt.date().isoformat())
            if key in seen_keys:
                continue
            seen_keys.add(key)

            events.append({
                "title": title,
                "startAt": isoformat_with_tz(start_dt, default_tz),
                "endAt": isoformat_with_tz(end_dt, default_tz),
                "registrationURL": normalize_url(href),
                "host": source.get("name", "TidyCal"),
                "provider": source.get("id", "tidycal"),
                "timeTBD": True if time_tbd else None,
            })
            
            log(f"  [TidyCal] ✅ {title} on {start_dt.date()}")

        except Exception as e:
            log(f"  [TidyCal] Error: {e}")
            continue

    log(f"  [TidyCal] Total: {len(events)} events")
    return events


# ===================================================================================================
# VO DOJO EXTRACTOR
# ===================================================================================================

def extract_vodojo_upcoming(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """
    ✅ IMPROVED: Extracts events from The VO Dojo calendar.
    
    The VO Dojo lists events with:
    - Event title in link
    - Date (e.g., "Feb 4, 2026")
    - Detail page link (/new-events/...)
    
    Filters out non-class items like mailing list, contests, etc.
    """
    base_url = source["url"]
    html = fetch_html(base_url, headers=source.get("headers"))
    soup = BeautifulSoup(html, "html.parser")

    junk_phrases = [
        "join our mailing list", "refer a friend", "refer-a-friend",
        "newsletter", "mailing list", "gift card", "donate", "sponsor",
        "spread the vo love",  # Contest
    ]

    def is_junk(title: str) -> bool:
        low = title.strip().lower()
        return not low or len(low) < 6 or any(p in low for p in junk_phrases)

    events: List[Dict[str, Any]] = []
    seen_keys = set()

    event_links = soup.find_all("a", href=re.compile(r"/new-events/"))
    
    log(f"  [VO Dojo] Found {len(event_links)} event links")

    for link in event_links:
        try:
            href = link.get("href", "").strip()
            if not href:
                continue
            
            if href.startswith("/"):
                detail_url = "https://www.thevodojo.com" + href
            else:
                detail_url = href

            container = link.find_parent(["article", "div", "section", "li"]) or link.parent

            # Get title - ALWAYS look for heading first, not link text
            title_text = None
            h = container.find(re.compile("^h[1-6]$"))
            if h:
                title_text = h.get_text(" ", strip=True)
            
            # Fallback to link text only if no heading found
            if not title_text:
                title_text = link.get_text(" ", strip=True)

            title = clean_title(title_text)
            
            if is_junk(title):
                log(f"  [VO Dojo] Skipping junk: {title}")
                continue

            # Extract date
            container_text = container.get_text("\n", strip=True)
            
            # Look for "Feb 4, 2026" or "February 12, 2026"
            date_matches = re.findall(r"([A-Za-z]{3,9})\s+(\d{1,2}),?\s+(\d{4})", container_text)
            
            base_dt = None
            if date_matches:
                month_str, day_str, year_str = date_matches[0]
                date_str = f"{month_str} {day_str}, {year_str}"
                base_dt = parse_date_any(date_str, default_tz)
            
            if not base_dt:
                # Try without year
                date_matches_no_year = re.findall(r"([A-Za-z]{3,9})\s+(\d{1,2})", container_text)
                if date_matches_no_year:
                    month_str, day_str = date_matches_no_year[0]
                    base_dt = infer_year_for_month_day(f"{month_str} {day_str}", default_tz)

            if not base_dt:
                log(f"  [VO Dojo] No date found for: {title}")
                continue

            # Try to extract time
            time_tbd = False
            tr = extract_time_from_text(container_text)
            if tr:
                start_dt = base_dt.replace(hour=tr.start_hour, minute=tr.start_minute, second=0, microsecond=0)
                if tr.end_hour is not None and tr.end_minute is not None:
                    end_dt = base_dt.replace(hour=tr.end_hour, minute=tr.end_minute, second=0, microsecond=0)
                else:
                    end_dt = start_dt + dt.timedelta(hours=2)
            else:
                time_tbd = True
                start_dt = base_dt.replace(hour=12, minute=0, second=0, microsecond=0)
                end_dt = start_dt + dt.timedelta(hours=2)

            key = (title.lower(), start_dt.date().isoformat(), detail_url)
            if key in seen_keys:
                continue
            seen_keys.add(key)

            events.append({
                "title": title,
                "startAt": isoformat_with_tz(start_dt, default_tz),
                "endAt": isoformat_with_tz(end_dt, default_tz),
                "registrationURL": normalize_url(detail_url),
                "host": source.get("name", "The VO Dojo"),
                "provider": source.get("id", "thevodojo"),
                "timeTBD": True if time_tbd else None,
            })
            
            log(f"  [VO Dojo] ✅ {title} on {start_dt.date()}")

        except Exception as e:
            log(f"  [VO Dojo] Error: {e}")
            continue

    log(f"  [VO Dojo] Total: {len(events)} events")
    return events


# ===================================================================================================
# VOICETRAX WEST EXTRACTOR
# ===================================================================================================

def extract_voicetraxwest_guest_instructors(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """
    ✅ IMPROVED: Extracts guest instructor workshops from VoiceTrax West.
    
    Classes are separated by <hr> tags and have:
    - Class title as heading
    - Schedule line: "TUESDAY, Feb 10 - 6:30pm PST"
    - Registration links (in person/online)
    """
    base_url = source["url"]
    html = fetch_html(base_url, headers=source.get("headers"))
    soup = BeautifulSoup(html, "html.parser")

    sched_re = re.compile(
        r'\b(?:mon|tue|wed|thu|fri|sat|sun)\w*\s*,?\s*(?P<mon>[A-Za-z]{3,9})\s*(?P<day>\d{1,2})\s*(?:-|–)\s*(?P<time>\d{1,2}:\d{2}\s*(?:am|pm))?',
        re.I
    )

    events: List[Dict[str, Any]] = []
    seen_keys = set()

    # SIMPLER APPROACH: Find all text that matches the schedule pattern,
    # then find the nearest heading above it
    all_text = soup.get_text("\n")
    
    for match in sched_re.finditer(all_text):
        try:
            month = match.group("mon")
            day = match.group("day")
            base_dt = infer_year_for_month_day(f"{month} {day}", default_tz)
            if not base_dt:
                continue
            
            # Find the element containing this match
            schedule_text = match.group(0)
            # Search for elements containing this exact schedule text
            candidates = soup.find_all(string=re.compile(re.escape(schedule_text)))
            
            if not candidates:
                continue
            
            # Get the first candidate's parent container
            elem = candidates[0]
            container = elem.parent
            
            # Walk up to find a reasonable container (has heading + links)
            for _ in range(10):
                if not container:
                    break
                # Check if this container has both a heading and links
                has_heading = container.find(re.compile("^h[1-6]$")) is not None
                has_link = container.find("a", href=True) is not None
                container_text = safe_text(container.get_text(" ", strip=True))
                
                if has_heading and has_link and len(container_text) > 50:
                    break
                    
                container = container.parent
            
            if not container:
                continue
            
            # Extract title from heading
            title = None
            for tag in ["h1", "h2", "h3", "h4", "h5", "h6"]:
                h = container.find(tag)
                if h:
                    title = safe_text(h.get_text(" ", strip=True))
                    if title and len(title) > 5 and not sched_re.search(title):
                        break
            
            if not title or len(title) < 6:
                continue
            
            title = clean_title(title)
            
            # Extract time
            section_text = container.get_text("\n", strip=True)
            time_tbd = False
            tr = extract_time_from_text(section_text)
            if tr:
                start_dt = base_dt.replace(hour=tr.start_hour, minute=tr.start_minute, second=0, microsecond=0)
                if tr.end_hour is not None and tr.end_minute is not None:
                    end_dt = base_dt.replace(hour=tr.end_hour, minute=tr.end_minute, second=0, microsecond=0)
                else:
                    end_dt = start_dt + dt.timedelta(hours=2)
            else:
                time_tbd = True
                start_dt = base_dt.replace(hour=12, minute=0, second=0, microsecond=0)
                end_dt = start_dt + dt.timedelta(hours=2)
            
            # Find registration link
            reg_url = None
            for link in container.find_all("a", href=True):
                href = link.get("href", "").strip()
                if any(domain in href for domain in ["as.me", "acuityscheduling", "calendly", "eventbrite", "voicetraxwest.com"]):
                    reg_url = normalize_url(href)
                    break
            
            if not reg_url:
                reg_url = base_url
            
            key = (title.lower(), start_dt.date().isoformat())
            if key in seen_keys:
                continue
            seen_keys.add(key)
            
            events.append({
                "title": title,
                "startAt": isoformat_with_tz(start_dt, default_tz),
                "endAt": isoformat_with_tz(end_dt, default_tz),
                "registrationURL": reg_url,
                "host": source.get("name", "Voice Trax West"),
                "provider": source.get("id", "voicetraxwest"),
                "timeTBD": True if time_tbd else None,
            })
            
            log(f"  [VoiceTrax] ✅ {title} on {start_dt.date()}")
        
        except Exception as e:
            log(f"  [VoiceTrax] Error: {e}")
            continue

    log(f"  [VoiceTrax] Total: {len(events)} events")
    return events


# ===================================================================================================
# OTHER EXTRACTORS (placeholder implementations - add full code as needed)
# ===================================================================================================

def extract_jsonld_events(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """Extract events from JSON-LD."""
    return []

def extract_soundonstudio_classsignup(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """Sound On Studio."""
    return []

def extract_thevopros_events_index(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """The VO Pros."""
    return []

def extract_halp_events_search(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """HALP Academy."""
    return []

def extract_van_shopify_products(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """Voice Actors Network."""
    return []

def extract_wix_service_list(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """Wix services (Real Voice LA)."""
    return []

def extract_aiva_upcoming_schedule(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """Adventures in Voice Acting."""
    return []

def extract_html_fallback(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """Generic HTML fallback."""
    return []


EXTRACTORS = {
    "jsonld_events": extract_jsonld_events,
    "tidycal_index": extract_tidycal_index,
    "soundonstudio_classsignup": extract_soundonstudio_classsignup,
    "thevopros_events_index": extract_thevopros_events_index,
    "halp_events_search": extract_halp_events_search,
    "van_shopify_products": extract_van_shopify_products,
    "wix_service_list": extract_wix_service_list,
    "vodojo_upcoming": extract_vodojo_upcoming,
    "voicetraxwest_guest_instructors": extract_voicetraxwest_guest_instructors,
    "aiva_upcoming_schedule": extract_aiva_upcoming_schedule,
    "html_fallback": extract_html_fallback
}


def apply_event_filters(events: List[Dict[str, Any]], filters: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Apply filters."""
    if not filters:
        events = [e for e in events if not is_ongoing_class(e.get("title", ""), e.get("detail", ""))]
        return events

    excl_title_res = [re.compile(p, re.IGNORECASE) for p in filters.get("exclude_title_regex", []) if p]
    excl_title_contains = [s.lower() for s in filters.get("exclude_title_contains", []) if isinstance(s, str) and s.strip()]

    out: List[Dict[str, Any]] = []
    for e in events:
        title = (e.get("title") or "").strip()
        detail = (e.get("detail") or "").strip()

        if not title:
            continue

        if is_ongoing_class(title, detail):
            continue

        t_low = title.lower()

        if excl_title_contains and any(s in t_low for s in excl_title_contains):
            continue
        if any(rx.search(title) for rx in excl_title_res):
            continue

        out.append(e)

    return out


def rebuild_workshops(existing: List[Dict[str, Any]], incoming: List[Dict[str, Any]], successfully_scraped_sources: set) -> List[Dict[str, Any]]:
    """Merge existing and new workshops."""
    by_provider: Dict[str, List[Dict[str, Any]]] = {}
    
    for w in incoming:
        p = w.get("provider", "unknown")
        by_provider.setdefault(p, []).append(w)
    
    for w in existing:
        p = w.get("provider", "unknown")
        if p not in successfully_scraped_sources:
            by_provider.setdefault(p, []).append(w)
    
    rebuilt = []
    for provider_events in by_provider.values():
        rebuilt.extend(provider_events)
    
    return rebuilt


def prune_events(events: List[Dict[str, Any]], prune_days_past: int, keep_days_future: int, default_tz: str) -> List[Dict[str, Any]]:
    """Remove old/future events."""
    tzinfo = datetz.gettz(default_tz)
    now = dt.datetime.now(tz=tzinfo)
    cutoff_past = now - dt.timedelta(days=prune_days_past)
    cutoff_future = now + dt.timedelta(days=keep_days_future)
    
    out: List[Dict[str, Any]] = []
    for e in events:
        start_str = e.get("startAt")
        if not start_str:
            continue
        try:
            start_dt = dateparser.isoparse(start_str)
            if cutoff_past <= start_dt <= cutoff_future:
                out.append(e)
        except Exception:
            out.append(e)
    
    return out


def normalize_existing_providers(existing: List[Dict[str, Any]], sources: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Map legacy provider values."""
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
    successfully_scraped_sources: set = set()

    for s in sources:
        source_id = s.get("id")
        extractor_name = s.get("extractor", "jsonld_events")
        fn = EXTRACTORS.get(extractor_name)
        if not fn:
            print(f"[WARN] Unknown extractor '{extractor_name}' for {source_id}")
            continue

        print(f"[INFO] Processing {s.get('name', source_id)}...")
        
        try:
            events = fn(s, args.default_tz)
            events = apply_event_filters(events, s.get("filters"))
            events = [ensure_workshop_schema(e, s, args.default_tz) for e in events if isinstance(e, dict)]
            events = events[:max_events_per_source]
            
            print(f"[OK] {source_id}: {len(events)} events")
            incoming_all.extend(events)
            
            allow_zero = bool(s.get("allow_zero_events"))
            if events or allow_zero:
                successfully_scraped_sources.add(source_id)
            else:
                log(f"  [WARN] {source_id}: 0 events; keeping old data")
        except Exception as ex:
            print(f"[ERROR] {source_id}: {ex}")

    print(f"\n[INFO] Scraped {len(successfully_scraped_sources)}/{len(sources)} sources")
    rebuilt = rebuild_workshops(existing, incoming_all, successfully_scraped_sources)
    rebuilt = prune_events(rebuilt, prune_days_past, keep_days_future, args.default_tz)

    # Deduplication
    seen_urls = {}
    url_deduped = []
    for w in rebuilt:
        url = w.get('registrationURL')
        if not url:
            url_deduped.append(w)
            continue
            
        if url in seen_urls:
            existing_title = seen_urls[url].get('title', '').upper()
            current_title = w.get('title', '').upper()
            generic_titles = ["THE VO PROS", "WORKSHOP", "CLASS", "EVENT"]
            
            if current_title not in generic_titles and existing_title in generic_titles:
                idx = url_deduped.index(seen_urls[url])
                url_deduped[idx] = w
                seen_urls[url] = w
        else:
            seen_urls[url] = w
            url_deduped.append(w)
    
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
            print(f"  (+{diff} new)")
        elif diff < 0:
            print(f"  ({diff} removed)")

if __name__ == "__main__":
    main()
