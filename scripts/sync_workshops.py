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

def fetch_html(url: str) -> str:
    r = requests.get(url, timeout=30, headers={"User-Agent": "VOTrackerWorkshopBot/1.2"})
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

def merge_workshops(existing: List[Dict[str, Any]], incoming: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_id: Dict[str, Dict[str, Any]] = {}
    for w in (existing or []):
        wid = w.get("id")
        if wid:
            by_id[wid] = dict(w)
    for w in (incoming or []):
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
                cur[k] = v
            by_id[wid] = cur
        else:
            by_id[wid] = dict(w)
    merged = list(by_id.values())
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
    """Extract events from JSON-LD structured data - most reliable source!"""
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

    return out

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
    """Scrape The VO Pros events pages."""
    index_html = fetch_html(source["url"])
    soup = BeautifulSoup(index_html, "html.parser")

    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/events/" in href and not href.endswith("/events/"):
            full = href if href.startswith("http") else urljoin(source["url"], href)
            full = normalize_url(full)
            if full and full not in links:
                links.append(full)

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

            date_m = re.search(r"Event Date:\s*([A-Za-z]+\s+\d{1,2},\s*\d{4})", txt)
            time_m = re.search(r"Event Time:\s*([0-9:]+\s*(?:am|pm)?(?:\s*[-–to]+\s*[0-9:]+\s*(?:am|pm))?)", txt, re.IGNORECASE)
            loc_m = re.search(r"Event Location:\s*([^\n]+)", txt)

            if not date_m:
                continue

            base_date = parse_date_any(date_m.group(1), default_tz)
            if not base_date:
                continue

            time_str = time_m.group(1) if time_m else ""
            log(f"  [VOPros] {title[:40]}... time_str='{time_str}'")
            start, end = apply_time_to_date(base_date, time_str, default_tz)

            start_s = isoformat_with_tz(start, default_tz)
            end_s = isoformat_with_tz(end, default_tz)

            venue = safe_text(loc_m.group(1), 160) if loc_m else None

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
            log(f"  [VOPros] Error: {e}")
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
    """Wix service pages (Real Voice LA, etc.)"""
    html = fetch_html(source["url"])
    soup = BeautifulSoup(html, "html.parser")

    service_urls: List[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "service-page" in href:
            full = href if href.startswith("http") else urljoin(source["url"], href)
            full = normalize_url(full)
            if full and full not in service_urls:
                service_urls.append(full)

    events: List[Dict[str, Any]] = []
    starts_re = re.compile(r"\b(Start(?:s|ed))\s+([A-Za-z]{3,9}\s+\d{1,2})\b", re.IGNORECASE)

    for surl in service_urls[:120]:
        try:
            sh = fetch_html(surl)
            ss = BeautifulSoup(sh, "html.parser")
            txt = ss.get_text("\n")

            h1 = ss.find(["h1","h2"])
            title = safe_text(h1.get_text(" ", strip=True), 200) if h1 else None
            if not title:
                title = "Workshop"

            m = starts_re.search(txt)
            if not m:
                continue

            md = m.group(2)
            base_date = infer_year_for_month_day(md, default_tz)

            date_pos = txt.find(md)
            context_start = max(0, date_pos - 150)
            context_end = min(len(txt), date_pos + 250)
            time_context = txt[context_start:context_end]
            
            log(f"  [Wix] {title[:40]}...")
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
    """VO Dojo - COMPLETELY REWRITTEN to parse per-event"""
    html = fetch_html(source["url"])
    soup = BeautifulSoup(html, "html.parser")
    full_text = soup.get_text("\n")
    
    events: List[Dict[str, Any]] = []
    date_re = re.compile(r"([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})")
    
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
                
                log(f"  [VODojo] Processing event at {date_str}...")
                start, end = apply_time_to_date(base, event_text, default_tz)
                
                start_s = isoformat_with_tz(start, default_tz)
                end_s = isoformat_with_tz(end, default_tz)
                
                title = "VO Dojo Event"
                for el in event_lines:
                    el_clean = el.strip()
                    if el_clean == date_str or date_re.fullmatch(el_clean):
                        continue
                    if len(el_clean) > 10 and not el_clean.isdigit():
                        title = safe_text(el_clean, 120)
                        break
                
                wid = compute_event_id(source["id"], title, start_s, None)
                
                is_dupe = any(
                    e.get("startAt", "")[:10] == start_s[:10] and 
                    e.get("title", "").lower()[:20] == title.lower()[:20]
                    for e in events
                )
                
                if not is_dupe:
                    events.append({
                        "id": wid,
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
                    })
                
                i = j
                continue
        
        i += 1
    
    log(f"  [VODojo] Found {len(events)} events")
    return events

def extract_html_fallback(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    return extract_jsonld_events(source, default_tz)

EXTRACTORS = {
    "jsonld_events": extract_jsonld_events,
    "soundonstudio_classsignup": extract_soundonstudio_classsignup,
    "thevopros_events_index": extract_thevopros_events_index,
    "halp_events_search": extract_halp_events_search,
    "van_shopify_products": extract_van_shopify_products,
    "wix_service_list": extract_wix_service_list,
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

    for s in sources:
        extractor_name = s.get("extractor", "jsonld_events")
        fn = EXTRACTORS.get(extractor_name)
        if not fn:
            print(f"[WARN] Unknown extractor '{extractor_name}' for {s.get('id')}, skipping.")
            continue

        print(f"[INFO] Processing {s.get('name', s.get('id'))}...")
        
        try:
            events = fn(s, args.default_tz)
            for e in events:
                if isinstance(e, dict):
                    e.setdefault("provider", s.get("name"))
            events = events[:max_events_per_source]
            print(f"[OK] {s.get('id')}: {len(events)} events")
            incoming_all.extend(events)
        except Exception as ex:
            print(f"[ERROR] {s.get('id')}: {ex}")

    merged = merge_workshops(existing, incoming_all)
    merged = prune_events(merged, prune_days_past, keep_days_future, args.default_tz)

    seen_keys = set()
    deduped = []
    for w in merged:
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

if __name__ == "__main__":
    main()
