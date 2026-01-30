#!/usr/bin/env python3
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

TRACKING_PARAMS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "fbclid", "gclid", "mc_cid", "mc_eid"
}

MONTHS = (
    "january","february","march","april","may","june",
    "july","august","september","october","november","december",
    "jan","feb","mar","apr","may","jun","jul","aug","sep","sept","oct","nov","dec"
)

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
    r = requests.get(url, timeout=30, headers={"User-Agent": "VOTrackerWorkshopBot/1.1"})
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
    # If reg URL exists, use it; else use (title + start)
    if reg_url:
        key = f"{source_id}|url|{reg_url}"
    else:
        key = f"{source_id}|ts|{title.strip().lower()}|{start_at}"
    return f"{source_id}-{sha1(key)[:16]}"

def merge_workshops(existing: List[Dict[str, Any]], incoming: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_id: Dict[str, Dict[str, Any]] = {}

    # Seed with existing
    for w in (existing or []):
        wid = w.get("id")
        if wid:
            by_id[wid] = dict(w)

    # Merge incoming
    for w in (incoming or []):
        wid = w.get("id")
        if not wid:
            continue
        if wid in by_id:
            cur = by_id[wid]
            # Update fields but do not overwrite good data with blanks
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

    # Sort by startAt
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
# Time parsing helpers
# -----------------------------

TIME_RANGE_RE = re.compile(
    r"\b(\d{1,2})(?::(\d{2}))?\s*(am|pm)\b\s*(?:[-–to]+\s*)\b(\d{1,2})(?::(\d{2}))?\s*(am|pm)\b",
    re.IGNORECASE,
)

SINGLE_TIME_RE = re.compile(r"\b(\d{1,2})(?::(\d{2}))?\s*(am|pm)\b", re.IGNORECASE)

def _to_24h(h: int, m: int, ap: str) -> Tuple[int, int]:
    ap = ap.lower()
    if ap == "am":
        if h == 12:
            h = 0
    else:
        if h != 12:
            h += 12
    return h, m

def apply_time_range(base_date: dt.datetime, text: str, default_tz: str) -> Tuple[dt.datetime, dt.datetime]:
    tzinfo = datetz.gettz(default_tz)
    base_date = base_date.replace(tzinfo=tzinfo, hour=0, minute=0, second=0, microsecond=0)

    m = TIME_RANGE_RE.search(text or "")
    if m:
        sh, sm, sap = int(m.group(1)), int(m.group(2) or 0), m.group(3)
        eh, em, eap = int(m.group(4)), int(m.group(5) or 0), m.group(6)
        sh24, sm = _to_24h(sh, sm, sap)
        eh24, em = _to_24h(eh, em, eap)
        start = base_date.replace(hour=sh24, minute=sm)
        end = base_date.replace(hour=eh24, minute=em)
        if end <= start:
            end = end + dt.timedelta(hours=DEFAULT_EVENT_DURATION_HOURS)
        return start, end

    # single time -> add default duration
    m2 = SINGLE_TIME_RE.search(text or "")
    if m2:
        sh, sm, sap = int(m2.group(1)), int(m2.group(2) or 0), m2.group(3)
        sh24, sm = _to_24h(sh, sm, sap)
        start = base_date.replace(hour=sh24, minute=sm)
        end = start + dt.timedelta(hours=DEFAULT_EVENT_DURATION_HOURS)
        return start, end

    # no time info
    start = base_date.replace(hour=18, minute=0)  # default 6pm local
    end = start + dt.timedelta(hours=DEFAULT_EVENT_DURATION_HOURS)
    return start, end

def infer_year_for_month_day(month_day_str: str, default_tz: str) -> dt.datetime:
    """
    month_day_str: "Feb 3" or "February 3"
    picks current year unless that date is very far in the past (then next year)
    """
    now = dt.datetime.now(tz=datetz.gettz(default_tz))
    d = dateparser.parse(f"{month_day_str} {now.year}", fuzzy=True)
    if not d:
        return now
    d = d.replace(tzinfo=datetz.gettz(default_tz))
    # If it parsed to > ~45 days in past, roll forward a year
    if d < (now - dt.timedelta(days=45)):
        d = d.replace(year=now.year + 1)
    return d

# -----------------------------
# Extractors
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
    """
    FIXED: do NOT give every item the same registrationURL (that collapses everything into 1).
    We set registrationURL = None and rely on (title + startAt) for stable IDs.
    """
    html = fetch_html(source["url"])
    text = BeautifulSoup(html, "html.parser").get_text("\n")

    # example: "2.23.26 - Making Promos Pop! with ..."
    pattern = re.compile(r"\b(\d{1,2})\.(\d{1,2})\.(\d{2})\s*-\s*([^\n$]+)")
    events: List[Dict[str, Any]] = []

    for m in pattern.finditer(text):
        mm, dd, yy = int(m.group(1)), int(m.group(2)), int(m.group(3))
        year = 2000 + yy
        title = safe_text(m.group(4), 200)
        if not title:
            continue

        base = dt.datetime(year, mm, dd, 0, 0, tzinfo=datetz.gettz(default_tz))
        start, end = apply_time_range(base, "", default_tz)  # default 6pm + 2h

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
            "registrationURL": None,
            "imageURL": None,
            "detail": None,
            "links": [{"title": "Class Signup", "url": normalize_url(source["url"])}]
        })

    return events

def extract_thevopros_events_index(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """
    Scrape https://www.thevopros.com/events/ and visit each /events/... page.
    Event pages contain:
      Event Date: January 5, 2026
      Event Time: 4:00 pm
      Event Location: Zoom / physical address
    """
    index_html = fetch_html(source["url"])
    soup = BeautifulSoup(index_html, "html.parser")

    # Grab event links
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
            txt = BeautifulSoup(html, "html.parser").get_text("\n")

            # Title: use <title> fallback to first H1-ish text
            title = None
            ps = BeautifulSoup(html, "html.parser")
            h1 = ps.find(["h1","h2"])
            if h1:
                title = safe_text(h1.get_text(" ", strip=True), 200)
            if not title:
                title = safe_text(ps.title.get_text(" ", strip=True), 200) if ps.title else "Workshop"

            date_m = re.search(r"Event Date:\s*([A-Za-z]+\s+\d{1,2},\s*\d{4})", txt)
            time_m = re.search(r"Event Time:\s*([0-9:]+\s*(?:am|pm))", txt, re.IGNORECASE)
            loc_m = re.search(r"Event Location:\s*([^\n]+)", txt)

            if not date_m:
                continue

            base_date = parse_date_any(date_m.group(1), default_tz)
            if not base_date:
                continue

            time_str = time_m.group(1) if time_m else ""
            start, end = apply_time_range(base_date, time_str, default_tz)

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
        except Exception:
            continue

    return events

def extract_halp_events_search(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """
    Scrape https://halpacademy.com/events/search/ (it contains "Upcoming Events" in HTML).
    We'll also pull product links and parse the date/time string from product pages.
    """
    html = fetch_html(source["url"])
    soup = BeautifulSoup(html, "html.parser")

    product_links: List[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/product/" in href:
            full = href if href.startswith("http") else urljoin(source["url"], href)
            full = normalize_url(full)
            if full and full not in product_links:
                product_links.append(full)

    events: List[Dict[str, Any]] = []
    for purl in product_links[:120]:
        try:
            ph = fetch_html(purl)
            ps = BeautifulSoup(ph, "html.parser")
            txt = ps.get_text("\n")

            # Title
            h1 = ps.find(["h1","h2"])
            title = safe_text(h1.get_text(" ", strip=True), 200) if h1 else None
            if not title:
                title = "Workshop"

            # Look for a line like: "Mon, Jan 12, 2026, 5:00 PM – 7:00 PM PST"
            # or: "Wed, Feb 18, 2026, 6:00 PM – 9:00 PM PST"
            dt_line = None
            for line in txt.splitlines():
                line = line.strip()
                if not line:
                    continue
                if re.search(r"\b\d{4}\b", line) and re.search(r"\b(?:am|pm)\b", line, re.IGNORECASE):
                    if any(m in line.lower() for m in MONTHS):
                        dt_line = line
                        break

            if not dt_line:
                continue

            # Split date part from times if possible
            base_date = parse_date_any(dt_line, default_tz)
            if not base_date:
                continue

            start, end = apply_time_range(base_date, dt_line, default_tz)

            start_s = isoformat_with_tz(start, default_tz)
            end_s = isoformat_with_tz(end, default_tz)

            # Venue hints
            venue = "Online / See listing"
            if "online" in txt.lower() and "zoom" in txt.lower():
                venue = "Zoom"
            if re.search(r"\b[A-Z]{2}\s+\d{5}\b", txt):
                venue = "In Person"

            reg = normalize_url(purl)
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
                "links": [{"title": "Event Page", "url": reg}]
            })
        except Exception:
            continue

    return events

def extract_van_shopify_products(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """
    VAN is Shopify. Best results come from crawling products from /collections/all
    and parsing the date/time embedded in the product title/description.
    """
    html = fetch_html(source["url"])
    soup = BeautifulSoup(html, "html.parser")

    product_urls: List[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/products/" in href:
            full = href if href.startswith("http") else urljoin(source["url"], href)
            full = normalize_url(full)
            if full and full not in product_urls:
                product_urls.append(full)

    events: List[Dict[str, Any]] = []

    # Regex tries to capture month/day/year + time range (many VAN pages contain it)
    date_hint_re = re.compile(
        r"(?:(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\s*,\s*)?"
        r"((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2})(?:st|nd|rd|th)?"
        r"(?:\s*,\s*(\d{4}))?",
        re.IGNORECASE
    )

    for purl in product_urls[:160]:
        try:
            ph = fetch_html(purl)
            ps = BeautifulSoup(ph, "html.parser")
            txt = ps.get_text("\n")
            lower = txt.lower()

            # Skip obvious non-events
            if any(k in lower for k in ["gift card", "membership", "subscription"]):
                continue

            # Title
            h1 = ps.find(["h1","h2"])
            title = safe_text(h1.get_text(" ", strip=True), 200) if h1 else None
            if not title:
                title = "Clinic"

            # Find a month/day/year inside the page text
            m = date_hint_re.search(txt)
            if not m:
                continue

            md = m.group(2)
            yr = m.group(3)
            if yr:
                base_date = parse_date_any(f"{md} {yr}", default_tz)
            else:
                base_date = infer_year_for_month_day(md, default_tz)

            if not base_date:
                continue

            # Find time range (7-10pm etc)
            start, end = apply_time_range(base_date, txt, default_tz)

            start_s = isoformat_with_tz(start, default_tz)
            end_s = isoformat_with_tz(end, default_tz)

            # Host often appears in title/description. Try “with X”
            host = None
            hm = re.search(r"\bwith\s+([A-Z][A-Za-z.\- ]{2,60})", txt)
            if hm:
                host = safe_text(hm.group(1), 120)

            # Venue heuristic
            venue = "See listing"
            if "zoom" in lower:
                venue = "Zoom"
            if "in person" in lower or "in-person" in lower or "in studio" in lower:
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
        except Exception:
            continue

    return events

def extract_wix_service_list(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """
    Wix sites often expose bookable offerings as /service-page/ links.
    We scrape service-page links from the list page and then parse each service page
    for a visible 'Starts/Started' date + optional time.
    This won't catch *every session* if the calendar loads dynamically, but it does
    pull many "Starts ..." offerings reliably.
    """
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

            # Title
            h1 = ss.find(["h1","h2"])
            title = safe_text(h1.get_text(" ", strip=True), 200) if h1 else None
            if not title:
                title = "Workshop"

            m = starts_re.search(txt)
            if not m:
                continue

            md = m.group(2)
            base_date = infer_year_for_month_day(md, default_tz)

            # Try find a time range nearby
            start, end = apply_time_range(base_date, txt, default_tz)

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
        except Exception:
            continue

    return events

def extract_vodojo_upcoming(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    """
    VO Dojo upcoming events page is typically a list with dates + titles + links.
    We'll:
      - collect all outbound links on the page
      - find nearby date strings in text
    """
    html = fetch_html(source["url"])
    soup = BeautifulSoup(html, "html.parser")

    # Grab all links on the page (many events are separate links)
    hrefs = []
    for a in soup.find_all("a", href=True):
        full = a["href"]
        if full.startswith("/"):
            full = urljoin(source["url"], full)
        full = normalize_url(full)
        if full and full not in hrefs:
            hrefs.append(full)

    txt = soup.get_text("\n")

    # Find date lines like "Feb 12, 2026" etc
    date_re = re.compile(r"\b([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})\b")

    events: List[Dict[str, Any]] = []
    dates = date_re.findall(txt)
    # If page doesn’t give structured blocks, we at least create “Upcoming Events” stubs for the next dates.
    for dstr in dates[:40]:
        base = parse_date_any(dstr, default_tz)
        if not base:
            continue
        start, end = apply_time_range(base, txt, default_tz)
        start_s = isoformat_with_tz(start, default_tz)
        end_s = isoformat_with_tz(end, default_tz)

        # Best-effort: attach the page itself
        reg = normalize_url(source["url"])
        title = "VO Dojo Event"
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
            "registrationURL": None,
            "imageURL": None,
            "detail": None,
            "links": [{"title": "Upcoming Events", "url": reg}] if reg else None
        })

    return events

def extract_html_fallback(source: Dict[str, Any], default_tz: str) -> List[Dict[str, Any]]:
    # Safe fallback: try JSON-LD; if none, return empty
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
        events = fn(s, args.default_tz)

        # ✅ ADD THIS RIGHT HERE
        # Ensure each event carries the provider/studio/site name for the app UI
        for e in events:
            if isinstance(e, dict):
                e.setdefault("provider", s.get("name"))

        # safety cap
        events = events[:max_events_per_source]
        print(f"[OK] {s.get('id')}: {len(events)} events")
        incoming_all.extend(events)
    except Exception as ex:
        print(f"[ERROR] {s.get('id')}: {ex}")


    merged = merge_workshops(existing, incoming_all)
    merged = prune_events(merged, prune_days_past, keep_days_future, args.default_tz)

    resources["workshops"] = merged
    resources["lastUpdated"] = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")

    with open(args.resources, "w", encoding="utf-8") as f:
        json.dump(resources, f, ensure_ascii=False, indent=2)
        f.write("\n")

if __name__ == "__main__":
    main()
