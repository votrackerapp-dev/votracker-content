import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
import requests
from bs4 import BeautifulSoup

import feedparser
from dateutil import parser as dateparser
from icalendar import Calendar

RESOURCES_JSON_PATH = os.environ.get("RESOURCES_JSON_PATH", "resources.json")
SOURCES_CONFIG_PATH = os.environ.get("SOURCES_CONFIG_PATH", "config/workshop_sources.json")

USER_AGENT = "VOTrackerWorkshopBot/1.0 (+https://github.com/)"
TIMEOUT = 20

# ---------- Models ----------

@dataclass
class Workshop:
  id: str
  title: str
  host: Optional[str]
  city: Optional[str]
  state: Optional[str]
  venue: Optional[str]
  startAt: str
  endAt: Optional[str]
  registrationURL: Optional[str]
  imageURL: Optional[str]
  detail: Optional[str]
  links: Optional[List[Dict[str, str]]]

  def to_dict(self) -> Dict[str, Any]:
    out = {
      "id": self.id,
      "title": self.title,
      "host": self.host,
      "city": self.city,
      "state": self.state,
      "venue": self.venue,
      "startAt": self.startAt,
      "endAt": self.endAt,
      "registrationURL": self.registrationURL,
      "imageURL": self.imageURL,
      "detail": self.detail,
      "links": self.links
    }
    # Remove null keys (keeps JSON clean)
    return {k: v for k, v in out.items() if v is not None}


# ---------- Helpers ----------

def http_get(url: str) -> str:
  r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT)
  r.raise_for_status()
  return r.text

def normalize_iso(dt: datetime) -> str:
  # Always output offset ISO8601
  if dt.tzinfo is None:
    dt = dt.replace(tzinfo=timezone.utc)
  return dt.isoformat()

def safe_id(prefix: str, title: str, start_iso: str) -> str:
  base = f"{prefix}-{title}-{start_iso}"
  base = base.lower()
  base = re.sub(r"[^a-z0-9]+", "-", base).strip("-")
  return base[:80]

def parse_datetime(s: str) -> Optional[datetime]:
  try:
    return dateparser.parse(s)
  except Exception:
    return None

def load_json(path: str) -> Dict[str, Any]:
  with open(path, "r", encoding="utf-8") as f:
    return json.load(f)

def save_json(path: str, data: Dict[str, Any]) -> None:
  with open(path, "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)
    f.write("\n")

def merge_workshops(existing: List[Dict[str, Any]], incoming: List[Workshop]) -> List[Dict[str, Any]]:
  """
  Merge by registrationURL if present, else by id.
  Incoming overwrites matching existing entries.
  """
  by_key: Dict[str, Dict[str, Any]] = {}

  def key_of(w: Dict[str, Any]) -> str:
    reg = w.get("registrationURL")
    if reg:
      return f"url:{reg}"
    return f"id:{w.get('id')}"

  for w in existing:
    by_key[key_of(w)] = w

  for w in incoming:
    wd = w.to_dict()
    k = key_of(wd)
    by_key[k] = wd

  # Sort by startAt ascending
  merged = list(by_key.values())
  merged.sort(key=lambda x: x.get("startAt", "9999"))
  return merged

def prune_past(workshops: List[Dict[str, Any]], keep_days_past: int = 1) -> List[Dict[str, Any]]:
  """
  Optional: keep very recent past events (yesterday) so users can still click them.
  """
  now = datetime.now(timezone.utc)
  kept: List[Dict[str, Any]] = []
  for w in workshops:
    start = parse_datetime(w.get("startAt", ""))
    if not start:
      kept.append(w)
      continue
    if start.tzinfo is None:
      start = start.replace(tzinfo=timezone.utc)
    delta_days = (start - now).total_seconds() / 86400.0
    if delta_days >= -keep_days_past:
      kept.append(w)
  return kept

# ---------- Extractors ----------

def extract_shopify_collection(source: Dict[str, Any]) -> List[Workshop]:
  """
  For Shopify sites like VAN:
  - Crawl collection page for product links
  - For each product page, parse title and time string in page text
  This is fragile (HTML changes break it), but works well enough with low frequency.
  """
  collection_url = source["collection_url"]
  html = http_get(collection_url)
  soup = BeautifulSoup(html, "html.parser")

  # Shopify product links tend to include /products/
  product_links = []
  for a in soup.select("a[href]"):
    href = a.get("href", "")
    if "/products/" in href:
      if href.startswith("/"):
        href = "https://voiceactorsnetwork.com" + href
      product_links.append(href)

  # Deduplicate
  product_links = list(dict.fromkeys(product_links))[:30]  # safety cap

  workshops: List[Workshop] = []
  for url in product_links:
    try:
      wh = http_get(url)
      psoup = BeautifulSoup(wh, "html.parser")

      title = (psoup.select_one("h1") or psoup.select_one("title"))
      title_text = title.get_text(strip=True) if title else "Workshop"

      # Pull a best-effort time string from the page body
      body_text = psoup.get_text(" ", strip=True)

      # VAN titles often embed: "Tuesday February 3rd 2026 4-7pm PT Zoom"
      # We'll try to find a "February" date + "pm" time chunk.
      m = re.search(r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}.*?\d{4}.*?(\d{1,2}(:\d{2})?\s*(am|pm))", body_text, re.IGNORECASE)
      start_dt: Optional[datetime] = None
      if m:
        # grab a slice around the match
        start_idx = max(0, m.start() - 40)
        end_idx = min(len(body_text), m.end() + 80)
        snippet = body_text[start_idx:end_idx]
        # try parse snippet
        start_dt = parse_datetime(snippet)

      # Fallback: use today (but we’d rather skip than inject garbage)
      if not start_dt:
        continue

      # Venue hint
      venue = "Zoom" if "zoom" in body_text.lower() else None
      if "in person" in body_text.lower():
        venue = "In Person"

      # Host guess (VAN product titles often: "Commercial Clinic - Carli Silver")
      host = None
      host_match = re.search(r"clinic\s*[-–]\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)", title_text)
      if host_match:
        host = host_match.group(1).strip()

      start_iso = normalize_iso(start_dt)
      wid = safe_id(source["id"], title_text, start_iso)

      workshops.append(
        Workshop(
          id=wid,
          title=title_text,
          host=host,
          city=None,
          state=None,
          venue=venue,
          startAt=start_iso,
          endAt=None,
          registrationURL=url,
          imageURL=None,
          detail=None,
          links=[{"title": "Listing", "url": url}]
        )
      )
    except Exception:
      continue

  return workshops

def extract_ics(source: Dict[str, Any]) -> List[Workshop]:
  ics_url = source["ics_url"]
  text = http_get(ics_url)
  cal = Calendar.from_ical(text)
  out: List[Workshop] = []

  for component in cal.walk():
    if component.name != "VEVENT":
      continue

    summary = str(component.get("summary", "Workshop"))
    dtstart = component.get("dtstart")
    dtend = component.get("dtend")
    location = str(component.get("location", "")) if component.get("location") else None
    url = str(component.get("url", "")) if component.get("url") else None
    desc = str(component.get("description", "")) if component.get("description") else None

    if not dtstart:
      continue

    start_dt = dtstart.dt
    if isinstance(start_dt, datetime) is False:
      # date-only events -> skip
      continue
    start_iso = normalize_iso(start_dt)

    end_iso = None
    if dtend and isinstance(dtend.dt, datetime):
      end_iso = normalize_iso(dtend.dt)

    wid = safe_id(source["id"], summary, start_iso)
    out.append(
      Workshop(
        id=wid,
        title=summary,
        host=source.get("name"),
        city=None,
        state=None,
        venue=location,
        startAt=start_iso,
        endAt=end_iso,
        registrationURL=url or ics_url,
        imageURL=None,
        detail=desc if desc else None,
        links=[{"title": "Calendar Source", "url": ics_url}]
      )
    )

  return out

def extract_rss(source: Dict[str, Any]) -> List[Workshop]:
  rss_url = source["rss_url"]
  feed = feedparser.parse(rss_url)
  out: List[Workshop] = []

  for e in feed.entries[:30]:
    title = getattr(e, "title", "Workshop")
    link = getattr(e, "link", None)
    summary = getattr(e, "summary", None)

    # RSS rarely contains structured dates; try common fields
    dt = None
    if hasattr(e, "published"):
      dt = parse_datetime(e.published)
    if not dt and hasattr(e, "updated"):
      dt = parse_datetime(e.updated)

    # If no date, skip (we can’t place it on a calendar)
    if not dt:
      continue

    start_iso = normalize_iso(dt)
    wid = safe_id(source["id"], title, start_iso)

    out.append(
      Workshop(
        id=wid,
        title=title,
        host=source.get("name"),
        city=None,
        state=None,
        venue=None,
        startAt=start_iso,
        endAt=None,
        registrationURL=link or rss_url,
        imageURL=None,
        detail=summary,
        links=[{"title": "Post", "url": link or rss_url}]
      )
    )

  return out

# ---------- Main ----------

def extract_from_source(src: Dict[str, Any]) -> List[Workshop]:
  t = src.get("type")
  if t == "shopify_collection":
    return extract_shopify_collection(src)
  if t == "ics":
    return extract_ics(src)
  if t == "rss":
    return extract_rss(src)
  return []

def main():
  resources = load_json(RESOURCES_JSON_PATH)
  cfg = load_json(SOURCES_CONFIG_PATH)

  sources = cfg.get("sources", [])
  incoming: List[Workshop] = []

  for src in sources:
    try:
      incoming.extend(extract_from_source(src))
    except Exception:
      continue

  existing = resources.get("workshops", [])
  merged = merge_workshops(existing, incoming)
  merged = prune_past(merged, keep_days_past=1)

  resources["workshops"] = merged
  resources["lastUpdated"] = datetime.now(timezone.utc).date().isoformat()

  save_json(RESOURCES_JSON_PATH, resources)
  print(f"Workshops now: {len(merged)} (incoming: {len(incoming)})")

if __name__ == "__main__":
  main()
