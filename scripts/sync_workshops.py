#!/usr/bin/env python3
"""
Simple, bulletproof workshop scraper
NO SILENT FAILURES - Either it works or you see the error
"""
import json
import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from dateutil import parser as dateparser
from dateutil import tz as datetz
from urllib.parse import urljoin
import hashlib

DEFAULT_TZ = "America/Los_Angeles"

def log(msg):
    print(f"  {msg}")

def fetch(url):
    """Fetch HTML - simple and clear"""
    try:
        r = requests.get(url, timeout=30, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        })
        r.raise_for_status()
        return r.text
    except Exception as e:
        log(f"⚠️  Failed to fetch {url}: {e}")
        return None

def parse_date(text, tz=DEFAULT_TZ):
    """Parse any date format"""
    try:
        d = dateparser.parse(text, fuzzy=True)
        if d and d.tzinfo is None:
            d = d.replace(tzinfo=datetz.gettz(tz))
        return d
    except:
        return None

def make_id(source, title, url):
    """Generate stable ID"""
    key = f"{source}|{url or title}"
    hash_val = hashlib.sha1(key.encode()).hexdigest()[:12]
    return f"{source}-{hash_val}"

def extract_time(text):
    """Extract time from text like '7pm' or '7:00pm-9:00pm'"""
    # Try range first: 7pm-9pm, 7:00pm-9:00pm
    m = re.search(r'(\d{1,2})(?::(\d{2}))?\s*(am|pm)\s*[-–to]+\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm)', text, re.I)
    if m:
        sh = int(m.group(1))
        sm = int(m.group(2) or 0)
        if m.group(3).lower() == 'pm' and sh < 12:
            sh += 12
        elif m.group(3).lower() == 'am' and sh == 12:
            sh = 0
        
        eh = int(m.group(4))
        em = int(m.group(5) or 0)
        if m.group(6).lower() == 'pm' and eh < 12:
            eh += 12
        elif m.group(6).lower() == 'am' and eh == 12:
            eh = 0
        
        # Validate hours
        if 0 <= sh <= 23 and 0 <= eh <= 23:
            return sh, sm, eh, em
    
    # Try single time: 7pm, 7:00pm
    m = re.search(r'(\d{1,2})(?::(\d{2}))?\s*(am|pm)', text, re.I)
    if m:
        sh = int(m.group(1))
        sm = int(m.group(2) or 0)
        if m.group(3).lower() == 'pm' and sh < 12:
            sh += 12
        elif m.group(3).lower() == 'am' and sh == 12:
            sh = 0
        
        # Validate hour
        if 0 <= sh <= 23:
            return sh, sm, sh + 2, sm  # Default 2hr duration
    
    return None

# ============================================================================
# VOICE ACTORS NETWORK - AGGRESSIVE SCRAPER
# ============================================================================
def scrape_van():
    """Voice Actors Network - scrape ALL products and check each one"""
    print("[VAN] Scraping Voice Actors Network...")
    events = []
    
    html = fetch("https://voiceactorsnetwork.com/collections/all")
    if not html:
        return events
    
    soup = BeautifulSoup(html, "html.parser")
    
    # Find ALL product links
    links = set()
    for a in soup.find_all("a", href=True):
        if "/products/" in a["href"]:
            url = urljoin("https://voiceactorsnetwork.com", a["href"])
            links.add(url)
    
    log(f"Found {len(links)} product pages")
    
    # Check each product page
    for url in list(links)[:30]:  # Limit to 30 to avoid timeout
        try:
            html = fetch(url)
            if not html:
                continue
            
            soup = BeautifulSoup(html, "html.parser")
            text = soup.get_text()
            
            # Skip non-workshops
            if any(x in text.lower() for x in ["gift card", "donation", "membership", "t-shirt", "merch"]):
                continue
            
            # Get title
            title_tag = soup.find("h1") or soup.find("title")
            title = title_tag.get_text(strip=True) if title_tag else "Workshop"
            title = title.replace(" – Voice Actors Network", "").strip()
            
            # Find date - look for patterns like "February 15, 2026"
            date_match = re.search(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4}', text)
            if not date_match:
                continue
            
            date = parse_date(date_match.group(0))
            if not date:
                continue
            
            # Extract time
            time_info = extract_time(text)
            if time_info:
                sh, sm, eh, em = time_info
                start = date.replace(hour=sh, minute=sm, second=0, microsecond=0)
                end = date.replace(hour=eh, minute=em, second=0, microsecond=0)
            else:
                start = date.replace(hour=18, minute=0, second=0, microsecond=0)
                end = start + timedelta(hours=2)
            
            events.append({
                "id": make_id("van", title, url),
                "title": title,
                "provider": "van",
                "host": "Voice Actors Network",
                "startAt": start.isoformat(),
                "endAt": end.isoformat(),
                "registrationURL": url,
                "venue": "Zoom" if "zoom" in text.lower() else "See listing"
            })
            log(f"✓ {title[:50]}")
            
        except Exception as e:
            log(f"Error on {url}: {e}")
            continue
    
    print(f"[VAN] Found {len(events)} events")
    return events

# ============================================================================
# THE VO PROS - KEEP EXISTING (IT WORKS)
# ============================================================================
def scrape_vopros():
    """The VO Pros - works well, keep it"""
    print("[VO PROS] Scraping The VO Pros...")
    events = []
    
    html = fetch("https://www.thevopros.com/shop/")
    if not html:
        return events
    
    soup = BeautifulSoup(html, "html.parser")
    
    # Find event links
    links = []
    for a in soup.find_all("a", href=True):
        if "/events/" in a["href"] and not a["href"].endswith("/events/"):
            url = urljoin("https://www.thevopros.com", a["href"])
            if url not in links:
                links.append(url)
    
    log(f"Found {len(links)} event pages")
    
    for url in links[:25]:
        try:
            html = fetch(url)
            if not html:
                continue
            
            soup = BeautifulSoup(html, "html.parser")
            text = soup.get_text()
            
            # Get title - try multiple methods
            title = None
            h1 = soup.find("h1")
            if h1:
                title = h1.get_text(strip=True)
            
            # If title is generic, parse from URL
            if not title or title.upper() in ["THE VO PROS", "EVENTS"]:
                # /events/agent-night-john-smith → "Agent Night John Smith"
                slug = url.split("/events/")[-1].split("?")[0].strip("/")
                title = slug.replace("-", " ").title()
            
            # Find date
            date_match = re.search(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4}', text)
            if not date_match:
                continue
            
            date = parse_date(date_match.group(0))
            if not date:
                continue
            
            # Extract time
            time_info = extract_time(text)
            if time_info:
                sh, sm, eh, em = time_info
                start = date.replace(hour=sh, minute=sm, second=0, microsecond=0)
                end = date.replace(hour=eh, minute=em, second=0, microsecond=0)
            else:
                start = date.replace(hour=18, minute=0, second=0, microsecond=0)
                end = start + timedelta(hours=2)
            
            events.append({
                "id": make_id("vopros", title, url),
                "title": title,
                "provider": "thevopros",
                "host": "The VO Pros",
                "startAt": start.isoformat(),
                "endAt": end.isoformat(),
                "registrationURL": url
            })
            log(f"✓ {title[:50]}")
            
        except Exception as e:
            log(f"Error on {url}: {e}")
            continue
    
    print(f"[VO PROS] Found {len(events)} events")
    return events

# ============================================================================
# REAL VOICE LA - KEEP EXISTING (IT WORKS)
# ============================================================================
def scrape_realvoice():
    """Real Voice LA - works well"""
    print("[REAL VOICE LA] Scraping...")
    events = []
    
    html = fetch("https://www.realvoicela.com/classes")
    if not html:
        return events
    
    soup = BeautifulSoup(html, "html.parser")
    
    # Find service links
    links = []
    for a in soup.find_all("a", href=True):
        if "service-page" in a["href"] or "book-online" in a["href"]:
            url = urljoin("https://www.realvoicela.com", a["href"])
            if url not in links:
                links.append(url)
    
    log(f"Found {len(links)} class pages")
    
    for url in links[:30]:
        try:
            html = fetch(url)
            if not html:
                continue
            
            soup = BeautifulSoup(html, "html.parser")
            text = soup.get_text()
            
            # Get title
            title_tag = soup.find("h1") or soup.find("h2")
            title = title_tag.get_text(strip=True) if title_tag else "Class"
            
            # Skip ongoing courses
            if any(x in title.lower() for x in ["pack", "series", "monthly", "curriculum"]):
                log(f"Skipping ongoing course: {title[:40]}")
                continue
            
            # Find date - try multiple patterns
            date = None
            # Try full format first
            date_match = re.search(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4}', text)
            if date_match:
                date = parse_date(date_match.group(0))
            
            # Try short format if full didn't work
            if not date:
                date_match = re.search(r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2}(?:st|nd|rd|th)?', text, re.I)
                if date_match:
                    now = datetime.now(datetz.gettz(DEFAULT_TZ))
                    date_text = date_match.group(0) + f", {now.year}"
                    date = parse_date(date_text)
                    # If in past, try next year
                    if date and date < now - timedelta(days=30):
                        date_text = date_match.group(0) + f", {now.year + 1}"
                        date = parse_date(date_text)
            
            if not date:
                log(f"No date found for: {title[:40]}")
                continue
            
            # Extract time
            time_info = extract_time(text)
            if time_info:
                sh, sm, eh, em = time_info
                start = date.replace(hour=sh, minute=sm, second=0, microsecond=0)
                end = date.replace(hour=eh, minute=em, second=0, microsecond=0)
            else:
                start = date.replace(hour=11, minute=0, second=0, microsecond=0)
                end = start + timedelta(hours=3)
            
            events.append({
                "id": make_id("realvoice", title, url),
                "title": title,
                "provider": "realvoicela",
                "host": "Real Voice LA",
                "startAt": start.isoformat(),
                "endAt": end.isoformat(),
                "registrationURL": url
            })
            log(f"✓ {title[:50]}")
            
        except Exception as e:
            log(f"Error on {url}: {e}")
            continue
    
    print(f"[REAL VOICE LA] Found {len(events)} events")
    return events

# ============================================================================
# VOICE TRAX WEST - NEW AGGRESSIVE SCRAPER
# ============================================================================
def scrape_voicetrax():
    """Voice Trax West - scrape guest instructors"""
    print("[VOICE TRAX WEST] Scraping...")
    events = []
    
    html = fetch("https://www.voicetraxwest.com/guest-instructors")
    if not html:
        return events
    
    soup = BeautifulSoup(html, "html.parser")
    
    # Find instructor links
    links = []
    for a in soup.find_all("a", href=True):
        if "guest-instructor-" in a["href"]:
            url = urljoin("https://www.voicetraxwest.com", a["href"])
            if url not in links:
                links.append(url)
    
    log(f"Found {len(links)} instructor pages")
    
    for url in links[:20]:
        try:
            html = fetch(url)
            if not html:
                continue
            
            soup = BeautifulSoup(html, "html.parser")
            text = soup.get_text()
            
            # Get title - parse from URL if needed
            title_tag = soup.find("h1")
            title = title_tag.get_text(strip=True) if title_tag else None
            
            if not title or title in ["Instructors", "Guest Instructors"]:
                # Parse from URL: /guest-instructor-jane-doe → "Guest Instructor: Jane Doe"
                slug = url.split("guest-instructor-")[-1].split("?")[0].strip("/")
                name = slug.replace("-", " ").title()
                title = f"Guest Instructor: {name}"
            
            # Find date
            date_match = re.search(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4}', text)
            if not date_match:
                log(f"No date found for {title}")
                continue
            
            date = parse_date(date_match.group(0))
            if not date:
                continue
            
            # Extract time
            time_info = extract_time(text)
            if time_info:
                sh, sm, eh, em = time_info
                start = date.replace(hour=sh, minute=sm, second=0, microsecond=0)
                end = date.replace(hour=eh, minute=em, second=0, microsecond=0)
            else:
                start = date.replace(hour=10, minute=0, second=0, microsecond=0)
                end = start + timedelta(hours=8)  # Full day
            
            events.append({
                "id": make_id("vtw", title, url),
                "title": title,
                "provider": "voicetraxwest",
                "host": "Voice Trax West",
                "startAt": start.isoformat(),
                "endAt": end.isoformat(),
                "registrationURL": url
            })
            log(f"✓ {title[:50]}")
            
        except Exception as e:
            log(f"Error on {url}: {e}")
            continue
    
    print(f"[VOICE TRAX WEST] Found {len(events)} events")
    return events

# ============================================================================
# RED SCYTHE STUDIO - TidyCal scraper
# ============================================================================
def scrape_redscythe():
    """Red Scythe Studio - TidyCal classes"""
    print("[RED SCYTHE] Scraping...")
    events = []
    
    html = fetch("https://tidycal.com/redscythestudio/")
    if not html:
        return events
    
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text()
    
    # TidyCal format: "1.31 | INSTRUCTOR NAME | Topic"
    pattern = r'(\d{1,2}\.\d{1,2})\s*\|\s*([^|]{5,60}?)\s*\|\s*([^\n|]{5,100})'
    
    for match in re.finditer(pattern, text):
        try:
            date_str = match.group(1)  # "1.31"
            instructor = match.group(2).strip()
            topic = match.group(3).strip()
            
            # Skip coaching/consultations
            combined = f"{instructor} {topic}".lower()
            if any(x in combined for x in ["coaching", "coach", "consult", "1:1"]):
                continue
            
            # Parse date (M.D format)
            month, day = map(int, date_str.split('.'))
            now = datetime.now(datetz.gettz(DEFAULT_TZ))
            year = now.year
            date = datetime(year, month, day, tzinfo=datetz.gettz(DEFAULT_TZ))
            
            # If date is in past, assume next year
            if date < now - timedelta(days=30):
                date = datetime(year + 1, month, day, tzinfo=datetz.gettz(DEFAULT_TZ))
            
            # Default time 10am, 3hr duration
            start = date.replace(hour=10, minute=0, second=0)
            end = start + timedelta(hours=3)
            
            title = f"{instructor} — {topic}"
            
            events.append({
                "id": make_id("redscythe", title, f"tidycal-{date_str}"),
                "title": title,
                "provider": "redscythe_tidycal_classes",
                "host": "Red Scythe Studio",
                "startAt": start.isoformat(),
                "endAt": end.isoformat(),
                "registrationURL": "https://tidycal.com/redscythestudio/"
            })
            log(f"✓ {title[:50]}")
            
        except Exception as e:
            log(f"Error parsing: {e}")
            continue
    
    print(f"[RED SCYTHE] Found {len(events)} events")
    return events

# ============================================================================
# SOUND ON STUDIO
# ============================================================================
def scrape_soundon():
    """Sound On Studio"""
    print("[SOUND ON STUDIO] Scraping...")
    events = []
    
    html = fetch("https://www.soundonstudio.com/classsignup")
    if not html:
        return events
    
    soup = BeautifulSoup(html, "html.parser")
    
    # Find class links
    links = []
    for a in soup.find_all("a", href=True):
        if "class" in a["href"].lower() or "event" in a["href"].lower():
            url = urljoin("https://www.soundonstudio.com", a["href"])
            if url not in links:
                links.append(url)
    
    # Also check the main page for dates
    text = soup.get_text()
    
    # Look for dates on main page
    for match in re.finditer(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4}', text):
        try:
            date = parse_date(match.group(0))
            if not date:
                continue
            
            # Try to find title nearby
            pos = match.start()
            nearby = text[max(0, pos-200):min(len(text), pos+200)]
            
            # Look for class name
            title_match = re.search(r'([A-Z][^.!?\n]{10,80})', nearby)
            title = title_match.group(1).strip() if title_match else "Workshop"
            
            # Extract time
            time_info = extract_time(nearby)
            if time_info:
                sh, sm, eh, em = time_info
                start = date.replace(hour=sh, minute=sm, second=0, microsecond=0)
                end = date.replace(hour=eh, minute=em, second=0, microsecond=0)
            else:
                start = date.replace(hour=18, minute=0, second=0, microsecond=0)
                end = start + timedelta(hours=2)
            
            events.append({
                "id": make_id("soundon", title, str(date)),
                "title": title,
                "provider": "soundonstudio",
                "host": "Sound On Studio",
                "startAt": start.isoformat(),
                "endAt": end.isoformat(),
                "registrationURL": "https://www.soundonstudio.com/classsignup"
            })
            log(f"✓ {title[:50]}")
            
        except Exception as e:
            log(f"Error: {e}")
            continue
    
    print(f"[SOUND ON STUDIO] Found {len(events)} events")
    return events

# ============================================================================
# HALP ACADEMY
# ============================================================================
def scrape_halp():
    """HALP Academy"""
    print("[HALP ACADEMY] Scraping...")
    events = []
    
    html = fetch("https://halpacademy.com/events/search/")
    if not html:
        return events
    
    soup = BeautifulSoup(html, "html.parser")
    
    # Find event links
    links = []
    for a in soup.find_all("a", href=True):
        if "/events/" in a["href"]:
            url = urljoin("https://halpacademy.com", a["href"])
            if url not in links and url != "https://halpacademy.com/events/search/":
                links.append(url)
    
    log(f"Found {len(links)} event links")
    
    for url in links[:20]:
        try:
            html = fetch(url)
            if not html:
                continue
            
            soup = BeautifulSoup(html, "html.parser")
            text = soup.get_text()
            
            # Get title
            title_tag = soup.find("h1")
            title = title_tag.get_text(strip=True) if title_tag else "Event"
            
            # Find date
            date_match = re.search(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4}', text)
            if not date_match:
                continue
            
            date = parse_date(date_match.group(0))
            if not date:
                continue
            
            # Extract time
            time_info = extract_time(text)
            if time_info:
                sh, sm, eh, em = time_info
                start = date.replace(hour=sh, minute=sm, second=0, microsecond=0)
                end = date.replace(hour=eh, minute=em, second=0, microsecond=0)
            else:
                start = date.replace(hour=14, minute=0, second=0, microsecond=0)
                end = start + timedelta(hours=2)
            
            events.append({
                "id": make_id("halp", title, url),
                "title": title,
                "provider": "halp_academy",
                "host": "HALP Academy",
                "startAt": start.isoformat(),
                "endAt": end.isoformat(),
                "registrationURL": url
            })
            log(f"✓ {title[:50]}")
            
        except Exception as e:
            log(f"Error on {url}: {e}")
            continue
    
    print(f"[HALP ACADEMY] Found {len(events)} events")
    return events

# ============================================================================
# VO DOJO
# ============================================================================
def scrape_vodojo():
    """The VO Dojo"""
    print("[VO DOJO] Scraping...")
    events = []
    
    html = fetch("https://www.thevodojo.com/upcoming-events-nav")
    if not html:
        return events
    
    soup = BeautifulSoup(html, "html.parser")
    
    # Find event links
    links = []
    for a in soup.find_all("a", href=True):
        if "/new-events/" in a["href"]:
            url = urljoin("https://www.thevodojo.com", a["href"])
            if url not in links:
                links.append(url)
    
    log(f"Found {len(links)} event links")
    
    for url in links[:30]:
        try:
            html = fetch(url)
            if not html:
                continue
            
            soup = BeautifulSoup(html, "html.parser")
            text = soup.get_text()
            
            # Get title - try multiple methods
            title = None
            
            # Try h1
            h1 = soup.find("h1")
            if h1:
                title = h1.get_text(strip=True)
            
            # If generic, parse from URL
            if not title or title.upper() in ["THE VO DOJO", "UPCOMING EVENTS"] or len(title) < 10:
                slug = url.split("/new-events/")[-1].split("?")[0].strip("/")
                title = slug.replace("-", " ").title()
            
            # Find date
            date_match = re.search(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4}', text)
            if not date_match:
                continue
            
            date = parse_date(date_match.group(0))
            if not date:
                continue
            
            # Extract time
            time_info = extract_time(text)
            if time_info:
                sh, sm, eh, em = time_info
                start = date.replace(hour=sh, minute=sm, second=0, microsecond=0)
                end = date.replace(hour=eh, minute=em, second=0, microsecond=0)
            else:
                start = date.replace(hour=12, minute=0, second=0, microsecond=0)
                end = start + timedelta(hours=2)
            
            events.append({
                "id": make_id("vodojo", title, url),
                "title": title,
                "provider": "thevodojo",
                "host": "The VO Dojo",
                "startAt": start.isoformat(),
                "endAt": end.isoformat(),
                "registrationURL": url
            })
            log(f"✓ {title[:50]}")
            
        except Exception as e:
            log(f"Error on {url}: {e}")
            continue
    
    print(f"[VO DOJO] Found {len(events)} events")
    return events

# ============================================================================
# ADVENTURES IN VOICE ACTING
# ============================================================================
def scrape_aiva():
    """Adventures in Voice Acting"""
    print("[AIVA] Scraping...")
    events = []
    
    html = fetch("https://www.adventuresinvoiceacting.com/")
    if not html:
        return events
    
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text()
    
    # Look for any dates on the page
    for match in re.finditer(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4}', text):
        try:
            date = parse_date(match.group(0))
            if not date:
                continue
            
            # Try to find title nearby
            pos = match.start()
            nearby = text[max(0, pos-200):min(len(text), pos+200)]
            
            # Look for class/workshop name
            title_match = re.search(r'([A-Z][^.!?\n]{15,80})', nearby)
            title = title_match.group(1).strip() if title_match else "Workshop"
            
            # Skip if looks like navigation/footer text
            if any(x in title.lower() for x in ["copyright", "reserved", "privacy", "terms"]):
                continue
            
            # Extract time
            time_info = extract_time(nearby)
            if time_info:
                sh, sm, eh, em = time_info
                start = date.replace(hour=sh, minute=sm, second=0, microsecond=0)
                end = date.replace(hour=eh, minute=em, second=0, microsecond=0)
            else:
                start = date.replace(hour=19, minute=0, second=0, microsecond=0)
                end = start + timedelta(hours=2)
            
            events.append({
                "id": make_id("aiva", title, str(date)),
                "title": title,
                "provider": "adventuresinvoiceacting",
                "host": "Adventures in Voice Acting",
                "startAt": start.isoformat(),
                "endAt": end.isoformat(),
                "registrationURL": "https://www.adventuresinvoiceacting.com/"
            })
            log(f"✓ {title[:50]}")
            
        except Exception as e:
            log(f"Error: {e}")
            continue
    
    print(f"[AIVA] Found {len(events)} events")
    return events

# ============================================================================
# MAIN SCRAPER
# ============================================================================
def main():
    print("\n" + "="*70)
    print("SIMPLE WORKSHOP SCRAPER - ALL 9 SOURCES")
    print("="*70 + "\n")
    
    all_events = []
    
    # Scrape ALL sources
    all_events.extend(scrape_van())
    all_events.extend(scrape_vopros())
    all_events.extend(scrape_realvoice())
    all_events.extend(scrape_voicetrax())
    all_events.extend(scrape_redscythe())
    all_events.extend(scrape_soundon())
    all_events.extend(scrape_halp())
    all_events.extend(scrape_vodojo())
    all_events.extend(scrape_aiva())
    
    # Simple deduplication by URL
    seen_urls = {}
    deduped = []
    for event in all_events:
        url = event.get("registrationURL")
        if url and url in seen_urls:
            continue
        if url:
            seen_urls[url] = event
        deduped.append(event)
    
    # Filter out past events
    now = datetime.now(datetz.gettz(DEFAULT_TZ))
    future = []
    for event in deduped:
        start = datetime.fromisoformat(event["startAt"])
        if start > (now - timedelta(days=7)):  # Keep events from last 7 days
            future.append(event)
    
    print(f"\n" + "="*70)
    print(f"TOTAL: {len(future)} workshops")
    print("="*70)
    
    # Load existing resources.json
    try:
        with open("resources.json", "r") as f:
            data = json.load(f)
    except:
        data = {"version": 2, "workshops": [], "announcements": [], "sponsors": [], "sections": []}
    
    # Update workshops
    data["workshops"] = future
    data["lastUpdated"] = datetime.now().strftime("%Y-%m-%d")
    
    # Save
    with open("resources.json", "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.write("\n")
    
    print(f"\n✅ Saved to resources.json")
    
    # Show breakdown
    print("\nBy source:")
    by_source = {}
    for e in future:
        src = e.get("provider", "unknown")
        by_source[src] = by_source.get(src, 0) + 1
    
    for src, count in sorted(by_source.items()):
        print(f"  {src}: {count} events")

if __name__ == "__main__":
    main()
