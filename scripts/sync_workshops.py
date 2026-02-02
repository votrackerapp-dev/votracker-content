#!/usr/bin/env python3
"""
Improved workshop scraper with better debugging and extraction
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

def fetch(url, headers=None):
    """Fetch HTML with better error handling"""
    try:
        default_headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        if headers:
            default_headers.update(headers)
        
        r = requests.get(url, timeout=30, headers=default_headers)
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
    """Extract time from text - IMPROVED VERSION"""
    # Try range first: 7pm-9pm, 7:00pm-9:00pm, 7-9pm
    patterns = [
        # Full range: 7:00pm-9:00pm
        r'(\d{1,2})(?::(\d{2}))?\s*(am|pm)\s*[-–to]+\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm)',
        # Compact range: 7-9pm
        r'(\d{1,2})\s*[-–]\s*(\d{1,2})\s*(am|pm)',
    ]
    
    for pattern in patterns:
        m = re.search(pattern, text, re.I)
        if m:
            groups = m.groups()
            
            # Handle compact format (7-9pm)
            if len(groups) == 3:
                sh = int(groups[0])
                sm = 0
                eh = int(groups[1])
                em = 0
                period = groups[2].lower()
                
                # Both times get same AM/PM
                if period == 'pm' and sh < 12:
                    sh += 12
                if period == 'pm' and eh < 12:
                    eh += 12
                elif period == 'am' and sh == 12:
                    sh = 0
                elif period == 'am' and eh == 12:
                    eh = 0
            else:
                # Full format
                sh = int(groups[0])
                sm = int(groups[1] or 0)
                if groups[2].lower() == 'pm' and sh < 12:
                    sh += 12
                elif groups[2].lower() == 'am' and sh == 12:
                    sh = 0
                
                eh = int(groups[3])
                em = int(groups[4] or 0)
                if groups[5].lower() == 'pm' and eh < 12:
                    eh += 12
                elif groups[5].lower() == 'am' and eh == 12:
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
# VOICE ACTORS NETWORK - FIXED TIME PARSING
# ============================================================================
def scrape_van():
    """Voice Actors Network - with better error handling"""
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
    for url in list(links)[:30]:
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
            
            # Find date
            date_match = re.search(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4}', text)
            if not date_match:
                continue
            
            date = parse_date(date_match.group(0))
            if not date:
                continue
            
            # Extract time - with better error handling
            time_info = extract_time(text)
            if time_info:
                sh, sm, eh, em = time_info
                try:
                    start = date.replace(hour=sh, minute=sm, second=0, microsecond=0)
                    end = date.replace(hour=eh, minute=em, second=0, microsecond=0)
                except ValueError as e:
                    log(f"⚠️  Invalid time on {title[:40]}: {sh}:{sm}-{eh}:{em}")
                    # Use default times
                    start = date.replace(hour=18, minute=0, second=0, microsecond=0)
                    end = start + timedelta(hours=2)
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
            log(f"⚠️  Error on {url.split('/')[-1]}: {e}")
            continue
    
    print(f"[VAN] Found {len(events)} events")
    return events

# ============================================================================
# VOICE TRAX WEST - IMPROVED
# ============================================================================
def scrape_voicetrax():
    """Voice Trax West - look harder for dates"""
    print("[VOICE TRAX WEST] Scraping...")
    events = []
    
    html = fetch("https://www.voicetraxwest.com/guest-instructors")
    if not html:
        return events
    
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text()
    
    # First, try to find dates on the main page
    log("Checking main page for upcoming classes...")
    date_matches = re.finditer(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4}', text)
    
    for match in date_matches:
        try:
            date = parse_date(match.group(0))
            if not date:
                continue
            
            # Get context around date
            pos = match.start()
            nearby = text[max(0, pos-300):min(len(text), pos+200)]
            
            # Look for instructor name or class title
            title_patterns = [
                r'Guest Instructor[:\s]+([A-Z][a-z]+ [A-Z][a-z]+)',
                r'with\s+([A-Z][a-z]+ [A-Z][a-z]+)',
                r'([A-Z][A-Z\s&]{10,60})'
            ]
            
            title = None
            for pattern in title_patterns:
                title_match = re.search(pattern, nearby)
                if title_match:
                    title = title_match.group(1).strip()
                    break
            
            if not title or len(title) < 5:
                continue
            
            # Extract time
            time_info = extract_time(nearby)
            if time_info:
                sh, sm, eh, em = time_info
                start = date.replace(hour=sh, minute=sm, second=0, microsecond=0)
                end = date.replace(hour=eh, minute=em, second=0, microsecond=0)
            else:
                start = date.replace(hour=10, minute=0, second=0, microsecond=0)
                end = start + timedelta(hours=8)
            
            events.append({
                "id": make_id("vtw", title, str(date)),
                "title": f"Guest Instructor: {title}",
                "provider": "voicetraxwest",
                "host": "Voice Trax West",
                "startAt": start.isoformat(),
                "endAt": end.isoformat(),
                "registrationURL": "https://www.voicetraxwest.com/guest-instructors"
            })
            log(f"✓ Guest Instructor: {title[:40]}")
            
        except Exception as e:
            log(f"⚠️  Error: {e}")
            continue
    
    print(f"[VOICE TRAX WEST] Found {len(events)} events")
    return events

# ============================================================================
# SOUND ON STUDIO - IMPROVED
# ============================================================================
def scrape_soundon():
    """Sound On Studio - try harder to find classes"""
    print("[SOUND ON STUDIO] Scraping...")
    events = []
    
    html = fetch("https://www.soundonstudio.com/classsignup")
    if not html:
        return events
    
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text()
    
    log("Looking for class dates...")
    
    # Look for any dates on the page
    date_matches = list(re.finditer(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4}', text))
    
    if date_matches:
        log(f"Found {len(date_matches)} potential dates")
    
    for match in date_matches:
        try:
            date = parse_date(match.group(0))
            if not date:
                continue
            
            # Get context
            pos = match.start()
            nearby = text[max(0, pos-200):min(len(text), pos+200)]
            
            # Look for class title
            title_match = re.search(r'([A-Z][^.!?\n]{15,80})', nearby)
            title = title_match.group(1).strip() if title_match else "Workshop"
            
            # Skip navigation text
            if any(x in title.lower() for x in ["copyright", "reserved", "privacy", "home", "about"]):
                continue
            
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
            log(f"⚠️  Error: {e}")
            continue
    
    # Also try to find embedded calendar/booking widget
    log("Checking for booking widgets...")
    scripts = soup.find_all("script")
    for script in scripts:
        if script.string and ("event" in script.string.lower() or "class" in script.string.lower()):
            # Look for JSON data
            try:
                json_match = re.search(r'\{[^{}]*"date"[^{}]*\}', script.string)
                if json_match:
                    log(f"Found potential event data in script")
            except:
                pass
    
    print(f"[SOUND ON STUDIO] Found {len(events)} events")
    return events

# ============================================================================
# HALP ACADEMY - FIXED LINK DETECTION
# ============================================================================
def scrape_halp():
    """HALP Academy - better link detection"""
    print("[HALP ACADEMY] Scraping...")
    events = []
    
    html = fetch("https://halpacademy.com/events/search/")
    if not html:
        return events
    
    soup = BeautifulSoup(html, "html.parser")
    
    # Find event links - be more specific
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        # Look for actual event slugs, not just /events/
        if "/events/" in href and href != "/events/" and href != "/events/search/":
            url = urljoin("https://halpacademy.com", href)
            if url not in links:
                links.append(url)
    
    log(f"Found {len(links)} event links")
    
    # If no links found, try scraping the main page
    if len(links) == 0:
        log("No event links found, checking main events page...")
        text = soup.get_text()
        
        # Look for dates on the search page itself
        for match in re.finditer(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4}', text):
            try:
                date = parse_date(match.group(0))
                if not date:
                    continue
                
                pos = match.start()
                nearby = text[max(0, pos-200):min(len(text), pos+200)]
                
                title_match = re.search(r'([A-Z][^.!?\n]{15,80})', nearby)
                title = title_match.group(1).strip() if title_match else "Event"
                
                time_info = extract_time(nearby)
                if time_info:
                    sh, sm, eh, em = time_info
                    start = date.replace(hour=sh, minute=sm, second=0, microsecond=0)
                    end = date.replace(hour=eh, minute=em, second=0, microsecond=0)
                else:
                    start = date.replace(hour=14, minute=0, second=0, microsecond=0)
                    end = start + timedelta(hours=2)
                
                events.append({
                    "id": make_id("halp", title, str(date)),
                    "title": title,
                    "provider": "halp_academy",
                    "host": "HALP Academy",
                    "startAt": start.isoformat(),
                    "endAt": end.isoformat(),
                    "registrationURL": "https://halpacademy.com/events/search/"
                })
                log(f"✓ {title[:50]}")
            except Exception as e:
                continue
    
    # Process individual event pages
    for url in links[:20]:
        try:
            html = fetch(url)
            if not html:
                continue
            
            soup = BeautifulSoup(html, "html.parser")
            text = soup.get_text()
            
            title_tag = soup.find("h1")
            title = title_tag.get_text(strip=True) if title_tag else "Event"
            
            date_match = re.search(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4}', text)
            if not date_match:
                continue
            
            date = parse_date(date_match.group(0))
            if not date:
                continue
            
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
            log(f"⚠️  Error on {url}: {e}")
            continue
    
    print(f"[HALP ACADEMY] Found {len(events)} events")
    return events

# ============================================================================
# AIVA - IMPROVED
# ============================================================================
def scrape_aiva():
    """Adventures in Voice Acting - better detection"""
    print("[AIVA] Scraping...")
    events = []
    
    html = fetch("https://www.adventuresinvoiceacting.com/")
    if not html:
        return events
    
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text()
    
    log("Searching for upcoming events...")
    
    # Look for dates
    date_matches = list(re.finditer(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4}', text))
    
    if date_matches:
        log(f"Found {len(date_matches)} potential dates")
    
    for match in date_matches:
        try:
            date = parse_date(match.group(0))
            if not date:
                continue
            
            # Check if it's in the future
            now = datetime.now(datetz.gettz(DEFAULT_TZ))
            if date < now - timedelta(days=30):
                continue
            
            pos = match.start()
            nearby = text[max(0, pos-250):min(len(text), pos+150)]
            
            # Look for workshop/class title
            title_patterns = [
                r'(?:Workshop|Class|Session)[:\s]+([A-Z][^.!?\n]{15,60})',
                r'([A-Z][A-Z\s]{15,60})',
                r'with\s+([A-Z][a-z]+ [A-Z][a-z]+)'
            ]
            
            title = None
            for pattern in title_patterns:
                title_match = re.search(pattern, nearby)
                if title_match:
                    title = title_match.group(1).strip()
                    # Skip if it looks like navigation
                    if any(x in title.lower() for x in ["copyright", "reserved", "about", "contact", "home"]):
                        title = None
                        continue
                    break
            
            if not title or len(title) < 10:
                continue
            
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
            log(f"⚠️  Error: {e}")
            continue
    
    print(f"[AIVA] Found {len(events)} events")
    return events

# ============================================================================
# THE VO PROS (KEEP - IT WORKS)
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
            log(f"⚠️  Error on {url}: {e}")
            continue
    
    print(f"[VO PROS] Found {len(events)} events")
    return events

# ============================================================================
# REAL VOICE LA (KEEP - IT WORKS)
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
            
            # Find date
            date = None
            date_match = re.search(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4}', text)
            if date_match:
                date = parse_date(date_match.group(0))
            
            if not date:
                date_match = re.search(r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2}(?:st|nd|rd|th)?', text, re.I)
                if date_match:
                    now = datetime.now(datetz.gettz(DEFAULT_TZ))
                    date_text = date_match.group(0) + f", {now.year}"
                    date = parse_date(date_text)
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
            log(f"⚠️  Error on {url}: {e}")
            continue
    
    print(f"[REAL VOICE LA] Found {len(events)} events")
    return events

# ============================================================================
# RED SCYTHE (KEEP - IT WORKS)
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
            log(f"⚠️  Error parsing: {e}")
            continue
    
    print(f"[RED SCYTHE] Found {len(events)} events")
    return events

# ============================================================================
# VO DOJO (KEEP - IT WORKS)
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
            
            # Get title
            title = None
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
            log(f"⚠️  Error on {url}: {e}")
            continue
    
    print(f"[VO DOJO] Found {len(events)} events")
    return events

# ============================================================================
# MAIN
# ============================================================================
def main():
    print("\n" + "="*70)
    print("IMPROVED WORKSHOP SCRAPER")
    print("="*70 + "\n")
    
    all_events = []
    
    # Test scrapers individually
    print("\n--- Testing improved scrapers ---\n")
    
    all_events.extend(scrape_van())
    all_events.extend(scrape_voicetrax())
    all_events.extend(scrape_soundon())
    all_events.extend(scrape_halp())
    all_events.extend(scrape_aiva())
    
    # Use working ones from original
    print("\n--- Using working scrapers from original ---\n")
    all_events.extend(scrape_vopros())
    all_events.extend(scrape_realvoice())
    all_events.extend(scrape_redscythe())
    all_events.extend(scrape_vodojo())
    
    # Dedup and filter
    seen_urls = {}
    deduped = []
    for event in all_events:
        url = event.get("registrationURL")
        if url and url in seen_urls:
            continue
        if url:
            seen_urls[url] = event
        deduped.append(event)
    
    now = datetime.now(datetz.gettz(DEFAULT_TZ))
    future = []
    for event in deduped:
        start = datetime.fromisoformat(event["startAt"])
        if start > (now - timedelta(days=7)):
            future.append(event)
    
    print(f"\n" + "="*70)
    print(f"TOTAL: {len(future)} workshops")
    print("="*70)
    
    # Save
    try:
        with open("resources.json", "r") as f:
            data = json.load(f)
    except:
        data = {"version": 2, "workshops": [], "announcements": [], "sponsors": [], "sections": []}
    
    data["workshops"] = future
    data["lastUpdated"] = datetime.now().strftime("%Y-%m-%d")
    
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
