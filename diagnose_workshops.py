#!/usr/bin/env python3
"""
diagnose_workshops.py - Analyze workshop data quality

Usage:
    python diagnose_workshops.py resources.json

This script analyzes your scraped workshop data and identifies issues:
- Missing/generic titles
- Default/placeholder times
- Ongoing courses that slipped through
- Missing required fields
- Suspicious patterns
"""

import json
import sys
import re
from datetime import datetime
from collections import defaultdict, Counter

def analyze_workshops(filepath):
    with open(filepath, 'r') as f:
        data = json.load(f)
    
    workshops = data.get('workshops', [])
    
    print(f"\n{'='*70}")
    print(f"  WORKSHOP DATA QUALITY REPORT")
    print(f"{'='*70}\n")
    
    print(f"📊 Total workshops: {len(workshops)}\n")
    
    # ============================================================================
    # 1. SOURCE BREAKDOWN
    # ============================================================================
    print(f"{'─'*70}")
    print("1. EVENTS PER SOURCE")
    print(f"{'─'*70}")
    
    by_provider = defaultdict(list)
    for w in workshops:
        # Extract source from ID (format: sourceid-hash)
        source_id = w.get('id', '').split('-')[0] if '-' in w.get('id', '') else 'unknown'
        provider = w.get('provider', source_id)
        by_provider[provider].append(w)
    
    for provider, events in sorted(by_provider.items(), key=lambda x: len(x[1]), reverse=True):
        status = "✅" if len(events) > 0 else "❌"
        print(f"{status} {provider:30s} {len(events):3d} events")
    
    # ============================================================================
    # 2. TITLE ISSUES
    # ============================================================================
    print(f"\n{'─'*70}")
    print("2. TITLE QUALITY")
    print(f"{'─'*70}")
    
    generic_titles = []
    short_titles = []
    
    for w in workshops:
        title = w.get('title', '').strip()
        
        # Check for generic/site name titles
        generic_patterns = [
            r'^the vo pros$',
            r'^voice actors network$',
            r'^workshop$',
            r'^class$',
            r'^event$',
        ]
        
        if any(re.match(pattern, title, re.I) for pattern in generic_patterns):
            generic_titles.append({
                'title': title,
                'provider': w.get('provider', 'Unknown'),
                'date': w.get('startAt', '')[:10]
            })
        
        # Check for too-short titles
        if len(title) < 5:
            short_titles.append({
                'title': title or '(empty)',
                'provider': w.get('provider', 'Unknown'),
                'date': w.get('startAt', '')[:10]
            })
    
    if generic_titles:
        print(f"\n⚠️  {len(generic_titles)} events with generic titles:")
        for item in generic_titles[:5]:  # Show first 5
            print(f"   • '{item['title']}' ({item['provider']}, {item['date']})")
        if len(generic_titles) > 5:
            print(f"   ... and {len(generic_titles) - 5} more")
    else:
        print("✅ No generic titles found")
    
    if short_titles:
        print(f"\n⚠️  {len(short_titles)} events with short/empty titles:")
        for item in short_titles[:3]:
            print(f"   • '{item['title']}' ({item['provider']}, {item['date']})")
    else:
        print("✅ All titles have good length")
    
    # ============================================================================
    # 3. TIME ANALYSIS
    # ============================================================================
    print(f"\n{'─'*70}")
    print("3. TIME QUALITY")
    print(f"{'─'*70}")
    
    time_patterns = Counter()
    unusual_times = []
    
    for w in workshops:
        start_str = w.get('startAt', '')
        end_str = w.get('endAt', '')
        
        try:
            # Extract time portion (HH:MM)
            start_time = start_str[11:16] if len(start_str) > 16 else None
            end_time = end_str[11:16] if len(end_str) > 16 else None
            
            if start_time and end_time:
                time_pattern = f"{start_time}-{end_time}"
                time_patterns[time_pattern] += 1
                
                # Check for unusual hours
                start_hour = int(start_time.split(':')[0])
                if start_hour < 6 or start_hour > 23:
                    unusual_times.append({
                        'title': w.get('title', '')[:40],
                        'time': time_pattern,
                        'provider': w.get('provider', 'Unknown')
                    })
        except:
            pass
    
    print(f"\nMost common time slots:")
    for time_pattern, count in time_patterns.most_common(10):
        # Convert to 12h format for readability
        start, end = time_pattern.split('-')
        sh, sm = map(int, start.split(':'))
        eh, em = map(int, end.split(':'))
        
        start_12h = f"{sh%12 or 12}:{sm:02d}{'pm' if sh >= 12 else 'am'}"
        end_12h = f"{eh%12 or 12}:{em:02d}{'pm' if eh >= 12 else 'am'}"
        
        # Flag suspiciously common times (likely defaults)
        flag = "⚠️ " if count > len(workshops) * 0.2 else "  "
        print(f"{flag} {start_12h}-{end_12h:12s} ({count:3d} events)")
    
    if unusual_times:
        print(f"\n⚠️  {len(unusual_times)} events with unusual times:")
        for item in unusual_times[:3]:
            print(f"   • '{item['title']}' at {item['time']} ({item['provider']})")
    
    # ============================================================================
    # 4. ONGOING CLASS DETECTION
    # ============================================================================
    print(f"\n{'─'*70}")
    print("4. ONGOING CLASS DETECTION")
    print(f"{'─'*70}")
    
    ongoing_patterns = [
        r'\b\d+\s*-?\s*(week|session|class|month)\s+(course|pack|series)',
        r'(pack|package|bundle|series)\b',
        r'(monthly|weekly|ongoing)',
        r'curriculum',
        r'semester',
    ]
    
    suspected_ongoing = []
    
    for w in workshops:
        title = w.get('title', '')
        detail = w.get('detail', '')
        combined = f"{title} {detail}".lower()
        
        for pattern in ongoing_patterns:
            if re.search(pattern, combined, re.I):
                suspected_ongoing.append({
                    'title': title,
                    'pattern': pattern,
                    'provider': w.get('provider', 'Unknown')
                })
                break
    
    if suspected_ongoing:
        print(f"\n⚠️  {len(suspected_ongoing)} possible ongoing courses found:")
        for item in suspected_ongoing[:5]:
            print(f"   • '{item['title'][:50]}'")
            print(f"     ({item['provider']}, matched: {item['pattern']})")
        if len(suspected_ongoing) > 5:
            print(f"   ... and {len(suspected_ongoing) - 5} more")
    else:
        print("✅ No ongoing courses detected")
    
    # ============================================================================
    # 5. MISSING FIELDS
    # ============================================================================
    print(f"\n{'─'*70}")
    print("5. DATA COMPLETENESS")
    print(f"{'─'*70}\n")
    
    missing_stats = {
        'registrationURL': 0,
        'startAt': 0,
        'endAt': 0,
        'title': 0,
    }
    
    for w in workshops:
        for field in missing_stats:
            if not w.get(field):
                missing_stats[field] += 1
    
    for field, count in missing_stats.items():
        pct = (count / len(workshops) * 100) if len(workshops) > 0 else 0
        status = "✅" if count == 0 else "⚠️ "
        print(f"{status} {field:20s} missing in {count:3d} events ({pct:5.1f}%)")
    
    # ============================================================================
    # 6. DATE RANGE
    # ============================================================================
    print(f"\n{'─'*70}")
    print("6. DATE RANGE")
    print(f"{'─'*70}\n")
    
    dates = []
    for w in workshops:
        start_str = w.get('startAt', '')
        if start_str:
            try:
                date = datetime.fromisoformat(start_str.replace('Z', '+00:00'))
                dates.append(date)
            except:
                pass
    
    if dates:
        earliest = min(dates)
        latest = max(dates)
        print(f"Earliest event: {earliest.strftime('%Y-%m-%d %I:%M%p')}")
        print(f"Latest event:   {latest.strftime('%Y-%m-%d %I:%M%p')}")
        print(f"Date span:      {(latest - earliest).days} days")
        
        # Check for past events
        now = datetime.now(earliest.tzinfo)
        past_events = [d for d in dates if d < now]
        if past_events:
            print(f"\n⚠️  {len(past_events)} events in the past (may need pruning)")
    
    # ============================================================================
    # 7. RECOMMENDATIONS
    # ============================================================================
    print(f"\n{'─'*70}")
    print("7. RECOMMENDATIONS")
    print(f"{'─'*70}\n")
    
    issues = []
    
    if generic_titles:
        issues.append(f"Fix {len(generic_titles)} generic titles (likely The VO Pros extractor)")
    
    # Check for sources with no events
    missing_sources = [p for p, events in by_provider.items() if len(events) == 0]
    if missing_sources:
        issues.append(f"Debug {len(missing_sources)} sources with no events: {', '.join(missing_sources[:3])}")
    
    # Check for default time overuse
    for time_pattern, count in time_patterns.most_common(3):
        if count > len(workshops) * 0.3:  # >30% using same time = suspicious
            issues.append(f"Time {time_pattern} used {count} times ({count/len(workshops)*100:.0f}%) - may be default")
    
    if suspected_ongoing:
        issues.append(f"Filter out {len(suspected_ongoing)} ongoing courses")
    
    if not issues:
        print("✅ No major issues detected! Workshop data looks good.")
    else:
        print("Priority fixes needed:\n")
        for i, issue in enumerate(issues, 1):
            print(f"{i}. {issue}")
    
    print(f"\n{'='*70}\n")

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python diagnose_workshops.py resources.json")
        sys.exit(1)
    
    analyze_workshops(sys.argv[1])
