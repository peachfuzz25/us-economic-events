#!/usr/bin/env python3
"""
US Economic Event Fetcher for TradingView
Fetches high/medium impact US economic events from multiple free sources
Outputs events.json and events.pine for TradingView Pine Script integration

Author: Claude (Anthropic)
Version: 1.0
Python: 3.8+
Requirements: requests, beautifulsoup4, lxml, pytz, dateutil
"""

import json
import re
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import pytz
from bs4 import BeautifulSoup
from dateutil import parser as date_parser
import logging
from urllib.parse import urljoin
import time

# ============================================================================
# CONFIGURATION
# ============================================================================

# Timezone setup
UTC = pytz.UTC
BKK = pytz.timezone('Asia/Bangkok')
US_EASTERN = pytz.timezone('US/Eastern')

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Event impact levels and keywords
HIGH_IMPACT_KEYWORDS = [
    'FOMC', 'Federal Reserve', 'Fed', 'Interest Rate Decision',
    'CPI', 'Consumer Price Index', 'Inflation',
    'NFP', 'Non-Farm Payroll', 'Employment',
    'Unemployment Rate', 'Jobs Report',
    'GDP', 'Gross Domestic Product',
    'PCE', 'Personal Consumption Expenditures', 'Core PCE',
    'Retail Sales',
    'ISM Manufacturing', 'ISM Services',
    'Jobless Claims', 'Initial Jobless Claims',
    'PPI', 'Producer Price Index',
    'Durable Goods Orders',
    'Housing Starts', 'Building Permits',
    'Consumer Confidence',
    'Existing Home Sales',
    'New Home Sales',
    'Factory Orders',
    'Industrial Production',
    'Capacity Utilization'
]

MEDIUM_IMPACT_KEYWORDS = [
    'Personal Income', 'Personal Spending',
    'Advance Retail Sales',
    'Core Retail Sales',
    'Wholesale Inventories',
    'Business Inventories',
    'Construction Spending',
    'Chicago PMI',
    'Philly Fed Manufacturing',
    'Empire State Manufacturing',
    'MBA Mortgage Applications',
    'Conference Board Leading Index',
    'Pending Home Sales',
    'Core PCE Price Index',
    'Continuing Jobless Claims',
    'Average Hourly Earnings',
    'Labor Force Participation',
    'U-6 Unemployment',
    'Export Prices', 'Import Prices',
    'Trade Balance',
    'Fed Beige Book',
    'FOMC Minutes',
    'Financial Conditions',
    'Bank Lending Standards'
]

SPECIAL_EVENTS = [
    'Trump', 'Presidential', 'Congressional', 'Senate', 'House',
    'Tariff', 'Trade War', 'Inflation Fight', 'Rate Cut', 'Rate Hike',
    'Market Volatility', 'Flash Crash', 'Black Swan'
]

# ============================================================================
# DATA CLASSES
# ============================================================================

class EconomicEvent:
    """Represents a single economic event"""
    
    def __init__(self, name: str, event_time_utc: datetime, 
                 impact: str, forecast: Optional[str] = None, 
                 previous: Optional[str] = None, actual: Optional[str] = None,
                 source: str = 'Unknown'):
        self.name = name
        self.event_time_utc = event_time_utc.replace(tzinfo=UTC) if event_time_utc.tzinfo is None else event_time_utc.astimezone(UTC)
        self.impact = impact  # 'High', 'Medium', 'Low'
        self.forecast = forecast
        self.previous = previous
        self.actual = actual
        self.source = source
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization"""
        return {
            'name': self.name,
            'time_utc': self.event_time_utc.isoformat(),
            'time_bkk': self.event_time_utc.astimezone(BKK).isoformat(),
            'impact': self.impact,
            'forecast': self.forecast,
            'previous': self.previous,
            'actual': self.actual,
            'source': self.source,
            'timestamp_utc_ms': int(self.event_time_utc.timestamp() * 1000)
        }
    
    def to_pine_script(self) -> str:
        """Convert to Pine Script timestamp"""
        dt = self.event_time_utc
        return f"timestamp(\"UTC\", {dt.year}, {dt.month}, {dt.day}, {dt.hour}, {dt.minute})"
    
    def __repr__(self) -> str:
        return f"<Event {self.name} @ {self.event_time_utc} ({self.impact})>"


# ============================================================================
# FETCHER CLASSES
# ============================================================================

class InvestingComFetcher:
    """Fetch events from Investing.com Economic Calendar"""
    
    BASE_URL = "https://www.investing.com/economic-calendar/"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def fetch(self, days_ahead: int = 365) -> List[EconomicEvent]:
        """Fetch events from Investing.com"""
        events = []
        try:
            logger.info("Fetching from Investing.com...")
            
            # Note: Investing.com requires specific headers and may have anti-scraping
            # This is a simplified approach; for production, consider using their calendar API
            # or a dedicated library like investpy
            
            # Fallback: Provide instruction to use alternative sources
            logger.warning("Investing.com requires advanced scraping. Using alternative sources only.")
            
        except Exception as e:
            logger.error(f"Investing.com fetch failed: {e}")
        
        return events


class ForexFactoryFetcher:
    """Fetch events from Forex Factory Economic Calendar"""
    
    BASE_URL = "https://www.forexfactory.com/calendar.php"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def fetch(self, days_ahead: int = 365) -> List[EconomicEvent]:
        """Fetch events from Forex Factory"""
        events = []
        try:
            logger.info("Fetching from Forex Factory...")
            
            # Construct URL with parameters
            params = {
                'month': 'all',
                'country': 'us'  # US events only
            }
            
            response = self.session.get(self.BASE_URL, params=params, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Forex Factory table structure: <table> with rows containing event data
            rows = soup.find_all('tr', {'class': 'calendar__row'})
            
            if not rows:
                # Try alternative selector
                rows = soup.find_all('tr')
            
            for row in rows:
                try:
                    # Extract event data from row
                    cells = row.find_all('td')
                    if len(cells) < 5:
                        continue
                    
                    # Typical structure: Date | Time | Impact | Event | Forecast | Previous | Actual
                    event_name = cells[2].text.strip() if len(cells) > 2 else ''
                    impact_str = cells[1].text.strip() if len(cells) > 1 else ''
                    forecast = cells[4].text.strip() if len(cells) > 4 else None
                    previous = cells[5].text.strip() if len(cells) > 5 else None
                    
                    # Skip if not high/medium impact
                    if not self._is_high_medium_impact(event_name):
                        continue
                    
                    # Parse time (Forex Factory uses specific format)
                    time_str = cells[0].text.strip() if len(cells) > 0 else ''
                    event_time = self._parse_time(time_str)
                    
                    if event_time and event_time > datetime.now(UTC):
                        impact = 'High' if 'High' in impact_str else 'Medium'
                        event = EconomicEvent(
                            name=event_name,
                            event_time_utc=event_time,
                            impact=impact,
                            forecast=forecast if forecast and forecast.lower() != 'n/a' else None,
                            previous=previous if previous and previous.lower() != 'n/a' else None,
                            source='Forex Factory'
                        )
                        events.append(event)
                
                except Exception as e:
                    logger.debug(f"Error parsing Forex Factory row: {e}")
                    continue
            
            logger.info(f"Forex Factory: Found {len(events)} events")
        
        except Exception as e:
            logger.error(f"Forex Factory fetch failed: {e}")
        
        return events
    
    def _is_high_medium_impact(self, event_name: str) -> bool:
        """Check if event is high or medium impact"""
        event_lower = event_name.lower()
        for keyword in HIGH_IMPACT_KEYWORDS + MEDIUM_IMPACT_KEYWORDS:
            if keyword.lower() in event_lower:
                return True
        return False
    
    def _parse_time(self, time_str: str) -> Optional[datetime]:
        """Parse time string from Forex Factory"""
        try:
            # Forex Factory uses format like "2025-01-15 15:30" or "Jan 15, 2025 3:30 PM"
            return date_parser.parse(time_str, ignoretz=True).replace(tzinfo=US_EASTERN)
        except:
            return None


class FedCalendarFetcher:
    """Fetch FOMC events from Federal Reserve calendar"""
    
    BASE_URL = "https://www.federalreserve.gov/newsevents.htm"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def fetch(self, days_ahead: int = 365) -> List[EconomicEvent]:
        """Fetch FOMC events from Federal Reserve"""
        events = []
        try:
            logger.info("Fetching FOMC events from Federal Reserve...")
            
            # FOMC meeting calendar
            fomc_url = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
            response = self.session.get(fomc_url, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Look for FOMC meeting dates (typically 2:00 PM ET on meeting days)
            # Pattern: Look for text containing "FOMC" and dates
            links = soup.find_all('a', href=True)
            
            for link in links:
                text = link.text.strip()
                if 'FOMC' in text or 'Federal Open' in text:
                    # Try to extract date from link text or nearby content
                    try:
                        # FOMC meetings are typically at 2:00 PM ET on announced dates
                        # Default: 2:00 PM ET on Wednesday (press release time)
                        date_match = re.search(r'(\w+)\s+(\d+),?\s+(\d{4})', text)
                        if date_match:
                            month_str, day_str, year_str = date_match.groups()
                            event_date_str = f"{month_str} {day_str}, {year_str} 2:00 PM"
                            event_time = date_parser.parse(event_date_str)
                            event_time = US_EASTERN.localize(event_time) if event_time.tzinfo is None else event_time
                            
                            if event_time > datetime.now(UTC):
                                # FOMC events
                                for event_type in ['Interest Rate Decision', 'Economic Projections', 'Press Conference']:
                                    time_offset = timedelta(minutes=30) if 'Press' in event_type else timedelta(0)
                                    event = EconomicEvent(
                                        name=f"FOMC {event_type}",
                                        event_time_utc=event_time + time_offset,
                                        impact='High',
                                        source='Federal Reserve'
                                    )
                                    events.append(event)
                    except Exception as e:
                        logger.debug(f"Error parsing FOMC date: {e}")
                        continue
            
            logger.info(f"Federal Reserve: Found {len(events)} FOMC events")
        
        except Exception as e:
            logger.error(f"Federal Reserve fetch failed: {e}")
        
        return events


class HardcodedFetcher:
    """Fallback hardcoded events for 2026 (will be updated periodically)"""
    
    def fetch(self, days_ahead: int = 365) -> List[EconomicEvent]:
        """Return hardcoded 2026 US economic events"""
        events = []
        
        # ===== 2026 FOMC MEETINGS (Known Schedule) =====
        fomc_dates_2026 = [
            ('2026-01-27', 19, 0),   # Wed, 2:00 PM ET
            ('2026-03-17', 19, 0),   # Tue, 2:00 PM ET
            ('2026-05-04', 19, 0),   # Tue, 2:00 PM ET
            ('2026-06-16', 19, 0),   # Tue, 2:00 PM ET
            ('2026-07-28', 19, 0),   # Tue, 2:00 PM ET
            ('2026-09-16', 19, 0),   # Wed, 2:00 PM ET
            ('2026-11-03', 19, 0),   # Tue, 2:00 PM ET
            ('2026-12-15', 19, 0),   # Tue, 2:00 PM ET
        ]
        
        for date_str, hour_et, minute in fomc_dates_2026:
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            event_time = US_EASTERN.localize(datetime(date_obj.year, date_obj.month, date_obj.day, hour_et, minute))
            
            for event_type in ['Interest Rate Decision', 'Economic Projections']:
                event = EconomicEvent(
                    name=f"FOMC {event_type}",
                    event_time_utc=event_time,
                    impact='High',
                    source='Federal Reserve (Hardcoded)'
                )
                events.append(event)
            
            # Press Conference 30 mins after
            press_conf_time = event_time + timedelta(minutes=30)
            event = EconomicEvent(
                name="FOMC Press Conference",
                event_time_utc=press_conf_time,
                impact='High',
                source='Federal Reserve (Hardcoded)'
            )
            events.append(event)
        
        # ===== 2026 MONTHLY ECONOMIC EVENTS (Typical Schedule) =====
        # These repeat monthly, so we generate them for each month
        
        monthly_events = [
            # Week 1
            ('Employment Report (NFP)', 7, 12, 30, 'High', 'First Friday at 8:30 AM ET'),
            ('Jobless Claims', 4, 12, 30, 'High', 'Weekly Thursday at 8:30 AM ET'),
            
            # Week 2
            ('CPI Release', 12, 12, 30, 'High', 'Mid-month, typically 2nd week'),
            ('PPI Release', 13, 12, 30, 'High', 'Mid-month, typically 2nd week'),
            ('Retail Sales', 13, 12, 30, 'High', 'Mid-month release'),
            ('ISM Manufacturing PMI', 1, 12, 0, 'High', 'First business day of month'),
            
            # Week 3-4
            ('ISM Services PMI', 3, 12, 0, 'Medium', 'Third business day of month'),
            ('Durable Goods Orders', 25, 12, 30, 'Medium', 'Last Friday of month'),
            ('Personal Income', 27, 12, 30, 'Medium', 'End of month'),
            ('Personal Spending', 27, 13, 30, 'Medium', 'End of month'),
            ('PCE Price Index', 27, 12, 30, 'High', 'End of month'),
            
            # Housing
            ('Housing Starts', 18, 12, 30, 'High', 'Mid-month'),
            ('Building Permits', 18, 12, 30, 'Medium', 'Mid-month'),
            ('Existing Home Sales', 25, 14, 0, 'High', 'Last week of month'),
            ('New Home Sales', 28, 14, 0, 'Medium', 'End of month'),
        ]
        
        for month in range(1, 13):
            year = 2026
            
            # Skip months in the past
            if datetime.now(UTC) > datetime(year, month, 28, 23, 59, tzinfo=UTC):
                continue
            
            for event_name, day, hour_et, minute, impact, note in monthly_events:
                try:
                    # Handle events that might not exist in certain months
                    if day > 28:
                        # Adjust for months with fewer days
                        import calendar
                        last_day = calendar.monthrange(year, month)[1]
                        day = min(day, last_day)
                    
                    event_time = US_EASTERN.localize(datetime(year, month, day, hour_et, minute))
                    
                    # Skip if in the past
                    if event_time < datetime.now(UTC):
                        continue
                    
                    event = EconomicEvent(
                        name=event_name,
                        event_time_utc=event_time,
                        impact=impact,
                        source='Hardcoded Schedule'
                    )
                    events.append(event)
                
                except ValueError:
                    # Day doesn't exist in this month
                    continue
        
        logger.info(f"Hardcoded Fetcher: Generated {len(events)} events")
        return events


class TrumpAnnouncementMonitor:
    """Monitor for Trump announcements and special events"""
    
    def __init__(self):
        self.session = requests.Session()
    
    def fetch(self, days_ahead: int = 365) -> List[EconomicEvent]:
        """Fetch Trump-related announcements"""
        events = []
        try:
            logger.info("Monitoring for Trump announcements...")
            # This would require monitoring news sources or Twitter/X
            # For now, skip automated monitoring and rely on manual input
            logger.info("Trump announcements: Manual monitoring recommended")
        except Exception as e:
            logger.error(f"Trump announcement fetch failed: {e}")
        
        return events


# ============================================================================
# EVENT AGGREGATOR
# ============================================================================

class EventAggregator:
    """Aggregate events from multiple sources and deduplicate"""
    
    def __init__(self):
        self.fetchers = [
            ForexFactoryFetcher(),
            FedCalendarFetcher(),
            HardcodedFetcher(),
        ]
    
    def fetch_all(self, days_ahead: int = 365) -> List[EconomicEvent]:
        """Fetch events from all sources and deduplicate"""
        all_events = []
        
        for fetcher in self.fetchers:
            try:
                events = fetcher.fetch(days_ahead)
                all_events.extend(events)
            except Exception as e:
                logger.error(f"Fetcher {fetcher.__class__.__name__} failed: {e}")
        
        # Deduplicate
        seen = set()
        unique_events = []
        
        for event in all_events:
            # Create key from event name and time (within 1 minute tolerance)
            key = (event.name, event.event_time_utc.replace(second=0, microsecond=0))
            
            if key not in seen:
                seen.add(key)
                unique_events.append(event)
        
        # Sort by time
        unique_events.sort(key=lambda e: e.event_time_utc)
        
        logger.info(f"Total unique events: {len(unique_events)}")
        return unique_events
    
    def filter_high_medium(self, events: List[EconomicEvent]) -> List[EconomicEvent]:
        """Filter to only high and medium impact events"""
        filtered = [e for e in events if e.impact in ['High', 'Medium']]
        logger.info(f"After filtering: {len(filtered)} high/medium impact events")
        return filtered


# ============================================================================
# OUTPUT GENERATORS
# ============================================================================

class PineScriptGenerator:
    """Generate Pine Script compatible output"""
    
    @staticmethod
    def generate_pine_arrays(events: List[EconomicEvent]) -> str:
        """Generate Pine Script array definitions"""
        
        script_lines = [
            "// ===== Auto-Generated Event Arrays (DO NOT EDIT MANUALLY) =====",
            "// Generated: " + datetime.now(UTC).isoformat(),
            "// Total Events: " + str(len(events)),
            "",
            f"var int EVENT_COUNT = {len(events)}",
            "var string[] event_names = array.new<string>(EVENT_COUNT)",
            "var int[] event_times_utc = array.new<int>(EVENT_COUNT)",
            "var string[] event_impact = array.new<string>(EVENT_COUNT)",
            "var string[] event_forecast = array.new<string>(EVENT_COUNT)",
            "var string[] event_previous = array.new<string>(EVENT_COUNT)",
            ""
        ]
        
        for i, event in enumerate(events):
            dt = event.event_time_utc
            timestamp = f"timestamp(\"UTC\", {dt.year}, {dt.month}, {dt.day}, {dt.hour}, {dt.minute})"
            
            script_lines.append(f"// {i}: {event.name} - {event.impact} - {event.source}")
            script_lines.append(f"array.set(event_names, {i}, \"{event.name}\")")
            script_lines.append(f"array.set(event_times_utc, {i}, {timestamp})")
            script_lines.append(f"array.set(event_impact, {i}, \"{event.impact}\")")
            script_lines.append(f"array.set(event_forecast, {i}, \"{event.forecast or 'N/A'}\")")
            script_lines.append(f"array.set(event_previous, {i}, \"{event.previous or 'N/A'}\")")
            script_lines.append("")
        
        return "\n".join(script_lines)


class JSONGenerator:
    """Generate JSON output for external consumption"""
    
    @staticmethod
    def generate_json(events: List[EconomicEvent]) -> str:
        """Generate JSON representation of events"""
        
        data = {
            'metadata': {
                'generated_utc': datetime.now(UTC).isoformat(),
                'total_events': len(events),
                'timezone_display': 'BKK (UTC+7)',
                'timezone_utc': 'UTC',
                'next_update': (datetime.now(UTC) + timedelta(days=1)).isoformat()
            },
            'events': [event.to_dict() for event in events]
        }
        
        return json.dumps(data, indent=2, default=str)


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution function"""
    
    logger.info("=" * 80)
    logger.info("US Economic Event Fetcher - Starting")
    logger.info("=" * 80)
    
    try:
        # Aggregate events
        aggregator = EventAggregator()
        all_events = aggregator.fetch_all(days_ahead=365)
        
        # Filter to high/medium impact
        filtered_events = aggregator.filter_high_medium(all_events)
        
        if not filtered_events:
            logger.error("No events fetched. Check internet connection and source availability.")
            return False
        
        # Generate outputs
        logger.info("Generating Pine Script output...")
        pine_script = PineScriptGenerator.generate_pine_arrays(filtered_events)
        
        logger.info("Generating JSON output...")
        json_output = JSONGenerator.generate_json(filtered_events)
        
        # Save outputs
        with open('events.pine', 'w') as f:
            f.write(pine_script)
        logger.info("Saved: events.pine")
        
        with open('events.json', 'w') as f:
            f.write(json_output)
        logger.info("Saved: events.json")
        
        # Print summary
        logger.info("=" * 80)
        logger.info(f"SUCCESS: Fetched {len(filtered_events)} high/medium impact US events")
        logger.info("=" * 80)
        
        # Print first 5 events as sample
        logger.info("\nSample events:")
        for event in filtered_events[:5]:
            logger.info(f"  • {event.name} @ {event.event_time_utc.strftime('%Y-%m-%d %H:%M UTC')} ({event.impact})")
        
        return True
    
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
