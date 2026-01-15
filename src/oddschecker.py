"""Web scraper for oddschecker.com odds."""
import time
import logging
import re
from datetime import datetime, timedelta
from typing import Iterator, Tuple, List, Dict, Optional

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager

from .config import LEAGUES, ODDSCHECKER_BOOKMAKERS, SCRAPER_DELAY_SECONDS

logger = logging.getLogger(__name__)

ODDSCHECKER_BASE_URL = "https://www.oddschecker.com"


class OddscheckerScraper:
    """Web scraper for oddschecker.com using Selenium."""

    def __init__(self, headless: bool = True):
        self.headless = headless
        self._driver: Optional[webdriver.Chrome] = None

    def _get_driver(self) -> webdriver.Chrome:
        """Get or create Selenium WebDriver."""
        if self._driver is None:
            options = Options()
            if self.headless:
                options.add_argument("--headless")
            options.add_argument("--disable-gpu")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--window-size=1920,1080")
            options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

            try:
                service = Service(ChromeDriverManager().install())
                self._driver = webdriver.Chrome(service=service, options=options)
            except Exception as e:
                logger.error(f"Failed to initialize Chrome WebDriver: {e}")
                raise

        return self._driver

    def close(self):
        """Clean up WebDriver resources."""
        if self._driver:
            self._driver.quit()
            self._driver = None

    def _parse_fractional_odds(self, odds_str: str) -> Optional[float]:
        """
        Convert fractional odds (e.g., '3/1') to decimal format.

        Args:
            odds_str: Odds string like '3/1', '11/10', 'EVS', or decimal like '2.50'

        Returns:
            Decimal odds or None if parsing fails
        """
        if not odds_str:
            return None

        odds_str = odds_str.strip().upper()

        # Handle special cases
        if odds_str in ("SP", "-", ""):
            return None
        if odds_str in ("EVS", "EVENS", "EVN"):
            return 2.0

        # Try fractional format (e.g., '3/1')
        if "/" in odds_str:
            try:
                num, den = odds_str.split("/")
                return (float(num) / float(den)) + 1
            except (ValueError, ZeroDivisionError):
                return None

        # Try decimal format
        try:
            return float(odds_str)
        except ValueError:
            return None

    def _parse_match_time(self, time_str: str) -> Optional[datetime]:
        """
        Parse match time string from oddschecker.

        Args:
            time_str: Time string like '15:00', 'Today 15:00', 'Tomorrow 15:00', 'Sat 15:00'

        Returns:
            datetime object or None
        """
        if not time_str:
            return None

        time_str = time_str.strip()
        now = datetime.now()

        # Try to extract time (HH:MM)
        time_match = re.search(r'(\d{1,2}):(\d{2})', time_str)
        if not time_match:
            return None

        hour, minute = int(time_match.group(1)), int(time_match.group(2))

        # Determine the date
        time_lower = time_str.lower()
        if "today" in time_lower:
            match_date = now.date()
        elif "tomorrow" in time_lower:
            match_date = (now + timedelta(days=1)).date()
        else:
            # Try to parse day name (Mon, Tue, etc.)
            days = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
            for i, day in enumerate(days):
                if day in time_lower:
                    current_day = now.weekday()
                    days_ahead = (i - current_day) % 7
                    if days_ahead == 0 and hour < now.hour:
                        days_ahead = 7  # Next week
                    match_date = (now + timedelta(days=days_ahead)).date()
                    break
            else:
                # Default to today
                match_date = now.date()

        return datetime.combine(match_date, datetime.min.time().replace(hour=hour, minute=minute))

    def scrape_league(self, league_path: str) -> Iterator[Tuple[Dict, List[Dict]]]:
        """
        Scrape odds for all matches in a league.

        Args:
            league_path: The oddschecker URL path (e.g., '/football/english/premier-league')

        Yields:
            Tuple of (match_info, list of odds from different bookmakers)
        """
        url = f"{ODDSCHECKER_BASE_URL}{league_path}"
        logger.info(f"Scraping {url}")

        driver = self._get_driver()

        try:
            driver.get(url)

            # Wait for the page to load
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )

            # Additional wait for dynamic content
            time.sleep(SCRAPER_DELAY_SECONDS)

            soup = BeautifulSoup(driver.page_source, "lxml")

            # Find match containers - oddschecker structure varies
            # Look for common patterns
            match_rows = soup.select("[data-event-id]") or soup.select(".betting-event")

            if not match_rows:
                logger.warning(f"No match rows found on {url}")
                # Try alternative selectors
                match_rows = soup.select(".event-information")

            for match_row in match_rows:
                try:
                    match_info, odds_list = self._parse_match_row(match_row)
                    if match_info and odds_list:
                        yield match_info, odds_list
                except Exception as e:
                    logger.warning(f"Error parsing match row: {e}")
                    continue

        except TimeoutException:
            logger.error(f"Timeout loading {url}")
        except WebDriverException as e:
            logger.error(f"WebDriver error: {e}")

    def _parse_match_row(self, row) -> Tuple[Optional[Dict], List[Dict]]:
        """
        Parse a single match row from oddschecker.

        Args:
            row: BeautifulSoup element containing match data

        Returns:
            Tuple of (match_info dict, list of odds dicts)
        """
        # Try to extract team names
        home_team = None
        away_team = None

        # Look for team name elements
        teams = row.select(".team-name") or row.select("[data-team]")
        if len(teams) >= 2:
            home_team = teams[0].get_text(strip=True)
            away_team = teams[1].get_text(strip=True)
        else:
            # Try alternative patterns
            match_name = row.select_one(".match-name, .event-name")
            if match_name:
                match_text = match_name.get_text(strip=True)
                if " v " in match_text:
                    parts = match_text.split(" v ")
                    home_team = parts[0].strip()
                    away_team = parts[1].strip() if len(parts) > 1 else None
                elif " vs " in match_text.lower():
                    parts = re.split(r"\s+vs\s+", match_text, flags=re.IGNORECASE)
                    home_team = parts[0].strip()
                    away_team = parts[1].strip() if len(parts) > 1 else None

        if not home_team or not away_team:
            return None, []

        # Get match time
        time_elem = row.select_one(".time, .event-time, [data-time]")
        commence_time = None
        if time_elem:
            commence_time = self._parse_match_time(time_elem.get_text(strip=True))

        if not commence_time:
            commence_time = datetime.now() + timedelta(hours=24)  # Default to tomorrow

        match_info = {
            "home_team": home_team,
            "away_team": away_team,
            "commence_time": commence_time,
        }

        # Parse odds from bookmaker columns
        odds_list = []

        # Look for odds cells with bookmaker data attributes
        for bookie_code, (bookie_key, bookie_name) in ODDSCHECKER_BOOKMAKERS.items():
            odds_cells = row.select(f"[data-bk='{bookie_code}']")
            if len(odds_cells) >= 3:
                home_odds = self._parse_fractional_odds(odds_cells[0].get_text(strip=True))
                draw_odds = self._parse_fractional_odds(odds_cells[1].get_text(strip=True))
                away_odds = self._parse_fractional_odds(odds_cells[2].get_text(strip=True))

                if home_odds and away_odds:
                    odds_list.append({
                        "bookmaker_key": bookie_key,
                        "bookmaker_name": bookie_name,
                        "home_win": home_odds,
                        "draw": draw_odds,
                        "away_win": away_odds,
                    })

        return match_info, odds_list

    def scrape_all_leagues(self, league_keys: List[str] = None) -> Iterator[Tuple[str, Dict, List[Dict]]]:
        """
        Scrape odds for all configured leagues.

        Args:
            league_keys: List of league keys to scrape (uses all if not provided)

        Yields:
            Tuple of (league_key, match_info, odds_list)
        """
        if league_keys is None:
            league_keys = list(LEAGUES.keys())

        for league_key in league_keys:
            league_config = LEAGUES.get(league_key)
            if not league_config:
                logger.warning(f"No oddschecker config for league {league_key}")
                continue

            oddschecker_path = league_config.get("oddschecker_path")
            if not oddschecker_path:
                continue

            try:
                for match_info, odds_list in self.scrape_league(oddschecker_path):
                    yield league_key, match_info, odds_list

                # Rate limiting between leagues
                time.sleep(SCRAPER_DELAY_SECONDS)

            except Exception as e:
                logger.error(f"Error scraping league {league_key}: {e}")
                continue


def scrape_odds(league_keys: List[str] = None, headless: bool = True) -> Iterator[Tuple[str, Dict, List[Dict]]]:
    """
    Convenience function to scrape odds from oddschecker.

    Args:
        league_keys: List of league keys to scrape
        headless: Run browser in headless mode

    Yields:
        Tuple of (league_key, match_info, odds_list)
    """
    scraper = OddscheckerScraper(headless=headless)
    try:
        yield from scraper.scrape_all_leagues(league_keys)
    finally:
        scraper.close()
