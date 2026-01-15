"""Client for The Odds API."""
import time
import logging
from datetime import datetime
from typing import Iterator, Tuple, List, Optional, Dict, Any

import requests

from .config import ODDS_API_KEY, ODDS_API_BASE_URL, DEFAULT_REGIONS, API_REQUESTS_PER_MINUTE, LEAGUES

logger = logging.getLogger(__name__)


class RateLimiter:
    """Simple rate limiter for API requests."""

    def __init__(self, requests_per_minute: int):
        self.interval = 60.0 / requests_per_minute
        self._last_request_time = 0.0

    def wait(self):
        """Block until a request can be made within rate limits."""
        now = time.time()
        time_since_last = now - self._last_request_time
        if time_since_last < self.interval:
            sleep_time = self.interval - time_since_last
            time.sleep(sleep_time)
        self._last_request_time = time.time()


class OddsAPIClient:
    """Client for The Odds API."""

    def __init__(self, api_key: str = ODDS_API_KEY, regions: List[str] = None):
        self.api_key = api_key
        self.base_url = ODDS_API_BASE_URL
        self.regions = regions or DEFAULT_REGIONS
        self.rate_limiter = RateLimiter(API_REQUESTS_PER_MINUTE)
        self._requests_remaining: Optional[int] = None
        self._requests_used: Optional[int] = None

    @property
    def requests_remaining(self) -> Optional[int]:
        """Get the number of remaining API requests this month."""
        return self._requests_remaining

    @property
    def requests_used(self) -> Optional[int]:
        """Get the number of API requests used this month."""
        return self._requests_used

    def _make_request(self, endpoint: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """Make a rate-limited request to the API."""
        if not self.api_key:
            raise ValueError("ODDS_API_KEY not set. Please set it in your .env file.")

        self.rate_limiter.wait()

        url = f"{self.base_url}/{endpoint}"
        request_params = {"apiKey": self.api_key}
        if params:
            request_params.update(params)

        logger.debug(f"Making request to {url}")
        response = requests.get(url, params=request_params)

        # Track quota from response headers
        self._requests_remaining = int(response.headers.get("x-requests-remaining", 0))
        self._requests_used = int(response.headers.get("x-requests-used", 0))

        logger.info(f"API quota: {self._requests_remaining} remaining, {self._requests_used} used")

        response.raise_for_status()
        return response.json()

    def get_sports(self) -> List[Dict[str, Any]]:
        """Get all available sports. This endpoint is free (no quota cost)."""
        return self._make_request("sports")

    def get_soccer_leagues(self) -> List[Dict[str, Any]]:
        """Get all available soccer leagues."""
        sports = self.get_sports()
        return [s for s in sports if s["key"].startswith("soccer_") and s["active"]]

    def get_odds(self, sport_key: str) -> List[Dict[str, Any]]:
        """
        Fetch current odds for all matches in a sport/league.

        Args:
            sport_key: The sport key (e.g., 'soccer_epl')

        Returns:
            List of match objects with odds from various bookmakers
        """
        params = {
            "regions": ",".join(self.regions),
            "markets": "h2h",
            "oddsFormat": "decimal"
        }
        return self._make_request(f"sports/{sport_key}/odds", params)

    def get_scores(self, sport_key: str, days_from: int = 3) -> List[Dict[str, Any]]:
        """
        Fetch scores for completed and live matches.

        Args:
            sport_key: The sport key (e.g., 'soccer_epl')
            days_from: Number of days in the past to include (1-3)

        Returns:
            List of match objects with scores
        """
        params = {"daysFrom": min(days_from, 3)}  # API max is 3 days
        return self._make_request(f"sports/{sport_key}/scores", params)

    def iter_odds(self, sport_key: str) -> Iterator[Tuple[Dict, List[Dict]]]:
        """
        Iterate over matches and their odds for a given sport.

        Yields:
            Tuple of (match_info, list of odds from different bookmakers)
        """
        data = self.get_odds(sport_key)

        for event in data:
            match_info = {
                "external_id": event["id"],
                "home_team": event["home_team"],
                "away_team": event["away_team"],
                "commence_time": datetime.fromisoformat(event["commence_time"].replace("Z", "+00:00")),
                "sport_key": event["sport_key"],
            }

            odds_list = []
            for bookmaker in event.get("bookmakers", []):
                for market in bookmaker.get("markets", []):
                    if market["key"] == "h2h":
                        outcomes = {o["name"]: o["price"] for o in market["outcomes"]}
                        odds_info = {
                            "bookmaker_key": bookmaker["key"],
                            "bookmaker_name": bookmaker["title"],
                            "home_win": outcomes.get(event["home_team"], 0),
                            "draw": outcomes.get("Draw"),
                            "away_win": outcomes.get(event["away_team"], 0),
                            "last_update": market.get("last_update"),
                        }
                        odds_list.append(odds_info)

            yield match_info, odds_list

    def iter_scores(self, sport_key: str, days_from: int = 3) -> Iterator[Dict]:
        """
        Iterate over completed matches with scores.

        Args:
            sport_key: The sport key (e.g., 'soccer_epl')
            days_from: Number of days in the past to include

        Yields:
            Match info with scores
        """
        data = self.get_scores(sport_key, days_from)

        for event in data:
            if not event.get("completed"):
                continue

            scores = event.get("scores", [])
            if len(scores) < 2:
                continue

            # Scores are returned in order: [home_team_score, away_team_score]
            home_score_obj = next((s for s in scores if s["name"] == event["home_team"]), None)
            away_score_obj = next((s for s in scores if s["name"] == event["away_team"]), None)

            if not home_score_obj or not away_score_obj:
                continue

            yield {
                "external_id": event["id"],
                "home_team": event["home_team"],
                "away_team": event["away_team"],
                "commence_time": datetime.fromisoformat(event["commence_time"].replace("Z", "+00:00")),
                "home_score": int(home_score_obj["score"]),
                "away_score": int(away_score_obj["score"]),
                "completed": True,
            }


def fetch_all_odds(client: OddsAPIClient = None, league_keys: List[str] = None) -> Iterator[Tuple[str, Dict, List[Dict]]]:
    """
    Fetch odds for all configured leagues.

    Args:
        client: OddsAPIClient instance (creates one if not provided)
        league_keys: List of league keys to fetch (uses all configured leagues if not provided)

    Yields:
        Tuple of (league_key, match_info, odds_list)
    """
    if client is None:
        client = OddsAPIClient()

    if league_keys is None:
        league_keys = list(LEAGUES.keys())

    for league_key in league_keys:
        try:
            logger.info(f"Fetching odds for {league_key}")
            for match_info, odds_list in client.iter_odds(league_key):
                yield league_key, match_info, odds_list
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                logger.warning(f"League {league_key} not found or no events available")
            else:
                logger.error(f"Error fetching odds for {league_key}: {e}")
        except Exception as e:
            logger.error(f"Error fetching odds for {league_key}: {e}")


def fetch_all_scores(client: OddsAPIClient = None, league_keys: List[str] = None, days_from: int = 3) -> Iterator[Tuple[str, Dict]]:
    """
    Fetch scores for all configured leagues.

    Args:
        client: OddsAPIClient instance
        league_keys: List of league keys to fetch
        days_from: Number of days in the past to include

    Yields:
        Tuple of (league_key, score_info)
    """
    if client is None:
        client = OddsAPIClient()

    if league_keys is None:
        league_keys = list(LEAGUES.keys())

    for league_key in league_keys:
        try:
            logger.info(f"Fetching scores for {league_key}")
            for score_info in client.iter_scores(league_key, days_from):
                yield league_key, score_info
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                logger.warning(f"League {league_key} not found or no scores available")
            else:
                logger.error(f"Error fetching scores for {league_key}: {e}")
        except Exception as e:
            logger.error(f"Error fetching scores for {league_key}: {e}")
