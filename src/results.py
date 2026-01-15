"""Results collection and orchestration."""
import logging
from typing import Dict, List, Optional
import sqlite3

from .config import LEAGUES
from .database import (
    get_connection,
    init_database,
    get_or_create_league,
    get_or_create_bookmaker,
    get_or_create_match,
    insert_odds,
    odds_changed,
    insert_result,
)
from .odds_api import OddsAPIClient, fetch_all_odds, fetch_all_scores
from .oddschecker import scrape_odds

logger = logging.getLogger(__name__)


def collect_odds_from_api(
    conn: sqlite3.Connection,
    league_keys: List[str] = None,
    client: OddsAPIClient = None
) -> Dict[str, int]:
    """
    Collect odds from The Odds API and store in database.

    Args:
        conn: Database connection
        league_keys: List of league keys to fetch (all if None)
        client: OddsAPIClient instance

    Returns:
        Stats dict with counts of matches and odds collected
    """
    stats = {"matches": 0, "odds_snapshots": 0, "errors": 0}

    if client is None:
        client = OddsAPIClient()

    for league_key, match_info, odds_list in fetch_all_odds(client, league_keys):
        try:
            # Get or create league
            league_config = LEAGUES.get(league_key, {})
            league_id = get_or_create_league(
                conn,
                league_key,
                league_config.get("name", league_key),
                league_config.get("country")
            )

            # Get or create match
            match_id = get_or_create_match(
                conn,
                league_id,
                match_info["home_team"],
                match_info["away_team"],
                match_info["commence_time"],
                match_info.get("external_id")
            )
            stats["matches"] += 1

            # Insert odds for each bookmaker
            for odds_info in odds_list:
                bookmaker_id = get_or_create_bookmaker(
                    conn,
                    odds_info["bookmaker_key"],
                    odds_info["bookmaker_name"]
                )

                # Only insert if odds have changed
                if odds_changed(
                    conn, match_id, bookmaker_id,
                    odds_info["home_win"],
                    odds_info.get("draw"),
                    odds_info["away_win"]
                ):
                    insert_odds(
                        conn, match_id, bookmaker_id,
                        odds_info["home_win"],
                        odds_info.get("draw"),
                        odds_info["away_win"],
                        source="odds_api"
                    )
                    stats["odds_snapshots"] += 1

        except Exception as e:
            logger.error(f"Error processing match {match_info}: {e}")
            stats["errors"] += 1

    return stats


def collect_odds_from_oddschecker(
    conn: sqlite3.Connection,
    league_keys: List[str] = None,
    headless: bool = True
) -> Dict[str, int]:
    """
    Collect odds from oddschecker via web scraping.

    Args:
        conn: Database connection
        league_keys: List of league keys to scrape
        headless: Run browser in headless mode

    Returns:
        Stats dict with counts
    """
    stats = {"matches": 0, "odds_snapshots": 0, "errors": 0}

    for league_key, match_info, odds_list in scrape_odds(league_keys, headless):
        try:
            league_config = LEAGUES.get(league_key, {})
            league_id = get_or_create_league(
                conn,
                league_key,
                league_config.get("name", league_key),
                league_config.get("country")
            )

            match_id = get_or_create_match(
                conn,
                league_id,
                match_info["home_team"],
                match_info["away_team"],
                match_info["commence_time"]
            )
            stats["matches"] += 1

            for odds_info in odds_list:
                bookmaker_id = get_or_create_bookmaker(
                    conn,
                    odds_info["bookmaker_key"],
                    odds_info["bookmaker_name"]
                )

                if odds_changed(
                    conn, match_id, bookmaker_id,
                    odds_info["home_win"],
                    odds_info.get("draw"),
                    odds_info["away_win"]
                ):
                    insert_odds(
                        conn, match_id, bookmaker_id,
                        odds_info["home_win"],
                        odds_info.get("draw"),
                        odds_info["away_win"],
                        source="oddschecker"
                    )
                    stats["odds_snapshots"] += 1

        except Exception as e:
            logger.error(f"Error processing scraped match {match_info}: {e}")
            stats["errors"] += 1

    return stats


def collect_results_from_api(
    conn: sqlite3.Connection,
    league_keys: List[str] = None,
    days_from: int = 3,
    client: OddsAPIClient = None
) -> Dict[str, int]:
    """
    Collect match results from The Odds API.

    Args:
        conn: Database connection
        league_keys: List of league keys to fetch
        days_from: Number of days to look back
        client: OddsAPIClient instance

    Returns:
        Stats dict with counts
    """
    stats = {"results": 0, "errors": 0}

    if client is None:
        client = OddsAPIClient()

    for league_key, score_info in fetch_all_scores(client, league_keys, days_from):
        try:
            league_config = LEAGUES.get(league_key, {})
            league_id = get_or_create_league(
                conn,
                league_key,
                league_config.get("name", league_key),
                league_config.get("country")
            )

            # Find or create the match
            match_id = get_or_create_match(
                conn,
                league_id,
                score_info["home_team"],
                score_info["away_team"],
                score_info["commence_time"],
                score_info.get("external_id")
            )

            # Insert result
            insert_result(
                conn,
                match_id,
                score_info["home_score"],
                score_info["away_score"],
                source="odds_api"
            )
            stats["results"] += 1

        except Exception as e:
            logger.error(f"Error processing result {score_info}: {e}")
            stats["errors"] += 1

    return stats


def run_full_update(
    conn: sqlite3.Connection,
    league_keys: List[str] = None,
    include_scraper: bool = False,
    days_from: int = 3
) -> Dict[str, int]:
    """
    Run a full update: fetch odds and results.

    Args:
        conn: Database connection
        league_keys: List of league keys (all if None)
        include_scraper: Also scrape oddschecker
        days_from: Days to look back for results

    Returns:
        Combined stats
    """
    total_stats = {
        "matches": 0,
        "odds_snapshots": 0,
        "results": 0,
        "errors": 0
    }

    # Initialize database
    init_database(conn)

    # Collect odds from API
    logger.info("Fetching odds from The Odds API...")
    api_stats = collect_odds_from_api(conn, league_keys)
    for key in ["matches", "odds_snapshots", "errors"]:
        total_stats[key] += api_stats.get(key, 0)

    # Optionally scrape oddschecker
    if include_scraper:
        logger.info("Scraping odds from oddschecker...")
        scraper_stats = collect_odds_from_oddschecker(conn, league_keys)
        for key in ["matches", "odds_snapshots", "errors"]:
            total_stats[key] += scraper_stats.get(key, 0)

    # Collect results
    logger.info("Fetching results from The Odds API...")
    results_stats = collect_results_from_api(conn, league_keys, days_from)
    total_stats["results"] += results_stats.get("results", 0)
    total_stats["errors"] += results_stats.get("errors", 0)

    return total_stats
