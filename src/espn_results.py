"""Fetch match results from ESPN (no API quota needed)."""
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
import requests
from bs4 import BeautifulSoup

from .config import DB_PATH
from .database import get_connection, insert_result


# Team name mappings: ESPN name -> our database name patterns
TEAM_ALIASES = {
    # Premier League
    "man united": ["manchester united"],
    "man city": ["manchester city"],
    "newcastle": ["newcastle united", "newcastle utd"],
    "nottm forest": ["nottingham forest"],
    "brighton": ["brighton and hove albion", "brighton & hove albion"],
    "tottenham": ["tottenham hotspur"],
    "west ham": ["west ham united"],
    "wolves": ["wolverhampton wanderers", "wolverhampton"],
    "leicester": ["leicester city"],
    # La Liga
    "atletico madrid": ["atlético madrid", "atletico de madrid", "atlético de madrid"],
    "athletic club": ["athletic bilbao", "athletic club bilbao"],
    "real betis": ["real betis balompié", "real betis balompie"],
    # Bundesliga
    "bayern munich": ["fc bayern munich", "bayern münchen", "fc bayern münchen"],
    "rb leipzig": ["rasenballsport leipzig", "rbl leipzig"],
    "dortmund": ["borussia dortmund"],
    "gladbach": ["borussia mönchengladbach", "borussia monchengladbach"],
    "leverkusen": ["bayer leverkusen", "bayer 04 leverkusen"],
    "hoffenheim": ["tsg hoffenheim", "1899 hoffenheim"],
    "wolfsburg": ["vfl wolfsburg"],
    "cologne": ["fc cologne", "1. fc köln", "fc köln", "1. fc koln"],
    "st. pauli": ["fc st. pauli", "st pauli"],
    "mainz": ["mainz 05", "1. fsv mainz 05"],
    # Serie A
    "inter": ["internazionale", "inter milan", "fc internazionale milano"],
    "ac milan": ["milan"],
    "roma": ["as roma"],
    "napoli": ["ssc napoli"],
    "juventus": ["juventus fc"],
    "lazio": ["ss lazio"],
    "atalanta": ["atalanta bc"],
    # Ligue 1
    "psg": ["paris saint-germain", "paris saint germain", "paris sg"],
    "monaco": ["as monaco"],
    "lyon": ["olympique lyonnais", "olympique lyon"],
    "marseille": ["olympique marseille", "olympique de marseille"],
    "lille": ["lille osc", "losc lille"],
    # Eredivisie
    "ajax": ["ajax amsterdam", "afc ajax"],
    "psv": ["psv eindhoven"],
    "feyenoord": ["feyenoord rotterdam"],
    "az": ["az alkmaar"],
    # Portugal
    "sporting": ["sporting cp", "sporting lisbon", "sporting clube de portugal"],
    "benfica": ["sl benfica"],
    "porto": ["fc porto"],
    # Swiss
    "young boys": ["bsc young boys"],
    # Belgium
    "club brugge": ["club brugge kv"],
    "anderlecht": ["rsc anderlecht"],
    "union sg": ["union saint-gilloise", "royale union saint-gilloise"],
}


def normalize_team_name(name: str) -> str:
    """Normalize team name for matching."""
    name = name.lower().strip()
    # Remove common suffixes
    for suffix in [" fc", " cf", " sc", " ac", " afc", " ssc"]:
        if name.endswith(suffix):
            name = name[:-len(suffix)]
    return name


def teams_match(espn_name: str, db_name: str) -> bool:
    """Check if ESPN team name matches database team name."""
    espn_norm = normalize_team_name(espn_name)
    db_norm = normalize_team_name(db_name)

    # Direct match
    if espn_norm == db_norm:
        return True

    # Check if one contains the other
    if espn_norm in db_norm or db_norm in espn_norm:
        return True

    # Check aliases
    for alias, variants in TEAM_ALIASES.items():
        if espn_norm == alias or espn_norm in variants:
            if db_norm == alias or db_norm in variants:
                return True
            for v in variants:
                if db_norm == v or v in db_norm or db_norm in v:
                    return True

    return False


def parse_espn_scoreboard(html: str) -> list:
    """Parse ESPN scoreboard HTML to extract match results."""
    results = []
    soup = BeautifulSoup(html, 'html.parser')

    # ESPN uses JSON data embedded in script tags
    # We'll parse the visible scoreboard instead
    for section in soup.find_all('section', class_='Card'):
        league_header = section.find('div', class_='Card__Header__Title')
        if not league_header:
            continue
        league_name = league_header.get_text(strip=True)

        for game in section.find_all('article', class_='ScoreboardScoreCell'):
            try:
                teams = game.find_all('div', class_='ScoreCell__TeamName')
                scores = game.find_all('div', class_='ScoreCell__Score')

                if len(teams) >= 2 and len(scores) >= 2:
                    results.append({
                        'league': league_name,
                        'away_team': teams[0].get_text(strip=True),
                        'home_team': teams[1].get_text(strip=True),
                        'away_score': int(scores[0].get_text(strip=True)),
                        'home_score': int(scores[1].get_text(strip=True)),
                    })
            except (ValueError, AttributeError):
                continue

    return results


def fetch_espn_results(date: datetime) -> list:
    """Fetch results from ESPN for a specific date."""
    date_str = date.strftime('%Y%m%d')
    url = f"https://www.espn.com/soccer/scoreboard/_/date/{date_str}"

    try:
        resp = requests.get(url, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }, timeout=30)
        resp.raise_for_status()
        return parse_espn_scoreboard(resp.text)
    except Exception as e:
        print(f"Error fetching ESPN: {e}")
        return []


def find_matching_match(conn: sqlite3.Connection, result: dict, date: datetime) -> Optional[int]:
    """Find a database match that corresponds to this result."""
    # Look for matches on this date (with some tolerance)
    date_start = (date - timedelta(hours=12)).isoformat()
    date_end = (date + timedelta(hours=36)).isoformat()

    matches = conn.execute('''
        SELECT id, home_team, away_team, commence_time, status
        FROM matches
        WHERE commence_time BETWEEN ? AND ?
        AND status = 'upcoming'
    ''', (date_start, date_end)).fetchall()

    for match in matches:
        if (teams_match(result['home_team'], match['home_team']) and
            teams_match(result['away_team'], match['away_team'])):
            return match['id']

    return None


def update_results_from_espn(days_back: int = 3) -> dict:
    """Update database with results from ESPN."""
    conn = get_connection()
    stats = {'matched': 0, 'updated': 0, 'not_found': 0}

    today = datetime.now(timezone.utc)

    for day_offset in range(days_back + 1):
        date = today - timedelta(days=day_offset)
        print(f"\nFetching results for {date.strftime('%Y-%m-%d')}...")

        # For now, we'll use pre-collected results since ESPN requires JS rendering
        # This function can be extended to use Selenium or similar for live scraping

    conn.close()
    return stats


def update_from_known_results(results_data: list) -> dict:
    """Update database from a list of known results.

    results_data: list of dicts with keys:
        - home_team, away_team, home_score, away_score
        - date (optional, for matching)
    """
    conn = get_connection()
    stats = {'matched': 0, 'updated': 0, 'not_found': []}

    for result in results_data:
        # Find matching match in database
        # Search by team names
        matches = conn.execute('''
            SELECT id, home_team, away_team, commence_time, status
            FROM matches
            WHERE status = 'upcoming'
            ORDER BY commence_time DESC
        ''').fetchall()

        match_id = None
        for match in matches:
            if (teams_match(result['home_team'], match['home_team']) and
                teams_match(result['away_team'], match['away_team'])):
                match_id = match['id']
                stats['matched'] += 1
                break

        if match_id:
            # Check if result already exists
            existing = conn.execute(
                'SELECT id FROM results WHERE match_id = ?', (match_id,)
            ).fetchone()

            if not existing:
                insert_result(
                    conn, match_id,
                    result['home_score'], result['away_score'],
                    source='espn'
                )
                stats['updated'] += 1
                print(f"  Updated: {result['home_team']} {result['home_score']}-{result['away_score']} {result['away_team']}")
        else:
            stats['not_found'].append(f"{result['home_team']} vs {result['away_team']}")

    conn.close()
    return stats


if __name__ == '__main__':
    # Test with sample results
    test_results = [
        {'home_team': 'Manchester United', 'away_team': 'Manchester City', 'home_score': 2, 'away_score': 0},
    ]
    stats = update_from_known_results(test_results)
    print(f"\nStats: {stats}")
