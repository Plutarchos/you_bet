"""Export database to JSON for the static website."""
import json
import sqlite3
from pathlib import Path
from datetime import datetime
from statistics import median, mean

MIN_BOOKMAKERS = 5
MIN_OVERROUND = 1.00  # 100% - minimum valid implied probability sum
MAX_OVERROUND = 1.50  # 150% - maximum valid (exchange placeholders often sum to ~300%)


def calculate_implied_probability(decimal_odds: float) -> float:
    """Convert decimal odds to implied probability."""
    if not decimal_odds or decimal_odds <= 0:
        return None
    return 1 / decimal_odds


def is_valid_odds_row(home: float, draw: float, away: float) -> bool:
    """Check if an odds row passes quality controls.

    Rejects:
    - All odds being the same (invalid placeholder data)
    - Implied probability sum outside reasonable range (100-150%)
    - Any odds below 1.02 (practically impossible)
    """
    if not all([home, draw, away]):
        return False

    # Reject if all odds are the same (placeholder data)
    if home == draw == away:
        return False

    # Reject if any odds are suspiciously low (< 1.02)
    if min(home, draw, away) < 1.02:
        return False

    # Calculate overround (sum of implied probabilities)
    overround = (1/home) + (1/draw) + (1/away)

    # Reject if outside reasonable range
    if overround < MIN_OVERROUND or overround > MAX_OVERROUND:
        return False

    return True


def deduplicate_bookmakers(odds_rows: list) -> list:
    """Remove duplicate bookmakers, keeping the one with best (lowest) overround.

    Some bookmakers appear multiple times (e.g., Betfair UK and Betfair EU).
    Keep only the entry with the most competitive odds.
    """
    by_name = {}
    for row in odds_rows:
        name = row['bookmaker']
        home, draw, away = row['home'], row['draw'], row['away']

        if not all([home, draw, away]):
            continue

        overround = (1/home) + (1/draw) + (1/away)

        if name not in by_name or overround < by_name[name]['overround']:
            by_name[name] = {**row, 'overround': overround}

    # Remove the temporary overround field
    return [{k: v for k, v in row.items() if k != 'overround'} for row in by_name.values()]


def calculate_probability_stats(odds_list: list[float], bookmaker_list: list[str]) -> dict:
    """Calculate probability statistics for a set of odds.

    Returns dict with median/mean/best odds and their implied probabilities.
    """
    if not odds_list or len(odds_list) == 0:
        return None

    valid_odds = [o for o in odds_list if o and o > 0]
    if not valid_odds:
        return None

    # Calculate median and mean of the odds directly
    median_odds = median(valid_odds)
    mean_odds = mean(valid_odds)

    # Best odds = highest decimal odds (most generous for bettor)
    best_idx = odds_list.index(max(odds_list))
    best_odds = odds_list[best_idx]
    best_bookmaker = bookmaker_list[best_idx] if best_idx < len(bookmaker_list) else None

    # Calculate implied probabilities from odds
    median_prob = calculate_implied_probability(median_odds)
    mean_prob = calculate_implied_probability(mean_odds)
    best_prob = calculate_implied_probability(best_odds)

    return {
        'median_odds': round(median_odds, 2),
        'median_prob': round(median_prob, 4),
        'mean_odds': round(mean_odds, 2),
        'mean_prob': round(mean_prob, 4),
        'best_odds': round(best_odds, 2),
        'best_prob': round(best_prob, 4),
        'best_bookmaker': best_bookmaker,
    }


def export_data():
    db_path = Path(__file__).parent.parent / "data" / "odds.db"
    output_path = Path(__file__).parent.parent / "website" / "src" / "data" / "odds.json"

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # Get all upcoming matches with their odds
    matches_data = []

    matches = conn.execute('''
        SELECT m.id, m.home_team, m.away_team, m.commence_time, m.status, l.name as league
        FROM matches m
        JOIN leagues l ON m.league_id = l.id
        WHERE m.status = 'upcoming'
        ORDER BY m.commence_time
    ''').fetchall()

    for match in matches:
        # Get odds for this match
        odds_raw = conn.execute('''
            SELECT b.name as bookmaker, b.key as bookmaker_key,
                   o.home_win, o.draw, o.away_win, o.scraped_at
            FROM odds o
            JOIN bookmakers b ON o.bookmaker_id = b.id
            WHERE o.match_id = ?
            ORDER BY b.name
        ''', (match['id'],)).fetchall()

        if not odds_raw:
            continue

        # Convert to list of dicts and apply quality controls
        odds_list = [
            {
                'bookmaker': o['bookmaker'],
                'bookmaker_key': o['bookmaker_key'],
                'home': o['home_win'],
                'draw': o['draw'],
                'away': o['away_win'],
            }
            for o in odds_raw
            if is_valid_odds_row(o['home_win'], o['draw'], o['away_win'])
        ]

        # Deduplicate bookmakers (keep best odds per bookmaker name)
        odds_list = deduplicate_bookmakers(odds_list)

        # Filter: skip matches with insufficient valid bookmakers
        if len(odds_list) <= MIN_BOOKMAKERS:
            continue

        # Extract odds lists with corresponding bookmaker names
        home_odds = [o['home'] for o in odds_list if o['home']]
        draw_odds = [o['draw'] for o in odds_list if o['draw']]
        away_odds = [o['away'] for o in odds_list if o['away']]

        home_bookmakers = [o['bookmaker'] for o in odds_list if o['home']]
        draw_bookmakers = [o['bookmaker'] for o in odds_list if o['draw']]
        away_bookmakers = [o['bookmaker'] for o in odds_list if o['away']]

        match_data = {
            'id': match['id'],
            'home_team': match['home_team'],
            'away_team': match['away_team'],
            'commence_time': match['commence_time'],
            'league': match['league'],
            'bookmaker_count': len(odds_list),
            'odds': odds_list,
            'best_odds': {
                'home': max(home_odds) if home_odds else None,
                'draw': max(draw_odds) if draw_odds else None,
                'away': max(away_odds) if away_odds else None,
            },
            'avg_odds': {
                'home': round(sum(home_odds) / len(home_odds), 2) if home_odds else None,
                'draw': round(sum(draw_odds) / len(draw_odds), 2) if draw_odds else None,
                'away': round(sum(away_odds) / len(away_odds), 2) if away_odds else None,
            },
            'probability': {
                'home': calculate_probability_stats(home_odds, home_bookmakers),
                'draw': calculate_probability_stats(draw_odds, draw_bookmakers),
                'away': calculate_probability_stats(away_odds, away_bookmakers),
            }
        }
        matches_data.append(match_data)

    # Get bookmakers list
    bookmakers = conn.execute('SELECT key, name FROM bookmakers ORDER BY name').fetchall()

    # Get completed results with odds data
    results = conn.execute('''
        SELECT m.id as match_id, m.home_team, m.away_team, m.commence_time, l.name as league,
               r.home_score, r.away_score, r.outcome
        FROM results r
        JOIN matches m ON r.match_id = m.id
        JOIN leagues l ON m.league_id = l.id
        ORDER BY m.commence_time DESC
        LIMIT 50
    ''').fetchall()

    results_data = []
    for result in results:
        # Get odds for this result's match
        odds_raw = conn.execute('''
            SELECT b.name as bookmaker, b.key as bookmaker_key,
                   o.home_win, o.draw, o.away_win
            FROM odds o
            JOIN bookmakers b ON o.bookmaker_id = b.id
            WHERE o.match_id = ?
            ORDER BY b.name
        ''', (result['match_id'],)).fetchall()

        # Convert to list of dicts and apply quality controls
        odds_list = [
            {
                'bookmaker': o['bookmaker'],
                'bookmaker_key': o['bookmaker_key'],
                'home': o['home_win'],
                'draw': o['draw'],
                'away': o['away_win'],
            }
            for o in odds_raw
            if is_valid_odds_row(o['home_win'], o['draw'], o['away_win'])
        ]

        # Deduplicate bookmakers (keep best odds per bookmaker name)
        odds_list = deduplicate_bookmakers(odds_list)

        # Skip results with insufficient valid bookmakers
        if len(odds_list) <= MIN_BOOKMAKERS:
            continue

        # Extract odds lists with corresponding bookmaker names
        home_odds = [o['home'] for o in odds_list if o['home']]
        draw_odds = [o['draw'] for o in odds_list if o['draw']]
        away_odds = [o['away'] for o in odds_list if o['away']]

        home_bookmakers = [o['bookmaker'] for o in odds_list if o['home']]
        draw_bookmakers = [o['bookmaker'] for o in odds_list if o['draw']]
        away_bookmakers = [o['bookmaker'] for o in odds_list if o['away']]

        result_data = {
            'home_team': result['home_team'],
            'away_team': result['away_team'],
            'commence_time': result['commence_time'],
            'league': result['league'],
            'home_score': result['home_score'],
            'away_score': result['away_score'],
            'outcome': result['outcome'],
            'bookmaker_count': len(odds_list),
            'odds': odds_list,
            'probability': {
                'home': calculate_probability_stats(home_odds, home_bookmakers),
                'draw': calculate_probability_stats(draw_odds, draw_bookmakers),
                'away': calculate_probability_stats(away_odds, away_bookmakers),
            }
        }
        results_data.append(result_data)

    export = {
        'generated_at': datetime.now().isoformat(),
        'matches': matches_data,
        'bookmakers': [{'key': b['key'], 'name': b['name']} for b in bookmakers],
        'results': results_data
    }

    conn.close()

    # Write JSON
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(export, f, indent=2)

    print(f"Exported {len(matches_data)} matches to {output_path}")
    print(f"  Bookmakers: {len(bookmakers)}")
    print(f"  Results: {len(results_data)}")

if __name__ == '__main__':
    export_data()
