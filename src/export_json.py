"""Export database to JSON for the static website."""
import json
import sqlite3
from pathlib import Path
from datetime import datetime

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
        odds = conn.execute('''
            SELECT b.name as bookmaker, b.key as bookmaker_key,
                   o.home_win, o.draw, o.away_win, o.scraped_at
            FROM odds o
            JOIN bookmakers b ON o.bookmaker_id = b.id
            WHERE o.match_id = ?
            ORDER BY b.name
        ''', (match['id'],)).fetchall()

        if not odds:
            continue

        # Calculate best odds and averages
        home_odds = [o['home_win'] for o in odds if o['home_win']]
        draw_odds = [o['draw'] for o in odds if o['draw']]
        away_odds = [o['away_win'] for o in odds if o['away_win']]

        match_data = {
            'id': match['id'],
            'home_team': match['home_team'],
            'away_team': match['away_team'],
            'commence_time': match['commence_time'],
            'league': match['league'],
            'odds': [
                {
                    'bookmaker': o['bookmaker'],
                    'bookmaker_key': o['bookmaker_key'],
                    'home': o['home_win'],
                    'draw': o['draw'],
                    'away': o['away_win'],
                }
                for o in odds
            ],
            'best_odds': {
                'home': max(home_odds) if home_odds else None,
                'draw': max(draw_odds) if draw_odds else None,
                'away': max(away_odds) if away_odds else None,
            },
            'avg_odds': {
                'home': round(sum(home_odds) / len(home_odds), 2) if home_odds else None,
                'draw': round(sum(draw_odds) / len(draw_odds), 2) if draw_odds else None,
                'away': round(sum(away_odds) / len(away_odds), 2) if away_odds else None,
            }
        }
        matches_data.append(match_data)

    # Get bookmakers list
    bookmakers = conn.execute('SELECT key, name FROM bookmakers ORDER BY name').fetchall()

    # Get completed results
    results = conn.execute('''
        SELECT m.home_team, m.away_team, m.commence_time, l.name as league,
               r.home_score, r.away_score, r.outcome
        FROM results r
        JOIN matches m ON r.match_id = m.id
        JOIN leagues l ON m.league_id = l.id
        ORDER BY m.commence_time DESC
        LIMIT 20
    ''').fetchall()

    export = {
        'generated_at': datetime.now().isoformat(),
        'matches': matches_data,
        'bookmakers': [{'key': b['key'], 'name': b['name']} for b in bookmakers],
        'results': [
            {
                'home_team': r['home_team'],
                'away_team': r['away_team'],
                'commence_time': r['commence_time'],
                'league': r['league'],
                'home_score': r['home_score'],
                'away_score': r['away_score'],
                'outcome': r['outcome'],
            }
            for r in results
        ]
    }

    conn.close()

    # Write JSON
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(export, f, indent=2)

    print(f"Exported {len(matches_data)} matches to {output_path}")
    print(f"  Bookmakers: {len(bookmakers)}")
    print(f"  Results: {len(results)}")

if __name__ == '__main__':
    export_data()
