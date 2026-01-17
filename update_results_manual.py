#!/usr/bin/env python3
"""Manually update results from ESPN data (January 16-17, 2026)."""
import sys
sys.path.insert(0, '.')

from src.espn_results import update_from_known_results

# Results collected from ESPN January 16, 2026
JAN_16_RESULTS = [
    # Ligue 1
    {'home_team': 'AS Monaco', 'away_team': 'Lorient', 'home_score': 1, 'away_score': 3},
    {'home_team': 'Paris Saint-Germain', 'away_team': 'Lille', 'home_score': 3, 'away_score': 0},
    # Bundesliga
    {'home_team': 'Werder Bremen', 'away_team': 'Eintracht Frankfurt', 'home_score': 3, 'away_score': 3},
    # Serie A
    {'home_team': 'Pisa', 'away_team': 'Atalanta', 'home_score': 1, 'away_score': 1},
    # La Liga
    {'home_team': 'Espanyol', 'away_team': 'Girona', 'home_score': 0, 'away_score': 2},
]

# Results collected from ESPN January 17, 2026
JAN_17_RESULTS = [
    # Premier League
    {'home_team': 'Manchester United', 'away_team': 'Manchester City', 'home_score': 2, 'away_score': 0},
    {'home_team': 'Chelsea', 'away_team': 'Brentford', 'home_score': 2, 'away_score': 0},
    {'home_team': 'Leeds United', 'away_team': 'Fulham', 'home_score': 1, 'away_score': 0},
    {'home_team': 'Liverpool', 'away_team': 'Burnley', 'home_score': 1, 'away_score': 1},
    {'home_team': 'Sunderland', 'away_team': 'Crystal Palace', 'home_score': 2, 'away_score': 1},
    {'home_team': 'Tottenham', 'away_team': 'West Ham', 'home_score': 1, 'away_score': 2},
    {'home_team': 'Nottingham Forest', 'away_team': 'Arsenal', 'home_score': 0, 'away_score': 0},
    # La Liga
    {'home_team': 'Real Betis', 'away_team': 'Villarreal', 'home_score': 2, 'away_score': 0},
    {'home_team': 'Real Madrid', 'away_team': 'Levante', 'home_score': 2, 'away_score': 0},
    {'home_team': 'Mallorca', 'away_team': 'Athletic Club', 'home_score': 3, 'away_score': 2},
    {'home_team': 'Osasuna', 'away_team': 'Real Oviedo', 'home_score': 3, 'away_score': 2},
    # Bundesliga
    {'home_team': 'Borussia Dortmund', 'away_team': 'St. Pauli', 'home_score': 3, 'away_score': 2},
    {'home_team': 'FC Cologne', 'away_team': 'Mainz', 'home_score': 2, 'away_score': 1},
    {'home_team': 'Hamburg', 'away_team': 'Borussia Mönchengladbach', 'home_score': 0, 'away_score': 0},
    {'home_team': 'TSG Hoffenheim', 'away_team': 'Bayer Leverkusen', 'home_score': 1, 'away_score': 0},
    {'home_team': 'VfL Wolfsburg', 'away_team': 'Heidenheim', 'home_score': 1, 'away_score': 1},
    {'home_team': 'Bayern Munich', 'away_team': 'RB Leipzig', 'home_score': 5, 'away_score': 1},
    # Serie A
    {'home_team': 'Internazionale', 'away_team': 'Udinese', 'home_score': 1, 'away_score': 0},
    {'home_team': 'Napoli', 'away_team': 'Sassuolo', 'home_score': 1, 'away_score': 0},
    {'home_team': 'Cagliari', 'away_team': 'Juventus', 'home_score': 1, 'away_score': 0},
    # Ligue 1
    {'home_team': 'Marseille', 'away_team': 'Angers', 'home_score': 4, 'away_score': 1},
    {'home_team': 'Lens', 'away_team': 'AJ Auxerre', 'home_score': 1, 'away_score': 0},
    {'home_team': 'Toulouse', 'away_team': 'Nice', 'home_score': 5, 'away_score': 1},
    # Championship
    {'home_team': 'Coventry City', 'away_team': 'Leicester City', 'home_score': 2, 'away_score': 1},
    {'home_team': 'Ipswich Town', 'away_team': 'Blackburn Rovers', 'home_score': 3, 'away_score': 0},
    {'home_team': 'Millwall', 'away_team': 'Watford', 'home_score': 2, 'away_score': 0},
    {'home_team': 'Sheffield United', 'away_team': 'Charlton Athletic', 'home_score': 1, 'away_score': 1},
    {'home_team': 'Bristol City', 'away_team': 'Oxford United', 'home_score': 0, 'away_score': 0},
    {'home_team': 'Derby County', 'away_team': 'Preston North End', 'home_score': 1, 'away_score': 0},
    {'home_team': 'Portsmouth', 'away_team': 'Sheffield Wednesday', 'home_score': 1, 'away_score': 0},
    {'home_team': 'Hull City', 'away_team': 'Southampton', 'home_score': 2, 'away_score': 1},
    {'home_team': 'Queens Park Rangers', 'away_team': 'Stoke City', 'home_score': 0, 'away_score': 0},
    {'home_team': 'Norwich City', 'away_team': 'Wrexham', 'home_score': 2, 'away_score': 1},
    {'home_team': 'Swansea City', 'away_team': 'Birmingham City', 'home_score': 1, 'away_score': 1},
    # Eredivisie
    {'home_team': 'NAC Breda', 'away_team': 'NEC Nijmegen', 'home_score': 2, 'away_score': 3},
    {'home_team': 'Ajax', 'away_team': 'Go Ahead Eagles', 'home_score': 2, 'away_score': 2},
    {'home_team': 'PEC Zwolle', 'away_team': 'AZ Alkmaar', 'home_score': 3, 'away_score': 1},
    {'home_team': 'Excelsior', 'away_team': 'Telstar', 'home_score': 2, 'away_score': 2},
    {'home_team': 'Fortuna Sittard', 'away_team': 'PSV Eindhoven', 'home_score': 1, 'away_score': 2},
    # Portugal
    {'home_team': 'Rio Ave', 'away_team': 'Benfica', 'home_score': 0, 'away_score': 2},
    {'home_team': 'Alverca', 'away_team': 'Moreirense', 'home_score': 2, 'away_score': 1},
    {'home_team': 'AVS Futebol SAD', 'away_team': 'Arouca', 'home_score': 0, 'away_score': 1},
]

if __name__ == '__main__':
    print("=" * 60)
    print("Updating database with ESPN results...")
    print("=" * 60)

    all_results = JAN_16_RESULTS + JAN_17_RESULTS
    print(f"\nTotal results to process: {len(all_results)}")

    stats = update_from_known_results(all_results)

    print("\n" + "=" * 60)
    print("Summary:")
    print(f"  Matched: {stats['matched']}")
    print(f"  Updated: {stats['updated']}")
    print(f"  Not found: {len(stats['not_found'])}")

    if stats['not_found']:
        print("\nMatches not found in database:")
        for m in stats['not_found'][:10]:
            print(f"  - {m}")
        if len(stats['not_found']) > 10:
            print(f"  ... and {len(stats['not_found']) - 10} more")
