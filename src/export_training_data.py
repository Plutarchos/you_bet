"""Export historical odds data to Parquet and CSV for training/analysis."""
import sqlite3
from pathlib import Path
from datetime import datetime

import pandas as pd

from .config import DB_PATH


def export_historical_parquet(output_path: Path = None) -> Path:
    """Export all historical odds snapshots to Parquet format.

    Each row contains a single odds snapshot from a bookmaker for a match,
    along with the match details and result (if available).

    Args:
        output_path: Output file path. Defaults to data/historical_odds.parquet

    Returns:
        Path to the created Parquet file.
    """
    if output_path is None:
        output_path = Path(__file__).parent.parent / "data" / "historical_odds.parquet"

    conn = sqlite3.connect(DB_PATH)

    # Query all odds with match and result info
    query = """
        SELECT
            o.id as odds_id,
            o.scraped_at,
            o.home_win,
            o.draw,
            o.away_win,
            o.source as odds_source,
            b.key as bookmaker_key,
            b.name as bookmaker_name,
            m.id as match_id,
            m.home_team,
            m.away_team,
            m.commence_time,
            m.status as match_status,
            l.key as league_key,
            l.name as league_name,
            r.home_score,
            r.away_score,
            r.outcome,
            r.recorded_at as result_recorded_at
        FROM odds o
        JOIN bookmakers b ON o.bookmaker_id = b.id
        JOIN matches m ON o.match_id = m.id
        JOIN leagues l ON m.league_id = l.id
        LEFT JOIN results r ON r.match_id = m.id
        ORDER BY o.scraped_at, m.id, b.name
    """

    df = pd.read_sql_query(query, conn)
    conn.close()

    # Convert datetime columns
    df['scraped_at'] = pd.to_datetime(df['scraped_at'])
    df['commence_time'] = pd.to_datetime(df['commence_time'])
    df['result_recorded_at'] = pd.to_datetime(df['result_recorded_at'])

    # Calculate implied probabilities
    df['home_prob'] = 1 / df['home_win']
    df['draw_prob'] = 1 / df['draw']
    df['away_prob'] = 1 / df['away_win']

    # Calculate overround (total implied probability)
    df['overround'] = df['home_prob'] + df['draw_prob'] + df['away_prob']

    # Add a run_id based on scraped_at date (for grouping snapshots)
    df['run_date'] = df['scraped_at'].dt.date

    # Save to Parquet
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)

    print(f"Exported {len(df)} odds records to {output_path}")
    print(f"  Matches: {df['match_id'].nunique()}")
    print(f"  Bookmakers: {df['bookmaker_key'].nunique()}")
    print(f"  Date range: {df['scraped_at'].min()} to {df['scraped_at'].max()}")

    return output_path


def export_training_csv(output_path: Path = None) -> Path:
    """Export completed matches with results and best odds to CSV.

    Only includes matches that have results (completed matches).
    Uses best odds ever recorded for each bookmaker.

    Args:
        output_path: Output file path. Defaults to website/public/data/training_data.csv

    Returns:
        Path to the created CSV file.
    """
    if output_path is None:
        output_path = Path(__file__).parent.parent / "website" / "public" / "data" / "training_data.csv"

    conn = sqlite3.connect(DB_PATH)

    # Get completed matches with results and best odds per bookmaker
    query = """
        SELECT
            m.id as match_id,
            m.home_team,
            m.away_team,
            m.commence_time,
            l.name as league,
            r.home_score,
            r.away_score,
            r.outcome,
            b.name as bookmaker,
            MAX(o.home_win) as home_odds,
            MAX(o.draw) as draw_odds,
            MAX(o.away_win) as away_odds
        FROM results r
        JOIN matches m ON r.match_id = m.id
        JOIN leagues l ON m.league_id = l.id
        JOIN odds o ON o.match_id = m.id
        JOIN bookmakers b ON o.bookmaker_id = b.id
        WHERE o.home_win > 1.02 AND o.draw > 1.02 AND o.away_win > 1.02
        GROUP BY m.id, b.id
        HAVING (1.0/home_odds + 1.0/draw_odds + 1.0/away_odds) BETWEEN 1.0 AND 1.5
        ORDER BY m.commence_time DESC, m.id, b.name
    """

    df = pd.read_sql_query(query, conn)
    conn.close()

    if len(df) == 0:
        print("No training data to export (no completed matches)")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        # Write empty CSV with headers
        pd.DataFrame(columns=[
            'match_id', 'home_team', 'away_team', 'commence_time', 'league',
            'home_score', 'away_score', 'outcome', 'bookmaker',
            'home_odds', 'draw_odds', 'away_odds',
            'home_prob', 'draw_prob', 'away_prob'
        ]).to_csv(output_path, index=False)
        return output_path

    # Calculate implied probabilities
    df['home_prob'] = (1 / df['home_odds']).round(4)
    df['draw_prob'] = (1 / df['draw_odds']).round(4)
    df['away_prob'] = (1 / df['away_odds']).round(4)

    # Round odds
    df['home_odds'] = df['home_odds'].round(2)
    df['draw_odds'] = df['draw_odds'].round(2)
    df['away_odds'] = df['away_odds'].round(2)

    # Save to CSV
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)

    matches_count = df['match_id'].nunique()
    print(f"Exported {len(df)} rows ({matches_count} matches) to {output_path}")

    return output_path


def export_csv_for_website(output_path: Path = None) -> Path:
    """Export latest odds summary to CSV for website download.

    Creates a user-friendly CSV with the latest odds for each match,
    including probability calculations and best bookmaker info.

    Args:
        output_path: Output file path. Defaults to website/public/data/odds_summary.csv

    Returns:
        Path to the created CSV file.
    """
    if output_path is None:
        output_path = Path(__file__).parent.parent / "website" / "public" / "data" / "odds_summary.csv"

    conn = sqlite3.connect(DB_PATH)

    # Get latest odds for each match/bookmaker combination
    query = """
        WITH latest_odds AS (
            SELECT
                o.*,
                ROW_NUMBER() OVER (
                    PARTITION BY o.match_id, o.bookmaker_id
                    ORDER BY o.scraped_at DESC
                ) as rn
            FROM odds o
        ),
        match_odds AS (
            SELECT
                m.id as match_id,
                m.home_team,
                m.away_team,
                m.commence_time,
                m.status,
                l.name as league,
                COUNT(DISTINCT lo.bookmaker_id) as bookmaker_count,
                MAX(lo.home_win) as best_home_odds,
                MAX(lo.draw) as best_draw_odds,
                MAX(lo.away_win) as best_away_odds,
                AVG(lo.home_win) as avg_home_odds,
                AVG(lo.draw) as avg_draw_odds,
                AVG(lo.away_win) as avg_away_odds,
                r.home_score,
                r.away_score,
                r.outcome
            FROM matches m
            JOIN leagues l ON m.league_id = l.id
            JOIN latest_odds lo ON lo.match_id = m.id AND lo.rn = 1
            LEFT JOIN results r ON r.match_id = m.id
            GROUP BY m.id
            HAVING bookmaker_count > 5
        )
        SELECT * FROM match_odds
        ORDER BY commence_time DESC
    """

    df = pd.read_sql_query(query, conn)
    conn.close()

    if len(df) == 0:
        print("No data to export")
        return output_path

    # Calculate implied probabilities from best odds
    df['best_home_prob'] = 1 / df['best_home_odds']
    df['best_draw_prob'] = 1 / df['best_draw_odds']
    df['best_away_prob'] = 1 / df['best_away_odds']

    # Calculate median probabilities from average odds
    df['avg_home_prob'] = 1 / df['avg_home_odds']
    df['avg_draw_prob'] = 1 / df['avg_draw_odds']
    df['avg_away_prob'] = 1 / df['avg_away_odds']

    # Value differences
    df['home_value_diff'] = df['avg_home_prob'] - df['best_home_prob']
    df['draw_value_diff'] = df['avg_draw_prob'] - df['best_draw_prob']
    df['away_value_diff'] = df['avg_away_prob'] - df['best_away_prob']

    # Round numeric columns
    numeric_cols = [
        'best_home_odds', 'best_draw_odds', 'best_away_odds',
        'avg_home_odds', 'avg_draw_odds', 'avg_away_odds',
        'best_home_prob', 'best_draw_prob', 'best_away_prob',
        'avg_home_prob', 'avg_draw_prob', 'avg_away_prob',
        'home_value_diff', 'draw_value_diff', 'away_value_diff'
    ]
    for col in numeric_cols:
        df[col] = df[col].round(4)

    # Reorder columns for clarity
    column_order = [
        'match_id', 'home_team', 'away_team', 'league', 'commence_time', 'status',
        'bookmaker_count',
        'best_home_odds', 'best_draw_odds', 'best_away_odds',
        'avg_home_odds', 'avg_draw_odds', 'avg_away_odds',
        'best_home_prob', 'best_draw_prob', 'best_away_prob',
        'avg_home_prob', 'avg_draw_prob', 'avg_away_prob',
        'home_value_diff', 'draw_value_diff', 'away_value_diff',
        'home_score', 'away_score', 'outcome'
    ]
    df = df[column_order]

    # Save to CSV
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)

    print(f"Exported {len(df)} matches to {output_path}")
    print(f"  Upcoming: {len(df[df['status'] == 'upcoming'])}")
    print(f"  Completed: {len(df[df['status'] == 'completed'])}")

    return output_path


def export_all(data_dir: Path = None) -> dict:
    """Export Parquet and CSV files.

    Args:
        data_dir: Base directory for exports. Defaults to project root.

    Returns:
        Dict with paths to created files.
    """
    if data_dir is None:
        data_dir = Path(__file__).parent.parent

    parquet_path = export_historical_parquet(data_dir / "data" / "historical_odds.parquet")
    csv_path = export_csv_for_website(data_dir / "website" / "public" / "data" / "odds_summary.csv")
    training_csv_path = export_training_csv(data_dir / "website" / "public" / "data" / "training_data.csv")

    return {
        "parquet": parquet_path,
        "csv": csv_path,
        "training_csv": training_csv_path,
    }


if __name__ == "__main__":
    export_all()
