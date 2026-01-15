"""Database connection and operations for the odds scraper."""
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Generator, Optional, List

from .config import DB_PATH
from .models import League, Bookmaker, Match, Odds, Result


def get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """Create a database connection with row factory enabled."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


@contextmanager
def transaction(conn: sqlite3.Connection) -> Generator[sqlite3.Connection, None, None]:
    """Context manager for database transactions."""
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def init_database(conn: sqlite3.Connection) -> None:
    """Initialize the database schema."""
    conn.executescript("""
        -- Bookmakers reference table
        CREATE TABLE IF NOT EXISTS bookmakers (
            id INTEGER PRIMARY KEY,
            key TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL
        );

        -- Leagues
        CREATE TABLE IF NOT EXISTS leagues (
            id INTEGER PRIMARY KEY,
            key TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            country TEXT
        );

        -- Matches/fixtures
        CREATE TABLE IF NOT EXISTS matches (
            id INTEGER PRIMARY KEY,
            league_id INTEGER REFERENCES leagues(id),
            home_team TEXT NOT NULL,
            away_team TEXT NOT NULL,
            commence_time DATETIME,
            status TEXT DEFAULT 'upcoming',
            external_id TEXT,
            UNIQUE(home_team, away_team, commence_time)
        );

        -- Match results (final scores)
        CREATE TABLE IF NOT EXISTS results (
            id INTEGER PRIMARY KEY,
            match_id INTEGER UNIQUE REFERENCES matches(id),
            home_score INTEGER NOT NULL,
            away_score INTEGER NOT NULL,
            outcome TEXT NOT NULL,
            source TEXT,
            recorded_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        -- Historical odds snapshots
        CREATE TABLE IF NOT EXISTS odds (
            id INTEGER PRIMARY KEY,
            match_id INTEGER REFERENCES matches(id),
            bookmaker_id INTEGER REFERENCES bookmakers(id),
            home_win REAL,
            draw REAL,
            away_win REAL,
            source TEXT,
            scraped_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        -- Indexes
        CREATE INDEX IF NOT EXISTS idx_odds_match ON odds(match_id);
        CREATE INDEX IF NOT EXISTS idx_odds_time ON odds(scraped_at);
        CREATE INDEX IF NOT EXISTS idx_matches_status ON matches(status);
        CREATE INDEX IF NOT EXISTS idx_results_match ON results(match_id);

        -- View: Compare final odds to actual results
        CREATE VIEW IF NOT EXISTS odds_vs_results AS
        SELECT
            m.home_team,
            m.away_team,
            m.commence_time,
            l.name as league,
            r.home_score,
            r.away_score,
            r.outcome,
            b.name as bookmaker,
            o.home_win as home_odds,
            o.draw as draw_odds,
            o.away_win as away_odds,
            CASE r.outcome
                WHEN 'home' THEN o.home_win
                WHEN 'draw' THEN o.draw
                WHEN 'away' THEN o.away_win
            END as winning_odds
        FROM matches m
        JOIN results r ON r.match_id = m.id
        JOIN leagues l ON m.league_id = l.id
        JOIN odds o ON o.match_id = m.id
        JOIN bookmakers b ON o.bookmaker_id = b.id
        WHERE o.scraped_at = (
            SELECT MAX(scraped_at) FROM odds
            WHERE match_id = m.id AND bookmaker_id = o.bookmaker_id
        );
    """)
    conn.commit()


# League operations
def get_or_create_league(conn: sqlite3.Connection, key: str, name: str, country: Optional[str] = None) -> int:
    """Get existing league or create new one, returning ID."""
    cursor = conn.execute("SELECT id FROM leagues WHERE key = ?", (key,))
    row = cursor.fetchone()
    if row:
        return row["id"]

    cursor = conn.execute(
        "INSERT INTO leagues (key, name, country) VALUES (?, ?, ?)",
        (key, name, country)
    )
    conn.commit()
    return cursor.lastrowid


def get_league_by_key(conn: sqlite3.Connection, key: str) -> Optional[League]:
    """Get a league by its API key."""
    cursor = conn.execute("SELECT * FROM leagues WHERE key = ?", (key,))
    row = cursor.fetchone()
    if row:
        return League(id=row["id"], key=row["key"], name=row["name"], country=row["country"])
    return None


# Bookmaker operations
def get_or_create_bookmaker(conn: sqlite3.Connection, key: str, name: str) -> int:
    """Get existing bookmaker or create new one, returning ID."""
    cursor = conn.execute("SELECT id FROM bookmakers WHERE key = ?", (key,))
    row = cursor.fetchone()
    if row:
        return row["id"]

    cursor = conn.execute(
        "INSERT INTO bookmakers (key, name) VALUES (?, ?)",
        (key, name)
    )
    conn.commit()
    return cursor.lastrowid


def get_all_bookmakers(conn: sqlite3.Connection) -> List[Bookmaker]:
    """Get all bookmakers."""
    cursor = conn.execute("SELECT * FROM bookmakers")
    return [Bookmaker(id=row["id"], key=row["key"], name=row["name"]) for row in cursor.fetchall()]


# Match operations
def get_or_create_match(
    conn: sqlite3.Connection,
    league_id: int,
    home_team: str,
    away_team: str,
    commence_time: datetime,
    external_id: Optional[str] = None
) -> int:
    """Get existing match or create new one, returning ID."""
    cursor = conn.execute(
        "SELECT id FROM matches WHERE home_team = ? AND away_team = ? AND commence_time = ?",
        (home_team, away_team, commence_time.isoformat())
    )
    row = cursor.fetchone()
    if row:
        # Update external_id if provided and not set
        if external_id:
            conn.execute(
                "UPDATE matches SET external_id = ? WHERE id = ? AND external_id IS NULL",
                (external_id, row["id"])
            )
            conn.commit()
        return row["id"]

    cursor = conn.execute(
        "INSERT INTO matches (league_id, home_team, away_team, commence_time, external_id) VALUES (?, ?, ?, ?, ?)",
        (league_id, home_team, away_team, commence_time.isoformat(), external_id)
    )
    conn.commit()
    return cursor.lastrowid


def get_match_by_id(conn: sqlite3.Connection, match_id: int) -> Optional[Match]:
    """Get a match by its ID."""
    cursor = conn.execute("SELECT * FROM matches WHERE id = ?", (match_id,))
    row = cursor.fetchone()
    if row:
        return Match(
            id=row["id"],
            league_id=row["league_id"],
            home_team=row["home_team"],
            away_team=row["away_team"],
            commence_time=datetime.fromisoformat(row["commence_time"]),
            status=row["status"],
            external_id=row["external_id"]
        )
    return None


def get_matches_by_status(conn: sqlite3.Connection, status: str) -> List[Match]:
    """Get all matches with a given status."""
    cursor = conn.execute("SELECT * FROM matches WHERE status = ?", (status,))
    return [
        Match(
            id=row["id"],
            league_id=row["league_id"],
            home_team=row["home_team"],
            away_team=row["away_team"],
            commence_time=datetime.fromisoformat(row["commence_time"]),
            status=row["status"],
            external_id=row["external_id"]
        )
        for row in cursor.fetchall()
    ]


def update_match_status(conn: sqlite3.Connection, match_id: int, status: str) -> None:
    """Update the status of a match."""
    conn.execute("UPDATE matches SET status = ? WHERE id = ?", (status, match_id))
    conn.commit()


# Odds operations
def insert_odds(
    conn: sqlite3.Connection,
    match_id: int,
    bookmaker_id: int,
    home_win: float,
    draw: Optional[float],
    away_win: float,
    source: str
) -> int:
    """Insert a new odds snapshot."""
    cursor = conn.execute(
        "INSERT INTO odds (match_id, bookmaker_id, home_win, draw, away_win, source) VALUES (?, ?, ?, ?, ?, ?)",
        (match_id, bookmaker_id, home_win, draw, away_win, source)
    )
    conn.commit()
    return cursor.lastrowid


def odds_changed(
    conn: sqlite3.Connection,
    match_id: int,
    bookmaker_id: int,
    home_win: float,
    draw: Optional[float],
    away_win: float
) -> bool:
    """Check if odds have changed from the last snapshot."""
    cursor = conn.execute(
        """
        SELECT home_win, draw, away_win FROM odds
        WHERE match_id = ? AND bookmaker_id = ?
        ORDER BY scraped_at DESC LIMIT 1
        """,
        (match_id, bookmaker_id)
    )
    row = cursor.fetchone()
    if row is None:
        return True  # No previous odds, always insert

    # Compare with small tolerance for floating point
    tolerance = 0.001
    if abs((row["home_win"] or 0) - home_win) > tolerance:
        return True
    if draw is not None and row["draw"] is not None:
        if abs(row["draw"] - draw) > tolerance:
            return True
    if abs((row["away_win"] or 0) - away_win) > tolerance:
        return True

    return False


def get_latest_odds_for_match(conn: sqlite3.Connection, match_id: int) -> List[dict]:
    """Get the most recent odds for a match from all bookmakers."""
    cursor = conn.execute(
        """
        SELECT o.*, b.name as bookmaker_name
        FROM odds o
        JOIN bookmakers b ON o.bookmaker_id = b.id
        WHERE o.match_id = ?
        AND o.scraped_at = (
            SELECT MAX(scraped_at) FROM odds
            WHERE match_id = o.match_id AND bookmaker_id = o.bookmaker_id
        )
        ORDER BY b.name
        """,
        (match_id,)
    )
    return [dict(row) for row in cursor.fetchall()]


# Result operations
def insert_result(
    conn: sqlite3.Connection,
    match_id: int,
    home_score: int,
    away_score: int,
    source: Optional[str] = None
) -> int:
    """Insert a match result."""
    # Determine outcome
    if home_score > away_score:
        outcome = "home"
    elif away_score > home_score:
        outcome = "away"
    else:
        outcome = "draw"

    cursor = conn.execute(
        """
        INSERT OR REPLACE INTO results (match_id, home_score, away_score, outcome, source)
        VALUES (?, ?, ?, ?, ?)
        """,
        (match_id, home_score, away_score, outcome, source)
    )

    # Update match status to completed
    conn.execute("UPDATE matches SET status = 'completed' WHERE id = ?", (match_id,))
    conn.commit()
    return cursor.lastrowid


def get_result_by_match(conn: sqlite3.Connection, match_id: int) -> Optional[Result]:
    """Get the result for a match."""
    cursor = conn.execute("SELECT * FROM results WHERE match_id = ?", (match_id,))
    row = cursor.fetchone()
    if row:
        return Result(
            id=row["id"],
            match_id=row["match_id"],
            home_score=row["home_score"],
            away_score=row["away_score"],
            outcome=row["outcome"],
            source=row["source"],
            recorded_at=datetime.fromisoformat(row["recorded_at"]) if row["recorded_at"] else None
        )
    return None


# Analysis queries
def get_odds_vs_results(conn: sqlite3.Connection, limit: int = 100) -> List[dict]:
    """Get odds compared to actual results."""
    cursor = conn.execute(
        "SELECT * FROM odds_vs_results ORDER BY commence_time DESC LIMIT ?",
        (limit,)
    )
    return [dict(row) for row in cursor.fetchall()]


def get_stats_by_bookmaker(conn: sqlite3.Connection) -> List[dict]:
    """Get average winning odds by bookmaker."""
    cursor = conn.execute(
        """
        SELECT bookmaker, AVG(winning_odds) as avg_winning_odds, COUNT(*) as matches
        FROM odds_vs_results
        WHERE winning_odds IS NOT NULL
        GROUP BY bookmaker
        ORDER BY avg_winning_odds DESC
        """
    )
    return [dict(row) for row in cursor.fetchall()]
