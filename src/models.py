"""Data models for the odds scraper."""
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class League:
    """Represents a football league/competition."""
    id: Optional[int]
    key: str
    name: str
    country: Optional[str] = None


@dataclass
class Bookmaker:
    """Represents a betting company."""
    id: Optional[int]
    key: str
    name: str


@dataclass
class Match:
    """Represents a football match."""
    id: Optional[int]
    league_id: int
    home_team: str
    away_team: str
    commence_time: datetime
    status: str = "upcoming"  # upcoming, live, completed
    external_id: Optional[str] = None


@dataclass
class Odds:
    """Represents odds for a match from a single bookmaker at a point in time."""
    id: Optional[int]
    match_id: int
    bookmaker_id: int
    home_win: float
    draw: Optional[float]
    away_win: float
    source: str  # 'odds_api' or 'oddschecker'
    scraped_at: datetime


@dataclass
class Result:
    """Represents the final result of a match."""
    id: Optional[int]
    match_id: int
    home_score: int
    away_score: int
    outcome: str  # 'home', 'draw', 'away'
    source: Optional[str] = None
    recorded_at: Optional[datetime] = None
