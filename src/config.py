"""Configuration and settings for the odds scraper."""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Base paths
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "odds.db"

# Ensure data directory exists
DATA_DIR.mkdir(exist_ok=True)

# API Configuration
ODDS_API_KEY = os.getenv("ODDS_API_KEY", "")
ODDS_API_BASE_URL = "https://api.the-odds-api.com/v4"

# Rate limiting
API_REQUESTS_PER_MINUTE = 10
SCRAPER_DELAY_SECONDS = 3  # Delay between scraper requests

# Default regions for odds (uk only - for UK bookmakers and exchanges)
DEFAULT_REGIONS = ["uk"]

# League mappings between The Odds API and oddschecker (25 leagues)
LEAGUES = {
    # England
    "soccer_epl": {
        "name": "English Premier League",
        "country": "England",
        "oddschecker_path": "/football/english/premier-league",
    },
    "soccer_efl_champ": {
        "name": "EFL Championship",
        "country": "England",
        "oddschecker_path": "/football/english/championship",
    },
    "soccer_england_league1": {
        "name": "EFL League One",
        "country": "England",
        "oddschecker_path": "/football/english/league-1",
    },
    "soccer_england_league2": {
        "name": "EFL League Two",
        "country": "England",
        "oddschecker_path": "/football/english/league-2",
    },
    "soccer_fa_cup": {
        "name": "FA Cup",
        "country": "England",
        "oddschecker_path": "/football/english/fa-cup",
    },
    "soccer_efl_cup": {
        "name": "EFL Cup",
        "country": "England",
        "oddschecker_path": "/football/english/league-cup",
    },
    # Scotland
    "soccer_scotland_premiership": {
        "name": "Scottish Premiership",
        "country": "Scotland",
        "oddschecker_path": "/football/scotland/premiership",
    },
    # Top 5 European Leagues
    "soccer_germany_bundesliga": {
        "name": "German Bundesliga",
        "country": "Germany",
        "oddschecker_path": "/football/germany/bundesliga",
    },
    "soccer_spain_la_liga": {
        "name": "La Liga",
        "country": "Spain",
        "oddschecker_path": "/football/spain/la-liga",
    },
    "soccer_italy_serie_a": {
        "name": "Serie A",
        "country": "Italy",
        "oddschecker_path": "/football/italy/serie-a",
    },
    "soccer_france_ligue_one": {
        "name": "Ligue 1",
        "country": "France",
        "oddschecker_path": "/football/france/ligue-1",
    },
    # Other European Leagues
    "soccer_netherlands_eredivisie": {
        "name": "Eredivisie",
        "country": "Netherlands",
        "oddschecker_path": "/football/netherlands/eredivisie",
    },
    "soccer_portugal_primeira_liga": {
        "name": "Primeira Liga",
        "country": "Portugal",
        "oddschecker_path": "/football/portugal/primeira-liga",
    },
    "soccer_belgium_first_div": {
        "name": "Belgian Pro League",
        "country": "Belgium",
        "oddschecker_path": "/football/belgium/first-division-a",
    },
    "soccer_turkey_super_league": {
        "name": "Turkish Super Lig",
        "country": "Turkey",
        "oddschecker_path": "/football/turkey/super-lig",
    },
    "soccer_greece_super_league": {
        "name": "Greek Super League",
        "country": "Greece",
        "oddschecker_path": "/football/greece/super-league",
    },
    "soccer_austria_bundesliga": {
        "name": "Austrian Bundesliga",
        "country": "Austria",
        "oddschecker_path": "/football/austria/bundesliga",
    },
    "soccer_switzerland_superleague": {
        "name": "Swiss Super League",
        "country": "Switzerland",
        "oddschecker_path": "/football/switzerland/super-league",
    },
    # Nordic Leagues
    "soccer_denmark_superliga": {
        "name": "Danish Superliga",
        "country": "Denmark",
        "oddschecker_path": "/football/denmark/superliga",
    },
    "soccer_sweden_allsvenskan": {
        "name": "Swedish Allsvenskan",
        "country": "Sweden",
        "oddschecker_path": "/football/sweden/allsvenskan",
    },
    "soccer_norway_eliteserien": {
        "name": "Norwegian Eliteserien",
        "country": "Norway",
        "oddschecker_path": "/football/norway/eliteserien",
    },
    # UEFA Competitions
    "soccer_uefa_champs_league": {
        "name": "UEFA Champions League",
        "country": "Europe",
        "oddschecker_path": "/football/champions-league",
    },
    "soccer_uefa_europa_league": {
        "name": "UEFA Europa League",
        "country": "Europe",
        "oddschecker_path": "/football/europa-league",
    },
    "soccer_uefa_europa_conference_league": {
        "name": "UEFA Conference League",
        "country": "Europe",
        "oddschecker_path": "/football/europa-conference-league",
    },
    # South America
    "soccer_brazil_campeonato": {
        "name": "Brazilian Serie A",
        "country": "Brazil",
        "oddschecker_path": "/football/brazil/serie-a",
    },
}

# Bookmaker key mappings from oddschecker HTML data attributes
ODDSCHECKER_BOOKMAKERS = {
    "B3": ("bet365", "Bet365"),
    "SK": ("skybet", "Sky Bet"),
    "PP": ("paddypower", "Paddy Power"),
    "WH": ("williamhill", "William Hill"),
    "LD": ("ladbrokes", "Ladbrokes"),
    "CE": ("coral", "Coral"),
    "BF": ("betfair_ex_eu", "Betfair"),
    "UN": ("unibet_eu", "Unibet"),
    "BY": ("betway", "Betway"),
    "FR": ("betfred", "Betfred"),
    "OE": ("888sport", "888sport"),
    "BO": ("boylesports", "BoyleSports"),
    "VB": ("betvictor", "BetVictor"),
}
