"""CLI entry point for the football odds scraper."""
import logging
import sys

import click
from rich.console import Console
from rich.table import Table
from rich.logging import RichHandler

from .config import DB_PATH, LEAGUES, ODDS_API_KEY
from .database import (
    get_connection,
    init_database,
    get_odds_vs_results,
    get_stats_by_bookmaker,
    get_all_bookmakers,
)
from .results import (
    collect_odds_from_api,
    collect_odds_from_oddschecker,
    collect_results_from_api,
    run_full_update,
)
from .odds_api import OddsAPIClient

console = Console()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[RichHandler(console=console, rich_tracebacks=True)]
)
logger = logging.getLogger(__name__)


@click.group()
@click.option("--debug", is_flag=True, help="Enable debug logging")
def cli(debug):
    """Football Odds & Results Scraper CLI."""
    if debug:
        logging.getLogger().setLevel(logging.DEBUG)


@cli.command()
def init():
    """Initialize the database."""
    console.print("[bold]Initializing database...[/bold]")
    conn = get_connection()
    init_database(conn)
    conn.close()
    console.print(f"[green]Database initialized at {DB_PATH}[/green]")


@cli.command("fetch-odds")
@click.option("--leagues", "-l", multiple=True, help="Specific league keys to fetch")
def fetch_odds(leagues):
    """Fetch odds from The Odds API."""
    if not ODDS_API_KEY:
        console.print("[red]Error: ODDS_API_KEY not set. Please set it in your .env file.[/red]")
        sys.exit(1)

    league_keys = list(leagues) if leagues else None

    console.print("[bold]Fetching odds from The Odds API...[/bold]")
    if league_keys:
        console.print(f"Leagues: {', '.join(league_keys)}")

    conn = get_connection()
    init_database(conn)

    client = OddsAPIClient()
    stats = collect_odds_from_api(conn, league_keys, client)

    conn.close()

    console.print("\n[bold green]Collection complete![/bold green]")
    console.print(f"  Matches processed: {stats['matches']}")
    console.print(f"  Odds snapshots saved: {stats['odds_snapshots']}")
    console.print(f"  Errors: {stats['errors']}")

    if client.requests_remaining is not None:
        console.print(f"\n[dim]API quota: {client.requests_remaining} requests remaining[/dim]")


@cli.command("fetch-results")
@click.option("--leagues", "-l", multiple=True, help="Specific league keys to fetch")
@click.option("--days", "-d", default=3, help="Days to look back (max 3)")
def fetch_results(leagues, days):
    """Fetch match results from The Odds API."""
    if not ODDS_API_KEY:
        console.print("[red]Error: ODDS_API_KEY not set. Please set it in your .env file.[/red]")
        sys.exit(1)

    league_keys = list(leagues) if leagues else None

    console.print(f"[bold]Fetching results from the last {days} days...[/bold]")

    conn = get_connection()
    init_database(conn)

    client = OddsAPIClient()
    stats = collect_results_from_api(conn, league_keys, days, client)

    conn.close()

    console.print("\n[bold green]Collection complete![/bold green]")
    console.print(f"  Results saved: {stats['results']}")
    console.print(f"  Errors: {stats['errors']}")

    if client.requests_remaining is not None:
        console.print(f"\n[dim]API quota: {client.requests_remaining} requests remaining[/dim]")


@cli.command("scrape-odds")
@click.option("--leagues", "-l", multiple=True, help="Specific league keys to scrape")
@click.option("--no-headless", is_flag=True, help="Show browser window")
def scrape_odds_cmd(leagues, no_headless):
    """Scrape odds from oddschecker.com."""
    league_keys = list(leagues) if leagues else None
    headless = not no_headless

    console.print("[bold]Scraping odds from oddschecker.com...[/bold]")
    if league_keys:
        console.print(f"Leagues: {', '.join(league_keys)}")

    conn = get_connection()
    init_database(conn)

    stats = collect_odds_from_oddschecker(conn, league_keys, headless)

    conn.close()

    console.print("\n[bold green]Scraping complete![/bold green]")
    console.print(f"  Matches processed: {stats['matches']}")
    console.print(f"  Odds snapshots saved: {stats['odds_snapshots']}")
    console.print(f"  Errors: {stats['errors']}")


@cli.command("update-all")
@click.option("--leagues", "-l", multiple=True, help="Specific league keys")
@click.option("--include-scraper", is_flag=True, help="Also scrape oddschecker")
@click.option("--days", "-d", default=3, help="Days to look back for results")
def update_all(leagues, include_scraper, days):
    """Run full update: fetch odds and results."""
    if not ODDS_API_KEY:
        console.print("[red]Error: ODDS_API_KEY not set. Please set it in your .env file.[/red]")
        sys.exit(1)

    league_keys = list(leagues) if leagues else None

    console.print("[bold]Running full update...[/bold]")

    conn = get_connection()
    stats = run_full_update(conn, league_keys, include_scraper, days)
    conn.close()

    console.print("\n[bold green]Update complete![/bold green]")
    console.print(f"  Matches processed: {stats['matches']}")
    console.print(f"  Odds snapshots saved: {stats['odds_snapshots']}")
    console.print(f"  Results saved: {stats['results']}")
    console.print(f"  Errors: {stats['errors']}")


@cli.command("show-results")
@click.option("--limit", "-n", default=20, help="Number of results to show")
def show_results(limit):
    """Show odds vs actual results."""
    conn = get_connection()
    results = get_odds_vs_results(conn, limit)
    conn.close()

    if not results:
        console.print("[yellow]No results found. Run fetch-odds and fetch-results first.[/yellow]")
        return

    table = Table(title="Odds vs Results")
    table.add_column("Home Team", style="cyan")
    table.add_column("Away Team", style="cyan")
    table.add_column("Score", justify="center")
    table.add_column("Outcome", justify="center")
    table.add_column("Bookmaker")
    table.add_column("Winning Odds", justify="right", style="green")

    for row in results:
        score = f"{row['home_score']}-{row['away_score']}"
        outcome_color = {"home": "green", "draw": "yellow", "away": "red"}.get(row["outcome"], "white")
        table.add_row(
            row["home_team"][:20],
            row["away_team"][:20],
            score,
            f"[{outcome_color}]{row['outcome']}[/{outcome_color}]",
            row["bookmaker"][:15],
            f"{row['winning_odds']:.2f}" if row['winning_odds'] else "-"
        )

    console.print(table)


@cli.command("show-stats")
def show_stats():
    """Show statistics by bookmaker."""
    conn = get_connection()
    stats = get_stats_by_bookmaker(conn)
    conn.close()

    if not stats:
        console.print("[yellow]No statistics available yet.[/yellow]")
        return

    table = Table(title="Average Winning Odds by Bookmaker")
    table.add_column("Bookmaker", style="cyan")
    table.add_column("Avg Winning Odds", justify="right", style="green")
    table.add_column("Matches", justify="right")

    for row in stats:
        table.add_row(
            row["bookmaker"],
            f"{row['avg_winning_odds']:.3f}",
            str(row["matches"])
        )

    console.print(table)


@cli.command("list-leagues")
def list_leagues():
    """List available leagues."""
    table = Table(title="Available Leagues")
    table.add_column("Key", style="cyan")
    table.add_column("Name")
    table.add_column("Country")

    for key, config in LEAGUES.items():
        table.add_row(key, config["name"], config.get("country", "-"))

    console.print(table)


@cli.command("list-bookmakers")
def list_bookmakers():
    """List bookmakers in the database."""
    conn = get_connection()
    bookmakers = get_all_bookmakers(conn)
    conn.close()

    if not bookmakers:
        console.print("[yellow]No bookmakers found. Run fetch-odds first.[/yellow]")
        return

    table = Table(title="Bookmakers")
    table.add_column("ID", justify="right")
    table.add_column("Key", style="cyan")
    table.add_column("Name")

    for bookie in bookmakers:
        table.add_row(str(bookie.id), bookie.key, bookie.name)

    console.print(table)


@cli.command("quota")
def check_quota():
    """Check API quota remaining."""
    if not ODDS_API_KEY:
        console.print("[red]Error: ODDS_API_KEY not set.[/red]")
        sys.exit(1)

    client = OddsAPIClient()
    # Make a free request to get quota info
    try:
        client.get_sports()
        console.print(f"[bold]API Quota Status[/bold]")
        console.print(f"  Requests used: {client.requests_used}")
        console.print(f"  Requests remaining: {client.requests_remaining}")
    except Exception as e:
        console.print(f"[red]Error checking quota: {e}[/red]")


def main():
    """Main entry point."""
    cli()


if __name__ == "__main__":
    main()
