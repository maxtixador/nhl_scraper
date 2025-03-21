"""Test module for NHL standings data scraping functionality."""

from datetime import date, timedelta

import pandas as pd
import pytest

from nhl_scraper.scraper.standings import scrapeLeagueStandings


@pytest.fixture
def valid_date():
    return (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")


def test_scrapeLeagueStandings_basic(valid_date):
    """Test basic functionality of scrapeLeagueStandings."""
    standings_df = scrapeLeagueStandings(valid_date)

    # Validate that the result is a non-empty DataFrame
    assert isinstance(standings_df, pd.DataFrame)
    assert not standings_df.empty
    assert standings_df.shape[0] >= 32  # NHL has 32 teams

    # Check required columns
    required_columns = {
        "conferenceAbbrev",
        "divisionAbbrev",
        "teamName",
        "gamesPlayed",
        "points",
        "wins",
        "losses",
    }
    assert required_columns.issubset(standings_df.columns)


def test_scrapeLeagueStandings_invalid_date():
    """Test scrapeLeagueStandings with invalid date."""
    with pytest.raises(ValueError):
        scrapeLeagueStandings("2023-13-45")  # Invalid date


def test_scrapeLeagueStandings_future_date():
    """Test scrapeLeagueStandings with future date."""
    future_date = (date.today() + timedelta(days=365)).strftime("%Y-%m-%d")
    standings_df = scrapeLeagueStandings(future_date)
    assert isinstance(standings_df, pd.DataFrame)


def test_scrapeLeagueStandings_data_types(valid_date):
    """Test data types of scrapeLeagueStandings output."""
    standings_df = scrapeLeagueStandings(valid_date)

    # Check numeric columns
    numeric_columns = ["gamesPlayed", "points", "wins", "losses", "otLosses"]
    for col in numeric_columns:
        assert pd.api.types.is_numeric_dtype(standings_df[col])

    # Check string columns
    string_columns = ["teamName", "conferenceAbbrev", "divisionAbbrev"]
    for col in string_columns:
        assert pd.api.types.is_string_dtype(standings_df[col])
