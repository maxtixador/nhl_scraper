"""Test module for specific NHL data scraping cases."""

import pandas as pd
import pytest
import requests

from nhl_scraper.scraper.draft import scrapeDraft
from nhl_scraper.scraper.game import scrapeGameComplete
from nhl_scraper.scraper.player import scrapePlayerProfile
from nhl_scraper.scraper.standings import scrapeLeagueStandings
from nhl_scraper.scraper.teams import scrapeTeamRoster, scrapeTeamSchedule, scrapeTeamStats


# Test fixtures for specific cases
@pytest.fixture
def specific_player_id():
    return 8481540  # Cole Caufield


@pytest.fixture
def specific_standings_date():
    return "2024-11-06"


@pytest.fixture
def specific_game_id():
    return 2024020447


@pytest.fixture
def specific_team():
    return "MTL"


@pytest.fixture
def specific_season():
    return "20232024"


@pytest.fixture
def specific_draft_year():
    return 2023


# Player Profile Tests
def test_specific_player_profile(specific_player_id):
    """Test scrapePlayerProfile with Cole Caufield's ID."""
    player_data = scrapePlayerProfile(specific_player_id)

    assert isinstance(player_data, dict)
    assert player_data["playerId"] == specific_player_id
    assert player_data["fullName"] == "Cole Caufield"
    assert player_data["position"] == "RW"
    assert player_data["currentTeamAbbrev"] == "MTL"


# Standings Tests
def test_specific_standings_date(specific_standings_date):
    """Test scrapeLeagueStandings with specific date."""
    try:
        standings_df = scrapeLeagueStandings(specific_standings_date)
        assert isinstance(standings_df, pd.DataFrame)
        assert not standings_df.empty
        assert "standingsDate" in standings_df.columns
        assert standings_df["standingsDate"].iloc[0] == specific_standings_date
    except requests.HTTPError as e:
        if "404" in str(e):
            pytest.skip("Standings data not available for future date")


# Game Tests
def test_specific_game(specific_game_id):
    """Test scrapeGameComplete with specific game ID."""
    try:
        pbp_df, rosters_df, shifts_df = scrapeGameComplete(specific_game_id)

        # Test Play-by-Play data
        assert isinstance(pbp_df, pd.DataFrame)
        assert "gameId" in pbp_df.columns
        assert pbp_df["gameId"].iloc[0] == specific_game_id

        # Test Rosters data
        assert isinstance(rosters_df, pd.DataFrame)
        assert "playerId" in rosters_df.columns

        # Test Shifts data
        assert isinstance(shifts_df, pd.DataFrame)
        assert "playerId" in shifts_df.columns
    except requests.HTTPError as e:
        if "404" in str(e):
            pytest.skip("Game data not available for future game")


# Team Tests
def test_specific_team_roster(specific_team, specific_season):
    """Test scrapeTeamRoster with Montreal Canadiens."""
    roster_df = scrapeTeamRoster(specific_team, specific_season)

    assert isinstance(roster_df, pd.DataFrame)
    assert not roster_df.empty
    assert roster_df["team"].iloc[0] == specific_team
    assert roster_df["season"].iloc[0] == specific_season


def test_specific_team_stats(specific_team, specific_season):
    """Test scrapeTeamStats with Montreal Canadiens."""
    # Test skater stats
    skater_stats = scrapeTeamStats(specific_team, specific_season, goalies=False)
    assert isinstance(skater_stats, pd.DataFrame)
    assert not skater_stats.empty
    assert "goals" in skater_stats.columns

    # Test goalie stats
    goalie_stats = scrapeTeamStats(specific_team, specific_season, goalies=True)
    assert isinstance(goalie_stats, pd.DataFrame)
    assert "savePct" in goalie_stats.columns


def test_specific_team_schedule(specific_team, specific_season):
    """Test scrapeTeamSchedule with Montreal Canadiens."""
    schedule_df = scrapeTeamSchedule(specific_team, specific_season)

    assert isinstance(schedule_df, pd.DataFrame)
    assert not schedule_df.empty
    assert schedule_df["teamAbbrev"].iloc[0] == specific_team
    assert schedule_df["season"].iloc[0] == specific_season


# Draft Tests
def test_specific_draft_year(specific_draft_year):
    """Test scrapeDraft with 2023 draft."""
    draft_df = scrapeDraft(year=specific_draft_year)

    assert isinstance(draft_df, pd.DataFrame)
    assert not draft_df.empty
    assert draft_df["draftYear"].iloc[0] == specific_draft_year

    # Test first overall pick
    first_pick = draft_df[(draft_df["round"] == 1) & (draft_df["pickInRound"] == 1)].iloc[0]
    assert first_pick["overallPick"] == 1
    assert first_pick["teamAbbrev"] == "CHI"  # Blackhawks had 1st overall in 2023


# Data Quality Tests
def test_data_quality_checks():
    """Test data quality for specific cases."""

    def check_df_quality(df: pd.DataFrame, name: str):
        """Helper function to check DataFrame quality."""
        assert not df.empty, f"{name} DataFrame is empty"
        assert not df.isnull().all().any(), f"{name} has columns with all null values"
        assert df["meta_datetime"].notna().all(), f"{name} missing metadata timestamps"

    # Test roster quality
    roster_df = scrapeTeamRoster("MTL", "20232024")
    check_df_quality(roster_df, "Roster")

    # Test stats quality
    stats_df = scrapeTeamStats("MTL", "20232024", goalies=False)
    check_df_quality(stats_df, "Stats")

    # Test schedule quality
    schedule_df = scrapeTeamSchedule("MTL", "20232024")
    check_df_quality(schedule_df, "Schedule")


# Error Handling Tests
@pytest.mark.parametrize(
    "invalid_input,expected_error",
    [
        ((99999999,), requests.HTTPError),  # Invalid player ID
        (("INVALID", "20232024"), ValueError),  # Invalid team
        (("MTL", "2023"), ValueError),  # Invalid season format
    ],
)
def test_error_handling(invalid_input, expected_error):
    """Test error handling for invalid inputs."""
    with pytest.raises(expected_error):
        if len(invalid_input) == 1:
            scrapePlayerProfile(invalid_input[0])
        else:
            scrapeTeamRoster(*invalid_input)
