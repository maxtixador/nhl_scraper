"""Test module for NHL player data scraping functionality."""

import pytest
import requests

from nhl_scraper.scraper.player import scrapePlayerProfile


@pytest.fixture
def valid_player_id():
    """Valid player ID for testing."""
    return 8478402  # Connor McDavid


def test_scrapePlayerProfile_basic(valid_player_id):
    """Test basic functionality of scrapePlayerProfile."""
    player_data = scrapePlayerProfile(valid_player_id)

    # Validate that essential fields are present
    assert isinstance(player_data, dict), "Result is not a dictionary"
    assert player_data["playerId"] == valid_player_id

    # Check required fields
    required_fields = {
        "firstName",
        "lastName",
        "fullName",
        "birthDate",
        "position",
        "currentTeamId",
        "currentTeamAbbrev",
    }
    assert all(field in player_data for field in required_fields)


def test_scrapePlayerProfile_invalid_id():
    """Test scrapePlayerProfile with invalid player ID."""
    with pytest.raises(requests.HTTPError):
        scrapePlayerProfile(99999999)


def test_scrapePlayerProfile_data_types(valid_player_id):
    """Test data types of scrapePlayerProfile output."""
    player_data = scrapePlayerProfile(valid_player_id)

    # Check specific data types
    assert isinstance(player_data["playerId"], int)
    assert isinstance(player_data["firstName"], str)
    assert isinstance(player_data["lastName"], str)
    assert isinstance(player_data["heightInInches"], (int, float, type(None)))
    assert isinstance(player_data["weightInPounds"], (int, float, type(None)))
