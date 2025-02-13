"""Test module for NHL scraper core functionality.

This module contains tests for the core functionality of the NHL scraper.
It includes tests for the scrapeDraft function.
"""

import pandas as pd
import pytest
import requests

from nhl_scraper.scraper.draft import scrapeDraft


# Fixtures to reuse parameters
@pytest.fixture
def valid_year():
    """Valid year for testing."""
    return 2023


@pytest.fixture
def valid_round():
    """Valid round for testing."""
    return "all"


@pytest.fixture
def specific_round():
    """Specific round for testing."""
    return 1


# Test cases
def test_scrapeDraft_basic(valid_year, valid_round):
    """Test basic functionality of scrapeDraft."""
    draft_df = scrapeDraft(year=valid_year, round=valid_round)

    # Validate that the result is a non-empty DataFrame
    assert isinstance(draft_df, pd.DataFrame), "Result is not a DataFrame"
    assert not draft_df.empty, "Draft DataFrame is empty"
    assert draft_df.shape[0] > 0, "Draft DataFrame has no rows"
    assert draft_df.shape[1] > 0, "Draft DataFrame has no columns"

    # Validate expected columns (updated based on actual API response)
    expected_columns = {"round", "pickInRound", "overallPick", "teamId", "teamAbbrev"}
    assert expected_columns.issubset(
        draft_df.columns
    ), f"Draft DataFrame missing columns: {expected_columns - set(draft_df.columns)}"


def test_scrapeDraft_specific_round(valid_year, specific_round):
    """Test scrapeDraft with specific round."""
    draft_df = scrapeDraft(year=valid_year, round=specific_round)
    assert not draft_df.empty, "Draft DataFrame is empty"
    assert all(
        draft_df["round"] == specific_round
    ), f"Draft DataFrame contains rounds other than {specific_round}"


@pytest.mark.parametrize(
    "invalid_year, invalid_round",
    [
        (1800, "all"),  # Year before NHL existed
        (2050, "all"),  # Year in the future
        (2023, -1),  # Invalid round
        (2023, 100),  # Round number that doesn't exist
    ],
)
def test_scrapeDraft_invalid_inputs(invalid_year, invalid_round):
    """Test scrapeDraft with invalid inputs."""
    with pytest.raises((ValueError, requests.exceptions.HTTPError)):
        scrapeDraft(year=invalid_year, round=invalid_round)


def test_scrapeDraft_data_types(valid_year, valid_round):
    """Test data types of scrapeDraft output."""
    draft_df = scrapeDraft(year=valid_year, round=valid_round)

    # Check data types of key columns
    assert pd.api.types.is_numeric_dtype(draft_df["round"]), "round is not numeric"
    assert pd.api.types.is_numeric_dtype(draft_df["pickInRound"]), "pickInRound is not numeric"
    assert pd.api.types.is_numeric_dtype(draft_df["overallPick"]), "overallPick is not numeric"
    assert pd.api.types.is_numeric_dtype(draft_df["teamId"]), "teamId is not numeric"
    assert pd.api.types.is_datetime64_any_dtype(
        draft_df["meta_datetime"]
    ), "meta_datetime is not datetime"


def test_scrapeDraft_metadata(valid_year, valid_round):
    """Test metadata in scrapeDraft output."""
    draft_df = scrapeDraft(year=valid_year, round=valid_round)

    # Check metadata column exists and is recent
    assert "meta_datetime" in draft_df.columns, "meta_datetime column missing"
    max_age = pd.Timedelta(minutes=5)
    assert (
        pd.Timestamp.now() - draft_df["meta_datetime"].max()
    ) < max_age, "meta_datetime is not recent enough"
