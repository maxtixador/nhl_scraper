"""
Scrape functions for the NHL Scraper module.

This module contains the functions for scraping data from the NHL API.
"""

from typing import Union

# import json
import pandas as pd
import polars as pl
import requests

from nhl_scraper.scraper.scrape.fetch import (
    _fetch_active_teams,
    _fetch_draft,
    _fetch_draft_rankings,
    _fetch_game_plays,
    _fetch_schedule,
    _fetch_team_prospects,
    _fetch_team_stats,
)
from nhl_scraper.scraper.scrape.preprocessing import (
    _preprocess_active_teams,
    _preprocess_draft,
    _preprocess_draft_rankings,
    _preprocess_game_plays,
    _preprocess_schedule,
    _preprocess_team_prospects,
    _preprocess_team_stats,
)

# from nhl_scraper.scraper.scrape.utils import endpoints
from nhl_scraper.scraper.scrape.utils import common, dicts


def scrapeDraft(
    year: int, round: Union[int, str] = "all", format: str = "polars"
) -> pl.DataFrame | pd.DataFrame:
    """
    Scrape draft data from the NHL API.

    Args:
        year (int): The year of the draft to scrape.
        round (int, str): The round of the draft to scrape.
        format (str): The format of the data to return.

    Returns:
        pl.DataFrame | pd.DataFrame: The draft data.

    Raises:
        ValueError: If the format is invalid.
        ValueError: If the year is invalid.
        ValueError: If the round is invalid.
    """
    # Validate year
    if not isinstance(year, int):
        try:
            year = int(year)
        except ValueError:
            raise ValueError(f"Invalid year: {year}")

    # Validate round
    if round not in [
        "all",
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        "1",
        "2",
        "3",
        "4",
        "5",
        "6",
        "7",
        "8",
        "9",
        "10",
    ]:
        raise ValueError(f"Invalid round: {round}")

    df = _fetch_draft(year, round, format)
    df = _preprocess_draft(df, year)
    df = common._add_metadata(df, "NHL API")
    return df


def scrapeDraftRankings(
    year: int, category: str, format: str = "polars"
) -> pl.DataFrame | pd.DataFrame:
    """
    Scrape draft rankings from the NHL API.

    Args:
        year (int): The year of the draft rankings to scrape.
        category (str): The category of the draft rankings to scrape.
        format (str): The format of the data to return.

    Returns:
        pl.DataFrame | pd.DataFrame: The draft rankings data.

    Raises:
        ValueError: If the format is invalid.
        ValueError: If the year is invalid.
        ValueError: If the category is invalid.
    """

    # TODO: Add validation for year

    # Validate category
    # if category is a string, convert it to an integer
    if isinstance(category, str):
        category = int(category)

    # Validate category
    if category not in dicts.DRAFT_RANKINGS_CATEGORIES.values():
        try:
            category = list(dicts.DRAFT_RANKINGS_CATEGORIES.values()).index(category)
        except ValueError:
            raise ValueError(f"Invalid category: {category}")

    df = _fetch_draft_rankings(year, category, format)
    df = _preprocess_draft_rankings(df, category)
    df = common._add_metadata(df, "NHL API")

    return df


def scrapeSchedule(team: str, season: str, format: str = "polars") -> pl.DataFrame | pd.DataFrame:
    """
    Scrape schedule data from the NHL API.

    Args:
        team (str): The team to scrape the schedule for.
        season (str): The season of the schedule to scrape.
        format (str): The format of the data to return.

    Returns:
        pl.DataFrame | pd.DataFrame: The schedule data.

    Raises:
        ValueError: If the format is invalid.
        ValueError: If the year is invalid.
        ValueError: If the team is invalid.
    """
    # Make sure season is a string
    if not isinstance(season, str):
        raise ValueError(f"Invalid season: {season}")

    df = _fetch_schedule(team, season, format)
    df = _preprocess_schedule(df, team, season)
    df = common._add_metadata(df, "NHL API")
    return df


def scrapeActiveTeams(format: str = "polars") -> pl.DataFrame | pd.DataFrame:
    """
    Scrape active teams from the NHL API.
    """
    df = _fetch_active_teams(format)
    df = _preprocess_active_teams(df)
    df = common._add_metadata(df, "NHL API")
    return df


def scrapeTeamStats(
    team: str, season: str, sessionId: int, goalies: bool = False, format: str = "polars"
) -> pl.DataFrame | pd.DataFrame:
    """
    Scrape team stats from the NHL API.
    """
    # Validate sessionId
    if sessionId not in [1, 2, 3]:
        raise ValueError(f"Invalid sessionId: {sessionId}")
    # Validate goalies
    if goalies not in [True, False]:
        raise ValueError(f"Invalid goalies: {goalies}")

    # Validate season
    if not isinstance(season, str):
        raise ValueError(f"Invalid season: {season}")

    df = _fetch_team_stats(team, season, sessionId, goalies, format)
    df = _preprocess_team_stats(df, team, season, sessionId, goalies)
    df = common._add_metadata(df, "NHL API")
    return df


def scrapeTeamProspects(team: str, format: str = "polars") -> pl.DataFrame | pd.DataFrame:
    """
    Scrape team current prospects from the NHL API.
    """
    df = _fetch_team_prospects(team, format)
    df = _preprocess_team_prospects(df, team)
    df = common._add_metadata(df, "NHL API")
    return df


def scrapeGameRosters(
    gameId: Union[int, str], format: str = "polars"
) -> pl.DataFrame | pd.DataFrame:
    """
    Scrape game rosters from the NHL API.
    """
    # Validate gameId
    if not isinstance(gameId, int):
        try:
            gameId = int(gameId)
        except ValueError:
            raise ValueError(f"Invalid gameId: {gameId}")

    url = f"https://api-web.nhle.com/v1/gamecenter/{gameId}/play-by-play"

    response = requests.get(url).json()

    abbrev_dict = {
        response["awayTeam"]["id"]: response["awayTeam"]["abbrev"],
        response["homeTeam"]["id"]: response["homeTeam"]["abbrev"],
    }
    if format == "polars":
        df = pl.json_normalize(response["rosterSpots"])
        df = df.with_columns(
            pl.concat_str(
                [pl.col("firstName.default"), pl.col("lastName.default")], separator=" "
            ).alias("playerName"),
            pl.col("teamId").cast(pl.Utf8).replace(abbrev_dict).alias("teamAbbrev"),
            (pl.col("teamId") == response["homeTeam"]["id"]).cast(pl.Int8).alias("is_home"),
            pl.lit(gameId).alias("gameId"),
        )
    elif format == "pandas":
        df = pd.json_normalize(response["rosterSpots"])
        df["playerName"] = df["firstName.default"] + " " + df["lastName.default"]
        df["teamAbbrev"] = df["teamId"].map(abbrev_dict)
        df["is_home"] = (df["teamId"] == response["homeTeam"]["id"]).astype(int)
        df["gameId"] = gameId
    else:
        raise ValueError(f"Invalid format: {format}")

    # No need to preprocess the data as it is already in the correct format

    df = common._add_metadata(df, "NHL API")

    return df


def scrapeGamePlays(gameId: Union[int, str], format: str = "polars") -> pl.DataFrame | pd.DataFrame:
    """
    Scrape game plays from the NHL API.
    """
    # Validate gameId
    if not isinstance(gameId, int):
        try:
            gameId = int(gameId)
        except ValueError:
            raise ValueError(f"Invalid gameId: {gameId}")

    df = _fetch_game_plays(gameId, format)
    df = _preprocess_game_plays(df, gameId)
    df = common._add_metadata(df, "NHL API")
    return df
