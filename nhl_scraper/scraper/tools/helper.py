"""Helper functions for processing NHL game data."""

from typing import Dict, Union

import numpy as np
import pandas as pd
from pandas import DataFrame

from nhl_scraper.scraper.tools.constants import PBP_DEFAULT_COLUMNS, PBP_DF_RENAME


def process_play_by_play(data: dict, game_id: Union[str, int], rosters_df: DataFrame) -> DataFrame:
    """
    Process play-by-play data from NHL API response.

    Args:
        data: Raw NHL API response data
        game_id: NHL game ID
        rosters_df: DataFrame containing roster information

    Returns:
        DataFrame: Processed play-by-play data
    """
    # Create initial DataFrame
    pbp_df = pd.json_normalize(data["plays"])

    # Validate essential columns
    required_columns = PBP_DEFAULT_COLUMNS
    missing_columns = [col for col in required_columns if col not in pbp_df.columns]
    if missing_columns:
        # raise ValueError(f"Missing required columns: {missing_columns}")
        for col in missing_columns:
            pbp_df[col] = None  # TODO: Add default values

    # Create player mapping
    rosters_df["fullName"] = rosters_df["firstName.default"] + " " + rosters_df["lastName.default"]
    rosters_dict = dict(zip(rosters_df["playerId"], rosters_df["fullName"]))

    # Team abbreviation mapping
    abbrev_dict = {
        data["awayTeam"]["id"]: data["awayTeam"]["abbrev"],
        data["homeTeam"]["id"]: data["homeTeam"]["abbrev"],
    }

    # Add basic game info
    pbp_df["eventTeam"] = pbp_df["details.eventOwnerTeamId"].map(abbrev_dict)
    pbp_df["gameId"] = game_id
    pbp_df["period"] = pd.to_numeric(pbp_df["periodDescriptor.number"])

    # Process events and players
    pbp_df = process_players(pbp_df, rosters_dict)

    # Process scores and stats
    pbp_df = process_game_stats(pbp_df, data)

    # Clean up column names
    pbp_df = pbp_df.rename(columns=PBP_DF_RENAME)

    # Process coordinates
    pbp_df = process_coordinates(pbp_df)

    return pbp_df


def process_players(pbp_df: DataFrame, rosters_dict: Dict[int, str]) -> DataFrame:
    """
    Process player information in play-by-play data.

    Args:
        pbp_df: Play-by-play DataFrame
        rosters_dict: Dictionary mapping player IDs to names

    Returns:
        DataFrame: Updated play-by-play data with processed player information
    """
    # Initialize player columns
    pbp_df[["playerId_1", "playerId_2", "playerId_3"]] = np.nan
    pbp_df[["playerName_1", "playerName_2", "playerName_3"]] = np.nan

    # Define event-player mappings
    event_columns = {
        "faceoff": ("details.winningPlayerId", "details.losingPlayerId"),
        "hit": ("details.hittingPlayerId", "details.hitteePlayerId"),
        "blocked-shot": ("details.shootingPlayerId", "details.blockingPlayerId"),
        "shot-on-goal": ("details.shootingPlayerId", None),
        "missed-shot": ("details.shootingPlayerId", None),
        "goal": ("details.scoringPlayerId", "details.assist1PlayerId", "details.assist2PlayerId"),
        "giveaway": ("details.playerId", None),
        "takeaway": ("details.playerId", None),
        "penalty": (
            "details.committedByPlayerId",
            "details.drawnByPlayerId",
            "details.servedByPlayerId",
        ),
        "failed-shot-attempt": ("details.shootingPlayerId", None),
    }

    # Assign player data based on event type
    for event, columns in event_columns.items():
        for i, col in enumerate(columns, start=1):
            if col:
                pbp_df.loc[pbp_df["typeDescKey"] == event, f"playerId_{i}"] = pbp_df.loc[
                    pbp_df["typeDescKey"] == event, col
                ]

    # Map player names
    for i in range(1, 4):
        pbp_df[f"playerName_{i}"] = pbp_df[f"playerId_{i}"].map(rosters_dict)

    return pbp_df


def process_game_stats(pbp_df: DataFrame, data: dict) -> DataFrame:
    """
    Process game statistics in play-by-play data.

    Args:
        pbp_df: Play-by-play DataFrame
        data: Raw NHL API response data

    Returns:
        DataFrame: Updated play-by-play data with processed game statistics
    """
    # Fill in scores
    score_columns = ["details.awaySOG", "details.homeSOG", "details.awayScore", "details.homeScore"]
    for col in score_columns:
        pbp_df[col] = pbp_df[col].ffill().fillna(0)

    # Calculate time stats
    # pbp_df["timeInPeriod_s"] = (pbp_df["timeInPeriod"].str.split(":")
    #                               .apply(lambda x: int(x[0]) * 60 + int(x[1])))
    # pbp_df["timeRemaining_s"] = (pbp_df["timeRemaining"].str.split(":")
    #                             .apply(lambda x: int(x[0]) * 60 + int(x[1])))
    pbp_df["elapsedTime"] = (pbp_df["period"] - 1) * 20 * 60 + pbp_df["timeInPeriod_s"]

    # Add team information
    pbp_df["homeTeam"] = data["homeTeam"]["abbrev"]
    pbp_df["awayTeam"] = data["awayTeam"]["abbrev"]
    pbp_df["homeTeamId"] = data["homeTeam"]["id"]
    pbp_df["awayTeamId"] = data["awayTeam"]["id"]
    pbp_df["eventTeamType"] = np.where(pbp_df["eventTeam"] == pbp_df["homeTeam"], "home", "away")

    return pbp_df


def process_coordinates(pbp_df: DataFrame) -> DataFrame:
    """
    Process and fix coordinates in play-by-play data.

    Args:
        pbp_df: Play-by-play DataFrame

    Returns:
        DataFrame: Updated play-by-play data with processed coordinates
    """
    pbp_df["xFixed"] = np.where(
        ((pbp_df["eventTeamType"] == "home") & (pbp_df["homeTeamDefendingSide"] == "right"))
        | ((pbp_df["eventTeamType"] == "away") & (pbp_df["homeTeamDefendingSide"] == "right")),
        0 - pbp_df["xCoord"],
        pbp_df["xCoord"],
    )

    pbp_df["yFixed"] = np.where(
        ((pbp_df["eventTeamType"] == "home") & (pbp_df["homeTeamDefendingSide"] == "right"))
        | ((pbp_df["eventTeamType"] == "away") & (pbp_df["homeTeamDefendingSide"] == "right")),
        0 - pbp_df["yCoord"],
        pbp_df["yCoord"],
    )

    return pbp_df
