"""
NHL Scraper Utility Functions.

This module provides utility functions used across the NHL Scraper package
for data validation, processing, and transformation.

Functions:
    validate_game_id: Validates NHL game ID format
    process_event_players: Processes player information from events
    fix_coordinates: Normalizes rink coordinates
    calculate_shift_stats: Calculates statistics from shift data
    validate_shift_data: Validates shift data format
    validate_event_types: Validates event types
    validate_game_data: Validates game data
    validate_roster_data: Validates roster data
    validate_draft_data: Validates draft data
    validate_draft_legacy_data: Validates draft legacy data
    validate_draft_rankings_data: Validates draft rankings data
"""

import json
import re
import warnings
from typing import Dict, Optional, Tuple, Union

import numpy as np
import pandas as pd
import requests
from ftfy import fix_text
from lxml import etree
from lxml.etree import XPathResult, _Element

from nhl_scraper.scraper.tools.types import EventDict  # and XPathResult

warnings.filterwarnings("ignore")


# Function to fix encoding issues in a JSON object
def fix_json_encoding(obj):
    """
    Fix encoding issues in a JSON object.

    Fix the encoding issues in the JSON object. This is useful for analysis and
    visualization.

    Args:
        obj: The JSON object to fix

    Returns:
        The fixed JSON object
    """
    if isinstance(obj, dict):
        return {key: fix_json_encoding(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [fix_json_encoding(item) for item in obj]
    elif isinstance(obj, str):
        return fix_text(obj)
    else:
        return obj


def scrape_game_data(game_id: str) -> json:
    """
    Scrape game data from NHL API.

    Scrape the game data from the NHL API. This is useful for analysis and
    visualization.

    Args:
        game_id: The ID of the game to scrape

    Returns:
        The game data as a json object

    Raises:
        ValueError: If the game data is not found
    """
    url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    return data


def validate_event_types(pbp_df: pd.DataFrame) -> None:
    """
    Validate event types against expected set.

    Validate the event types in the play-by-play data against a predefined
    set of expected event types. This helps ensure the data is consistent and
    complete.

    Args:
        pbp_df: Play-by-play DataFrame

    Raises:
        ValueError: If unexpected event types are found
        KeyError: If 'typeDescKey' column is missing
        TypeError: If 'typeDescKey' column is not of string type


    """
    expected_events = {
        "period-start",
        "faceoff",
        "hit",
        "blocked-shot",
        "shot-on-goal",
        "stoppage",
        "giveaway",
        "delayed-penalty",
        "penalty",
        "failed-shot-attempt",
        "missed-shot",
        "goal",
        "takeaway",
        "period-end",
        "shootout-complete",
        "game-end",
    }

    actual_events = set(pbp_df["typeDescKey"].unique())
    unexpected_events = actual_events - expected_events

    if unexpected_events:
        raise ValueError(f"Unexpected event types found: {unexpected_events}")


def merge_player_info(pbp_df: pd.DataFrame, rosters_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge play-by-play data with roster information.

    Merge the play-by-play data with the roster information to add player
    names to the event data. This is useful for analysis and visualization.

    Args:
        pbp_df: Play-by-play DataFrame
        rosters_df: Roster DataFrame

    Returns:
        DataFrame with merged player information
    """
    # Create player name mapping
    rosters_df["fullName"] = rosters_df["firstName.default"] + " " + rosters_df["lastName.default"]
    rosters_dict = dict(zip(rosters_df["playerId"], rosters_df["fullName"]))

    # Process events and players
    pbp_df = process_event_players(pbp_df, rosters_dict)

    return pbp_df


def validate_game_data(
    pbp_df: pd.DataFrame, rosters_df: pd.DataFrame, shifts_df: pd.DataFrame
) -> None:
    """
    Validate consistency across game data components.

    Validate the consistency of the game data across the play-by-play,
    roster, and shift data. This helps ensure the data is consistent and
    complete.

    Args:
        pbp_df: Play-by-play DataFrame
        rosters_df: Roster DataFrame
        shifts_df: Shifts DataFrame

    Raises:
        ValueError: If data inconsistencies are found
    """
    # Check game IDs match
    game_ids = {
        "pbp": pbp_df["gameId"].unique(),
        "rosters": rosters_df["gameId"].unique(),
        "shifts": shifts_df["gameId"].unique(),
    }

    if not (len(game_ids["pbp"]) == len(game_ids["rosters"]) == len(game_ids["shifts"]) == 1):
        raise ValueError("Inconsistent game IDs across datasets")

    if not (game_ids["pbp"][0] == game_ids["rosters"][0] == game_ids["shifts"][0]):
        raise ValueError("Game IDs do not match across datasets")


def validate_shift_data(df: pd.DataFrame) -> None:
    """
    Validate shift DataFrame for required columns and data integrity.

    Validate the shift DataFrame for the required columns and data integrity.
    This helps ensure the data is consistent and complete.

    Args:
        df: Shift DataFrame to validate

    Raises:
        ValueError: If validation fails
    """
    required_columns = [
        "id",
        "detailCode",
        "duration",
        "endTime",
        "eventDescription",
        "eventDetails",
        "eventNumber",
        "firstName",
        "gameId",
        "hexValue",
        "lastName",
        "period",
        "playerId",
        "shiftNumber",
        "startTime",
        "teamAbbrev",
        "teamId",
        "teamName",
        "typeCode",
    ]

    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    if df.empty:
        raise ValueError("No shift data found")

    # Reorder columns for better readability
    expected_order = [
        "id",
        "playerId",
        "firstName",
        "lastName",
        "startTime",
        "endTime",
        "duration",
        "period",
        "shiftNumber",
        "teamAbbrev",
        "teamId",
        "teamName",
        "gameId",
        "hexValue",
        "eventDescription",
        "eventDetails",
        "eventNumber",
        "typeCode",
        "detailCode",
    ]
    df = df[expected_order]

    # Validate important data types
    # str : startTime, endTime,	duration
    for col in ["startTime", "endTime", "duration"]:
        if not pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].astype(str)

    # int : id, playerId, shiftNumber, period, eventNumber, detailCode, typeCode,
    for col in [
        "id",
        "playerId",
        "shiftNumber",
        "period",
        "eventNumber",
        "detailCode",
        "typeCode",
    ]:
        if not pd.api.types.is_integer_dtype(df[col]):
            df[col] = df[col].astype(int)

    return df


def parse_html_time_etree(time_text: str) -> Tuple[str, str, str]:
    """
    Parse shift time information from HTML text.

    Parse the shift time information from the HTML text. This is useful for
    historical games where the API data is unavailable.

    Args:
        time_text: Text containing shift time information

    Returns:
        Tuple of (start_time, end_time, duration)
    """
    try:
        # Extract times using regex
        times = re.findall(r"[\d:]+", time_text)
        if len(times) != 3:
            raise ValueError(f"Expected 3 time values, found {len(times)}")

        return tuple(times)
    except Exception as e:
        raise ValueError(f"Error parsing time: {str(e)}")


def clean_player_name_etree(element: etree._Element) -> Tuple[Optional[int], str]:
    """
    Extract player number and name from HTML element.

    Extract the player number and name from the HTML element. This is useful
    for historical games where the API data is unavailable.

    Args:
        element: etree element containing player information

    Returns:
        Tuple of (jersey_number, player_name)
    """
    # Safely get text content, handling None case
    text = "".join(str(item) for item in element.itertext()).strip() if element is not None else ""
    if not text:
        return None, ""

    number_match = re.search(r"(\d+)\s", text)
    number = int(number_match.group(1)) if number_match else None
    name = re.sub(r"^\d+\s*", "", text).strip()
    return number, name


def process_player_name(player_data: Dict) -> Dict:
    """
    Process player name data from NHL API response.

    Process the player name data from the NHL API response. This is useful
    for analysis and visualization.

    Args:
        player_data: Dictionary containing player information

    Returns:
        Dictionary with processed name fields
    """
    return {
        "firstName": player_data.get("firstName", {}).get("default", ""),
        "lastName": player_data.get("lastName", {}).get("default", ""),
        "fullName": (
            f"{player_data.get('firstName', {}).get('default', '')} "
            f"{player_data.get('lastName', {}).get('default', '')}"
        ).strip(),
    }


def validate_roster_data(df: pd.DataFrame) -> None:
    """
    Validate roster DataFrame for required columns and data integrity.

    Validate the roster DataFrame for the required columns and data integrity.
    This helps ensure the data is consistent and complete.

    Args:
        df: Roster DataFrame to validate

    Raises:
        ValueError: If validation fails
    """
    required_columns = [
        "teamId",
        "playerId",
        "sweaterNumber",
        "positionCode",
        "headshot",
        "firstName.default",
        "lastName.default",
    ]

    missing_columns = [col for col in required_columns if col not in df.columns]

    # Add new position column for F : Ÿenseman, G : Goalie
    df["position"] = np.where(~df["positionCode"].isin(["G", "D"]), "F", df["positionCode"])

    # Reorder columns to put position after positionCode
    cols = df.columns.tolist()
    pos_idx = cols.index("positionCode")
    cols.remove("position")
    cols.insert(pos_idx + 1, "position")
    df = df[cols]

    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    if df.empty:
        raise ValueError("No roster data found")

    return df


def convert_time_to_seconds(time_str: str) -> int:
    """
    Convert time string to seconds. Handles both HH:MM:SS and MM:SS formats.

    Args:
        time_str: Time in either HH:MM:SS or MM:SS format

    Returns:
        Total seconds as integer

    Raises:
        ValueError: If time format is invalid
    """
    # Handle None or empty string cases
    if time_str is None or time_str == "" or time_str == "None":
        return 0

    try:
        # Split the time string
        parts = time_str.split(":")

        if len(parts) == 3:  # HH:MM:SS
            hours, minutes, seconds = map(int, parts)
            return int(hours * 3600 + minutes * 60 + seconds)
        elif len(parts) == 2:  # MM:SS
            minutes, seconds = map(int, parts)
            return int(minutes * 60 + seconds)
        else:
            raise ValueError(f"Invalid time format: {time_str}. Expected HH:MM:SS or MM:SS")
    except (ValueError, AttributeError) as e:
        raise ValueError(f"Error converting time: {time_str}. Details: {str(e)}")


def format_seconds_to_time(seconds: int, include_hours: bool = False) -> str:
    """
    Convert seconds to time string format.

    Convert the seconds to a time string format. This is useful for
    analysis and visualization.

    Args:
        seconds: Number of seconds
        include_hours: Whether to include hours in output

    Returns:
        Formatted time string (HH:MM:SS or MM:SS)
    """
    if include_hours:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    else:
        minutes = seconds // 60
        secs = seconds % 60
        return f"{minutes:02d}:{secs:02d}"


def calculate_shift_stats(shift_data: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate additional shift statistics.

    Calculate the additional shift statistics. This is useful for analysis and
    visualization.

    Args:
        shift_data: DataFrame containing raw shift data

    Returns:
        DataFrame with additional shift statistics
    """
    # Calculate shift duration
    shift_data["shiftDuration"] = shift_data.apply(
        lambda x: convert_time_to_seconds(x["duration"]), axis=1
    )

    # Calculate period start/end times in seconds
    shift_data["startTimeInSeconds"] = shift_data.apply(
        lambda x: convert_time_to_seconds(x["startTime"]), axis=1
    )
    shift_data["endTimeInSeconds"] = shift_data.apply(
        lambda x: convert_time_to_seconds(x["endTime"]), axis=1
    )

    # Calculate game time (cumulative seconds)
    shift_data["gameTimeStart"] = (shift_data["period"] - 1) * 1200 + shift_data[
        "startTimeInSeconds"
    ]
    shift_data["gameTimeEnd"] = (shift_data["period"] - 1) * 1200 + shift_data["endTimeInSeconds"]

    # # Add formatted duration for display
    # shift_data["durationFormatted"] = shift_data["shiftDuration"].apply(
    #     lambda x: format_seconds_to_time(x)
    # )

    # # Add game time formatted
    # shift_data["gameTimeStartFormatted"] = shift_data["gameTimeStart"].apply(
    #     lambda x: format_seconds_to_time(x, include_hours=True)
    # )
    # shift_data["gameTimeEndFormatted"] = shift_data["gameTimeEnd"].apply(
    #     lambda x: format_seconds_to_time(x, include_hours=True)
    # )

    return shift_data


def validate_game_id(game_id: Union[str, int]) -> str:
    """Validate and format NHL game ID."""
    game_id_str = str(game_id)
    if not game_id_str.isdigit() or len(game_id_str) != 10:
        raise ValueError(f"Invalid game ID: {game_id}. Must be a 10-digit number.")
    return game_id_str


def process_event_players(df: pd.DataFrame, roster_dict: Dict) -> pd.DataFrame:
    """Process player information in event data.

    Process the player information in the event data. This is useful for
    analysis and visualization.

    Args:
        df: DataFrame containing event data
        roster_dict: Dictionary containing roster information

    Returns:
        DataFrame with processed player information
    """
    # Initialize player columns
    df[["playerId_1", "playerId_2", "playerId_3"]] = np.nan
    df[["playerName_1", "playerName_2", "playerName_3"]] = np.nan

    # Event column mapping
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
                df.loc[df["typeDescKey"] == event, f"playerId_{i}"] = df.loc[
                    df["typeDescKey"] == event, col
                ]

    # Map player names
    for i in range(1, 5):
        df[f"playerName_{i}"] = df[f"playerId_{i}"].map(roster_dict)

    return df


def fix_coordinates(df: pd.DataFrame) -> pd.DataFrame:
    """Fix and normalize rink coordinates.

    Fix and normalize the rink coordinates. This is useful for analysis and
    visualization.

    Args:
        df: DataFrame containing event data

    Returns:
        DataFrame with fixed coordinates
    """
    df["eventTeamType"] = np.where(df["eventTeam"] == df["homeTeam"], "home", "away")
    df["xFixed"] = np.where(
        ((df["eventTeamType"] == "home") & (df["homeTeamDefendingSide"] == "right"))
        | ((df["eventTeamType"] == "away") & (df["homeTeamDefendingSide"] == "right")),
        0 - df["xCoord"],
        df["xCoord"],
    )

    df["yFixed"] = np.where(
        ((df["eventTeamType"] == "home") & (df["homeTeamDefendingSide"] == "right"))
        | ((df["eventTeamType"] == "away") & (df["homeTeamDefendingSide"] == "right")),
        0 - df["yCoord"],
        df["yCoord"],
    )

    return df


def process_event(event: _Element) -> EventDict:
    """
    Process an event element from the play-by-play data.

    Args:
        event: An lxml Element representing a game event

    Returns:
        Dict containing processed event information
    """
    if not isinstance(event, _Element):
        return {}

    try:
        result: EventDict = {}
        players: XPathResult = event.xpath(".//player")
        if not isinstance(players, list):
            return {}

        # Process players
        for player in players:
            if isinstance(player, _Element):
                name = player.get("name", "")
                if name:
                    result.setdefault("players", []).append(name)

        # Get event result
        event_result = event.xpath("result")
        if isinstance(event_result, list) and event_result:
            first_result = event_result[0]
            if isinstance(first_result, _Element):
                result["result"] = first_result.text or ""

        return result
    except (AttributeError, IndexError):
        return {}


# def clean_player_name_etree(element: _Element) -> str:
#     """
#     Extract player number and name from HTML element.

#     Use this method for historical games where the API data is unavailable.

#     Args:
#         element: etree element containing player information

#     Returns:
#         Tuple of (player number, player name)
#     """
#     # Safely get text content, handling None case
#     text = "".join(str(item) for item in element.itertext()).strip()\
#                       if element is not None else ""
#     if not text:
#         return None, ""

#     number_match = re.search(r"(\d+)\s", text)
#     number = int(number_match.group(1)) if number_match else None
#     name = re.sub(r"^\d+\s*", "", text).strip()
#     return number, name
