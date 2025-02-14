"""
NHL Game Data Scraper Module.

This module provides functions to scrape NHL game data including:
- Play-by-play events
- Game rosters
- Player shifts
- Game statistics

Functions:
    scrapeGamePlayByPlay: Get play-by-play data
    scrapeGameComplete: Get comprehensive game data
    scrapeGameRosters: Get game rosters
    scrapeGameShifts: Get player shifts
    scrapeGameShiftsLegacy: Get legacy shift data

"""

import re
import warnings
from functools import lru_cache
from typing import Tuple, Union

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from lxml.etree import _Element
from pandas import DataFrame

from nhl_scraper.scraper.types import EventDict, XPathResult
from nhl_scraper.scraper.utils import (
    calculate_shift_stats,
    convert_time_to_seconds,
    etree,
    fix_coordinates,
    fix_json_encoding,
    process_event_players,
    scrape_game_data,
    validate_game_id,
    validate_roster_data,
    validate_shift_data,
)

warnings.filterwarnings("ignore")


# Play-by-Play
def scrapeGamePlayByPlay(game_id: Union[str, int]) -> DataFrame:
    """
    Scrapes play-by-play data for an NHL game.

    Args:
        game_id: NHL game ID (10-digit number)

    Returns:
        pd.DataFrame: Play-by-play data with columns including:
            - Event details (type, time, period, coordinates)
            - Player information
            - Team information
            - Score information
            - Metadata

    Raises:
        ValueError: If game ID is invalid
        requests.HTTPError: If API request fails
        KeyError: If response format is unexpected
    """
    try:
        # Validate game ID
        game_id = validate_game_id(game_id)

        # Make API request
        data = scrape_game_data(game_id)

        # Create initial DataFrame
        pbp_df = pd.json_normalize(data["plays"])

        # Add game ID and metadata
        pbp_df["gameId"] = game_id
        pbp_df["meta_datetime"] = pd.to_datetime("now")
        pbp_df["meta_source"] = "NHL API"

        # Get rosters for player mapping
        rosters_df = scrapeGameRosters(game_id)
        rosters_df["fullName"] = (
            rosters_df["firstName.default"] + " " + rosters_df["lastName.default"]
        )
        rosters_dict = dict(zip(rosters_df["playerId"], rosters_df["fullName"]))

        # Process events and players
        pbp_df = process_event_players(pbp_df, rosters_dict)

        # Fill in score information
        score_columns = [
            "details.awaySOG",
            "details.homeSOG",
            "details.awayScore",
            "details.homeScore",
        ]
        for col in score_columns:
            pbp_df[col] = pbp_df[col].ffill().fillna(0)

        # Clean up column names
        pbp_df = pbp_df.rename(
            columns={
                "typeDescKey": "event",
                "periodDescriptor.number": "periodNumber",
                "periodDescriptor.periodType": "periodType",
                "details.eventOwnerTeamId": "teamId",
                # ... add other column renames as needed
            }
        )

        # Process coordinates
        pbp_df = fix_coordinates(pbp_df)

        # Add team information
        pbp_df["homeTeam"] = data["homeTeam"]["abbrev"]
        pbp_df["awayTeam"] = data["awayTeam"]["abbrev"]
        pbp_df["homeTeamId"] = data["homeTeam"]["id"]
        pbp_df["awayTeamId"] = data["awayTeam"]["id"]
        pbp_df["eventTeamType"] = np.where(
            pbp_df["eventTeam"] == pbp_df["homeTeam"], "home", "away"
        )

        # Validate essential columns
        required_columns = [
            "event",
            "periodNumber",
            "timeInPeriod",
            "homeTeam",
            "awayTeam",
            "gameId",
        ]
        missing_columns = [col for col in required_columns if col not in pbp_df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        return pbp_df

    except requests.RequestException as e:
        raise requests.HTTPError(f"Failed to fetch play-by-play data: {str(e)}")
    except (KeyError, ValueError) as e:
        raise ValueError(f"Error processing play-by-play data: {str(e)}")
    except Exception as e:
        raise Exception(f"An unexpected error occurred: {str(e)}")


@lru_cache(maxsize=1000)  # Cache up to 1000 unique gameIds
def scrapeGameComplete(game_id: Union[str, int]) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """
    Scrapes comprehensive game data including play-by-play, rosters, and shifts.

    Args:
        game_id: NHL game ID (10-digit number)

    Returns:
        Tuple containing:
            - Play-by-play DataFrame
            - Rosters DataFrame
            - Shifts DataFrame
    """
    try:
        # Make API request
        url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Create play-by-play DataFrame
        pbp_df = pd.json_normalize(data["plays"])

        # Compare columns
        og_cols = [
            "eventId",
            "timeInPeriod",
            "timeRemaining",
            "situationCode",
            "homeTeamDefendingSide",
            "typeCode",
            "typeDescKey",
            "sortOrder",
            "periodDescriptor.number",
            "periodDescriptor.periodType",
            "periodDescriptor.maxRegulationPeriods",
            "details.eventOwnerTeamId",
            "details.losingPlayerId",
            "details.winningPlayerId",
            "details.xCoord",
            "details.yCoord",
            "details.zoneCode",
            "details.hittingPlayerId",
            "details.hitteePlayerId",
            "details.blockingPlayerId",
            "details.shootingPlayerId",
            "details.reason",
            "details.shotType",
            "details.goalieInNetId",
            "details.awaySOG",
            "details.homeSOG",
            "details.playerId",
            "details.typeCode",
            "details.descKey",
            "details.duration",
            "details.committedByPlayerId",
            "details.drawnByPlayerId",
            "pptReplayUrl",
            "details.scoringPlayerId",
            "details.scoringPlayerTotal",
            "details.assist1PlayerId",
            "details.assist1PlayerTotal",
            "details.assist2PlayerId",
            "details.assist2PlayerTotal",
            "details.awayScore",
            "details.homeScore",
            "details.highlightClipSharingUrl",
            "details.highlightClipSharingUrlFr",
            "details.highlightClip",
            "details.highlightClipFr",
            "details.discreteClip",
            "details.discreteClipFr",
            "details.secondaryReason",
            "details.servedByPlayerId",
            "zoneStartSide_1",
            "zoneStartSideDetail_1",
        ]
        pbp_columns = set(pbp_df.columns.tolist())
        expected_columns = set(og_cols)

        if pbp_columns != expected_columns:
            new_columns = pbp_columns - expected_columns
            missing_columns = expected_columns - pbp_columns
            if new_columns:
                print(f"New columns in the dataset: {new_columns}")
            if missing_columns:
                for col in missing_columns:
                    pbp_df[col] = np.nan

        # Get rosters for player mapping
        rosters_df = scrapeGameRosters(game_id)
        rosters_df["fullName"] = (
            rosters_df["firstName.default"] + " " + rosters_df["lastName.default"]
        )
        rosters_dict = dict(zip(rosters_df["playerId"], rosters_df["fullName"]))

        # Team abbreviation mapping
        abbrev_dict = {
            data["awayTeam"]["id"]: data["awayTeam"]["abbrev"],
            data["homeTeam"]["id"]: data["homeTeam"]["abbrev"],
        }
        pbp_df["eventTeam"] = pbp_df["details.eventOwnerTeamId"].map(abbrev_dict)
        pbp_df["gameId"] = game_id
        pbp_df["period"] = pd.to_numeric(pbp_df["periodDescriptor.number"])

        # Test for unexpected events
        expected_events = [
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
        ]
        actual_events = pbp_df["typeDescKey"].unique()
        missing_events = set(actual_events) - set(expected_events)
        if missing_events:
            raise ValueError(f"The following events are not in the dataset: {missing_events}")

        # Process events and players
        event_columns = {
            "faceoff": ("details.winningPlayerId", "details.losingPlayerId"),
            "hit": ("details.hittingPlayerId", "details.hitteePlayerId"),
            "blocked-shot": ("details.shootingPlayerId", "details.blockingPlayerId"),
            "shot-on-goal": ("details.shootingPlayerId", None),
            "missed-shot": ("details.shootingPlayerId", None),
            "goal": (
                "details.scoringPlayerId",
                "details.assist1PlayerId",
                "details.assist2PlayerId",
            ),
            "giveaway": ("details.playerId", None),
            "takeaway": ("details.playerId", None),
            "penalty": (
                "details.committedByPlayerId",
                "details.drawnByPlayerId",
                "details.servedByPlayerId",
            ),
            "failed-shot-attempt": ("details.shootingPlayerId", None),
        }

        # Initialize player columns
        pbp_df[["playerId_1", "playerId_2", "playerId_3"]] = np.nan
        pbp_df[["playerName_1", "playerName_2", "playerName_3"]] = np.nan

        # Assign player data based on event type
        for event, columns in event_columns.items():
            for i, col in enumerate(columns, start=1):
                if col:
                    pbp_df.loc[pbp_df["typeDescKey"] == event, f"playerId_{i}"] = pbp_df.loc[
                        pbp_df["typeDescKey"] == event, col
                    ]

        # Fill in scores
        for col in ["details.awaySOG", "details.homeSOG", "details.awayScore", "details.homeScore"]:
            pbp_df[col] = pbp_df[col].ffill().fillna(0)

        # Drop clip columns and other unnecessary columns
        clip_columns = pbp_df.filter(like="Clip").columns.tolist()
        columns_to_drop = [
            "details.losingPlayerId",
            "details.winningPlayerId",
            "details.hittingPlayerId",
            "details.hitteePlayerId",
            "details.shootingPlayerId",
            "details.blockingPlayerId",
            "details.playerId",
            "details.committedByPlayerId",
            "details.drawnByPlayerId",
            "periodDescriptor.maxRegulationPeriods",
            "situationCode",
            "typeCode",
            "pptReplayUrl",
            "details.scoringPlayerId",
            "details.assist1PlayerId",
            "details.assist2PlayerId",
            "details.servedByPlayerId",
        ] + clip_columns
        pbp_df = pbp_df.drop(columns=columns_to_drop)

        # Map player names
        pbp_df["playerName_1"] = pbp_df["playerId_1"].map(rosters_dict)
        pbp_df["playerName_2"] = pbp_df["playerId_2"].map(rosters_dict)
        pbp_df["playerName_3"] = pbp_df["playerId_3"].map(rosters_dict)

        # Rename columns
        pbp_df = pbp_df.rename(
            columns={
                "typeDescKey": "event",
                "periodDescriptor.number": "periodNumber",
                "periodDescriptor.periodType": "periodType",
                "details.eventOwnerTeamId": "teamId",
                "details.xCoord": "xCoord",
                "details.yCoord": "yCoord",
                "details.zoneCode": "zoneCode",
                "details.reason": "reason",
                "details.shotType": "shotType",
                "details.goalieInNetId": "goalieInNetId",
                "details.awaySOG": "awaySOG",
                "details.homeSOG": "homeSOG",
                "details.typeCode": "typeCode",
                "details.descKey": "descKey",
                "details.duration": "duration",
                "details.scoringPlayerTotal": "scoringPlayerTotal",
                "details.assist1PlayerTotal": "assist1PlayerTotal",
                "details.assist2PlayerTotal": "assist2PlayerTotal",
                "details.awayScore": "awayScore",
                "details.homeScore": "homeScore",
                "details.secondaryReason": "secondaryReason",
            }
        )

        # Calculate elapsed time
        pbp_df["timeInPeriod_s"] = (
            pbp_df["timeInPeriod"].str.split(":").apply(lambda x: int(x[0]) * 60 + int(x[1]))
        )
        pbp_df["timeRemaining_s"] = (
            pbp_df["timeRemaining"].str.split(":").apply(lambda x: int(x[0]) * 60 + int(x[1]))
        )
        pbp_df["elapsedTime"] = (pbp_df["period"] - 1) * 20 * 60 + pbp_df["timeInPeriod_s"]

        # Add team information
        pbp_df["homeTeam"] = data["homeTeam"]["abbrev"]
        pbp_df["awayTeam"] = data["awayTeam"]["abbrev"]
        pbp_df["homeTeamId"] = data["homeTeam"]["id"]
        pbp_df["awayTeamId"] = data["awayTeam"]["id"]
        pbp_df["eventTeamType"] = np.where(
            pbp_df["eventTeam"] == pbp_df["homeTeam"], "home", "away"
        )

        # Fix coordinates
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

        # Get shifts data
        try:
            shifts_df = scrapeGameShifts(game_id)
        except Exception as e:
            print(f"Warning: Using legacy shifts data due to: {e}")
            shifts_df = scrapeGameShiftsLegacy(game_id)

        return pbp_df, rosters_df, shifts_df

    except requests.RequestException as e:
        raise requests.HTTPError(f"Failed to fetch game data: {str(e)}")
    except ValueError as e:
        raise ValueError(f"Error processing game data: {str(e)}")
    except Exception as e:
        raise Exception(f"An unexpected error occurred: {str(e)}")


# Rosters
def scrapeGameRosters(game_id: Union[str, int]) -> DataFrame:
    """
    Scrapes roster information for both teams in an NHL game.

    Args:
        game_id: NHL game ID (10-digit number)

    Returns:
        pd.DataFrame: Roster information including:
            - Player details (ID, name, position, number)
            - Team information
            - Game status (starter, scratched, etc.)
            - Metadata

    Raises:
        ValueError: If game ID is invalid
        requests.HTTPError: If API request fails
        KeyError: If response format is unexpected
    """
    try:
        # Validate game ID
        game_id = validate_game_id(game_id)

        # Make API request
        data = scrape_game_data(game_id)

        roster_df = pd.json_normalize(data["rosterSpots"])

        abbrev_dict = {
            data["awayTeam"]["id"]: data["awayTeam"]["abbrev"],
            data["homeTeam"]["id"]: data["homeTeam"]["abbrev"],
        }

        roster_df["teamAbbrev"] = roster_df["teamId"].map(abbrev_dict)

        roster_df["is_home"] = (roster_df["teamId"] == data["homeTeam"]["id"]).astype(int)

        # Add game information
        roster_df["gameId"] = game_id
        roster_df["gameDatetime"] = pd.to_datetime(data.get("gameDate"))
        roster_df["gameType"] = data.get("gameType")
        roster_df["season"] = data.get("season")

        # Add metadata
        roster_df["meta_datetime"] = pd.to_datetime("now")
        roster_df["meta_source"] = "NHL API"

        # Validate data
        roster_df = validate_roster_data(roster_df)

        # Sort DataFrame
        sort_columns = ["teamAbbrev", "positionCode", "sweaterNumber"]
        roster_df = roster_df.sort_values(sort_columns)

        return roster_df

    except requests.RequestException as e:
        raise requests.HTTPError(f"Failed to fetch roster data: {str(e)}")
    except (KeyError, ValueError) as e:
        raise ValueError(f"Error processing roster data: {str(e)}")
    except Exception as e:
        raise Exception(f"An unexpected error occurred: {str(e)}")


# Shifts
def scrapeGameShifts(game_id: Union[str, int]) -> DataFrame:
    """
    Scrapes shift data for all players in an NHL game.

    Args:
        game_id: NHL game ID (10-digit number)

    Returns:
        pd.DataFrame: Shift data including:
            - Player information (ID, name, team)
            - Shift details (start, end, duration)
            - Period information
            - Calculated statistics (game time, etc.)
            - Metadata

    Raises:
        ValueError: If game ID is invalid
        requests.HTTPError: If API request fails
        KeyError: If response format is unexpected
    """
    try:
        # Validate game ID
        game_id = validate_game_id(game_id)

        # Make API requests
        shifts_url = f"https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId={game_id}"
        game_data = scrape_game_data(game_id)
        shifts_response = requests.get(shifts_url)

        shifts_response.raise_for_status()

        shifts_data = shifts_response.json()

        # Fix encoding issues in the JSON object
        shifts_data = fix_json_encoding(shifts_data)

        # Get roster information for player details
        roster_df = scrapeGameRosters(game_id)
        roster_df = validate_roster_data(roster_df)

        player_info = roster_df[
            [
                "teamId",
                "playerId",
                "sweaterNumber",
                "positionCode",
                "position",
                "headshot",
                "teamAbbrev",
                "is_home",
            ]
        ]

        # Process shifts data
        if "data" not in shifts_data or not shifts_data["data"]:
            raise ValueError(f"No shift data found for game {game_id}")

        shifts_df = pd.json_normalize(shifts_data["data"])

        # Validate shift data
        shifts_df = validate_shift_data(shifts_df)

        # # Merge with player information
        shifts_df = shifts_df.merge(
            player_info, on=["playerId", "teamId", "teamAbbrev"], how="left"
        )

        # Add full name
        shifts_df["fullName"] = shifts_df["firstName"] + " " + shifts_df["lastName"]

        # Calculate additional statistics
        shifts_df = calculate_shift_stats(shifts_df)

        # Add game information
        shifts_df["gameId"] = game_id
        shifts_df["gameDatetime"] = pd.to_datetime(game_data.get("gameDate"))
        shifts_df["gameType"] = game_data.get("gameType")
        shifts_df["season"] = game_data.get("season")

        # Add metadata
        shifts_df["meta_datetime"] = pd.to_datetime("now")
        shifts_df["meta_source"] = "NHL API"

        # Validate data
        # validate_shift_data(shifts_df)

        # Sort DataFrame
        sort_columns = [
            "teamAbbrev",
            "period",
        ]  # "startTimeInSeconds", "playerId"]
        shifts_df = shifts_df.sort_values(sort_columns)

        # Add shift numbers
        shifts_df["shiftNumber"] = shifts_df.groupby("playerId").cumcount() + 1

        return shifts_df

    except requests.RequestException as e:
        raise requests.HTTPError(f"Failed to fetch shift data: {str(e)}")
    except (KeyError, ValueError) as e:
        raise ValueError(f"Error processing shift data: {str(e)}")
    except Exception as e:
        raise Exception(f"An unexpected error occurred: {str(e)}")


def scrapeGameShiftsLegacy(game_id: Union[str, int]) -> DataFrame:
    """
    Scrape shift data from the NHL's legacy HTML reports using lxml.etree.

    Use this method for historical games where the API data is unavailable.

    Args:
        game_id: NHL game ID (10-digit number)

    Returns:
        pd.DataFrame: Shift data including:
            - Player information (name, number, team)
            - Shift details (start, end, duration)
            - Period information
            - Calculated statistics

    Raises:
        ValueError: If game ID is invalid
        requests.HTTPError: If HTML request fails
        ValueError: If HTML parsing fails
    """
    try:
        # Validate game ID
        game_id = validate_game_id(game_id)

        year1 = str(game_id)[:4]
        year2 = int(year1) + 1
        year1_2 = f"{year1}{year2}"

        shortId = str(game_id)[4:]

        url_template = f"https://www.nhl.com/scores/htmlreports/{year1}{year2}/T{{HV}}{shortId}.HTM"

        team_shifts_dfs = []

        try:
            for side in ["H", "V"]:
                url = url_template.format(HV=side)

            # Fetch the webpage content
            response = requests.get(url)
            response.raise_for_status()

            # Use BeautifulSoup with lxml parser
            soup = BeautifulSoup(response.content, "lxml")

            # Convert BeautifulSoup object to lxml etree
            tree = etree.HTML(str(soup))

            # Extract player names using XPath
            player_names = tree.xpath('.//td[@class="playerHeading + border"]/text()')

            # Extract shift details using XPath
            shift_rows = tree.xpath('.//tr[@class="evenColor"] | .//tr[@class="oddColor"]')

            # Store extracted data
            shift_data = []
            for row in shift_rows:
                shift_number = (
                    row.xpath("./td[1]/text()")[0].strip() if row.xpath("./td[1]/text()") else ""
                )
                period = (
                    row.xpath("./td[2]/text()")[0].strip() if row.xpath("./td[2]/text()") else ""
                )
                start_time = (
                    row.xpath("./td[3]/text()")[0].strip() if row.xpath("./td[3]/text()") else ""
                )
                end_time = (
                    row.xpath("./td[4]/text()")[0].strip() if row.xpath("./td[4]/text()") else ""
                )
                duration = (
                    row.xpath("./td[5]/text()")[0].strip() if row.xpath("./td[5]/text()") else ""
                )
                event = (
                    row.xpath("./td[6]/text()")[0].strip() if row.xpath("./td[6]/text()") else ""
                )

                shift_data.append(
                    {
                        "Shift Number": shift_number,
                        "Period": period,
                        "Start Time": start_time,
                        "End Time": end_time,
                        "Duration": duration,
                        "Event": event,
                    }
                )

            # Output extracted data
            shifts_df = pd.DataFrame(shift_data)

            shifts_df["is_home"] = 1 if side == "H" else 0

            # Replace 'OT' with 4 ### TO FIX EVENTUALLY BECAUSE OF PLAYOFFS
            shifts_df["Period"] = shifts_df["Period"].replace("OT", 4)
            shifts_df["Period"] = pd.to_numeric(shifts_df["Period"], errors="coerce")

            shifts_df["Shift Number"] = pd.to_numeric(shifts_df["Shift Number"], errors="coerce")

            # Assign a row with 1 where 	Start Time has a / in it and filter out 0s
            shifts_df["dummy"] = np.where(shifts_df["Start Time"].str.contains("/"), 1, 0)
            shifts_df = shifts_df[shifts_df["dummy"] == 1]
            shifts_df = shifts_df.drop(columns=["dummy"])

            # Assign player names
            shifts_df["Player Name"] = None
            player_index = 0

            shifts_df["Shift Number"] = pd.to_numeric(shifts_df["Shift Number"], errors="coerce")
            shifts_df = shifts_df.reset_index(drop=True)
            # Iterate through shifts and assign player names
            for i in range(len(shifts_df)):
                if player_index < len(player_names):
                    shifts_df.loc[i, "Player Name"] = player_names[player_index]

                # If shift number decreases, move to the next player
                if (
                    i > 0
                    and shifts_df.loc[i, "Shift Number"] < shifts_df.loc[i - 1, "Shift Number"]
                ):
                    player_index += 1  # Move to the next player

            # Split the "Player Name" column into "Player Number" and "Player Name"
            shifts_df[["Player Number", "Player Name"]] = shifts_df["Player Name"].str.split(
                " ", n=1, expand=True
            )

            # Convert "Player Number" to numeric for sorting or analysis
            shifts_df["Player Number"] = pd.to_numeric(shifts_df["Player Number"], errors="coerce")
            shifts_df["firstName"] = shifts_df["Player Name"].str.split(", ").str[1]

            shifts_df["lastName"] = shifts_df["Player Name"].str.split(", ").str[0]
            # Remove number from firstName
            shifts_df["lastName"] = shifts_df["lastName"].str.replace(r"\d+", "")
            shifts_df["lastName"] = shifts_df["lastName"].str.strip()

            shifts_df[["Start Time (Elapsed)", "Start Time (Remaining)"]] = shifts_df[
                "Start Time"
            ].str.split(" ", n=1, expand=True)
            shifts_df[["End Time (Elapsed)", "End Time (Remaining)"]] = shifts_df[
                "End Time"
            ].str.split(" ", n=1, expand=True)

            # Strip "/ " in remaining cols
            shifts_df["Start Time (Remaining)"] = shifts_df["Start Time (Remaining)"].str.replace(
                "/ ", ""
            )
            shifts_df["End Time (Remaining)"] = shifts_df["End Time (Remaining)"].str.replace(
                "/ ", ""
            )

            shifts_df = shifts_df.drop(columns=["Start Time", "End Time"])

            for col in [
                "Start Time (Elapsed)",
                "Start Time (Remaining)",
                "End Time (Elapsed)",
                "End Time (Remaining)",
                "Duration",
            ]:
                col_with_seconds = col + " (Seconds)"
                shifts_df[col_with_seconds] = shifts_df[col].apply(convert_time_to_seconds)

            team_shifts_dfs.append(shifts_df)

        except requests.RequestException as e:
            raise requests.HTTPError(f"Failed to fetch HTML shift report: {str(e)}")
        except (ValueError, etree.ParseError) as e:
            raise ValueError(f"Error processing HTML shift data: {str(e)}")
        except Exception as e:
            print(f"Error processing HTML shift data: {str(e)}")
            return pd.DataFrame()

        shifts_df = pd.concat(team_shifts_dfs)
        shifts_df = shifts_df.reset_index(drop=True)

        shifts_df["startTimeInSeconds"] = (
            shifts_df["Start Time (Elapsed) (Seconds)"] + (shifts_df["Period"] - 1) * 60 * 20
        )
        shifts_df["endTimeInSeconds"] = (
            shifts_df["End Time (Elapsed) (Seconds)"] + (shifts_df["Period"] - 1) * 60 * 20
        )

        shifts_df["sweaterNumber"] = shifts_df["Player Number"]
        shifts_df["gameId"] = game_id

        # Add game information
        shifts_df["gameId"] = game_id
        shifts_df["source"] = "HTML"
        shifts_df["season"] = year1_2

        # Add metadata
        shifts_df["meta_datetime"] = pd.to_datetime("now")
        shifts_df["meta_source"] = "NHL HTML Reports"

        # Sort DataFrame
        sort_columns = ["Period", "startTimeInSeconds", "Player Number"]
        shifts_df = shifts_df.sort_values(sort_columns)

        return shifts_df
    except Exception as e:
        raise Exception(f"An unexpected error occurred: {str(e)}")


def clean_player_name_etree(element: _Element) -> str:
    """
    Extract player number and name from HTML element.

    Use this method for historical games where the API data is unavailable.

    Args:
        element: etree element containing player information

    Returns:
        Tuple of (player number, player name)
    """
    # Safely get text content, handling None case
    text = "".join(str(item) for item in element.itertext()).strip() if element is not None else ""
    if not text:
        return None, ""

    number_match = re.search(r"(\d+)\s", text)
    number = int(number_match.group(1)) if number_match else None
    name = re.sub(r"^\d+\s*", "", text).strip()
    return number, name


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
