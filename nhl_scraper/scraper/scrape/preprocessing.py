"""
Preprocessing functions for the NHL Draft module.
"""

from typing import Union

import pandas as pd
import polars as pl

# import nhl_scraper.scraper.scrape.utils.common as common
import nhl_scraper.scraper.scrape.utils.dicts as dicts

# from datetime import datetime


def _preprocess_schedule(
    df: pl.DataFrame | pd.DataFrame, team: str, season: str
) -> pl.DataFrame | pd.DataFrame:
    """
    Preprocess the schedule data.
    """
    try:
        if isinstance(df, pl.DataFrame):
            format = "polars"
        elif isinstance(df, pd.DataFrame):
            format = "pandas"

        if format == "polars":
            df = df.with_columns(pl.lit(team).alias("team"))
            df = df.with_columns(pl.lit(season).alias("season"))
            df = df.with_columns(
                pl.col("gameDate").str.to_datetime("%Y-%m-%d").dt.date().alias("gameDate")
            )
            df = df.with_columns(
                pl.col("gameType")
                .cast(pl.Utf8)
                .replace(dicts.SESSION_TYPES_REVERSE)
                .alias("session")
            )
        elif format == "pandas":
            df["team"] = team
            df["season"] = season
            df["date"] = pd.to_datetime(df["date"])
            df["session"] = df["gameType"].map(dicts.SESSION_TYPES_REVERSE)

    except Exception as e:
        raise ValueError(f"Error preprocessing schedule data: {e}")

    return df


def _preprocess_draft(df: pl.DataFrame | pd.DataFrame, year: int) -> pl.DataFrame | pd.DataFrame:
    """
    Preprocess the draft data.
    """
    try:
        if isinstance(df, pl.DataFrame):
            format = "polars"
        elif isinstance(df, pd.DataFrame):
            format = "pandas"

        if format == "polars":
            df = df.with_columns(pl.lit(year).alias("year"))
        elif format == "pandas":
            df["year"] = year
    except Exception as e:
        raise ValueError(f"Error preprocessing draft data: {e}")
    return df


def _preprocess_draft_rankings(
    df: pl.DataFrame | pd.DataFrame, category: Union[int, str]
) -> pl.DataFrame | pd.DataFrame:
    """
    Preprocess the draft rankings data.
    """

    # get the category name (key)
    category_lookup = {value: key for key, value in dicts.DRAFT_RANKINGS_CATEGORIES.items()}
    category_name = category_lookup.get(category, "")  # Avoid KeyError with a default value

    # Find what format the data is in
    try:
        if isinstance(df, pl.DataFrame):
            format = "polars"
        elif isinstance(df, pd.DataFrame):
            format = "pandas"

        # Add category name and code to the dataframe
        if format == "polars":
            df = df.with_columns(pl.lit(category_name).alias("category_name"))
            df = df.with_columns(pl.lit(category).alias("category_code"))
        elif format == "pandas":
            df["category_name"] = category_name
            df["category_code"] = category
    except Exception as e:
        raise ValueError(f"Error finding data format: {e}")

    return df


def _preprocess_active_teams(df: pl.DataFrame | pd.DataFrame) -> pl.DataFrame | pd.DataFrame:
    """
    Preprocess the active teams data.
    """
    try:
        if isinstance(df, pl.DataFrame):
            format = "polars"
        elif isinstance(df, pd.DataFrame):
            format = "pandas"
        else:
            raise ValueError("Invalid data format")

        # Select the columns we want and rename them
        if format == "polars":
            df = df.select(
                [
                    "id",
                    "abbrev",
                    "name.default",
                    "commonName.default",
                    "placeNameWithPreposition.default",
                    "logo",
                    "darkLogo",
                ]
            )
            df = df.rename({"id": "teamId"})
            df = df.rename({col: col.replace(".default", "") for col in df.columns})

        elif format == "pandas":
            df = df[
                [
                    "id",
                    "abbrev",
                    "name.default",
                    "commonName.default",
                    "placeNameWithPreposition.default",
                    "logo",
                    "darkLogo",
                ]
            ]
            df = df.rename(columns={"id": "teamId"})
            df.columns = [col.replace(".default", "") for col in df.columns]

    except Exception as e:
        raise ValueError(f"Error preprocessing active teams data: {e}")

    return df


def _preprocess_team_stats(
    df: pl.DataFrame | pd.DataFrame, team: str, season: str, sessionId: int, goalies: bool
) -> pl.DataFrame | pd.DataFrame:
    """
    Preprocess the team stats data.
    """
    try:
        if isinstance(df, pl.DataFrame):
            format = "polars"
        elif isinstance(df, pd.DataFrame):
            format = "pandas"

        if format == "polars":
            df = df.with_columns(
                pl.concat_str(["firstName.default", "lastName.default"], separator=" ").alias(
                    "fullName"
                )
            )
            df = df.with_columns(pl.lit(team).alias("team"))
            df = df.with_columns(pl.lit(season).alias("season"))
            df = df.with_columns(pl.lit(sessionId).alias("sessionId"))

        elif format == "pandas":
            df["fullName"] = df["firstName.default"] + " " + df["lastName.default"]
            df["team"] = team
            df["season"] = season
            df["sessionId"] = sessionId

    except Exception as e:
        raise ValueError(f"Error preprocessing team stats data: {e}")

    return df


def _preprocess_team_prospects(
    df: pl.DataFrame | pd.DataFrame, team: str
) -> pl.DataFrame | pd.DataFrame:
    """
    Preprocess the team prospects data.
    """
    try:
        if isinstance(df, pl.DataFrame):
            format = "polars"
        elif isinstance(df, pd.DataFrame):
            format = "pandas"

        # TODO : - Add D0
        #        - Add Sept15 and Dec31 age
        #        - Add draft round and pick and other draft info if available
        if format == "polars":
            # Convert birthDate to date format without time
            df = df.with_columns(
                pl.col("birthDate").str.to_date("%Y-%m-%d").dt.date().alias("birthDate")
            )
            df = df.with_columns(
                pl.concat_str(["firstName.default", "lastName.default"], separator=" ").alias(
                    "fullName"
                )
            )
            df = df.with_columns(pl.lit(team).alias("team"))
            df = df.rename({"id": "playerId"})
        elif format == "pandas":
            df["fullName"] = df["firstName.default"] + " " + df["lastName.default"]
            df["birthDate"] = pd.to_datetime(df["birthDate"])
            df["team"] = team
            df = df.rename(columns={"id": "playerId"})
    except Exception as e:
        raise ValueError(f"Error preprocessing team prospects data: {e}")

    return df


def _preprocess_game_plays(
    df: pl.DataFrame | pd.DataFrame, gameId: int
) -> pl.DataFrame | pd.DataFrame:
    """
    Preprocess the game plays data.
    """
    try:
        if isinstance(df, pl.DataFrame):
            format = "polars"
        elif isinstance(df, pd.DataFrame):
            format = "pandas"

        if format == "polars":
            df = df.with_columns(pl.lit(gameId).alias("gameId"))
            df = df.with_columns(
                (
                    pl.col("timeInPeriod").str.split(":").list.get(0).cast(pl.Int64)
                    * 60  # Minutes to seconds
                    + pl.col("timeInPeriod")
                    .str.split(":")
                    .list.get(1)
                    .cast(pl.Int64)  # Add seconds
                    + (pl.col("periodDescriptor.number") - 1)
                    * 20
                    * 60  # Add period * 20 minutes * 60 seconds
                ).alias("elapsedSeconds")
            )
        elif format == "pandas":
            df["gameId"] = gameId
            df["elapsedSeconds"] = df["timeInPeriod"].apply(
                lambda x: int(x.split(":")[0]) * 60
                + int(x.split(":")[1])
                + (df["periodDescriptor.number"] - 1) * 20 * 60
            )

    except Exception as e:
        raise ValueError(f"Error preprocessing game plays data: {e}")

    return df
