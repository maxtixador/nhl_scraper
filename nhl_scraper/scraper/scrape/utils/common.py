"""
Common utility functions for the NHL Scraper module.

This module contains utility functions that are used throughout the NHL Scraper module.
"""

from datetime import datetime
from typing import Union

import pandas as pd
import polars as pl
import requests


def _get_api_data(url: str, key: str = None, params: dict = None) -> dict:
    """
    Get data from the NHL API.
    """
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    return data[key] if key else data


def _add_metadata(df: pd.DataFrame, source: Union[str, int]) -> pl.DataFrame | pd.DataFrame:
    """
    Add metadata to a DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to add metadata to.
        source (str): The source of the data.

    Returns:
        pl.DataFrame | pd.DataFrame: The DataFrame with metadata added.

    Raises:
        ValueError: If the format is invalid.
        ValueError: If the source is invalid.
    """
    try:
        if isinstance(df, pl.DataFrame):
            format = "polars"
        elif isinstance(df, pd.DataFrame):
            format = "pandas"
    except Exception as e:
        raise ValueError(f"Error finding data format: {e}")

    # Validate source
    if source not in ["NHL API", "NHL.com", "NHL Edge", "NHL Stats API", "NHL HTML"]:
        raise ValueError(f"Invalid source: {source}")

    if format == "polars":
        df = df.with_columns(pl.lit(datetime.now()).alias("meta_datetime"))
        df = df.with_columns(pl.lit(source).alias("meta_source"))
    elif format == "pandas":
        df["meta_datetime"] = pd.to_datetime("now")
        df["meta_source"] = source

    return df
