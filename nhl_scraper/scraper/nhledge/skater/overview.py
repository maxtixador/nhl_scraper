"""
Collect skater overview data from NHL Edge.

This script scrapes skater overview data from NHL Edge.
It uses the setup_chrome_driver function to create a Chrome driver.
It uses the retry_on_failure decorator to retry the function on failure.
It uses the extract_radar_data function to extract the radar data.
It uses the scrapeSkaterOverview function to scrape the skater overview data.

"""

import logging
import time

# TODO: Add radar data to estimate real percentile
from datetime import datetime
from functools import wraps
from typing import Any, Dict, Optional

import pandas as pd
from selenium.common.exceptions import StaleElementReferenceException, WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from ..utils import setup_chrome_driver

# Configure logging to only show WARNING and above
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)


def retry_on_failure(max_attempts: int = 3, delay: float = 2.0):
    """
    Retry function execution on failure with exponential backoff.

    Args:
        max_attempts (int): Maximum number of attempts
        delay (float): Delay between attempts in seconds

    Returns:
        Decorator function
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_attempts - 1:  # Last attempt
                        raise e
                    wait_time = delay * (2**attempt)  # Exponential backoff
                    logger.warning(f"Attempt {attempt + 1} failed, retrying in {wait_time}s...")
                    time.sleep(wait_time)
            return None

        return wrapper

    return decorator


def extract_radar_data(driver, metrics: list, timeout: int) -> Dict[str, Any]:
    """Extract radar chart data with retry logic."""
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            radar_elements = WebDriverWait(driver, timeout).until(
                EC.presence_of_all_elements_located(
                    (By.CSS_SELECTOR, ".sl-webc__radar-chart__area-datum")
                )
            )
            return {
                metric: {
                    "x": float(element.get_attribute("cx")),
                    "y": float(element.get_attribute("cy")),
                }
                for element, metric in zip(radar_elements, metrics)
            }
        except StaleElementReferenceException:
            if attempt == max_attempts - 1:
                raise
            time.sleep(2)
    return {}


@retry_on_failure(max_attempts=3, delay=2.0)
def scrapeSkaterOverview(
    player_id: str, season: str, session: str, timeout: int = 15
) -> Optional[pd.DataFrame]:
    """
    Fetch and process skater overview data from NHL Edge.

    Args:
        player_id (str): NHL player ID
        season (str): Season year
        session (str): Session identifier (e.g. "regular", "playoffs")
        timeout (int): Seconds to wait for page elements

    Returns:
        Optional[pd.DataFrame]: DataFrame containing player metrics, None if scraping fails
    """
    METRICS = [
        "Top Skating Speed (mph)",
        "Speed Bursts Over 20 mph",
        "Skating Distance (mi)",
        "Top Shot Speed (mph)",
        "Shots on Goal",
        "Shooting %",
        "Goals",
        "Off. Zone Time (ES)",
    ]

    url = f"https://edge.nhl.com/en/skater/{season}-{session}-{player_id}"
    driver = None

    try:
        # Try up to 3 times to initialize the driver
        for attempt in range(3):
            driver = setup_chrome_driver(timeout=timeout)
            if driver:
                break
            time.sleep(2)

        if not driver:
            raise WebDriverException("Failed to initialize Chrome driver after multiple attempts")

        # Add a small delay before loading the page
        time.sleep(1)
        driver.get(url)

        # Wait for initial page load
        time.sleep(2)

        # Wait for and get overview section with retry logic
        overview_section = WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, '[data-section="overview"]'))
        )

        # Ensure the section is fully loaded
        time.sleep(1)
        overview_text = overview_section.text

        # Extract radar data with built-in retry logic
        # radar_data = extract_radar_data(driver, METRICS, timeout)

        # Process overview text data
        data_rows = []
        text_lines = overview_text.split("\n")[3:11]

        if len(text_lines) != len(METRICS):
            raise ValueError(
                f"Mismatch in metrics count. Expected {len(METRICS)}, got {len(text_lines)}"
            )

        for line, metric in zip(text_lines, METRICS):
            values = line.strip(metric + " ").split(" ")
            if len(values) < 3:
                raise ValueError(f"Invalid data format for metric {metric}: {line}")

            value = (
                float(values[0].replace("%", "")) / 100 if "%" in values[0] else float(values[0])
            )
            pos_avg = (
                float(values[1].replace("%", "")) / 100 if "%" in values[1] else float(values[1])
            )
            percentile = "Below 50th" if values[2].isalpha() else float(values[2])

            data_rows.append([metric, value, pos_avg, percentile])

        # Create and merge DataFrames
        overview_df = pd.DataFrame(
            data_rows, columns=["Metric", "Value", "League average by position (F/D)", "Percentile"]
        )

        # TODO: Add radar data to estimate real percentile

        # # Merge data and add metadata
        # final_df = overview_df.merge(radar_df, on='Metric', how='left')
        final_df = overview_df

        final_df["player_id"] = player_id
        final_df["season"] = season
        final_df["session"] = session
        final_df["url"] = url
        final_df["scraped_at"] = datetime.now()

        return final_df

    except Exception as e:
        logger.warning(f"Failed to scrape {url}: {str(e)}")
        return None

    finally:
        if driver:
            try:
                driver.quit()
            except Exception as e:
                logger.warning(f"Failed to quit driver: {str(e)}")
                pass


# Add alias for backward compatibility
get_skater_overview = scrapeSkaterOverview
