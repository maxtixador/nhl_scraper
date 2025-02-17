"""
Collect skater speed data from NHL Edge.

This script scrapes skater speed data from NHL Edge.
It uses the setup_chrome_driver function to create a Chrome driver.
It uses the retry_on_failure decorator to retry the function on failure.
It uses the extract_radar_data function to extract the radar data.
It uses the scrapeSkaterSpeed function to scrape the skater speed data.

"""

import time
from datetime import datetime
from functools import wraps
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd
from selenium.common.exceptions import StaleElementReferenceException, WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from ..utils import logger, setup_chrome_driver


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
def scrapeSkaterSpeed(
    player_id: str, season: str, session: str, timeout: int = 15
) -> Optional[pd.DataFrame]:
    """
    Fetch and process skater speed data from NHL Edge.

    Args:
        player_id (str): NHL player ID
        season (str): Season year
        session (str): Session identifier
        timeout (int): Seconds to wait for page elements

    Returns:
        Optional[pd.DataFrame]: DataFrame containing speed metrics, None if scraping fails
    """
    METRICS = ["Top Speed (mph)", "22+ mph bursts", "20-22 mph bursts", "18-20 mph bursts"]

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

        # Wait for and get speed section with retry logic
        speed_section = WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located(
                (By.XPATH, '//*[@id="skatingspeed-section-content"]/div[1]/div/table/tbody')
            )
        )

        # Ensure the section is fully loaded
        time.sleep(1)
        speed_text = speed_section.text

        # Extract radar data with built-in retry logic
        radar_data = extract_radar_data(driver, METRICS, timeout)

        # Process speed text data
        data_rows = []
        text_lines = speed_text.split("\n")

        if len(text_lines) != len(METRICS):
            raise ValueError(
                f"Mismatch in metrics count. Expected {len(METRICS)}, got {len(text_lines)}"
            )

        for line, metric in zip(text_lines, METRICS):
            line1 = line.replace(metric + " ", "")
            parts = line1.split(" ")
            value = float(parts[0].replace("%", "")) / 100 if "%" in parts[0] else float(parts[0])
            pos_avg = float(parts[1].replace("%", "")) / 100 if "%" in parts[1] else float(parts[1])
            percentile = "Below 50th" if parts[2].isalpha() else float(parts[2])

            data_rows.append([metric, value, pos_avg, percentile])

        # Create and merge DataFrames
        speed_df = pd.DataFrame(
            data_rows, columns=["Metric", "Value", "League average by position (F/D)", "Percentile"]
        )

        radar_df = pd.DataFrame(radar_data).T.reset_index()
        radar_df.columns = ["Metric", "x", "y"]
        radar_df["Distance from centre"] = np.sqrt(
            (radar_df["x"] - 0) ** 2 + (radar_df["y"] - 0) ** 2
        )

        def estimate_percentile(row):
            distance = row["Distance from centre"]
            value = row["Value"]

            # If value is 0 or very close to 0, force percentile to 1
            if abs(value) < 0.001:
                return 1

            # Otherwise use our regular scaling
            if distance < 45:
                return max(5, min(45, (distance / 90) * 35))
            else:
                return min(50 + ((distance - 45) / 45) * 50, 100)

        radar_df["Percentile estimation"] = radar_df.apply(estimate_percentile, axis=1)

        # Merge data and add metadata
        final_df = speed_df.merge(radar_df, on="Metric", how="left")
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
get_player_speed = scrapeSkaterSpeed
