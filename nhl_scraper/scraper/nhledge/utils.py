"""
NHL Legacy Utils.

This module contains utility functions for the NHL Legacy Scraper.
It uses the setup_chrome_driver function to create a Chrome driver.
It uses the retry_on_failure decorator to retry the function on failure.
It uses the extract_radar_data function to extract the radar data.
It uses the scrapeSkaterSpeed function to scrape the skater speed data.
"""

import logging
import os
from typing import Optional

from selenium import webdriver
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.driver_cache import DriverCacheManager

# Configure logging to only show WARNING and above
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

# Add Chromium binary location to PATH
os.environ["PATH"] += ":/usr/lib/chromium-browser/"


def setup_chrome_driver(headless: bool = True, timeout: int = 30) -> Optional[webdriver.Chrome]:
    """
    Set up and return a configured Chrome WebDriver instance.

    Args:
        headless (bool): Whether to run browser in headless mode
        timeout (int): Page load timeout in seconds

    Returns:
        webdriver.Chrome: Configured Chrome WebDriver instance
        None: If driver creation fails
    """
    try:
        chrome_options = Options()
        if headless:
            chrome_options.add_argument("--headless")

        # Essential options for stability
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-infobars")
        chrome_options.add_argument("--disable-notifications")
        chrome_options.page_load_strategy = "eager"

        # Use cached driver if available
        driver_manager = ChromeDriverManager(cache_manager=DriverCacheManager())
        driver_path = driver_manager.install()

        # Suppress Selenium logging
        service = Service(driver_path)
        service.log_path = os.devnull

        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.set_page_load_timeout(timeout)
        driver.implicitly_wait(timeout // 2)

        return driver

    except WebDriverException as e:
        logger.warning(f"Failed to create Chrome WebDriver session: {str(e)}")
        return None
    except Exception as e:
        logger.warning(f"Unexpected error creating WebDriver session: {str(e)}")
        return None


def create_browser_session(*args, **kwargs) -> Optional[webdriver.Chrome]:
    """
    Alias for setup_chrome_driver for backward compatibility.

    Args:
        *args: Positional arguments
        **kwargs: Keyword arguments

    Returns:
        webdriver.Chrome: Configured Chrome WebDriver instance
    """
    return setup_chrome_driver(*args, **kwargs)
