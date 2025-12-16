import os
import pandas as pd
from datetime import datetime
import requests

# Adjusting import paths for modular structure
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import HAIL_DATA_URL, HAIL_REPORTS_DIR
from utils import setup_logging, ensure_dir_exists

logger = setup_logging()

def download_hail_report():
    """
    Downloads the daily hail report from the NOAA website if it doesn't already exist.

    Returns:
        str: The file path of the downloaded or existing hail report.
    """
    ensure_dir_exists(HAIL_REPORTS_DIR, logger)

    today_str = datetime.today().strftime("%Y-%m-%d")
    filepath = os.path.join(HAIL_REPORTS_DIR, f"{today_str}.csv")

    if not os.path.exists(filepath):
        logger.info(f"Downloading hail report from: {HAIL_DATA_URL}")
        try:
            response = requests.get(HAIL_DATA_URL)
            response.raise_for_status()  # Raise an exception for bad status codes

            # Save the content to a file
            with open(filepath, 'w') as f:
                f.write(response.text)

            logger.info(f"Saved hail report to: {filepath}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download hail report: {e}")
            raise
    else:
        logger.info(f"Using existing hail report: {filepath}")

    return filepath
