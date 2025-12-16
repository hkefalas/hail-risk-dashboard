import os
import sys

# Adjusting import paths to find project-level modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.download_data import download_hail_report
from pipeline.load_data import (
    load_all_tracts,
    load_all_vehicle_ownership,
    load_income_data,
    load_hail_data,
)
from pipeline.process_data import process_all_data
from config import PROCESSED_DATA_DIR, STATES
from utils import setup_logging, save_geojson, ensure_dir_exists

# Setup logger
logger = setup_logging()

def main():
    """
    Main function to run the entire data processing pipeline.
    """
    logger.info("--- Starting Hail Risk Data Pipeline ---")

    # --- 1. Download Data ---
    try:
        hail_csv_path = download_hail_report()
    except Exception as e:
        logger.error(f"Pipeline stopped: Could not download hail report. Reason: {e}")
        return

    # --- 2. Load Data ---
    logger.info("--- Loading all data sources ---")
    try:
        tracts_gdf = load_all_tracts()
        vehicles_df = load_all_vehicle_ownership()
        income_df = load_income_data()
        hail_gdf = load_hail_data(hail_csv_path)
    except Exception as e:
        logger.error(f"Pipeline stopped: Failed to load data. Reason: {e}")
        return

    # --- 3. Process Data ---
    logger.info("--- Processing and analyzing data ---")
    try:
        final_gdf = process_all_data(tracts_gdf, vehicles_df, income_df, hail_gdf)
    except Exception as e:
        logger.error(f"Pipeline stopped: Failed during data processing. Reason: {e}")
        return

    # --- 4. Save Processed Data ---
    logger.info("--- Saving processed data for each state ---")
    ensure_dir_exists(PROCESSED_DATA_DIR, logger)

    for state_abbr in STATES.keys():
        state_gdf = final_gdf[final_gdf['state_abbr'] == state_abbr]
        output_path = os.path.join(PROCESSED_DATA_DIR, f"gdf_{state_abbr}_with_hail_risk.geojson")

        if not state_gdf.empty:
            try:
                save_geojson(state_gdf, output_path, logger)
                logger.info(f"Successfully saved processed data for {state_abbr} to {output_path}")
            except Exception as e:
                logger.error(f"Could not save data for {state_abbr}. Reason: {e}")
        else:
            logger.warning(f"No data to save for state: {state_abbr}")

    logger.info("--- Hail Risk Data Pipeline Finished Successfully ---")

if __name__ == "__main__":
    main()
