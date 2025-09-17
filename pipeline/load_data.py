import pandas as pd
import geopandas as gpd
import os

# Adjusting import paths for modular structure
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import STATES, INCOME_CSV_PATH
from utils import load_csv, load_geojson, setup_logging

logger = setup_logging()

def load_all_tracts() -> gpd.GeoDataFrame:
    """
    Loads and concatenates census tract shapefiles for all states defined in the config.
    """
    gdfs = []
    for state_abbr, state_info in STATES.items():
        logger.info(f"Loading tract data for {state_abbr}...")
        gdf = gpd.read_file(state_info["shapefile"])
        gdf['state_abbr'] = state_abbr  # Add state abbreviation for reference
        gdfs.append(gdf)

    logger.info("Concatenating all tract data...")
    all_tracts_gdf = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True))
    return all_tracts_gdf

def load_all_vehicle_ownership() -> pd.DataFrame:
    """
    Loads and concatenates vehicle ownership CSVs for all states.
    """
    dfs = []
    for state_abbr, state_info in STATES.items():
        logger.info(f"Loading vehicle ownership data for {state_abbr}...")
        df = load_csv(state_info["vehicle_csv"], dtype={'tract_geoid': str}, logger=logger)
        df['state_abbr'] = state_abbr
        dfs.append(df)

    logger.info("Concatenating all vehicle ownership data...")
    all_vehicles_df = pd.concat(dfs, ignore_index=True)
    return all_vehicles_df

def load_income_data() -> pd.DataFrame:
    """
    Loads the per capita and median income data from the CSV file.
    """
    logger.info(f"Loading income data from {INCOME_CSV_PATH}...")
    return load_csv(INCOME_CSV_PATH, dtype={'tract_geoid': str}, logger=logger)

def load_hail_data(hail_csv_path: str) -> gpd.GeoDataFrame:
    """
    Loads the hail report CSV and converts it to a GeoDataFrame.
    """
    logger.info(f"Loading hail data from {hail_csv_path}...")
    df = load_csv(hail_csv_path, logger=logger)
    df = df.dropna(subset=["Lat", "Lon"])

    hail_gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df.Lon, df.Lat),
        crs="EPSG:4326"
    )
    return hail_gdf
