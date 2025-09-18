import geopandas as gpd
import pandas as pd
import os

# Adjusting import paths
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import MISSOURI_LONGITUDE_FILTER
from utils import setup_logging

logger = setup_logging()

def merge_data(tracts_gdf, vehicles_df, income_df):
    """
    Merges tract, vehicle, and income data into a single GeoDataFrame.
    """
    logger.info("Merging all data sources...")

    # Standardize GEOID columns for merging
    tracts_gdf["GEOID"] = tracts_gdf["GEOID"].astype(str).str.zfill(11)
    vehicles_df["tract_geoid"] = vehicles_df["tract_geoid"].astype(str).str.zfill(11)
    income_df["tract_geoid"] = income_df["tract_geoid"].astype(str).str.zfill(11)

    # Merge tracts with vehicle data
    merged_gdf = tracts_gdf.merge(
        vehicles_df.drop(columns=['state_abbr']),  # Drop redundant state_abbr
        left_on="GEOID",
        right_on="tract_geoid",
        how="left"
    )

    # Merge with income data
    merged_gdf = merged_gdf.merge(
        income_df,
        left_on="GEOID",
        right_on="tract_geoid",
        how="left",
        suffixes=('', '_income') # Add suffix to avoid column name conflicts
    )

    # Clean up columns
    merged_gdf.drop(columns=['tract_geoid', 'tract_geoid_income'], inplace=True, errors='ignore')

    return merged_gdf

def calculate_densities_and_ownership(gdf):
    """
    Calculates vehicle ownership, population density, and car ownership density.
    """
    logger.info("Calculating densities and vehicle ownership...")

    vehicle_cols = [
        "households_with_1_vehicle", "households_with_2_vehicles",
        "households_with_3_vehicles", "households_with_4_vehicles",
        "households_with_5_vehicles", "households_with_6_vehicles",
        "households_with_7_vehicles", "households_with_8_or_more_vehicles"
    ]
    gdf["households_with_vehicles"] = gdf[vehicle_cols].sum(axis=1)

    gdf["land_area_km2"] = gdf["ALAND"].astype(float) / 1_000_000
    gdf["population_density"] = gdf["total_population"] / gdf["land_area_km2"]
    gdf["car_ownership_density"] = gdf["households_with_vehicles"] / gdf["land_area_km2"]

    # Handle potential division by zero or missing data
    gdf.fillna({
        "population_density": 0,
        "car_ownership_density": 0,
        "households_with_vehicles": 0
    }, inplace=True)

    return gdf

def calculate_hail_risk(merged_gdf, hail_gdf):
    """
    Performs spatial join to count hail reports per tract and calculates risk score.
    """
    logger.info("Performing spatial join to count hail reports per tract...")

    # Ensure CRSs match before spatial join
    if merged_gdf.crs != hail_gdf.crs:
        hail_gdf = hail_gdf.to_crs(merged_gdf.crs)

    hail_per_tract = gpd.sjoin(hail_gdf, merged_gdf, how="inner", predicate="within")
    hail_counts = hail_per_tract.groupby("GEOID").size().reset_index(name="hail_reports")

    # Merge hail counts back to the main GeoDataFrame
    final_gdf = merged_gdf.merge(hail_counts, on="GEOID", how="left")
    final_gdf["hail_reports"] = final_gdf["hail_reports"].fillna(0).astype(int)

    logger.info("Calculating hail risk score...")
    final_gdf["hail_risk_score"] = final_gdf["hail_reports"] * final_gdf["car_ownership_density"]

    return final_gdf

def apply_filters(gdf):
    """
    Applies any specific filters to the data, e.g., for Missouri.
    """
    logger.info("Applying data filters...")

    # Apply Missouri longitude filter
    mo_filter = (gdf['state_abbr'] == 'MO') & (gdf['INTPTLON'].astype(float) >= MISSOURI_LONGITUDE_FILTER)
    gdf = gdf[~mo_filter] # Keep rows that DON'T meet this condition

    return gdf

def process_all_data(tracts_gdf, vehicles_df, income_df, hail_gdf):
    """
    Main function to orchestrate the entire data processing workflow.
    """
    logger.info("Starting data processing workflow...")

    merged_gdf = merge_data(tracts_gdf, vehicles_df, income_df)
    merged_gdf = calculate_densities_and_ownership(merged_gdf)

    merged_gdf = apply_filters(merged_gdf) # Apply filters before spatial join for efficiency

    final_gdf = calculate_hail_risk(merged_gdf, hail_gdf)

    logger.info("Data processing complete.")
    return final_gdf
