import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import pyart
import cartopy.crs as ccrs
import boto3
from botocore import UNSIGNED
from botocore.config import Config
from geopy.distance import geodesic
import os
from datetime import datetime, timedelta
import pytz
from pathlib import Path
import logging

# Setup logger
logger = logging.getLogger(__name__)

# Constants
BUCKET_NAME = "noaa-nexrad-level2"
STATION_LIST_PATH = os.path.join("station_list", "nexrad_sites.csv")

def find_closest_nexrad_id(hail_lat, hail_lon, site_csv_path=STATION_LIST_PATH):
    """Finds the closest NEXRAD station ID to a given lat/lon."""
    if not os.path.exists(site_csv_path):
        logger.error(f"Station list not found at {site_csv_path}")
        return None

    try:
        sites = pd.read_csv(site_csv_path)
        # Clean column names
        sites.columns = [col.strip() for col in sites.columns]

        # Drop rows with missing coordinates
        sites = sites.dropna(subset=["LATITUDE_N", "LONGITUDE_W"])

        # Compute distance
        sites["distance_km"] = sites.apply(
            lambda row: geodesic(
                (hail_lat, hail_lon), (row["LATITUDE_N"], -abs(row["LONGITUDE_W"]))
            ).kilometers,
            axis=1
        )

        closest = sites.sort_values("distance_km").iloc[0]
        return closest["ID"]
    except Exception as e:
        logger.error(f"Error finding closest station: {e}")
        return None

def download_radar_files(site_id, event_time, output_folder="radar_data"):
    """Downloads Level 2 radar data from AWS S3."""
    os.makedirs(output_folder, exist_ok=True)

    # Use anonymous access
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    # Define time window (+/- 15 mins)
    start_time = event_time - timedelta(minutes=15)
    end_time = event_time + timedelta(minutes=15)

    # S3 prefix structure: YYYY/MM/DD/SITE_ID/
    prefix = f"{event_time.year:04}/{event_time.month:02}/{event_time.day:02}/{site_id}/"
    logger.info(f"Looking for radar data in {BUCKET_NAME}/{prefix}")

    downloaded_files = []

    try:
        response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
        if "Contents" not in response:
            logger.warning("No radar files found in S3 bucket for this date/station.")
            return []

        for obj in response["Contents"]:
            key = obj["Key"]
            filename = key.split("/")[-1]

            # Parse timestamp from filename (e.g., KDVN20250712_224026_V06)
            # Standard format: XXXXYYYYMMDD_HHMMSS_Vxx
            try:
                # Extract time part: characters 4 to 19
                # Example: KDVN 20250712 _ 224026
                time_part = filename[4:19]
                file_time = datetime.strptime(time_part, "%Y%m%d_%H%M%S").replace(tzinfo=pytz.UTC)

                if start_time <= file_time <= end_time:
                    out_path = os.path.join(output_folder, filename)
                    if not os.path.exists(out_path):
                        logger.info(f"Downloading {filename}")
                        s3.download_file(BUCKET_NAME, key, out_path)
                    downloaded_files.append(out_path)
            except Exception as e:
                # Ignore files that don't match the expected format or other errors
                continue

    except Exception as e:
        logger.error(f"Error accessing S3: {e}")
        return []

    return sorted(downloaded_files)

def generate_radar_image(radar_file, output_image_path):
    """
    Generates a PNG image of reflectivity from a radar file.
    Returns the image path and bounds [west, south, east, north].
    """
    try:
        radar = pyart.io.read_nexrad_archive(radar_file)
        display = pyart.graph.RadarMapDisplay(radar)

        # Determine bounds based on radar range (e.g., 100km around station)
        # Or simpler: use a fixed small domain around the center
        lat = radar.latitude['data'][0]
        lon = radar.longitude['data'][0]
        delta = 1.0 # approx 111km

        min_lon, max_lon = lon - delta, lon + delta
        min_lat, max_lat = lat - delta, lat + delta

        # Setup plot
        fig = plt.figure(figsize=(6, 6))
        # Use PlateCarree for simple lat/lon projection
        ax = plt.subplot(1, 1, 1, projection=ccrs.PlateCarree())

        # Plot reflectivity
        # We set make_image=True to try to get a raster, but plot_ppi_map does complex things.
        # To get a clean image for overlay, we need to remove axes and margins.
        display.plot_ppi_map(
            'reflectivity', 0,
            vmin=-32, vmax=64,
            min_lon=min_lon, max_lon=max_lon,
            min_lat=min_lat, max_lat=max_lat,
            resolution='10m',
            projection=ccrs.PlateCarree(),
            colorbar_flag=False,
            title_flag=False,
            ax=ax,
            embellish=False
        )

        # Remove axis
        ax.axis('off')

        # Save to file with tight bounding box and transparent background
        plt.savefig(output_image_path, bbox_inches='tight', pad_inches=0, transparent=True, dpi=100)
        plt.close(fig)

        # The bounds we asked for might not be exactly what saved due to aspect ratio and bbox_inches='tight'.
        # However, for a simple visualization, passing the requested bounds might be close enough
        # IF the plot actually respected them fully.
        # A safer way for map overlay is using pyart to export to GeoTIFF or similar,
        # but sticking to the requested bounds [min_lon, min_lat, max_lon, max_lat] is a reasonable approximation
        # if the aspect ratio matches.

        return output_image_path, [min_lon, min_lat, max_lon, max_lat]

    except Exception as e:
        logger.error(f"Error generating radar image: {e}")
        if 'plt' in locals():
            plt.close()
        return None, None
