import pandas as pd
from geopy.distance import geodesic
import boto3
from botocore import UNSIGNED
from botocore.config import Config
import os
import glob
from datetime import datetime, timedelta
import pytz
from config import NEXRAD_SITES_PATH, RADAR_FRAMES_DIR

# Setup S3 client (anonymous access)
s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
BUCKET_NAME = "noaa-nexrad-level2"

def find_closest_nexrad_id(lat, lon):
    """
    Finds the nearest NEXRAD station ID for a given latitude and longitude.
    """
    try:
        if not os.path.exists(NEXRAD_SITES_PATH):
            print(f"Error: {NEXRAD_SITES_PATH} not found.")
            return None

        sites = pd.read_csv(NEXRAD_SITES_PATH)
        # Clean column names
        sites.columns = [col.strip() for col in sites.columns]

        # Ensure we have coordinates
        sites = sites.dropna(subset=["LATITUDE_N", "LONGITUDE_W"])

        # Calculate distance
        sites["distance_km"] = sites.apply(
            lambda row: geodesic((lat, lon), (row["LATITUDE_N"], -abs(row["LONGITUDE_W"]))).kilometers,
            axis=1
        )

        closest = sites.sort_values("distance_km").iloc[0]
        return closest["ID"]
    except Exception as e:
        print(f"Error finding closest NEXRAD station: {e}")
        return None

def download_radar_files(site_id, event_time, output_folder=RADAR_FRAMES_DIR):
    """
    Downloads NEXRAD Level 2 data from AWS S3 for a specific site and time window.
    """
    os.makedirs(output_folder, exist_ok=True)

    # Ensure event_time is timezone aware (UTC)
    if event_time.tzinfo is None:
        event_time = event_time.replace(tzinfo=pytz.UTC)

    start_time = event_time - timedelta(minutes=15)
    end_time = event_time + timedelta(minutes=15)

    prefix = f"{event_time.year:04}/{event_time.month:02}/{event_time.day:02}/{site_id}/"
    print(f"Searching S3: {BUCKET_NAME}/{prefix}")

    try:
        response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
        if "Contents" not in response:
            print("No radar files found.")
            return []

        downloaded_files = []
        for obj in response["Contents"]:
            key = obj["Key"]
            filename = key.split("/")[-1]

            # Parse time from filename (e.g., KDVN20250712_224026_V06)
            try:
                # Format is usually SITE + YYYYMMDD_HHMMSS + _V06
                # We need to find the date part.
                # Assuming standard format: KXXXYYYYMMDD_HHMMSS...
                # KDVN is 4 chars. But some are 3? NEXRAD IDs are 4.
                # Let's extract by splitting or fixed position if ID length varies?
                # Actually, filename usually starts with ID.
                # Let's try to parse the timestamp part.

                # Robust extraction: find the string that looks like YYYYMMDD_HHMMSS
                import re
                match = re.search(r"(\d{8}_\d{6})", filename)
                if match:
                    ts_str = match.group(1)
                    file_time = datetime.strptime(ts_str, "%Y%m%d_%H%M%S").replace(tzinfo=pytz.UTC)

                    if start_time <= file_time <= end_time:
                        out_path = os.path.join(output_folder, filename)
                        if not os.path.exists(out_path):
                            print(f"Downloading: {filename}")
                            s3.download_file(BUCKET_NAME, key, out_path)
                        downloaded_files.append(out_path)
            except Exception as e:
                print(f"Skipping {filename}: {e}")

        return downloaded_files

    except Exception as e:
        print(f"Error accessing S3: {e}")
        return []

# Note: Image generation using pyart is omitted here as it requires complex dependencies (arm-pyart).
# For this task, we will focus on downloading the raw data or checking if we can reuse existing frames.
