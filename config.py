import os

# --- Directory Paths ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "census_data")
HAIL_REPORTS_DIR = os.path.join(BASE_DIR, "hail_reports")
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, "processed")
RADAR_FRAMES_DIR = os.path.join(BASE_DIR, "radar_frames")

# --- File Paths ---
INCOME_CSV_PATH = os.path.join(DATA_DIR, "income_by_tract.csv")
NEXRAD_SITES_PATH = os.path.join(BASE_DIR, "station_list", "nexrad_sites.csv")

# --- Data Source URLs ---
HAIL_DATA_URL = "https://www.spc.noaa.gov/climo/reports/today_filtered_hail.csv"

# --- State Information ---
# This dictionary centralizes all state-specific information.
# - fips: The FIPS code for the state.
# - center: The latitude and longitude for centering maps.
# - shapefile: The path to the census tract shapefile.
# - vehicle_csv: The path to the vehicle ownership CSV file.
STATES = {
    "MO": {
        "fips": "29",
        "center": (38.5, -92.5),
        "shapefile": os.path.join(DATA_DIR, "tracts/tl_2024_29_tract/tl_2024_29_tract.shp"),
        "vehicle_csv": os.path.join(DATA_DIR, "vehicle_ownership/vehicle_ownership_by_tract_MO.csv"),
    },
    "KS": {
        "fips": "20",
        "center": (38.5, -98.0),
        "shapefile": os.path.join(DATA_DIR, "tracts/tl_2024_20_tract/tl_2024_20_tract.shp"),
        "vehicle_csv": os.path.join(DATA_DIR, "vehicle_ownership/vehicle_ownership_by_tract_KS.csv"),
    },
    "IA": {
        "fips": "19",
        "center": (42.0, -93.0),
        "shapefile": os.path.join(DATA_DIR, "tracts/tl_2024_19_tract/tl_2024_19_tract.shp"),
        "vehicle_csv": os.path.join(DATA_DIR, "vehicle_ownership/vehicle_ownership_by_tract_IA.csv"),
    },
    "NE": {
        "fips": "31",
        "center": (41.5, -99.5),
        "shapefile": os.path.join(DATA_DIR, "tracts/tl_2024_31_tract/tl_2024_31_tract.shp"),
        "vehicle_csv": os.path.join(DATA_DIR, "vehicle_ownership/vehicle_ownership_by_tract_NE.csv"),
    },
}

# --- Map and Visualization Settings ---
LAYER_OPTIONS = {
    "Vehicle Ownership Density": "car_ownership_density",
    "Population Density": "population_density",
    "Median Income": "median_income",
    "Per Capita Income": "per_capita_income",
    "Hail Risk Score": "hail_risk_score",
}

# --- Radar Settings ---
# Approximate lat/lon bounds for the radar frames (Iowa/Illinois region - KDVN)
# Format: [west, south, east, north]
RADAR_BOUNDS = [-93.6, 41.4, -90.6, 43.2]

# A special value for a specific data filter in the original pipeline.
# This is preserved to maintain the original logic.
# This longitude is a proxy for highway 63, where everything west of the highway is in the business area.
MISSOURI_LONGITUDE_FILTER = -92.3
