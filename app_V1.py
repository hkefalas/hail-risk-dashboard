import os
import geopandas as gpd
import pandas as pd
from datetime import datetime
from census import Census
from us import states
from shapely.geometry import Point

# Define the URL and target folder
url = "https://www.spc.noaa.gov/climo/reports/today_filtered_hail.csv"
folder = "hail_reports"
os.makedirs(folder, exist_ok=True)

# Format today's date
today_str = datetime.today().strftime("%Y-%m-%d")
filename = os.path.join(folder, f"{today_str}.csv")

# Only download/save if file doesn't exist
if not os.path.exists(filename):
    hail_df = pd.read_csv(url)
    hail_df.to_csv(filename, index=False)
    print(f"Downloaded and saved hail report to: {filename}")
else:
    hail_df = pd.read_csv(filename)
    print(f"File already exists: {filename}")


def load_and_merge_state(state_abbr, shapefile_path, csv_path):
    gdf = gpd.read_file(shapefile_path)
    df = pd.read_csv(csv_path)

    gdf["GEOID"] = gdf["GEOID"].astype(str).str.zfill(11)
    df["tract_geoid"] = df["tract_geoid"].astype(str).str.zfill(11)

    gdf = gdf.merge(df, left_on="GEOID", right_on="tract_geoid", how="left")
    return gdf

def load_and_merge_tracts(state_abbr, fips_code, shapefile_path, csv_path):
    # Load shapefile and ownership data
    gdf = gpd.read_file(shapefile_path)
    df = pd.read_csv(csv_path)

    # Standardize keys
    gdf["GEOID"] = gdf["GEOID"].astype(str).str.zfill(11)
    df["tract_geoid"] = df["tract_geoid"].astype(str).str.zfill(11)

    # Merge
    gdf = gdf.merge(df, left_on="GEOID", right_on="tract_geoid", how="left")

    # Recalculate households_with_vehicles
    vehicle_cols = [
        "households_with_1_vehicle",
        "households_with_2_vehicles",
        "households_with_3_vehicles",
        "households_with_4_vehicles",
        "households_with_5_vehicles",
        "households_with_6_vehicles",
        "households_with_7_vehicles",
        "households_with_8_or_more_vehicles"
    ]

    if all(col in gdf.columns for col in vehicle_cols):
        gdf["households_with_vehicles"] = gdf[vehicle_cols].sum(axis=1)
    else:
        raise ValueError(f"Missing expected vehicle columns for {state_abbr}")

    return gdf

# File paths
shapefiles = {
    "MO": "tracts/tl_2024_29_tract/tl_2024_29_tract.shp",
    "KS": "tracts/tl_2024_20_tract/tl_2024_20_tract.shp",
    "IA": "tracts/tl_2024_19_tract/tl_2024_19_tract.shp",
    "NE": "tracts/tl_2024_31_tract/tl_2024_31_tract.shp"
}

csvs = {
    "MO": "vehicle_ownership/vehicle_ownership_by_tract_MO.csv",
    "KS": "vehicle_ownership/vehicle_ownership_by_tract_KS.csv",
    "IA": "vehicle_ownership/vehicle_ownership_by_tract_IA.csv",
    "NE": "vehicle_ownership/vehicle_ownership_by_tract_NE.csv"
}

# Load and merge each
gdf_mo = load_and_merge_tracts("MO", 29, shapefiles["MO"], csvs["MO"])
gdf_ks = load_and_merge_tracts("KS", 20, shapefiles["KS"], csvs["KS"])
gdf_ia = load_and_merge_tracts("IA", 19, shapefiles["IA"], csvs["IA"])
gdf_ne = load_and_merge_tracts("NE", 31, shapefiles["NE"], csvs["NE"])

gdf_mo = gdf_mo[gdf_mo["INTPTLON"].astype(float) < -92.3] # west of highway 63 approximation
gdf_all = pd.concat([gdf_mo, gdf_ks, gdf_ia, gdf_ne], ignore_index=True)

# Compute density
gdf_all["land_area_km2"] = gdf_all["ALAND"].astype(float) / 1_000_000
gdf_all["car_ownership_density"] = gdf_all["households_with_vehicles"] / gdf_all["land_area_km2"]
gdf_all["car_ownership_density"] = pd.to_numeric(gdf_all["car_ownership_density"], errors="coerce")
gdf_all = gdf_all.replace([np.inf, -np.inf], np.nan)
gdf_all = gdf_all.dropna(subset=["car_ownership_density"])

# Remove missing coordinates
hail_df = hail_df.dropna(subset=["Lat", "Lon"])

# Convert to GeoDataFrame
hail_gdf = gpd.GeoDataFrame(
    hail_df,
    geometry=gpd.points_from_xy(hail_df.Lon, hail_df.Lat),
    crs="EPSG:4326"
)

# Aggregate again
hail_counts = hail_per_tract.groupby("GEOID").size().reset_index(name="hail_reports")

# Merge back
gdf_all = gdf_all.merge(hail_counts, on="GEOID", how="left")

# Final fill and type
gdf_all["hail_reports"] = gdf_all["hail_reports"].fillna(0).astype(int)

gdf_all["hail_risk_score"] = gdf_all["hail_reports"] * gdf_all["car_ownership_density"]

m = folium.Map(location=[39.5, -96.5], zoom_start=6, tiles="cartodbpositron")

folium.Choropleth(
    geo_data=gdf_all,
    data=gdf_all,
    columns=["GEOID", "hail_risk_score"],
    key_on="feature.properties.GEOID",
    fill_color="YlOrRd",
    fill_opacity=0.7,
    line_opacity=0.2,
    nan_fill_color="white",
    legend_name="Hail Risk Score (Car Density × Hail Reports)"
).add_to(m)

m

risky_tracts = gdf_all[gdf_all["hail_risk_score"] > 0].copy()

risky_tracts[["GEOID", "hail_reports", "car_ownership_density", "hail_risk_score"]].sort_values(
    by="hail_risk_score", ascending=False
)
