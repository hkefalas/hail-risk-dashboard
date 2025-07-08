# --- hail_pipeline.py ---
import os
import pandas as pd
import geopandas as gpd
from datetime import datetime
from shapely.geometry import Point

HAIL_FOLDER = "hail_reports"
TRACT_FOLDER = "census_data/tracts"
OWNERSHIP_FOLDER = "census_data/vehicle_ownership"

os.makedirs(HAIL_FOLDER, exist_ok=True)

def download_hail_report():
    url = "https://www.spc.noaa.gov/climo/reports/today_filtered_hail.csv"
    today_str = datetime.today().strftime("%Y-%m-%d")
    filepath = os.path.join(HAIL_FOLDER, f"{today_str}.csv")

    if not os.path.exists(filepath):
        df = pd.read_csv(url)
        df.to_csv(filepath, index=False)
    else:
        df = pd.read_csv(filepath)
    return df

def load_and_merge_tracts(state_abbr, shapefile_path, csv_path):
    gdf = gpd.read_file(shapefile_path)
    df = pd.read_csv(csv_path)
    gdf["GEOID"] = gdf["GEOID"].astype(str).str.zfill(11)
    df["tract_geoid"] = df["tract_geoid"].astype(str).str.zfill(11)
    gdf = gdf.merge(df, left_on="GEOID", right_on="tract_geoid", how="left")

    vehicle_cols = [
        "households_with_1_vehicle", "households_with_2_vehicles",
        "households_with_3_vehicles", "households_with_4_vehicles",
        "households_with_5_vehicles", "households_with_6_vehicles",
        "households_with_7_vehicles", "households_with_8_or_more_vehicles"
    ]
    if all(col in gdf.columns for col in vehicle_cols):
        gdf["households_with_vehicles"] = gdf[vehicle_cols].sum(axis=1)
    else:
        raise ValueError(f"Missing vehicle columns for {state_abbr}")
    return gdf

def run_hail_risk_pipeline():
    hail_df = download_hail_report()
    hail_df = hail_df.dropna(subset=["Lat", "Lon"])
    hail_gdf = gpd.GeoDataFrame(
        hail_df,
        geometry=gpd.points_from_xy(hail_df.Lon, hail_df.Lat),
        crs="EPSG:4326"
    )

    states_info = {
        "MO": ("29", f"{TRACT_FOLDER}/tl_2024_29_tract/tl_2024_29_tract.shp", f"{OWNERSHIP_FOLDER}/vehicle_ownership_by_tract_MO.csv"),
        "KS": ("20", f"{TRACT_FOLDER}/tl_2024_20_tract/tl_2024_20_tract.shp", f"{OWNERSHIP_FOLDER}/vehicle_ownership_by_tract_KS.csv"),
        "IA": ("19", f"{TRACT_FOLDER}/tl_2024_19_tract/tl_2024_19_tract.shp", f"{OWNERSHIP_FOLDER}/vehicle_ownership_by_tract_IA.csv"),
        "NE": ("31", f"{TRACT_FOLDER}/tl_2024_31_tract/tl_2024_31_tract.shp", f"{OWNERSHIP_FOLDER}/vehicle_ownership_by_tract_NE.csv")
    }

    all_gdfs = []
    for abbr, (fips, shp, csv) in states_info.items():
        gdf = load_and_merge_tracts(abbr, shp, csv)
        if abbr == "MO":
            gdf = gdf[gdf["INTPTLON"].astype(float) < -92.3]  # west of Hwy 63
        all_gdfs.append(gdf)

    gdf_all = pd.concat(all_gdfs, ignore_index=True)
    gdf_all["land_area_km2"] = gdf_all["ALAND"].astype(float) / 1_000_000
    gdf_all["car_ownership_density"] = gdf_all["households_with_vehicles"] / gdf_all["land_area_km2"]
    gdf_all = gdf_all.replace([float("inf"), float("-inf")], pd.NA).dropna(subset=["car_ownership_density"])

    hail_gdf = hail_gdf.to_crs(gdf_all.crs)
    combined_shape = gdf_all.geometry.unary_union
    hail_gdf = hail_gdf[hail_gdf.within(combined_shape)].copy()

    hail_per_tract = gpd.sjoin(hail_gdf, gdf_all, how="inner", predicate="within")
    hail_counts = hail_per_tract.groupby("GEOID").size().reset_index(name="hail_reports")
    gdf_all = gdf_all.merge(hail_counts, on="GEOID", how="left")
    gdf_all["hail_reports"] = gdf_all["hail_reports"].fillna(0).astype(int)
    gdf_all["hail_risk_score"] = gdf_all["hail_reports"] * gdf_all["car_ownership_density"]

    return gdf_all, hail_gdf
