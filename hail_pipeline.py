import os
import pandas as pd
import geopandas as gpd
from datetime import datetime
from shapely.geometry import Point

HAIL_FOLDER = "hail_reports"
TRACT_FOLDER = "census_data/tracts"
OWNERSHIP_FOLDER = "census_data/vehicle_ownership"
PROCESSED_FOLDER = "census_data"
INCOME_CSV_PATH = "census_data/income_by_tract.csv"

os.makedirs(HAIL_FOLDER, exist_ok=True)
os.makedirs(TRACT_FOLDER, exist_ok=True)
os.makedirs(OWNERSHIP_FOLDER, exist_ok=True)
os.makedirs(PROCESSED_FOLDER, exist_ok=True)

def download_hail_report():
    url = "https://www.spc.noaa.gov/climo/reports/today_filtered_hail.csv"
    today_str = datetime.today().strftime("%Y-%m-%d")
    filepath = os.path.join(HAIL_FOLDER, f"{today_str}.csv")

    if not os.path.exists(filepath):
        df = pd.read_csv(url)
        df.to_csv(filepath, index=False)
        print(f"Downloaded and saved hail report to: {filepath}")
    else:
        df = pd.read_csv(filepath)
        print(f"Using existing hail report: {filepath}")
    return df

def load_and_merge_tracts(abbr, shapefile_path, csv_path):
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
    gdf["households_with_vehicles"] = gdf[vehicle_cols].sum(axis=1)
    return gdf

def generate_state_data():
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

    for abbr, (fips, shp, csv) in states_info.items():
        gdf = load_and_merge_tracts(abbr, shp, csv)
        if abbr == "MO":
            gdf = gdf[gdf["INTPTLON"].astype(float) < -92.3]

        income_df = pd.read_csv(INCOME_CSV_PATH)
        income_df["state"] = income_df["state"].astype(str).str.zfill(2)
        income_df["county"] = income_df["county"].astype(str).str.zfill(3)
        income_df["tract_geoid"] = income_df["tract_geoid"].astype(str).str.zfill(11)
        income_cols = ["tract_geoid", "per_capita_income", "median_income", "total_population"]
        gdf = gdf.merge(income_df[income_cols], left_on="GEOID", right_on="tract_geoid", how="left")

        gdf["land_area_km2"] = gdf["ALAND"].astype(float) / 1_000_000
        gdf["car_ownership_density"] = gdf["households_with_vehicles"] / gdf["land_area_km2"]
        gdf["population_density"] = gdf["total_population"] / gdf["land_area_km2"]

        gdf["car_ownership_density"] = gdf["car_ownership_density"].fillna(0).round(2)
        gdf["population_density"] = gdf["population_density"].fillna(0).round(2)

        hail_gdf_proj = hail_gdf.to_crs(gdf.crs)
        hail_within = hail_gdf_proj[hail_gdf_proj.within(gdf.geometry.union_all())]

        hail_per_tract = gpd.sjoin(hail_within, gdf, how="inner", predicate="within")
        hail_counts = hail_per_tract.groupby("GEOID").size().reset_index(name="hail_reports")

        gdf = gdf.merge(hail_counts, on="GEOID", how="left")
        gdf["hail_reports"] = gdf["hail_reports"].fillna(0).astype(int)
        gdf["hail_risk_score"] = gdf["hail_reports"] * gdf["car_ownership_density"]

        gdf.to_file(f"{PROCESSED_FOLDER}/gdf_{abbr}_with_hail_risk.geojson", driver="GeoJSON")
        hail_within.to_file(f"{PROCESSED_FOLDER}/hail_points_{abbr}.geojson", driver="GeoJSON")
        print(f"Saved processed files for {abbr}")
        print(f"Columns in gdf for {abbr}:", gdf.columns.tolist())
        print(f"Missing data in gdf for {abbr}:\n", gdf.isna().sum()[gdf.isna().sum() > 0])

def run_hail_risk_pipeline():
    state_abbrs = ["MO", "KS", "IA", "NE"]

    gdf_all = gpd.GeoDataFrame(pd.concat([
        gpd.read_file(f"{PROCESSED_FOLDER}/gdf_{abbr}_with_hail_risk.geojson")
        for abbr in state_abbrs if os.path.exists(f"{PROCESSED_FOLDER}/gdf_{abbr}_with_hail_risk.geojson")
    ], ignore_index=True), crs="EPSG:4326")

    income_df = pd.read_csv(INCOME_CSV_PATH)
    income_df["tract_geoid"] = income_df["tract_geoid"].astype(str).str.zfill(11)
    gdf_all["GEOID"] = gdf_all["GEOID"].astype(str).str.zfill(11)

    gdf_all = gdf_all.merge(
        income_df[["tract_geoid", "median_income", "per_capita_income", "total_population"]],
        left_on="GEOID", right_on="tract_geoid", how="left"
    )

    hail_gdf = gpd.GeoDataFrame(pd.concat([
        gpd.read_file(f"{PROCESSED_FOLDER}/hail_points_{abbr}.geojson")
        for abbr in state_abbrs if os.path.exists(f"{PROCESSED_FOLDER}/hail_points_{abbr}.geojson")
    ], ignore_index=True), crs="EPSG:4326")

    return gdf_all, hail_gdf

if __name__ == "__main__":
    generate_state_data()
