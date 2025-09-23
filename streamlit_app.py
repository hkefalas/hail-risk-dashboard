import streamlit as st
import pandas as pd
import geopandas as gpd
import pydeck as pdk
import requests
import os
from datetime import datetime, timedelta

# --- Constants and Configuration ---
HAIL_REPORTS_FOLDER = "hail_reports"
CENSUS_DATA_FOLDER = "census_data"
TRACTS_FOLDER = os.path.join(CENSUS_DATA_FOLDER, "tracts")
OWNERSHIP_FOLDER = os.path.join(CENSUS_DATA_FOLDER, "vehicle_ownership")
INCOME_CSV_PATH = os.path.join(CENSUS_DATA_FOLDER, "income_by_tract.csv")

# Create folders if they don't exist
os.makedirs(HAIL_REPORTS_FOLDER, exist_ok=True)

STATE_OPTIONS = ["MO", "KS", "IA", "NE"]
LAYER_OPTIONS = {
    "Hail Risk Score": "hail_risk_score",
    "Vehicle Ownership Density": "car_ownership_density",
    "Population Density": "population_density",
    "Median Income": "median_income",
    "Per Capita Income": "per_capita_income",
}
STATE_CENTERS = {
    "MO": (38.5, -92.5), "KS": (38.5, -98.0),
    "IA": (42.0, -93.0), "NE": (41.5, -99.5)
}

# --- Data Loading and Processing Functions ---

def download_hail_report(for_date: datetime.date):
    """Downloads and saves a hail report, returning its local filepath."""
    date_str = for_date.strftime("%Y-%m-%d")
    filepath = os.path.join(HAIL_REPORTS_FOLDER, f"{date_str}.csv")

    if not os.path.exists(filepath):
        with st.spinner(f"Downloading report for {date_str}..."):
            url = f"https://www.spc.noaa.gov/climo/reports/{for_date.strftime('%y%m%d')}_rpts_hail.csv"
            try:
                response = requests.get(url)
                response.raise_for_status()
                with open(filepath, 'w') as f:
                    f.write(response.text)
                st.success(f"Downloaded report for {date_str}.")
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    st.warning(f"No hail report found on NOAA for {date_str}.")
                    with open(filepath, 'w') as f:
                        f.write("Time,Lat,Lon,Location\n")
                else:
                    st.error(f"Failed to download report: {e}")
                return None
            except Exception as e:
                st.error(f"An error occurred: {e}")
                return None
    return filepath

def state_abbr_to_fips(abbr):
    fips_map = {"MO": "29", "KS": "20", "IA": "19", "NE": "31"}
    return fips_map.get(abbr)

def load_and_merge_tracts(shapefile_path, ownership_csv_path):
    """Loads tract shapes and merges with vehicle ownership data."""
    gdf = gpd.read_file(shapefile_path)
    df = pd.read_csv(ownership_csv_path)
    gdf["GEOID"] = gdf["GEOID"].astype(str).str.zfill(11)
    df["tract_geoid"] = df["tract_geoid"].astype(str).str.zfill(11)
    gdf = gdf.merge(df, left_on="GEOID", right_on="tract_geoid", how="left")

    vehicle_cols = [col for col in gdf.columns if "households_with" in col and "no_vehicle" not in col]
    gdf["households_with_vehicles"] = gdf[vehicle_cols].sum(axis=1)
    return gdf

@st.cache_data
def process_state_data(state_abbr, hail_report_path):
    """
    Main data processing function. Loads all data for a state, merges it,
    and performs spatial analysis with the given hail report.
    Returns the main GeoDataFrame and a GeoDataFrame for hail points.
    """
    with st.spinner(f"Processing data for {state_abbr}..."):
        fips = state_abbr_to_fips(state_abbr)
        shapefile_path = os.path.join(TRACTS_FOLDER, f"tl_2024_{fips}_tract", f"tl_2024_{fips}_tract.shp")
        ownership_csv_path = os.path.join(OWNERSHIP_FOLDER, f"vehicle_ownership_by_tract_{state_abbr}.csv")

        if not all(os.path.exists(p) for p in [shapefile_path, ownership_csv_path, INCOME_CSV_PATH]):
            st.error(f"Missing base data for {state_abbr}. Please ensure census, ownership, and income files are present.")
            return gpd.GeoDataFrame(), gpd.GeoDataFrame()

        # 1. Load and merge base census, ownership, and income data
        gdf = load_and_merge_tracts(shapefile_path, ownership_csv_path)
        income_df = pd.read_csv(INCOME_CSV_PATH)
        income_df["tract_geoid"] = income_df["tract_geoid"].astype(str).str.zfill(11)
        income_cols = ["tract_geoid", "per_capita_income", "median_income", "total_population"]
        gdf = gdf.merge(income_df[income_cols], left_on="GEOID", right_on="tract_geoid", how="left")

        # 2. Calculate density metrics
        gdf["land_area_km2"] = gdf["ALAND"].astype(float) / 1_000_000
        gdf.loc[gdf['land_area_km2'] == 0, 'land_area_km2'] = 0.01 # Avoid division by zero
        gdf["car_ownership_density"] = (gdf["households_with_vehicles"] / gdf["land_area_km2"]).fillna(0).round(2)
        gdf["population_density"] = (gdf["total_population"] / gdf["land_area_km2"]).fillna(0).round(2)

        # 3. Spatially join with hail data
        try:
            if hail_report_path and os.path.exists(hail_report_path) and pd.read_csv(hail_report_path).shape[0] > 0:
                hail_df = pd.read_csv(hail_report_path).dropna(subset=["Lat", "Lon"])
                hail_gdf = gpd.GeoDataFrame(hail_df, geometry=gpd.points_from_xy(hail_df.Lon, hail_df.Lat), crs="EPSG:4326")
                hail_gdf_proj = hail_gdf.to_crs(gdf.crs)
                hail_per_tract = gpd.sjoin(gdf, hail_gdf_proj, how="inner", predicate="contains")
                hail_counts = hail_per_tract.groupby("GEOID").size().reset_index(name="hail_reports")
                gdf = gdf.merge(hail_counts, on="GEOID", how="left")
                gdf["hail_reports"] = gdf["hail_reports"].fillna(0).astype(int)
            else:
                gdf["hail_reports"] = 0
                hail_gdf = gpd.GeoDataFrame(columns=['geometry'], geometry='geometry', crs="EPSG:4326")
        except (pd.errors.EmptyDataError, KeyError): # Handle empty/malformed CSV
            gdf["hail_reports"] = 0
            hail_gdf = gpd.GeoDataFrame(columns=['geometry'], geometry='geometry', crs="EPSG:4326")

        # 4. Calculate hail risk score
        gdf["hail_risk_score"] = (gdf["hail_reports"] * gdf["car_ownership_density"]).fillna(0)

        return gdf.to_crs(epsg=4326), hail_gdf.to_crs(epsg=4326)


@st.cache_data
def generate_radar_links(report_path):
    if not report_path or not os.path.exists(report_path): return pd.DataFrame()
    try:
        df = pd.read_csv(report_path)
        if df.empty: return pd.DataFrame()
    except (pd.errors.EmptyDataError, FileNotFoundError): return pd.DataFrame()
    df.columns = [col.strip().lower() for col in df.columns]
    if 'time' not in df.columns: return pd.DataFrame()
    df['time_parsed'] = pd.to_datetime(df['time'].astype(str).str.zfill(4), format='%H%M', errors='coerce').dt.time
    links = []
    for _, row in df.dropna(subset=['time_parsed']).iterrows():
        report_date = datetime.strptime(os.path.basename(report_path).split('.')[0], '%Y-%m-%d').date()
        event_time = datetime.combine(report_date, row["time_parsed"])
        for scan_time in pd.date_range(event_time - timedelta(minutes=15), event_time + timedelta(minutes=15), freq="5min"):
            radar_url = f"https://mesonet.agron.iastate.edu/archive/data/{scan_time:%Y/%m/%d}/GIS/ridge/RADAR/N0Q/DMX/N0Q_DMX_{scan_time:%Y%m%d_%H%M}.png"
            try:
                if requests.head(radar_url, timeout=2).status_code == 200:
                    links.append({"Event Time": event_time.strftime("%H:%M"), "Radar Time": scan_time.strftime("%H:%M"), "Location": row.get("location", "N/A"), "Radar URL": radar_url})
                    break
            except requests.exceptions.RequestException: continue
    return pd.DataFrame(links)

# --- Streamlit App UI ---
st.set_page_config(layout="wide")
st.title("Interactive Hail Risk Dashboard")

# --- Sidebar Controls ---
st.sidebar.header("Controls")
selected_state = st.sidebar.selectbox("Choose a state:", STATE_OPTIONS, index=0)
selected_date = st.sidebar.date_input("Select Date", datetime(2025, 7, 12))
selected_layer_name = st.sidebar.selectbox("Select layer:", list(LAYER_OPTIONS.keys()), index=0)
selected_field = LAYER_OPTIONS[selected_layer_name]

# --- Main App Logic ---
hail_report_path = download_hail_report(selected_date)
gdf, hail_gdf = process_state_data(selected_state, hail_report_path)

if gdf.empty:
    st.error("Could not load or process data for the selected state. Please check the data files.")
    st.stop()

# --- Map Visualization ---
st.header("Map")
st.pydeck_chart(pdk.Deck(
    map_style="mapbox://styles/mapbox/light-v9",
    initial_view_state=pdk.ViewState(
        latitude=STATE_CENTERS[selected_state][0], longitude=STATE_CENTERS[selected_state][1],
        zoom=6, pitch=45,
    ),
    layers=[
        pdk.Layer(
            "GeoJsonLayer",
            data=gdf,
            opacity=0.8, stroked=False, filled=True,
            get_fill_color=f"[255, (1 - (properties.{selected_field} || 0) / (({gdf[selected_field].max() if gdf[selected_field].max() > 0 else 1})) ) * 255, 0, 140]",
            pickable=True, auto_highlight=True,
        ),
        pdk.Layer(
            "ScatterplotLayer",
            data=hail_gdf,
            get_position="geometry.coordinates",
            get_radius=2000, get_fill_color=[255, 0, 0, 200],
        ),
    ],
    tooltip={
        "html": f"""
            <b>Tract:</b> {{GEOID}} <br/>
            <b>{selected_layer_name}:</b> {{{selected_field}}} <br/>
            <b>Hail Reports on this day:</b> {{hail_reports}}
        """
    }
))

# --- Radar Images Section ---
st.header(f"Hail Event Radar for {selected_date.strftime('%B %d, %Y')}")
radar_df = generate_radar_links(hail_report_path)

if not radar_df.empty:
    col1, col2 = st.columns([1, 2])

    with col1:
        st.markdown("##### Select a Hail Event")
        # Create a more descriptive label for the selectbox
        radar_df['event_label'] = radar_df['Event Time'] + " - " + radar_df['Location']
        selected_event_label = st.selectbox(
            "Events",
            options=radar_df['event_label'].tolist(),
            label_visibility="collapsed"
        )

    with col2:
        if selected_event_label:
            selected_url = radar_df[radar_df['event_label'] == selected_event_label]["Radar URL"].iloc[0]
            st.image(selected_url, caption=f"Radar for {selected_event_label}", use_column_width=True)

    with st.expander("View All Event Data"):
        st.dataframe(radar_df[['Event Time', 'Radar Time', 'Location', 'Radar URL']])
else:
    st.info("No hail reports with valid radar images found for this date.")
