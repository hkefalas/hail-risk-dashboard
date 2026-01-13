import streamlit as st
import pandas as pd
import pydeck as pdk
import requests
import os
import json
import glob
from datetime import datetime
from config import STATES, LAYER_OPTIONS, PROCESSED_DATA_DIR, HAIL_REPORTS_DIR, RADAR_BOUNDS, RADAR_FRAMES_DIR
from utils import setup_logging, load_geojson, load_csv

# Setup logger
logger = setup_logging()

# --- UI Controls ---
st.title("Hail Risk Dashboard")

# Navigation
view_mode = st.sidebar.radio("Select View Mode", ["Risk Analysis", "Radar & Hail Reports"])

if view_mode == "Risk Analysis":
    # Use keys from the STATES dictionary for the dropdown
    state_options = list(STATES.keys())
    selected_state = st.selectbox("Choose a state:", state_options, index=0)

    # Use keys from LAYER_OPTIONS for the layer selection
    selected_layer = st.selectbox("Select layer to visualize:", list(LAYER_OPTIONS.keys()), index=0)

    # --- Load GeoJSON Data ---
    geojson_filename = f"gdf_{selected_state}_with_hail_risk.geojson"
    geojson_path = os.path.join(PROCESSED_DATA_DIR, geojson_filename)

    if not os.path.exists(geojson_path):
        st.warning(f"Processed data for {selected_state} not found at {geojson_path}.")
        st.warning("Please run the data pipeline first by executing 'python pipeline/main.py' in your terminal.")
        st.stop()

    try:
        # Use the utility function to load the GeoJSON
        gdf = load_geojson(geojson_path, logger)

        # Pydeck requires a plain dictionary for its data source, so we convert gdf to json
        # This is a common pattern when using pydeck with geopandas
        data = json.loads(gdf.to_json())

    except Exception as e:
        st.error(f"An error occurred while loading the data for {selected_state}: {e}")
        logger.error(f"Failed to load or process GeoJSON {geojson_path}: {e}")
        st.stop()


    # --- Data Transformation for Pydeck ---
    field_to_visualize = LAYER_OPTIONS[selected_layer]

    # Define color functions for visualization
    def get_color(value, layer):
        # Default color
        color = [200, 200, 200, 100]
        if pd.isna(value):
            return color

        if layer == "car_ownership_density":
            # Green to Red scale
            intensity = min(1, value / 150) # Normalize to 0-1 range, capped at 150
            red = int(255 * intensity)
            green = int(255 * (1 - intensity))
            color = [red, green, 0, 150]
        elif layer == "population_density":
            # Blue scale
            intensity = min(1, value / 1000) # Normalize to 0-1 range, capped at 1000
            blue = int(100 + 155 * intensity)
            color = [0, 0, blue, 150]
        elif layer in ["median_income", "per_capita_income"]:
            # Purple scale
            cap = 100000 if layer == "median_income" else 75000
            intensity = min(1, value / cap)
            purple = int(100 + 155 * intensity)
            color = [purple, 0, purple, 150]
        elif layer == "hail_risk_score":
            # Yellow to Orange/Red scale
            intensity = min(1, value / 500) # Normalize, cap at 500
            red = 255
            green = int(255 * (1 - intensity))
            color = [red, green, 0, 160]

        return color

    # Add color and tooltip to each feature
    for feature in data["features"]:
        props = feature["properties"]
        value = props.get(field_to_visualize, 0)
        props["fill_color"] = get_color(value, field_to_visualize)

        # Handle None/NaN values for tooltip
        if pd.isna(value):
            formatted_value = "N/A"
        else:
            formatted_value = f"{value:,.2f}"

        props["tooltip_text"] = f"{selected_layer}: {formatted_value}<br>Tract: {props.get('GEOID', 'N/A')}"

    # --- Pydeck Layer ---
    polygon_layer = pdk.Layer(
        "GeoJsonLayer",
         data=data,
        get_fill_color="properties.fill_color",
        pickable=True,
        auto_highlight=True,
    )

    # --- View Setup ---
    # Get the center from the config
    lat, lon = STATES[selected_state]["center"]
    view_state = pdk.ViewState(latitude=lat, longitude=lon, zoom=6, pitch=30)

    # --- Render Map ---
    r = pdk.Deck(
        layers=[polygon_layer],
        initial_view_state=view_state,
        tooltip={"html": "{tooltip_text}", "style": {"color": "white"}}

    )

    st.pydeck_chart(r, use_container_width=True)

elif view_mode == "Radar & Hail Reports":
    st.subheader("Radar & Hail Reports")

    # --- Load Image Frames ---
    @st.cache_data
    def load_frame_paths():
        if not os.path.exists(RADAR_FRAMES_DIR):
            return []
        files = sorted(glob.glob(os.path.join(RADAR_FRAMES_DIR, "*.png")))
        return files

    frame_files = load_frame_paths()

    if not frame_files:
        st.error(f"No radar frames found in {RADAR_FRAMES_DIR}")
        st.stop()

    # Extract timestamps
    def parse_timestamp(filename):
        base = os.path.basename(filename).split('.')[0]
        try:
            return datetime.strptime(base, "%Y%m%d_%H%M%S")
        except ValueError:
            return base

    timestamps = [parse_timestamp(f) for f in frame_files]

    # --- Time Slider ---
    frame_index = st.slider(
        "Time Frame",
        min_value=0,
        max_value=len(frame_files) - 1,
        value=0,
        format="%d"
    )

    # Timestamp display
    current_ts = timestamps[frame_index]
    if isinstance(current_ts, datetime):
        st.markdown(f"**Radar Timestamp:** {current_ts.strftime('%Y-%m-%d %H:%M:%S')} (UTC)")
    else:
        st.markdown(f"**Frame:** {current_ts}")

    # --- Load Hail Reports ---
    # Ideally allow selection, but for now default to the one matching radar context
    # or list available reports
    report_files = sorted(glob.glob(os.path.join(HAIL_REPORTS_DIR, "*.csv")))
    report_options = [os.path.basename(f) for f in report_files]

    # Try to select '2025-07-11.csv' by default as it matches the radar location context
    default_report_index = 0
    if "2025-07-11.csv" in report_options:
        default_report_index = report_options.index("2025-07-11.csv")

    selected_report_file = st.selectbox("Select Hail Report Date:", report_options, index=default_report_index)
    
    hail_df = pd.DataFrame()
    if selected_report_file:
        report_path = os.path.join(HAIL_REPORTS_DIR, selected_report_file)
        try:
            hail_df = load_csv(report_path, logger=logger)
            st.success(f"Loaded {len(hail_df)} hail reports.")
        except Exception as e:
            st.error(f"Error loading hail reports: {e}")

    # --- Pydeck Layers ---
    layers = []

    # Radar Layer
    radar_layer = pdk.Layer(
        "BitmapLayer",
        image=frame_files[frame_index],
        bounds=RADAR_BOUNDS,
        opacity=0.6,
        pickable=False
    )
    layers.append(radar_layer)

    # Hail Reports Layer
    if not hail_df.empty:
        # Construct tooltip text
        hail_df["tooltip"] = hail_df.apply(
            lambda row: f"Time: {row['Time']}<br>Size: {row['Size']}<br>Loc: {row['Location']}<br>County: {row['County']}, {row['State']}",
            axis=1
        )

        # Scatterplot for hail
        hail_layer = pdk.Layer(
            "ScatterplotLayer",
            data=hail_df,
            get_position=["Lon", "Lat"],
            get_color=[255, 0, 0, 200], # Red
            get_radius=2000, # Fixed radius for visibility, or scale by size
            pickable=True,
            auto_highlight=True
        )
        layers.append(hail_layer)

    # --- View State ---
    # Center map on radar bounds
    view_state = pdk.ViewState(
        latitude=(RADAR_BOUNDS[1] + RADAR_BOUNDS[3]) / 2,
        longitude=(RADAR_BOUNDS[0] + RADAR_BOUNDS[2]) / 2,
        zoom=7,
        pitch=0
    )

    # --- Render Map ---
    st.pydeck_chart(
        pdk.Deck(
            layers=layers,
            initial_view_state=view_state,
            map_style="mapbox://styles/mapbox/dark-v10",
            tooltip={"html": "{tooltip}", "style": {"color": "white"}}
        )
    )
