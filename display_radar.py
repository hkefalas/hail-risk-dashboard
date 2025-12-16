# streamlit_radar_viewer.py

import streamlit as st
import pydeck as pdk
import glob
import os
from datetime import datetime

# --- Configuration ---
FRAMES_DIR = "radar_frames"  # Change if your radar PNGs are elsewhere

# Approximate lat/lon bounds for your radar site
# Format: [west, south, east, north]
BOUNDS = [-93.6, 41.4, -90.6, 43.2]  # Example: Iowa/Illinois region (KDVN)

# --- Load Image Frames ---
@st.cache_data
def load_frame_paths():
    files = sorted(glob.glob(os.path.join(FRAMES_DIR, "*.png")))
    return files

frame_files = load_frame_paths()

# Extract timestamps from filenames (if they follow YYYYMMDD_HHMMSS.png)
def parse_timestamp(filename):
    base = os.path.basename(filename).split('.')[0]
    try:
        return datetime.strptime(base, "%Y%m%d_%H%M%S")
    except ValueError:
        return base  # fallback to filename

timestamps = [parse_timestamp(f) for f in frame_files]

# --- UI ---
st.title("📡 Radar Viewer")
st.caption("Use the slider to view radar reflectivity frames.")

# Time slider
frame_index = st.slider(
    "Frame",
    min_value=0,
    max_value=len(frame_files) - 1,
    value=0,
    format="%d"
)

# Timestamp display
if isinstance(timestamps[frame_index], datetime):
    st.markdown(f"**Timestamp:** {timestamps[frame_index].strftime('%Y-%m-%d %H:%M:%S')}")
else:
    st.markdown(f"**Frame:** {timestamps[frame_index]}")

# Create pydeck BitmapLayer for selected frame
layer = pdk.Layer(
    "BitmapLayer",
    image=frame_files[frame_index],
    bounds=BOUNDS,
    opacity=0.5
)

# View settings
view_state = pdk.ViewState(
    latitude=(BOUNDS[1] + BOUNDS[3]) / 2,
    longitude=(BOUNDS[0] + BOUNDS[2]) / 2,
    zoom=6
)

# Show pydeck map
st.pydeck_chart(
    pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        map_style="mapbox://styles/mapbox/dark-v10"
    )
)
