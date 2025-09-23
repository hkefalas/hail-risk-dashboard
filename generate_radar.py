# --- generate_radar.py ---
import pandas as pd
from datetime import datetime
from datetime import timedelta
import requests

def parse_utc_time(timestr):
    try:
        return datetime.strptime(timestr.strip(), "%H%M")  # e.g., 1422 -> 14:22 UTC
    except Exception:
        return None

def generate_radar_url(report_path):
    df = pd.read_csv(report_path)
    links = []

    for _, row in df.iterrows():
        try:
            event_time = pd.to_datetime(row["Time"])
            dt_floor = (event_time - timedelta(minutes=30)).floor("5min")
            dt_ceil = (event_time + timedelta(minutes=30)).ceil("5min")

            # Build potential radar image URLs
            for scan_time in pd.date_range(dt_floor, dt_ceil, freq="5min"):
                radar_url = (
                    f"https://mesonet.agron.iastate.edu/archive/data/"
                    f"{scan_time:%Y/%m/%d}/GIS/ridge/RADAR/N0Q/DMX/N0Q_DMX_{scan_time:%Y%m%d_%H%M}.png"
                )

                # Optional: verify the image exists
                try:
                    r = requests.head(radar_url, timeout=2)
                    if r.status_code == 200:
                        links.append({
                            "Event Time": event_time,
                            "Radar Time": scan_time,
                            "Radar URL": radar_url,
                            "location": row.get("location", ""),
                            "state": row.get("state", ""),
                            "size": row.get("size", "")
                        })
                        break  # Use first found image
                except Exception:
                    continue
        except Exception as e:
            print(f"Skipping row due to error: {e}")

    if not links:
        print("⚠️ No radar images found for hail reports.")
    return pd.DataFrame(links)

def get_hail_events_from_csv(csv_path: str, station="DMX") -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    records = []
    for _, row in df.iterrows():
        time_str = str(row.get("Time (UTC)", "")).zfill(4)
        time_obj = parse_utc_time(time_str)
        radar_url = generate_radar_url(time_obj, station=station)

        records.append({
            "Time (UTC)": time_str,
            "Location": row.get("Location", ""),
            "Size (in)": row.get("Size", ""),
            "State": row.get("State", ""),
            "RadarURL": radar_url
        })
    return pd.DataFrame(records)

import pyart
import matplotlib.pyplot as plt
from pathlib import Path
import cartopy.crs as ccrs

def plot_radar_file(file_path):
    radar = pyart.io.read_nexrad_archive(file_path)

    display = pyart.graph.RadarMapDisplay(radar)
    fig = plt.figure(figsize=(10, 8))

    ax = plt.subplot(1, 1, 1, projection=ccrs.PlateCarree())
    display.plot_ppi_map(
        'reflectivity', 0, vmin=-32, vmax=64,
        min_lon=radar.longitude['data'][0] - 2,
        max_lon=radar.longitude['data'][0] + 2,
        min_lat=radar.latitude['data'][0] - 2,
        max_lat=radar.latitude['data'][0] + 2,
        resolution='50m', projection=ccrs.PlateCarree(), ax=ax
    )

    display.plot_range_rings([50, 100, 150], ax=ax)
    plt.title(f"{Path(file_path).name}")
    plt.show()

import pydeck as pdk
import numpy as np
from PIL import Image
import io
import base64

def radar_bitmap_layer(image_array, bounds):
    # Normalize to 0–255 and convert to RGBA image
    norm = ((image_array - image_array.min()) / (image_array.ptp())) * 255
    rgba_image = np.stack([norm]*3 + [np.full_like(norm, 180)], axis=-1).astype(np.uint8)

    # Save to in-memory PNG
    img = Image.fromarray(rgba_image)
    buf = io.BytesIO()
    img.save(buf, format='PNG')
    data_uri = "data:image/png;base64," + base64.b64encode(buf.getvalue()).decode("utf-8")

    # Return as Pydeck BitmapLayer
    return pdk.Layer(
        "BitmapLayer",
        data=None,
        image=data_uri,
        bounds=bounds,
        opacity=0.5
    )

radar_file_path = "radar_data/KDVN20250712_224026_V06"  # adjust path as needed
radar = pyart.io.read_nexrad_archive(radar_file_path)

def generate_radar_frames(radar_folder="radar_data", output_folder="radar_frames") -> list[str]:
    import os
    import pyart
    import matplotlib.pyplot as plt
    import cartopy.crs as ccrs
    from datetime import datetime

    os.makedirs(output_folder, exist_ok=True)

    files = sorted([
        f for f in os.listdir(radar_folder)
        if f.startswith("K") and "_" in f
    ], key=lambda x: datetime.strptime(x[4:19], "%Y%m%d_%H%M%S"))

    frame_paths = []

    for i, filename in enumerate(files):
        try:
            filepath = os.path.join(radar_folder, filename)
            radar = pyart.io.read_nexrad_archive(filepath)

            fig = plt.figure(figsize=(8, 6))
            ax = plt.subplot(1, 1, 1, projection=ccrs.PlateCarree())

            display = pyart.graph.RadarMapDisplay(radar)
            display.plot_ppi_map(
                'reflectivity', 0,
                vmin=-32, vmax=64,
                colorbar_flag = False,
                min_lon=radar.longitude['data'][0] - 1.5,
                max_lon=radar.longitude['data'][0] + 1.5,
                min_lat=radar.latitude['data'][0] - 1.5,
                max_lat=radar.latitude['data'][0] + 1.5,
                resolution='50m', projection=ccrs.PlateCarree(), ax=ax
            )
          #  display.plot_range_rings([50, 100, 150], ax=ax)
            plt.title(filename)
            plt.tight_layout()

            # Save with radar timestamp-based filename for slider sorting
            timestamp_str = filename[4:19]  # YYYYMMDD_HHMMSS
            out_path = os.path.join(output_folder, f"{timestamp_str}.png")
            plt.savefig(out_path, bbox_inches='tight', pad_inches=0)
            plt.close()

            frame_paths.append(out_path)
        except Exception as e:
            print(f"⚠️ Skipping {filename} due to error: {e}")

    if not frame_paths:
        print("❌ No radar frames generated.")
    else:
        print(f"✅ Generated {len(frame_paths)} radar frame images.")

    return frame_paths

if __name__ == "__main__":
    paths = generate_radar_frames()
    for p in paths:
        print(p)