import logging
import pandas as pd
import geopandas as gpd
import os

def setup_logging():
    """
    Configures and returns a logger instance.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger(__name__)

def load_csv(filepath: str, dtype=None, logger=None):
    """
    Loads a CSV file into a pandas DataFrame with error handling.
    """
    if logger:
        logger.info(f"Loading CSV file from: {filepath}")
    try:
        return pd.read_csv(filepath, dtype=dtype)
    except FileNotFoundError:
        if logger:
            logger.error(f"File not found: {filepath}")
        raise
    except Exception as e:
        if logger:
            logger.error(f"Error loading CSV file {filepath}: {e}")
        raise

def save_geojson(gdf, filepath: str, logger=None):
    """
    Saves a GeoDataFrame to a GeoJSON file.
    """
    if logger:
        logger.info(f"Saving GeoJSON file to: {filepath}")
    try:
        gdf.to_file(filepath, driver="GeoJSON")
    except Exception as e:
        if logger:
            logger.error(f"Error saving GeoJSON file {filepath}: {e}")
        raise

def load_geojson(filepath: str, logger=None):
    """
    Loads a GeoJSON file into a GeoDataFrame with error handling.
    """
    if logger:
        logger.info(f"Loading GeoJSON file from: {filepath}")
    try:
        return gpd.read_file(filepath)
    except FileNotFoundError:
        if logger:
            logger.error(f"File not found: {filepath}")
        raise
    except Exception as e:
        if logger:
            logger.error(f"Error loading GeoJSON file {filepath}: {e}")
        raise

def ensure_dir_exists(directory_path: str, logger=None):
    """
    Ensures that a directory exists, creating it if necessary.
    """
    if not os.path.exists(directory_path):
        if logger:
            logger.info(f"Creating directory: {directory_path}")
        os.makedirs(directory_path)
