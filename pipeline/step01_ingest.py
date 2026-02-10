import geopandas as gpd
import yaml

def run():
    cfg = yaml.safe_load(open("config/config.yaml"))

    gdf = gpd.read_file("data_raw/rivers.shp")
    gdf = gdf.to_crs(cfg["crs"])
    gdf = gdf[gdf.geometry.is_valid]

    gdf.to_file("data_processed/rivers_clean.gpkg", driver="GPKG")
