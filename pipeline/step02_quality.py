import geopandas as gpd

def run():
    gdf = gpd.read_file("data_processed/rivers_clean.gpkg")
    gdf["data_quality_flag"] = 1
    gdf["data_quality_notes"] = "Moderate confidence"

    gdf.to_file("data_processed/rivers_quality.gpkg", driver="GPKG")
