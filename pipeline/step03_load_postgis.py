import geopandas as gpd
import yaml
from sqlalchemy import create_engine

def run():
    cfg = yaml.safe_load(open("config/config.yaml"))
    db = cfg["database"]

    engine = create_engine(
        f"postgresql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['name']}"
    )

    gdf = gpd.read_file("data_processed/rivers_quality.gpkg")
    gdf.to_postgis("rivers", engine, if_exists="replace", index=False)
