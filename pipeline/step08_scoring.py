import pandas as pd
import yaml
from sqlalchemy import create_engine

def run():
    cfg = yaml.safe_load(open("config/config.yaml"))
    db = cfg["database"]

    engine = create_engine(
        f"postgresql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['name']}"
    )

    df = pd.read_sql("SELECT * FROM river_candidates", engine)

    df["score"] = (
        0.4 * (df["dist_to_settlement"] > 5000).astype(int)
        + 0.3 * (df["stable_geology"] == True).astype(int)
        + 0.3 * (df["data_quality_flag"] == 0).astype(int)
    )

    df.to_sql("dam_suitability", engine, if_exists="replace", index=False)
