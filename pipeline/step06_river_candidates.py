import yaml
from sqlalchemy import create_engine, text

def run():
    cfg = yaml.safe_load(open("config/config.yaml"))
    db = cfg["database"]

    engine = create_engine(
        f"postgresql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['name']}"
    )

    buffer_dist = cfg["buffers"]["settlement"]

    sql = f"""
    CREATE TABLE river_candidates AS
    SELECT
    ST_LineInterpolatePoint(r.geom, gs) AS geom,
    r.data_quality_flag
    FROM rivers r,
    generate_series(0, 1, 0.01) gs;

    CREATE INDEX ON river_candidates USING GIST (geom);
    """

    with engine.begin() as conn:
        conn.execute(text(sql))
