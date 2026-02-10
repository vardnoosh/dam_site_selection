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
    CREATE TABLE settlement_buffer AS
    SELECT ST_Buffer(geom, 5000) AS geom
    FROM settlements;
    
    CREATE INDEX ON settlement_buffer USING GIST (geom);
    """

    with engine.begin() as conn:
        conn.execute(text(sql))
