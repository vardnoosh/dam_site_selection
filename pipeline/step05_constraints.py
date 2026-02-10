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
    DROP TABLE IF EXISTS settlement_buffer;
    CREATE TABLE settlement_buffer AS
    SELECT ST_Buffer(geom, {buffer_dist}) AS geom
    FROM settlements;
    """

    with engine.begin() as conn:
        conn.execute(text(sql))
