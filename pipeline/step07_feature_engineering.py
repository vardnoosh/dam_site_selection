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
    ALTER TABLE river_candidates ADD COLUMN dist_to_settlement DOUBLE PRECISION;

    UPDATE river_candidates r
    SET dist_to_settlement =
    ST_Distance(r.geom, sb.geom)
    FROM settlement_buffer sb;
    """

    with engine.begin() as conn:
        conn.execute(text(sql))
