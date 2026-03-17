
# etl/load/load.py

import pandas as pd
from sqlalchemy import create_engine, text
import os

def get_engine():
    host     = os.getenv("POSTGRES_HOST", "postgres")
    port     = os.getenv("POSTGRES_PORT", "5432")
    db       = os.getenv("POSTGRES_DB",   "mobilite_paris")
    user     = os.getenv("POSTGRES_USER", "e4_user")
    password = os.getenv("POSTGRES_PASSWORD", "e4_password_2026")
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url)

def load_dataframe(df: pd.DataFrame, table: str, if_exists: str = "replace"):
    engine = get_engine()
    with engine.begin() as conn:
        if if_exists == "replace":
            conn.execute(text(f'DROP TABLE IF EXISTS "{table}" CASCADE'))
    df.to_sql(table, engine, if_exists="append", index=False, method="multi", chunksize=500)
    print(f"  ✅ {table} : {len(df)} lignes chargées")
























# import pandas as pd
# from sqlalchemy import create_engine, text
# import os

# def get_engine():
#     host     = os.getenv("POSTGRES_HOST", "postgres")
#     port     = os.getenv("POSTGRES_PORT", "5432")
#     db       = os.getenv("POSTGRES_DB",   "mobilite_paris")
#     user     = os.getenv("POSTGRES_USER", "e4_user")
#     password = os.getenv("POSTGRES_PASSWORD", "e4_password_2026")
#     url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
#     return create_engine(url)

# def load_dataframe(df: pd.DataFrame, table: str, if_exists: str = "replace"):
#     engine = get_engine()
#     df.to_sql(table, engine, if_exists=if_exists, index=False, method="multi", chunksize=500)
#     print(f"  ✅ {table} : {len(df)} lignes chargées")