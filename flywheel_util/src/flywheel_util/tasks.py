import pandas as pd 
from pandas import DataFrame

from prefect import task 
from sqlalchemy import text, create_engine

@task
def df_to_sql(engine, df: DataFrame, table_name: str, drop: bool=True): 
    if drop: 
        with engine.connect() as conn: 
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            conn.commit()
    df.to_sql(table_name, con=engine)