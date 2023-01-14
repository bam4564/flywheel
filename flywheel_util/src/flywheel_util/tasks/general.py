import pandas as pd 
from pandas import DataFrame

from prefect import task 
from sqlalchemy import text


@task
def df_to_sql(engine, df: DataFrame, table_name: str, drop: bool=True, **kwargs): 
    if drop: 
        with engine.connect() as conn: 
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            conn.commit()
    if 'index' not in kwargs: 
        kwargs['index'] = False
    df.to_sql(table_name, con=engine, **kwargs)