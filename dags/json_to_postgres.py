# coding: utf-8
import os

import pandas as pd
from airflow.decorators import dag, task
from pendulum import datetime
from sqlalchemy import create_engine


@dag(
	start_date=datetime(2024, 1, 1),
	schedule=None,
	catchup=False
)
def json_to_postgres():
    
    @task
    def get_json():
        return pd.read_json('/tmp/filmes.json', encoding='utf-8')

    @task
    def to_postgres(df: pd.DataFrame):
        engine = create_engine('postgresql://postgres:l12345@192.168.18.87/home')
        
        df = (
            df
            .assign(
                titulo_dublado = lambda df: df['titulo_dublado'].str.replace('–', '-')
            )
        )

        df.to_sql(
            name = 'filmes',
            con = engine,
            schema = 'scraper',
            if_exists='replace',
            index=False 
        )
        

    json = get_json()
    to_postgres(json)

json_to_postgres()