import os

import pandas as pd
from airflow.decorators import dag, task
from astro import sql as aql
from astro.files import File
from astro.sql import Metadata, Table
from pendulum import datetime


@aql.transform
def transform_json(df: pd.DataFrame):
    
	print(df.head())
	return df


@dag(
	start_date = datetime(2024, 1, 1),
	schedule=None,
	catchup=False
)
def scraper_insert_postgres():

	json_file = aql.load_file(
		File(
			"/tmp/filmes.json",
		)
  	)
 
	transform_json(json_file, output_table=Table(
		name = 'filmes',
		conn_id='postgres_5431',
		metadata=Metadata(
			schema = 'scraper',
			database = 'home'
		)
	)
	)

	aql.cleanup() 
 
scraper_insert_postgres()