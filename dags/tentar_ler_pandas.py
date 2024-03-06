from datetime import datetime

import duckdb
from airflow.decorators import dag, task
from astro import sql as aql
from astro.files import File
from astro.sql import Metadata
from astro.table import Table
from pandas import DataFrame


@aql.dataframe()
def top_five_animations(input_table: DataFrame):
    return (
        input_table.loc[lambda d: d["Genre1"] == "Animation"]
        .sort_values("Rating", ascending=False)
        .loc[
            :,
            [
                "Title",
                "Rating",
            ],
        ]
        .head(5)
    )


@dag(catchup=False, start_date=datetime(2021, 1, 1), schedule=None)
def tentar_ler_pandas():
    dataframe = aql.load_file(File("s3://airflow-levy/data.csv"))

    top_five_animations(
        dataframe,
        output_table=Table(
            name="top_animation",
            conn_id="postgres_5431",
            metadata=Metadata(
                schema="imdb_s3",
                database="home",
            ),
            temp=False,
        ),
    )

    aql.cleanup()


tentar_ler_pandas()
