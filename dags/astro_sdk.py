from datetime import datetime

import pandas as pd
from airflow import DAG
from astro import sql as aql
from astro.files import File
from astro.sql import Metadata
from astro.sql.table import Table


#  return """
#         SELECT Title, Rating
#         FROM {{input_table}}
#         WHERE Genre1=='Animation'
#         ORDER BY Rating desc
#         LIMIT 5;
#     """
@aql.dataframe()
def top_five_animations(input_table: pd.DataFrame):
    return (
        input_table.loc[lambda d: d["Genre1"] == "Animation"]
        .sort_values("Rating", ascending=False)
        .loc[:, ["Title", "Rating"]]
        .head(5)
    )


with DAG(
    "calculate_popular_movies",
    schedule_interval=None,
    start_date=datetime(2000, 1, 1),
    catchup=False,
) as dag:
    imdb_movies = aql.load_file(
        File(
            "include/data/data.parquet",
        ),
    )
    top_five_animations(
        input_table=imdb_movies,
        output_table=Table(
            name="top_animation",
            conn_id="postgres_5431",
            metadata=Metadata(schema="imdb"),
        ),
    )

    # aql.cleanup()
