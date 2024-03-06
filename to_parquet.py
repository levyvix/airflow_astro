import pandas as pd

(
    pd.read_csv("/root/airflow/include/data/data.csv").assign(
        Domestic=lambda d: d["Domestic"].str.replace("$", ""),
        Worldwide=lambda d: d["Worldwide"].str.replace("$", ""),
    )
).to_parquet("/root/airflow/include/data/data.parquet")
