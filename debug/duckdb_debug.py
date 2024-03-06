import duckdb
from pandas import DataFrame, read_csv


def duck_transform(input_table: DataFrame):
    return duckdb.sql(
        """
            select Title, Rating
            from input_table
            where Genre1 = 'Animation'
            order by Rating desc
            """
    ).df()


def load_file():

    return read_csv("/root/airflow/include/data/data.csv")


if __name__ == "__main__":
    dataframe = load_file()

    clean_dataframe = duck_transform(dataframe)

    print(clean_dataframe.head())
