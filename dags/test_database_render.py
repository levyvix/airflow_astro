from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pendulum


@dag(schedule=None, start_date=pendulum.datetime(2022, 1, 1, tz="UTC"), catchup=False)
def test_database_render():
    postgres_operator = PostgresOperator(
        task_id="test_conection",
        postgres_conn_id="postgres_render",
        sql = """
        select 1
        """
    )

    postgres_operator


test_database_render()
