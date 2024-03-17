from airflow.decorators import dag, task
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from pendulum import datetime


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["example"],
)
def activate_airyte():
    trigger_sync = AirbyteTriggerSyncOperator(
        task_id="trigger_sync",
        airbyte_conn_id="airbyte_default",
        connection_id="06f8b0b7-95a2-4fcd-9e70-d83b89a28bcf",
        asynchronous=True,
    )

    trigger_sync


activate_airyte()
