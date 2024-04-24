import os
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

from cosmos import (
    DbtTaskGroup,
    ProfileConfig,
    ProjectConfig,
)
from cosmos.profiles import PostgresUserPasswordProfileMapping

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_5431",
        profile_args={"schema": "airbnb"},
    ),
)


@dag(
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
)
def airbnb_basic_cosmos_task_group() -> None:
    pre_dbt = EmptyOperator(task_id="pre_dbt")

    news = DbtTaskGroup(
        group_id="news",
        project_config=ProjectConfig(
            (DBT_ROOT_PATH / "airbnb_rj").as_posix(),
        ),
        operator_args={"install_deps": True},
        profile_config=profile_config,
        default_args={"retries": 2},
    )

    post_dbt = EmptyOperator(task_id="post_dbt")

    pre_dbt >> news >> post_dbt


airbnb_basic_cosmos_task_group()
