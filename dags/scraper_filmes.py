import os

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import datetime

cur_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), 'filmes'))


@dag(start_date=datetime(2024, 1, 1),
     schedule="@daily",
     catchup=False
)
def scrape_comandola():

	enter_dir = BashOperator(
		task_id = 'enter',
		bash_command=f'cd {cur_dir} && python run_scraper.py'
	)


	# run_transformation = TriggerDagRunOperator(
	# 	trigger_dag_id='scraper_transform', 
	# )
	enter_dir

scrape_comandola()