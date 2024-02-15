import datetime

import boto3
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import (
    LocalFilesystemToS3Operator,
)

BUCKET_NAME = "airflow-levy"


@dag(
    default_args={"owner": "levy"},
    schedule=None,
    start_date=datetime.datetime(2021, 1, 1),
    tags=["holidays"],
    catchup=False,
)
def glue_operations():
    @task
    def create_classifier():
        aws_hook = AwsBaseHook(aws_conn_id="aws_default")

        aws_conn = aws_hook.get_connection(conn_id="aws_default")

        glue_client = boto3.client(
            "glue",
            region_name="us-east-1",
            aws_access_key_id=aws_conn.login,
            aws_secret_access_key=aws_conn.password,
        )
        try:
            glue_client.delete_classifier(Name="calendarific_csv")

            glue_client.create_classifier(
                CsvClassifier={
                    "Name": "calendarific_csv",
                    "Delimiter": ";",
                    "QuoteSymbol": '"',
                    "ContainsHeader": "PRESENT",
                    "Header": [
                        "name",
                        "description",
                        "type",
                        "primary_type",
                        "canonical_url",
                        "urlid",
                        "locations",
                        "states",
                        "country.id",
                        "country.name",
                        "date.iso",
                        "date.datetime.year",
                        "date.datetime.month",
                        "date.datetime.day",
                        "date.datetime.minute",
                        "date.datetime.second",
                        "date.timezone.offset",
                        "date.timezone.zoneabb",
                        "date.timezone.zoneoffset",
                    ],
                }
            )
        except glue_client.exceptions.AlreadyExistsException:
            return "already exists"

        return "created"

    glue_crawler_config = {
        "Name": "airflow-levy-crawler",
        "Role": "airflow-levi-role",
        "DatabaseName": "holidays",
        "Targets": {
            "S3Targets": [
                {
                    "Path": f"s3://{BUCKET_NAME}/holidays",
                }
            ]
        },
        "Classifiers": ["calendarific_csv"],
    }

    crawl_s3 = GlueCrawlerOperator(
        task_id="crawl_s3",
        config=glue_crawler_config,
        region_name="us-east-1",
    )

    # TODO: https://www.youtube.com/watch?v=5tu1aixBjnw

    # git_pull = BashOperator(
    #     task_id="git_pull",
    #     bash_command="cd /tmp && git clone https://github.com/levyvix/glue_scripts.git",
    # )
    
    upload_script = LocalFilesystemToS3Operator(
        task_id = 'upload_script',
        filename='/usr/local/airflow/include/scripts/pyspark/glue_job_calendarific.py',
        dest_key = f's3://{BUCKET_NAME}/scripts/glue_job_calendarific.py',
        replace=True
    )

    submit_glue_job = GlueJobOperator(
        task_id="submit_glue_job",
        iam_role_name="airflow-levi-role",
        job_name="job_airflow",
        script_location=f"s3://{BUCKET_NAME}/scripts/glue_job_calendarific.py",
        s3_bucket=BUCKET_NAME,
        create_job_kwargs={
            "GlueVersion": "3.0",
            "NumberOfWorkers": 2,
            "WorkerType": "G.1X",
        },
        region_name='us-east-1'
    )
    
    BUCKET_SILVER = 'dataeng-clean-zone-957'
    config_silver = {
        "Name": "airflow-levy-crawler-silver",
        "Role": "airflow-levi-role",
        "DatabaseName": "curatedzonedb",
        "Targets": {
            "S3Targets": [
                {
                    "Path": f"s3://{BUCKET_SILVER}/holidays",
                }
            ]
        }
    }
    
    crawl_s3_silver =  GlueCrawlerOperator(
        task_id="crawl_s3_silver",
        config=config_silver,
        region_name="us-east-1",
    )

    create_classifier_task = create_classifier()

    create_classifier_task >> crawl_s3 >> upload_script >> submit_glue_job >> crawl_s3_silver


glue_operations()
