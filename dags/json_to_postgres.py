import pandas as pd
from airflow.decorators import dag, task
from filmes.send_email.send_email import send_email
from pendulum import datetime
from sqlalchemy import create_engine, text


@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["comandola", "scraper"],
)
def json_to_postgres():
    @task
    def get_json():
        return pd.read_json("/tmp/filmes.json")

    @task(multiple_outputs=True)
    def to_postgres(df: pd.DataFrame, **context):
        engine = create_engine(
            "postgresql://postgres:l12345@192.168.18.87/home?client_encoding=utf8"
        )

        df = df.assign(
            titulo_dublado=lambda df: df["titulo_dublado"].str.replace("–", "-"),
        )

        try:
            existing = pd.read_sql_query("select * from scraper.filmes")
            union = pd.concat([df, existing])

            union = union.drop_duplicates()

        except Exception as e:
            print("tabela ou schema ainda nao existe", e)
            union = df.copy()

            engine.execute(text("create schema if not exists scraper;"))

        union.to_sql(
            name="filmes",
            con=engine,
            schema="scraper",
            if_exists="replace",
            index=False,
        )

        print(context)

        return {
            "message": union.sort_values("date_updated", ascending=False).head(20),
            "subject": context["ds"],
        }

    json = get_json()

    @task()
    def send_mail(message, subject):
        send_email(message, "Daily dose of movies from " + subject)

    message_subject = to_postgres(json)

    send_mail(message_subject["message"], message_subject["subject"])


json_to_postgres()
