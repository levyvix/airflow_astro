import pandas as pd
import requests as rq
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.postgres.operators.postgres import PostgresOperator
from filmes.send_email.send_email import send_email
from pendulum import datetime
import pendulum
from sqlalchemy import create_engine

# Define the function to fetch and update news data


# Define the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}


@dag(
    "fetch_news_data",
    default_args=default_args,
    description="Fetch and update news data from News API",
    schedule_interval="@hourly",
    start_date=datetime(2024, 3, 30),
    catchup=False,
)
def fetch_news_data():
    create_postgres_table = PostgresOperator(
        task_id="create_postgres_table",
        postgres_conn_id="postgres_render",
        sql="""
            CREATE TABLE IF NOT EXISTS news (
                author TEXT,
                title TEXT,
                description TEXT,
                url TEXT PRIMARY KEY,
                urlToImage TEXT,
                publishedAt TEXT,
                content TEXT,
                source_id TEXT,
                source_name TEXT
            );
        """,
    )

    @task()
    def fetch_and_update_news():
        engine = create_engine(
            "postgresql://ada:PR1gvstOECPAb8Yqat1OptlTPm2orbDC@dpg-co6a0ka0si5c73cck8d0-a.oregon-postgres.render.com/database_vx82"
        )

        existing_data = pd.read_sql("SELECT * FROM news", engine)

        params = {
            "q": "Apple",
            "sortBy": "popularity",
            "apiKey": "45a541a896864ea0becf962b10f5a79e",
        }
        response = rq.get("https://newsapi.org/v2/top-headlines", params=params)
        new_data = pd.json_normalize(response.json()["articles"], sep="_")

        new_data = new_data[~new_data["url"].isin(existing_data["url"])].loc[
            lambda df: df["url"] != "https://removed.com"
        ]

        print(new_data)

        merged_data = (
            pd.concat([existing_data, new_data])
            .drop_duplicates()
            .loc[
                :,
                [
                    "author",
                    "title",
                    "description",
                    "url",
                    "urlToImage",
                    "publishedAt",
                    "content",
                    "source_id",
                    "source_name",
                ],
            ]
            .rename(columns={"urlToImage": "urltoimage", "publishedAt": "publishedat"})
        )

        # update database
        # truncate table, because other tables depend on it, so I can't drop it.
        engine.execute("TRUNCATE TABLE news")

        # append because the table already exists and can't be dropped
        merged_data.to_sql("news", engine, if_exists="append", index=False)

        print("Webhook sent with new information:", new_data.shape[0], "new articles.")

        return new_data

    new_data = fetch_and_update_news()

    @task()
    def send_mail(message):
        if len(message) == 0:
            return
        subject = f"New articles from News API: {message.shape[0]} articles"
        context = get_current_context()
        # html message

        html_content = f"""
        <html>
        <head>
            <meta charset="UTF-8">
            <style>
                body {{
                    font-family: Arial, sans-serif;
                }}
                h1 {{
                    color: #333;
                }}
                .article {{
                    margin-bottom: 20px;
                    padding: 10px;
                    border: 1px solid #ccc;
                    border-radius: 5px;
                }}
                .article img {{
                    max-width: 100%;
                    border-radius: 5px;
                }}
            </style>
        </head>
        <body>
            <h1>{subject}</h1>
        """

        # Adding each news article to the email body
        for index, row in message.iterrows():
            html_content += f"""
            <div class="article">
                <h2>{row['title']}</h2>
                <h3> By: {row['source_name'].title()}</h3>
                <p>{row['description']}</p>
                <img src="{row['urlToImage']}" alt="{row['title']}">
                <p>Published at: {row['publishedAt'].split('T')[0]}</p>
                <a href="{row['url']}">Read more</a>
            </div>
            """

        html_content += """
        </body>
        </html>
        """

        saopaulo = pendulum.timezone("America/Sao_Paulo")

        context["execution_date"] = context["execution_date"].in_timezone(saopaulo)

        send_email(
            html_content,
            subject + " - " + context["execution_date"].strftime("%Y-%m-%d %H:%M"),
        )

    create_postgres_table >> new_data >> send_mail(message=new_data)


# Define task dependencies
fetch_news_data()
