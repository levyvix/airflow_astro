with news as (
    select * from {{ ref("stg_news") }}
)

select
    author,
    count(*) as news_count
from news
group by author