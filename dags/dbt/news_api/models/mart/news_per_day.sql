with news as (
    select * from {{ref("stg_news")}}
)

select
    date(published_at) as date,
    count(*) as news_count
from news
group by date
order by date asc