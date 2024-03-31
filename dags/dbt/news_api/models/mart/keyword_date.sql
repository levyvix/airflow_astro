with news as (
    select * from {{ ref("stg_news") }}
),
keywords as (

select
    case when title like '%Apple%' then 1 else 0 end as keyword_1,
    case when title like '%Microsoft%' then 1 else 0 end as keyword_2,
    case when title like '%Google%' then 1 else 0 end as keyword_3,
    published_at
from news
)

select
    date(published_at) as date,
    sum(keyword_1) as keyword_1_count,
    sum(keyword_2) as keyword_2_count,
    sum(keyword_3) as keyword_3_count
from keywords
group by date(published_at)
order by date(published_at) asc