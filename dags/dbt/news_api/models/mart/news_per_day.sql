with news as (
    select * from {{ref("stg_news")}}
)

SELECT
   date_part('year', cast(published_at as date)) as ano_publicacao,
   date_part('month', cast(published_at as date)) as mes_publicacao,
   date_part('day', cast(published_at as date)) as dia_publicacao,
   count(*) as contagem_noticias
from news
group by ano_publicacao, mes_publicacao, dia_publicacao