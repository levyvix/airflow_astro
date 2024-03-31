with source as (
      select * from {{ source('news', 'news') }}
),
renamed as (
    select
        {{ adapter.quote("author") }},
        {{ adapter.quote("title") }},
        {{ adapter.quote("description") }},
        {{ adapter.quote("url") }},
        {{ adapter.quote("urlToImage") }} as utl_to_image,
        {{ adapter.quote("publishedAt") }} as published_at,
        {{ adapter.quote("content") }},
        {{ adapter.quote("source_id") }},
        {{ adapter.quote("source_name") }}

    from source
)
select * from renamed