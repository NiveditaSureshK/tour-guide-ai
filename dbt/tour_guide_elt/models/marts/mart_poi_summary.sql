{{ config(materialized = 'table') }}

with p as (
    select * from {{ ref('stg_pois') }}
)

select
    regexp_extract(kinds[SAFE_OFFSET(0)], r'[^:]+')  as primary_kind,
    count(*)                                         as poi_count,
    round(avg(rating), 2)                            as avg_rating
from p
group by primary_kind
order by poi_count desc
