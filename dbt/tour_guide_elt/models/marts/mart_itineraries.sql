{{ config(materialized = 'table') }}

with legs as (
    select
        itinerary_id,
        day_index,
        seq_in_day,
        src_poi_id,
        dst_poi_id,
        time_min,
        cost_usd
    from {{ ref('stg_edges') }}
),

pois as (
    select
        id,
        name,
        regexp_extract(kinds[SAFE_OFFSET(0)], r'[^:]+') as primary_kind
    from {{ ref('stg_pois') }}
),

joined as (
    select
        l.*,
        src.name          as src_name,
        dst.name          as dst_name,
        src.primary_kind  as src_kind,
        dst.primary_kind  as dst_kind
    from legs l
    left join pois src on l.src_poi_id = src.id
    left join pois dst on l.dst_poi_id = dst.id
)

select
    itinerary_id,
    sum(time_min)                 as total_time_min,
    round(sum(cost_usd), 2)       as total_cost_usd,
    count(distinct src_poi_id) +
    count(distinct dst_poi_id)    as poi_count
from joined
group by itinerary_id
