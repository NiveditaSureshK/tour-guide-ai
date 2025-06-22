{{ config(materialized = 'table') }}

with e as (
    select
        date(collected_at)          as trip_date,
        mode,
        sum(distance_km)            as total_distance_km,
        sum(time_min)               as total_time_min,
        round(avg(cost_usd), 2)     as avg_cost_usd,
        count(*)                    as hop_count
    from {{ ref('stg_edges') }}
    group by trip_date, mode
)

select *
from e
order by trip_date, mode
