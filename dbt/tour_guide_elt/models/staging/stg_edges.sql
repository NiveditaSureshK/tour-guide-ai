{{ config(materialized = 'view') }}

with raw as (

    select
        src_poi_id,
        dst_poi_id,
        mode,                         -- 'walk' | 'uber' | 'tube' | …
        distance_meters,
        time_seconds,
        cost_usd,
        start_lat,
        start_lon,
        end_lat,
        end_lon,
        collected_at
    from {{ source('raw', 'edges_raw') }}

)

select
    itinerary_id,
    day_index,
    seq_in_day,
    src_poi_id,
    dst_poi_id,
    mode,
    distance_meters / 1000.0          as distance_km,
    time_seconds    / 60.0            as time_min,
    cost_usd,
    st_geogpoint(start_lon, start_lat) as src_geom,
    st_geogpoint(end_lon,   end_lat)   as dst_geom,
    collected_at,                      -- ★ keep original name
    current_timestamp()                as load_ts
from {{ source('raw', 'edges_raw') }}

