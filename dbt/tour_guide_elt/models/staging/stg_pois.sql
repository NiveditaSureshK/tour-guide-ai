{{ config(materialized='view') }}

select
    id,
    name,
    lat,
    lon,
    rating,
    kinds,
    open_hours,
    st_geogpoint(lon, lat) as geom,
    current_timestamp() as load_ts
from {{ source('raw', 'pois_raw') }}
