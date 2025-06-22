{{ config(materialized = 'view') }}

select
    poi_id,
    obs_datetime,
    temp_c,
    precipitation_mm,
    snow_depth_cm,
    case when snow_depth_cm > 0 then 1 else 0 end as snow_flag,
    st_geogpoint(lon, lat)             as geom,
    current_timestamp()                as load_ts
from {{ source('raw', 'weather_raw') }}
