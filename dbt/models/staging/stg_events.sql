with src as (
    select *
    from {{ source('raw', 'events') }}
)

select
    event_time,
    toDate(event_time) as event_date,
    user_id,
    session_id,
    event_name,
    platform,
    price,
    props_json
from src

