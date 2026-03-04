with events as (
    select *
    from {{ ref('stg_events') }}
)

select
    event_date,
    platform,
    countIf(event_name = 'purchase') as purchases,
    uniqExactIf(user_id, event_name = 'purchase') as buyers,
    sumIf(price, event_name = 'purchase') as revenue
from events
group by
    event_date,
    platform

