select
    event_id,
    complaint_id,
    event_timestamp,
    lower(trim(event_type)) as event_type,
    lower(trim(actor)) as event_actor

from {{ source('insurance_complaints_raw', 'complaint_events')}}