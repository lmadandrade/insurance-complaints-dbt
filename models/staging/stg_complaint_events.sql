select
    event_id,
    complaint_id,
    event_timestamp,
    event_type,
    actor

from {{ source('insurance_complaints_raw', 'complaint_events')}}