-- This model turns the complaint timeline into SLA timings and SLA status flags
-- Each row still represents one complaint
-- It measures how quickly complaints were acknowledged, responded to, and resolved


with timeline as (

    select
        complaint_id,
        complaint_received_at,
        first_acknowledged_at,
        first_response_sent_at,
        first_resolved_at
    from {{ ref('int_complaint_timeline') }}
    
),


-- Calculate how long each key complaint step took from the moment the complaint was received

sla_calculations as (

    select
        complaint_id,
        complaint_received_at,
        first_acknowledged_at,
        first_response_sent_at,
        first_resolved_at,

        timestamp_diff(first_acknowledged_at, complaint_received_at, hour) as hours_to_first_acknowledgment,
        timestamp_diff(first_response_sent_at, complaint_received_at, hour) as hours_to_first_response,
        timestamp_diff(first_resolved_at, complaint_received_at, day) as days_to_first_resolution

    from timeline

),

-- Turn the time differences into SLA results for acknowledgment, response, and resolution

sla_flags as (

    select
        complaint_id,
        complaint_received_at,
        first_acknowledged_at,
        first_response_sent_at,
        first_resolved_at,
        hours_to_first_acknowledgment,
        hours_to_first_response,
        days_to_first_resolution,

        case
            when hours_to_first_acknowledgment is null then null
            when hours_to_first_acknowledgment <= 24 then true
            else false
        end as ack_sla_met_flag,

        case
            when hours_to_first_response is null then null
            when hours_to_first_response <= 72 then true
            else false
        end as response_sla_met_flag,

        case
            when days_to_first_resolution is null then null
            when days_to_first_resolution <= 5 then true
            else false
        end as resolution_sla_met_flag

    from sla_calculations

)


select * 
from sla_flags