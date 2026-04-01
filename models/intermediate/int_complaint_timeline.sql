-- This model builds a complaint-level timeline from the event history
-- Each row represents one complaint, with the key timestamps showing what happened and when
-- It helps us understand the lifecycle of a complaint before calculating SLA and performance metrics

-- Start with one row per complaint so every complaint stays in the final result
with complaints as (

    select
        complaint_id,
        complaint_received_at
    from {{ ref('stg_complaints') }}

),

-- Bring in the event history we need so we can work out what happened to each complaint and when
events as (
    select
        event_id,
        complaint_id,
        event_timestamp,
        event_type
    from {{ ref('stg_complaint_events') }}

),

-- Turn the event history into a complaint timeline
-- For each complaint, keep the first time each important event happened and count how many events it had overall
-- This model keeps the first occurrence only, so repeated reopen or response events are not tracked separately here
timeline as (
    select
        complaint_id,
        min(case when event_type = 'acknowledgment_sent' then event_timestamp end) as first_acknowledged_at,
        min(case when event_type = 'response_sent' then event_timestamp end) as first_response_sent_at,
        min(case when event_type = 'complaint_resolved' then event_timestamp end) as first_resolved_at,
        min(case when event_type = 'ombudsman_escalated' then event_timestamp end) as first_ombudsman_escalated_at,
        min(case when event_type = 'complaint_reopened' then event_timestamp end) as first_reopened_at,
        count(event_id) as total_event_count
    from events
    group by complaint_id
)

select
    c.complaint_id,
    c.complaint_received_at,
    t.first_acknowledged_at,
    t.first_response_sent_at,
    t.first_resolved_at,
    t.first_ombudsman_escalated_at,
    t.first_reopened_at,
    t.total_event_count
from complaints c
left join timeline t
    on c.complaint_id = t.complaint_id
