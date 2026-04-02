-- This model brings together complaint details, policy context, lifecycle milestones,
-- and SLA metrics into one complaint-level table for reporting and analysis

with complaints as (

    select
        complaint_id,
        policy_id,
        complaint_received_at,
        complaint_channel,
        complaint_classification,
        complaint_category,
        complaint_reason,
        complaint_priority,
        complaint_status,
        ombudsman_status,
        resolution_type,
        redress_amount,
        is_vulnerable_customer,
        summary
    from {{ ref('stg_complaints') }}
),

policies as (

    select
        policy_id,
        customer_id,
        product_name,
        business_partner,
        risk_carrier,
        policy_start_date,
        policy_end_date,
        policy_status,
        customer_country
    from {{ ref('stg_policies') }}
),


timeline as (

    select
        complaint_id,
        first_acknowledged_at,
        first_response_sent_at,
        first_resolved_at,
        first_ombudsman_escalated_at,
        total_event_count
    from {{ ref('int_complaint_timeline') }}
),


sla as (

    select
        complaint_id,
        hours_to_first_acknowledgment,
        hours_to_first_response,
        days_to_first_resolution,
        ack_sla_met_flag,
        response_sla_met_flag,
        resolution_sla_met_flag
    from {{ ref('int_complaint_sla') }}
)

-- Combine complaint data with policy context, lifecycle milestones and SLA metrics

select 
    c.complaint_id,
    c.policy_id,
    p.customer_id,
    p.product_name,
    p.business_partner,
    p.risk_carrier,
    p.policy_start_date,
    p.policy_end_date,
    p.policy_status,
    p.customer_country,
    c.complaint_received_at,
    c.complaint_channel,
    c.complaint_classification,
    c.complaint_category,
    c.complaint_reason,
    c.complaint_priority,
    c.complaint_status,
    c.ombudsman_status,
    c.resolution_type,
    c.redress_amount,
    c.is_vulnerable_customer,
    c.summary,
    t.first_acknowledged_at,
    t.first_response_sent_at,
    t.first_resolved_at,
    t.first_ombudsman_escalated_at,
    t.total_event_count,
    s.hours_to_first_acknowledgment,
    s.hours_to_first_response,
    s.days_to_first_resolution,
    s.ack_sla_met_flag,
    s.response_sla_met_flag,
    s.resolution_sla_met_flag

from complaints c
left join policies p
    on c.policy_id = p.policy_id
left join timeline t
    on c.complaint_id = t.complaint_id
left join sla s
    on c.complaint_id = s.complaint_id

