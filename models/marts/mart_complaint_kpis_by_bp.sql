-- This model summarizes complaint performance into monthly KPI views by business partner
-- so the data can be used for reporting and monitoring

with complaints as (

    select
        date_trunc(date(complaint_received_at), month) as complaint_month,
        business_partner,
        complaint_status,
        first_ombudsman_escalated_at,
        redress_amount,
        hours_to_first_acknowledgment,
        hours_to_first_response,
        days_to_first_resolution,
        ack_sla_met_flag,
        response_sla_met_flag,
        resolution_sla_met_flag
    from {{ ref('fct_complaints') }}

),

-- Group complaints into monthly KPI views by business partner for performance comparison

kpis as (
    select
        complaint_month,
        business_partner,

        count(*) as total_complaints,

        countif(complaint_status = 'resolved') as resolved_complaints,
        countif(first_ombudsman_escalated_at is not null) as escalated_complaints,

        round(avg(hours_to_first_acknowledgment), 2) as avg_hours_to_first_acknowledgment,
        round(avg(hours_to_first_response), 2) as avg_hours_to_first_response,
        round(avg(days_to_first_resolution), 2) as avg_days_to_first_resolution,

        round(
            avg(
                case
                    when ack_sla_met_flag is true then 1
                    when ack_sla_met_flag is false then 0
                    else null
                end
            ),
        2) as ack_sla_met_rate,

        round(
            avg(
                case
                    when response_sla_met_flag is true then 1
                    when response_sla_met_flag is false then 0
                    else null
                end
            ),
        2) as response_sla_met_rate,

        round(
            avg(
                case
                    when resolution_sla_met_flag is true then 1
                    when resolution_sla_met_flag is false then 0
                    else null
                end
            ),
        2) as resolution_sla_met_rate,

        sum (redress_amount) as total_redress_amount,
        round(avg(redress_amount), 2) as avg_redress_amount
    from complaints
    group by 
        complaint_month,
        business_partner
)

select * from kpis