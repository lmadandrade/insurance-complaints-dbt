-- This model summarizes complaint outcomes by month, business partner and resolution type
-- so we can understand how complaints ended and where redress costs are happening

with complaints as (
    select
        date_trunc(date(complaint_received_at), month) as complaint_month,
        business_partner,
        resolution_type,
        redress_amount
    from {{ ref('fct_complaints') }}

),

-- Group complaints into outcome views by month, business partner and resolution type

outcomes as (

    select
        complaint_month,
        business_partner,
        resolution_type,

        count(*) as total_complaints,
        countif(redress_amount > 0) as complaints_with_redress,
        round(avg(case when redress_amount > 0 then 1 else 0 end), 2) as redress_rate,

        sum(redress_amount) as total_redress_amount,
        round(avg(redress_amount), 2) as avg_redress_amount,
        max(redress_amount) as max_redress_amount

    from complaints
    group by
        complaint_month,
        business_partner,
        resolution_type
)

select * 
from outcomes
