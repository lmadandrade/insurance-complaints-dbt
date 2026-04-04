-- This model summarizes complaint outcomes by month
-- so we can understand where redress costs are happening

with complaints as (
    select
        date_trunc(date(complaint_received_at), month) as complaint_month,
        redress_amount

    from {{ ref('fct_complaints') }}

),

-- Group complaints into redress views by month

outcomes as (

    select
        complaint_month,

        count(*) as total_complaints,
        countif(redress_amount > 0) as complaints_with_redress,
        round(avg(case when redress_amount > 0 then 1 else 0 end), 2) as redress_rate,

        sum(redress_amount) as total_redress_amount,
        round(avg(redress_amount), 2) as avg_redress_amount,
        max(redress_amount) as max_redress_amount

    from complaints
    group by
        complaint_month
)

select * 
from outcomes
