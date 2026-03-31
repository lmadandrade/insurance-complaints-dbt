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
from {{ source('insurance_complaints_raw', 'policies') }}