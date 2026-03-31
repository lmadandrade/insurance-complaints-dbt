select
    complaint_id,
    policy_id,
    complaint_received_at,

    lower(trim(channel)) as complaint_channel,
    lower(trim(classification)) as complaint_classification,
    lower(trim(category)) as complaint_category,
    lower(trim(reason)) as complaint_reason,
    lower(trim(priority)) as complaint_priority,
    lower(trim(status)) as complaint_status,
    lower(trim(ombudsman_status)) as ombudsman_status,
    lower(trim(resolution_type)) as resolution_type,

    redress_amount,
    is_vulnerable_customer,
    summary
    
from {{ source('insurance_complaints_raw', 'complaints') }}