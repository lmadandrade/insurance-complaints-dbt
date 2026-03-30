select
    complaint_id,
    policy_id,
    complaint_received_at,

    lower(channel) as channel,
    lower(classification) as classification,
    lower(category) as category,
    lower(reason) as reason,
    lower(priority) as priority,
    lower(status) as status,
    lower(ombudsman_status) as ombudsman_status,
    lower(resolution_type) as resolution_type,

    redress_amount,
    is_vulnerable_customer,
    summary
    
from {{ source('insurance_complaints_raw', 'complaints') }}