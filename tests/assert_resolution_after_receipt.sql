select *
from {{ ref('fct_complaints') }}
where first_resolved_at < complaint_received_at