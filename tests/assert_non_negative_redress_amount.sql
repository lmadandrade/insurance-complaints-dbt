select *
from {{ ref('fct_complaints') }}
where redress_amount < 0