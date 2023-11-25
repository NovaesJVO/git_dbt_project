select 
    company_name,
    contact_name,
    count(*) as count
from {{ref('customers')}}
group by 1, 2
having count >= 1