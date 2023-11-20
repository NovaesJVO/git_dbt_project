with

    calc_employees as (
        select 
            CAST( date_part(YEAR, current_date)  - date_part(YEAR, birth_date) as INTEGER) as age,
            CAST( date_part(YEAR, current_date)  - date_part(YEAR, hire_date) as INTEGER) as lengthofservice,
            first_name || ' ' || last_name as name,
            *
        from {{source('sources', 'employees')}}
    )

select 
    *
from calc_employees