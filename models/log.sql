select 
    distinct event_name,
    event_timestamp,
    event_schema,
    event_model,
    event_user,
    event_target
from {{target.schema}}_meta.dbt_audit_log
