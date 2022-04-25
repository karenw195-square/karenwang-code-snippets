SELECT
    query_id
    ,query_text
    ,start_time
FROM snowflake_usage.global_usage.query_history
WHERE user_name = 'KARENWANG' 
  AND query_text ilike '%mcg_churn%'
  AND start_time >= '2021-04-01'
ORDER BY start_time DESC
;


select
    coalesce(nullif(user_name,'LOOKER'),upper(query_tag)) as user_name
    ,WAREHOUSE_NAME
    ,count(1) as frequency
    ,max(start_time) as last_used_time
from 
   snowflake_usage.account_usage.query_history
where
    lower(query_text) like '%customer_data.cdp_event_logger.merchant_complete_payment%'
and start_time >= dateadd(day,-365,current_date)
group by
    1,2
order by
    2 desc
