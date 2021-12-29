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
