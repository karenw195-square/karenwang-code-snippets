create or replace table personal_karenwang.public.monthly_term_gpv as
with term_nna_date as (
 select du.merchant_token
, min(dps.report_date) as term_nna_date
FROM APP_BI.APP_BI_DW.vfact_daily_processing_summary dps
JOIN APP_BI.APP_BI_DW.dim_product dp
 ON dps.key_product = dp.key_product
JOIN APP_BI.APP_BI_DW.dim_user du
 ON dps.key_user = du.key_user
WHERE dp.product_name_id = 30010
group by 1
  )


select du.merchant_token
  , tnd.term_nna_date
  , date_trunc('month',dps.report_date) as month
  , du.merchant_activation_address_country_code as country_code
  , sum(dps.revenue_net_var_usd - dps.fee_total_combined_var_usd) as adj_rev
  , sum(dps.gpv_net_var_usd) as net_gpv_usd
  , sum(transaction_count_net) as trx
FROM APP_BI.APP_BI_DW.vfact_daily_processing_summary AS dps
JOIN APP_BI.APP_BI_DW.dim_product dp
 ON dps.key_product = dp.key_product
JOIN APP_BI.APP_BI_DW.dim_user du
 ON dps.key_user = du.key_user
join term_nna_date tnd 
  on tnd.merchant_token = du.merchant_token
WHERE LAST_DAY(dps.report_date) >= '2018-10-01'::DATE
 AND dp.product_name_id = 30010
GROUP BY 1,2,3,4
;

with base as (
select merchant_token
, sum(net_gpv_usd) as net_gpv_usd
, sum(trx) as trx
from personal_karenwang.public.monthly_term_gpv
  where year(month) = 2020
group by 1
  )
, rank as (
select *
  , row_number() over (order by net_gpv_usd desc) as rn
  , count(*) over () as total_merchants
from base
)  
, find_20_point as (
select *, row_number() over (order by net_gpv_usd desc) as rn2 from rank where rn = round(0.2 * total_merchants)
)
select 
sum(case when r.net_gpv_usd >= f.net_gpv_usd then r.net_gpv_usd else 0 end)/sum(r.net_gpv_usd)
from rank r
cross join find_20_point f
where f.rn2 = 1
