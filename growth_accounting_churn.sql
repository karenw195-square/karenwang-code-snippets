create or replace table app_payments.app_payments_analyticstemp.growth_accounting_churn as
with term as (
select du.merchant_token
  , date_trunc('month',dps.report_date) as month
  , du.merchant_activation_address_country_code as country_code
  , sum(dps.gpv_net_var_usd) as gpv_usd
FROM app_bi.app_bi_dw.vfact_daily_processing_summary AS dps
JOIN app_bi.app_bi_dw.dim_product dp
 ON dps.key_product = dp.key_product
JOIN app_bi.app_bi_dw.dim_user du
 ON dps.key_user = du.key_user
WHERE LAST_DAY(dps.report_date) >= '2018-10-01'::DATE
 AND dp.product_name_id = 30010
 GROUP BY 1,2,3
  )

,  non_term as (
select du.merchant_token
  , date_trunc('month',dps.report_date) as month
  , du.merchant_activation_address_country_code as country_code
  , sum(dps.gpv_net_var_usd) as gpv_usd
FROM app_bi.app_bi_dw.vfact_daily_processing_summary AS dps
JOIN app_bi.app_bi_dw.dim_product dp
 ON dps.key_product = dp.key_product
JOIN app_bi.app_bi_dw.dim_user du
 ON dps.key_user = du.key_user
WHERE LAST_DAY(dps.report_date) >= '2018-10-01'::DATE
 AND dp.product_name_id != 30010
 AND dp.product_category = 'Processing'
GROUP BY 1,2,3
)
, combine as (
  select coalesce(t0.merchant_token,t1.merchant_token,t2.merchant_token,nt0.merchant_token,nt1.merchant_token,nt2.merchant_token) as merchant_token
, coalesce(t0.country_code,t1.country_code,t2.country_code,nt0.country_code,nt1.country_code,nt2.country_code) as country_code
, coalesce(t0.month,nt0.month,dateadd('month',1,t1.month),dateadd('month',2,t2.month),dateadd('month',1,nt1.month),dateadd('month',2,nt2.month)) as month
, mfa.merchant_net_new_seller
, mfa.existing_seller
, sum(t0.gpv_usd) as t0_amount
, sum(t1.gpv_usd) as t1_amount
, sum(t2.gpv_usd) as t2_amount
, sum(nt0.gpv_usd) as nt0_amount
, sum(nt1.gpv_usd) as nt1_amount
, sum(nt2.gpv_usd) as nt2_amount
from term t0 --current month
full outer join term t1 on t0.merchant_token = t1.merchant_token and t1.month = dateadd('month',-1,t0.month) --last month
full outer join term t2 on t0.merchant_token = t2.merchant_token and t2.month = dateadd('month',-2,t0.month) --last 2 months
full outer join non_term nt0 on nt0.merchant_token = t0.merchant_token and nt0.month = t0.month --current month
full outer join non_term nt1 on nt1.merchant_token = t0.merchant_token and nt1.month = dateadd('month',-1,t0.month) --last month
full outer join non_term nt2 on nt2.merchant_token = t0.merchant_token and nt2.month = dateadd('month',-2,t0.month) --last 2 months
left JOIN app_bi.app_bi_dw.dim_merchant_first_activity mfa
 ON mfa.merchant_token = coalesce(t0.merchant_token,t1.merchant_token,t2.merchant_token,nt0.merchant_token,nt1.merchant_token,nt2.merchant_token)
 AND mfa.product_name = 'Register Terminal'
 AND date_trunc('month',mfa.merchant_net_new_date) = coalesce(t0.month,nt0.month)
group by 1,2,3,4,5
order by 1,2,3,4,5
)

select c.month
, c.country_code
, du.business_category
, g.merchant_segment
, count(distinct case when t0_amount is not null then c.merchant_token else null end) as t0_term_active
, count(distinct case when t1_amount is not null then c.merchant_token else null end) as t1_term_active
, count(distinct case when merchant_net_new_seller = 1 then c.merchant_token else null end) as term_nna
, count(distinct case when existing_seller = 1 then c.merchant_token else null end) as term_nnae
, count(distinct case when t0_amount is not null and t1_amount is not null then c.merchant_token else null end) as term_retained
, count(distinct case when t0_amount is not null and t1_amount is null and merchant_net_new_seller is null and existing_seller is null then c.merchant_token else null end) as term_reactivated
, count(distinct case when t0_amount is null and t1_amount is not null and nt0_amount is not null then c.merchant_token else null end) as switch_product
, count(distinct case when t0_amount is null and t1_amount is not null and nt0_amount is null then c.merchant_token else null end) as true_churn
from combine c
inner join app_bi.pentagon.dim_user du on c.merchant_token = du.user_token and du.user_type = 'MERCHANT'
left join app_bi.app_bi_dw.dim_merchant_gpv_segment g on c.merchant_token = g.merchant_token and dateadd('day', -1, dateadd('month',1,c.month)) between g.effective_begin and g.effective_end
where month >= '2019-01-01'
group by 1,2,3,4
order by 1,2,3,4
;
