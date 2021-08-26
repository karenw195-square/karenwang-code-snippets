create or replace table app_payments.app_payments_analyticstemp.period_trailing_churn as
with base as (
select coalesce(t0.merchant_token , t1.merchant_token) as merchant_token
, coalesce(t0.currency_code , t1.currency_code) as currency_code
, coalesce(t0.product_name , t1.product_name) as product_name
, coalesce(t0.product_category , t1.product_category) as product_category
, coalesce(t0.report_date , dateadd('day',28, t1.report_date)) as report_date
, coalesce(t0.merchant_country , t1.merchant_country) as merchant_country
, coalesce(t0.merchant_mcc , t1.merchant_mcc) as merchant_mcc
, coalesce(t0.gpv_segment , t1.gpv_segment) as gpv_segment
, t0.merchant_028d_cohort as t0_merchant_028d_cohort
, t1.merchant_028d_cohort as t1_merchant_028d_cohort
, sum(t0.gpv_net_var_usd_28d) as t0_amount
, sum(t1.gpv_net_var_usd_28d) as t1_amount
from (select * from app_bi.app_bi_dw.dim_merchant_mature_cohort where report_date >= '2020-10-01'::DATE and product_name in ('ALL','Register Terminal')) t0
full outer join (select * from app_bi.app_bi_dw.dim_merchant_mature_cohort where report_date >= '2020-10-01'::DATE and product_name in ('ALL','Register Terminal')) t1 
             on t0.merchant_token = t1.merchant_token 
             and t0.currency_code = t1.currency_code 
             and t0.product_name = t1.product_name 
             and t0.product_category = t1.product_category 
             and t0.report_date = dateadd('day',28, t1.report_date) --t1 is 28 days ago
group by 1,2,3,4,5,6,7,8,9,10
)

select b1.report_date
, b1.currency_code
, b1.merchant_country
, b1.merchant_mcc
, b1.gpv_segment
, count(distinct case when b2.t0_amount > 0 then b2.merchant_token else null end) as t0_term_active
, count(distinct case when b2.t1_amount > 0 then b2.merchant_token else null end) as t1_term_active
, count(distinct case when merchant_net_new_seller = 1 then b2.merchant_token else null end) as term_nna
, count(distinct case when existing_seller = 1 then b2.merchant_token else null end) as term_nnae
, count(distinct case when b2.t0_amount > 0 and b2.t1_amount > 0 then b2.merchant_token else null end) as term_retained
, count(distinct case when b2.t0_amount > 0 and coalesce(b2.t1_amount,0) <= 0 and merchant_net_new_seller is null and existing_seller is null then b2.merchant_token else null end) as term_reactivated
, count(distinct case when coalesce(b2.t0_amount,0) <= 0 and b2.t1_amount > 0 and b1.t0_amount > 0 then b2.merchant_token else null end) as switch_product
, count(distinct case when coalesce(b2.t0_amount,0) <= 0 and b2.t1_amount > 0 and coalesce(b1.t0_amount,0) <= 0 then b2.merchant_token else null end) as true_churn
from (select * from base where product_name = 'ALL' and report_date >= '2021-01-01') b1
left join (select * from base where product_name = 'Register Terminal' and report_date >= '2021-01-01') b2 
       on b1.merchant_token = b2.merchant_token
       and b1.currency_code = b2.currency_code 
       and b1.report_date = b2.report_date 
left join app_bi.app_bi_dw.dim_merchant_first_activity mfa
       on mfa.merchant_token = b2.merchant_token
       and mfa.product_name = 'Register Terminal'
       and mfa.merchant_net_new_date = b2.report_date 
group by 1,2,3,4,5
;
