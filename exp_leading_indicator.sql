/*adoption metric:
-npa
-test payment exclusion
-curing window 1-35d
-mcc,product,card presence
*/
create or replace table app_payments.app_payments_analyticstemp.exp_leading_indicator_activated_2019 as 
with activated_2019 as (
select user_token as merchant_token
    , first_successful_activation_request_created_at as act_at
    , npa_total / 100 as npa_usd
    , business_category
from app_bi.pentagon.dim_user 
where user_type = 'MERCHANT'
    and country_code = 'US'
    and year(first_successful_activation_request_created_at) = 2019
)

select a.*
    , min(fpt.payment_trx_recognized_at) as first_pmt_at
    , min(case when fpt.amount_base_unit > 100 then fpt.payment_trx_recognized_at else null end) as first_non_test_pmt_at
    , count(case when datediff('day',a.act_at,fpt.payment_trx_recognized_at) < 7 then payment_token else null end) as pmt_7d
    , count(case when datediff('day',a.act_at,fpt.payment_trx_recognized_at) < 14 then payment_token else null end) as pmt_14d
    , count(case when datediff('day',a.act_at,fpt.payment_trx_recognized_at) < 21 then payment_token else null end) as pmt_21d
    , count(case when datediff('day',a.act_at,fpt.payment_trx_recognized_at) < 28 then payment_token else null end) as pmt_28d
    , count(case when datediff('day',a.act_at,fpt.payment_trx_recognized_at) < 35 then payment_token else null end) as pmt_35d
    , sum(case when datediff('day',a.act_at,fpt.payment_trx_recognized_at) < 7 then ceil(amount_base_unit / 10000) else 0 end) as amount_7d
    , sum(case when datediff('day',a.act_at,fpt.payment_trx_recognized_at) < 14 then ceil(amount_base_unit / 10000) else 0 end) as amount_14d
    , sum(case when datediff('day',a.act_at,fpt.payment_trx_recognized_at) < 21 then ceil(amount_base_unit / 10000) else 0 end) as amount_21d
    , sum(case when datediff('day',a.act_at,fpt.payment_trx_recognized_at) < 28 then ceil(amount_base_unit / 10000) else 0 end) as amount_28d
    , sum(case when datediff('day',a.act_at,fpt.payment_trx_recognized_at) < 35 then ceil(amount_base_unit / 10000) else 0 end) as amount_35d
    , count(case when fpt.amount_base_unit > 100 and datediff('day',a.act_at,fpt.payment_trx_recognized_at) < 7 then payment_token else null end) as non_test_pmt_7d
    , count(case when fpt.amount_base_unit > 100 and datediff('day',a.act_at,fpt.payment_trx_recognized_at) < 14 then payment_token else null end) as non_test_pmt_14d
    , count(case when fpt.amount_base_unit > 100 and datediff('day',a.act_at,fpt.payment_trx_recognized_at) < 21 then payment_token else null end) as non_test_pmt_21d
    , count(case when fpt.amount_base_unit > 100 and datediff('day',a.act_at,fpt.payment_trx_recognized_at) < 28 then payment_token else null end) as non_test_pmt_28d
    , count(case when fpt.amount_base_unit > 100 and datediff('day',a.act_at,fpt.payment_trx_recognized_at) < 35 then payment_token else null end) as non_test_pmt_35d
    , sum(case when fpt.amount_base_unit > 100 and datediff('day',a.act_at,fpt.payment_trx_recognized_at) < 7 then ceil(amount_base_unit / 10000) else 0 end) as non_test_amount_7d
    , sum(case when fpt.amount_base_unit > 100 and datediff('day',a.act_at,fpt.payment_trx_recognized_at) < 14 then ceil(amount_base_unit / 10000) else 0 end) as non_test_amount_14d
    , sum(case when fpt.amount_base_unit > 100 and datediff('day',a.act_at,fpt.payment_trx_recognized_at) < 21 then ceil(amount_base_unit / 10000) else 0 end) as non_test_amount_21d
    , sum(case when fpt.amount_base_unit > 100 and datediff('day',a.act_at,fpt.payment_trx_recognized_at) < 28 then ceil(amount_base_unit / 10000) else 0 end) as non_test_amount_28d
    , sum(case when fpt.amount_base_unit > 100 and datediff('day',a.act_at,fpt.payment_trx_recognized_at) < 35 then ceil(amount_base_unit / 10000) else 0 end) as non_test_amount_35d
    , sum(case when fpt.amount_base_unit > 100 and datediff('day',a.act_at,fpt.payment_trx_recognized_at) < 365 and product_name = 'Register Terminal' then ceil(amount_base_unit / 10000) else 0 end) as term_nva
    , sum(case when fpt.amount_base_unit > 100 and datediff('day',a.act_at,fpt.payment_trx_recognized_at) < 365 and product_name = 'Register POS' then ceil(amount_base_unit / 10000) else 0 end) as spos_nva
    , sum(case when fpt.amount_base_unit > 100 and datediff('day',a.act_at,fpt.payment_trx_recognized_at) < 365 and product_name = 'Virtual Terminal' then ceil(amount_base_unit / 10000) else 0 end) as vt_nva
    , sum(case when fpt.amount_base_unit > 100 and datediff('day',a.act_at,fpt.payment_trx_recognized_at) < 365 and product_name = 'Invoices' then ceil(amount_base_unit / 10000) else 0 end) as inv_nva
    , sum(case when fpt.amount_base_unit > 100 and datediff('day',a.act_at,fpt.payment_trx_recognized_at) < 365 and product_name = 'eCommerce API' then ceil(amount_base_unit / 10000) else 0 end) as ecom_nva
    , sum(case when fpt.amount_base_unit > 100 and datediff('day',a.act_at,fpt.payment_trx_recognized_at) < 365 and is_card_present = 1 then ceil(amount_base_unit / 10000) else 0 end) as cp_nva
    , sum(case when fpt.amount_base_unit > 100 and datediff('day',a.act_at,fpt.payment_trx_recognized_at) < 365 and is_card_present = 0 then ceil(amount_base_unit / 10000) else 0 end) as cnp_nva
    from app_bi.pentagon.fact_payment_transactions fpt
    left join app_bi.pentagon.dim_user du on fpt.unit_token = du.user_token and du.user_type = 'UNIT'
    inner join activated_2019 a on a.merchant_token = du.best_available_merchant_token
    where is_gpv = 1
    group by 1,2,3,4
;


/*retention metric:
-pre-churn GPV run rate
-post-churn GPV run rate (true churn?) 
-trialist churn exclusion
-deactivation exclusion
-curing window 1-91d
-mcc,product,card presence
*/

create or replace table app_payments.app_payments_analyticstemp.exp_leading_daily_pmt_2019 as 
with active_2019 as (
select du.best_available_merchant_token as merchant_token
    , du.business_category
    , max(du.is_currently_deactivated) as is_deactivated
    from app_bi.pentagon.aggregate_seller_daily_payment_summary dps
    left join app_bi.pentagon.dim_user du on dps.unit_token = du.user_token and du.user_type = 'UNIT'
where year(dps.payment_trx_recognized_date) = 2019
    and du.country_code = 'US'
group by 1,2
having sum(gpv_payment_count) > 0
)

select a.*
    , dps.payment_trx_recognized_date as pmt_date
    , sum(dps.gpv_payment_count) as pmt
    from app_bi.pentagon.aggregate_seller_daily_payment_summary dps
    left join app_bi.pentagon.dim_user du on dps.unit_token = du.user_token and du.user_type = 'UNIT'
    inner join active_2019 a on a.merchant_token = du.best_available_merchant_token
    where dps.payment_trx_recognized_date >= '2019-01-01'
    and dps.payment_trx_recognized_date <= '2020-05-01'
    group by 1,2,3,4
;

create or replace table app_payments.app_payments_analyticstemp.exp_leading_indicator_daily_churn_check_2019 as 
select d0.merchant_token
    , d0.business_category
    , d0.is_deactivated
    , d0.pmt_date
    , d0.pmt
    , case when datediff('day',d0.pmt_date, min(coalesce(d1.pmt_date,'9999-09-09'))) < 7 then 0 else 1 end as churn_7d
    , case when datediff('day',d0.pmt_date, min(coalesce(d1.pmt_date,'9999-09-09'))) < 14 then 0 else 1 end as churn_14d
    , case when datediff('day',d0.pmt_date, min(coalesce(d1.pmt_date,'9999-09-09'))) < 21 then 0 else 1 end as churn_21d
    , case when datediff('day',d0.pmt_date, min(coalesce(d1.pmt_date,'9999-09-09'))) < 28 then 0 else 1 end as churn_28d
    , case when datediff('day',d0.pmt_date, min(coalesce(d1.pmt_date,'9999-09-09'))) < 35 then 0 else 1 end as churn_35d
    , case when datediff('day',d0.pmt_date, min(coalesce(d1.pmt_date,'9999-09-09'))) < 42 then 0 else 1 end as churn_42d
    , case when datediff('day',d0.pmt_date, min(coalesce(d1.pmt_date,'9999-09-09'))) < 49 then 0 else 1 end as churn_49d
    , case when datediff('day',d0.pmt_date, min(coalesce(d1.pmt_date,'9999-09-09'))) < 56 then 0 else 1 end as churn_56d
    , case when datediff('day',d0.pmt_date, min(coalesce(d1.pmt_date,'9999-09-09'))) < 63 then 0 else 1 end as churn_63d
    , case when datediff('day',d0.pmt_date, min(coalesce(d1.pmt_date,'9999-09-09'))) < 70 then 0 else 1 end as churn_70d
    , case when datediff('day',d0.pmt_date, min(coalesce(d1.pmt_date,'9999-09-09'))) < 77 then 0 else 1 end as churn_77d
    , case when datediff('day',d0.pmt_date, min(coalesce(d1.pmt_date,'9999-09-09'))) < 84 then 0 else 1 end as churn_84d
    , case when datediff('day',d0.pmt_date, min(coalesce(d1.pmt_date,'9999-09-09'))) < 91 then 0 else 1 end as churn_91d
    from app_payments.app_payments_analyticstemp.exp_leading_daily_pmt_2019 d0 
    left join app_payments.app_payments_analyticstemp.exp_leading_daily_pmt_2019 d1 on d0.merchant_token = d1.merchant_token and d1.pmt_date > d0.pmt_date 
    where year(d0.pmt_date) = 2019
group by 1,2,3,4,5
;

create or replace table app_payments.app_payments_analyticstemp.exp_leading_indicator_churn_final_2019 as 
with fp_check as (
select l.*
    , case when a.latest_card_payment_date > pmt_date then 1 else 0 end false_positive
    , case when a.first_card_payment_date = a.latest_card_payment_date then 1 else 0 end trialist_churn
from app_payments.app_payments_analyticstemp.exp_leading_indicator_daily_churn_check_2019 l
    left join app_bi.pentagon.aggregate_merchant_lifetime_summary a on a.merchant_token = l.merchant_token
where churn_7d + churn_14d + churn_21d + churn_28d + churn_35d + churn_42d + churn_49d + churn_56d + churn_63d + churn_70d + churn_77d + churn_84d + churn_91d > 0
)

, bias_check as (
select du.best_available_merchant_token as merchant_token
    , sum(case when product_name = 'Register Terminal' then amount_base_unit / 100 else 0 end) as term_gpv
    , sum(case when product_name = 'Register POS' then amount_base_unit / 100 else 0 end) as spos_gpv
    , sum(case when product_name = 'Virtual Terminal' then amount_base_unit / 100 else 0 end) as vt_gpv
    , sum(case when product_name = 'Invoices' then amount_base_unit / 100 else 0 end) as inv_gpv
    , sum(case when product_name = 'eCommerce API' then amount_base_unit / 100 else 0 end) as ecom_gpv
    , sum(case when is_card_present = 1 then amount_base_unit / 100 else 0 end) as cp_gpv
    , sum(case when is_card_present = 0 then amount_base_unit / 100 else 0 end) as cnp_gpv
    from app_bi.pentagon.fact_payment_transactions fpt
    left join app_bi.pentagon.dim_user du on fpt.unit_token = du.user_token and du.user_type = 'UNIT'
    where is_gpv = 1
    and year(fpt.payment_trx_recognized_at) = 2019
    and du.best_available_merchant_token in (select merchant_token from app_payments.app_payments_analyticstemp.exp_leading_daily_pmt_2019)
    group by 1
)

select f.*
, b.term_gpv
, b.spos_gpv
, b.vt_gpv
, b.inv_gpv
, b.ecom_gpv
, b.cp_gpv
, b.cnp_gpv
from fp_check f
left join bias_check b on f.merchant_token = b.merchant_token

;

create or replace table app_payments.app_payments_analyticstemp.exp_leading_indicator_churn_run_rate_2019 as 
select f.*
, datediff('month',s.first_card_payment_date,f.pmt_date) as tenure_at_churn
, sum(m.volume_gross_var_usd) as gpv_run_rate_91
from app_payments.app_payments_analyticstemp.exp_leading_indicator_churn_final_2019 f
left join app_bi.pentagon.aggregate_merchant_lifetime_summary s on f.merchant_token = s.best_available_merchant_token
left join app_bi.app_bi_dw.vfact_merchant_revenue_summary m on f.merchant_token = m.merchant_token and m.report_date between dateadd('day',-91,f.pmt_date) and f.pmt_date
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28
;
