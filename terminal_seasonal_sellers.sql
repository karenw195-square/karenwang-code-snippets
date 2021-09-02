create or replace table app_payments.app_payments_analyticstemp.seasonal_tagging as 
with mature_cohort_monthly as (
select du.best_available_merchant_token
, s.country_code
, date_trunc('month',payment_trx_recognized_date) as month_x
, count(distinct case when du.is_currently_frozen = 0 and du.is_currently_deactivated = 0 then du.best_available_merchant_token else null end) as is_active_account
, sum(total_payment_count) as pmt
, sum(cnp_card_payment_count) as cnp_pmt
, sum(gpv_payment_amount_base_unit_usd / 100) as gpv_usd
, sum(cnp_card_payment_amount_base_unit_usd / 100) as cnp_gpv_usd
from app_bi.pentagon.aggregate_seller_daily_payment_summary s
left join app_bi.pentagon.dim_user du on s.user_token = du.user_token
left join app_bi.pentagon.aggregate_merchant_lifetime_summary a on a.merchant_token = du.best_available_merchant_token
where 1=1
    --and s.user_token = '4YRFNR5N5P73H' -- Coachella Music Festival LLC 
    and a.first_card_payment_date <= current_date - 456 --mature cohort (give enough time to show yoy pattern)
group by 1,2,3
)
, mature_cohort_yearly as (
select best_available_merchant_token
, country_code
, is_active_account
, year(month_x) as year_x
, sum(pmt) as yearly_pmt
, sum(cnp_pmt) as yearly_cnp_pmt
, sum(gpv_usd) as yearly_gpv_usd
, sum(cnp_gpv_usd) as yearly_cnp_gpv_usd
, count(month_x) as active_months
, min(month_x) as min_month
, max(month_x) as max_month
, datediff('month',min_month,max_month) + 1 as duration
, dateadd('month',floor(duration/2), min(month_x)) as median_month
, datediff('month',min_month,median_month) as min_to_median
, datediff('month',median_month,max_month) as median_to_max
from mature_cohort_monthly
group by 1,2,3,4
order by 1,2,3,4
)
, check_repetition as (
select y.*
, case when active_months = duration and min_to_median < 3 and median_to_max < 3 then true else false end is_compact -- have all payments concentrated within 6 consecutive months of a year
, lag(is_compact,1) over (partition by best_available_merchant_token order by year_x) as last_is_compact
, lag(is_compact,2) over (partition by best_available_merchant_token order by year_x) as last_two_is_compact 
, lag(median_month,1) over (partition by best_available_merchant_token order by year_x) as last_median
, lag(median_month,2) over (partition by best_available_merchant_token order by year_x) as last_two_median
, lag(duration,1) over (partition by best_available_merchant_token order by year_x) as last_duration
, lag(duration,2) over (partition by best_available_merchant_token order by year_x) as last_two_duration
from mature_cohort_yearly y
    )
   
select best_available_merchant_token
, country_code
, is_active_account
, year_x
, yearly_pmt
, yearly_cnp_pmt
, yearly_gpv_usd
, yearly_cnp_gpv_usd
, active_months
, min_month
, max_month
, duration
, median_month
, min_to_median
, median_to_max
, case when sum(yearly_gpv_usd) over (partition by best_available_merchant_token) <= 1 then 'test payment'
       when sum(active_months) over (partition by best_available_merchant_token) = 1 then 'single month'
       when datediff('month',min(min_month) over (partition by best_available_merchant_token),max(max_month) over (partition by best_available_merchant_token)) < 12 then 'single year, multi months'
       when count(*) over (partition by best_available_merchant_token) > 1 --multi year
            and sum(case when active_months >= 10 then 1 else 0 end) over (partition by best_available_merchant_token) -- number of years with 11+ active months
                >= count(*) over (partition by best_available_merchant_token) * 0.5
            then 'multi year, all year'     
       when count(*) over (partition by best_available_merchant_token) > 1 --multi year
            and count(case when is_compact = true then 1 else null end) over (partition by best_available_merchant_token) >= count(*) over (partition by best_available_merchant_token) * 0.5 --all years are compact
            and stddev_samp(month(median_month)) over (partition by best_available_merchant_token) <=2 --similar median month
            and stddev_samp(duration) over (partition by best_available_merchant_token) <=2 --similar duration
       then 'multi year, seasonal' else 'multi year, casual' end business_type
, case when max(max_month) over (partition by best_available_merchant_token) >= dateadd('month',-3,date_trunc('month',current_date)) then 1 else 0 end is_active
from check_repetition
order by best_available_merchant_token, year_x
;

create or replace table app_payments.app_payments_analyticstemp.merchant_term_gpv as 
select merchant_token
, sum(case when product_name = 'Register Terminal' then volume_gross_var_usd else 0 end) as term_gpv
, sum(case when product_name = 'Register POS' then volume_gross_var_usd else 0 end) as spos_gpv
, sum(case when product_category = 'Processing' then volume_gross_var_usd else 0 end) as gpv
, sum(case when product_name = 'Register Terminal' then volume_gross_var_usd else 0 end) / nullif(sum(case when product_category = 'Processing' then volume_gross_var_usd else 0 end),0) as term_pct
, sum(case when product_name = 'Register POS' then volume_gross_var_usd else 0 end) / nullif(sum(case when product_category = 'Processing' then volume_gross_var_usd else 0 end),0) as spos_pct
from app_bi.app_bi_dw.vfact_merchant_revenue_summary
group by 1
;
