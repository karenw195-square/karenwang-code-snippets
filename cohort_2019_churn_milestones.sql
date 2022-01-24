create or replace table app_payments.app_payments_analyticstemp.cohort_2019_churn_milestones as
with cohort_2019 as (
select du.user_token as merchant_token
, du.business_category
, du.user_created_at
, m.first_card_payment_date
, m.latest_card_payment_date
, m.card_payment_count
from app_bi.pentagon.dim_user du
left join app_bi.pentagon.aggregate_merchant_lifetime_summary m on du.user_token = m.best_available_merchant_token
where du.country_code = 'US'
and du.user_type = 'MERCHANT'
and year(m.first_card_payment_date) = 2019
)

, fill_date as (
select c.merchant_token
, dd.report_date
from app_bi.app_bi_dw.dim_date dd
cross join cohort_2019 c
where year(dd.report_date) >= 2019 and dd.report_date <> '9999-09-09'
    and dd.report_date >= c.first_card_payment_date
    and dd.report_date <= (select max(report_date) from app_payments.app_payments_analytics.merchant_daily_trailing_cohorts)
)

, churns as (
select f.merchant_token
, f.report_date
, lag(f.report_date) over (partition by f.merchant_token order by f.report_date) as last_report_date
, amount_current
from fill_date f
left join app_payments.app_payments_analytics.merchant_daily_trailing_cohorts c 
     on f.merchant_token = c.merchant_token 
     and f.report_date = c.report_date
     and c.product_name = 'ALL'
     and c.trailing_window = '91 days trailing'
     and c.merchant_country = 'US'
     and c.amount_current > 0 
 where c.amount_current is null  

)

, first_churns as (
select merchant_token
, min(report_date) as first_churn_date
, count(distinct case when last_report_date <> dateadd('day',-1,report_date) or last_report_date is null then report_date else null end) as num_churns
from churns
    
group by 1
)

select c.merchant_token
, business_category
, first_card_payment_date
, latest_card_payment_date
, card_payment_count / (datediff('day', c.first_card_payment_date, c.latest_card_payment_date) + 1) * 365 as annualized_pmt
, coalesce(num_churns,0) as num_churns
, case when latest_card_payment_date < current_date - 91 then true else false end is_currently_churn
, greatest(dateadd('day', -91, first_churn_date),first_card_payment_date) as first_churn_decision_date
, datediff('day', c.first_card_payment_date, first_churn_decision_date) as tenure_at_churn_decision
, ceil((tenure_at_churn_decision + 1)/30) as month_at_churn_decision
, g.merchant_sub_segment as merchant_sub_segment_at_churn_decision
, sum(transaction_count) / 91 *365 as annualized_pmt_at_churn_decision
, sum(case when card_presence = 'CNP' then transaction_count else 0 end) / sum(transaction_count) as cnp_rate_at_churn_decision
from cohort_2019 c
left join first_churns f on c.merchant_token = f.merchant_token
left join app_bi.app_bi_dw.dim_merchant_gpv_segment g on g.currency_code = 'USD' and g.merchant_token = c.merchant_token and greatest(dateadd('day', -91, first_churn_date),first_card_payment_date) between g.effective_begin and g.effective_end
left join app_bi.app_bi_dw.dim_user du on c.merchant_token = du.merchant_token and du.user_type = 'UNIT'
left join app_bi.app_bi_dw.vfact_daily_processing_summary m on m.key_user = du.key_user and m.report_date between dateadd('day',-91,greatest(dateadd('day', -91, first_churn_date),first_card_payment_date)) and greatest(dateadd('day', -91, first_churn_date),first_card_payment_date)
group by 1,2,3,4,5,6,7,8,9,10,11
;
