create or replace table app_payments.app_payments_analyticstemp.cohort_2019_churn_milestones as
with churns as (
select merchant_token
, report_date
, lag(report_date) over (partition by merchant_token order by report_date) as last_report_date
from app_payments.app_payments_analytics.merchant_daily_trailing_cohorts
where product_name = 'ALL'
and trailing_window = '91 days trailing'
and merchant_country = 'US'
and amount_current <= 0
)

, first_churns as (
select merchant_token
, min(report_date) as first_churn_date
, count(distinct case when last_report_date <> dateadd('day',-1,report_date) or last_report_date is null then report_date else null end) as num_churns
from churns
group by 1
)

, cohort_2019 as (
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
and year(du.user_created_at) = 2019
)

select c.merchant_token
, business_category
, first_card_payment_date
, latest_card_payment_date
, card_payment_count / (datediff('day', c.first_card_payment_date, c.latest_card_payment_date) + 1) * 365 as annualized_pmt
, coalesce(num_churns,0) as num_churns
, case when latest_card_payment_date < current_date - 91 then true else false end is_currently_churn
, dateadd('day', -91, first_churn_date) as first_churn_decision_date
, datediff('day', c.first_card_payment_date, first_churn_decision_date) as tenure_at_churn_decision
, ceil((tenure_at_churn_decision + 1)/30) as month_at_churn_decision
, g.merchant_sub_segment as merchant_sub_segment_at_churn_decision
, sum(volume_count) / 91 *365 as annualized_pmt_at_churn_decision
from cohort_2019 c
left join first_churns f on c.merchant_token = f.merchant_token
left join app_bi.app_bi_dw.dim_merchant_gpv_segment g on g.currency_code = 'USD' and g.merchant_token = c.merchant_token and dateadd('day', -91, first_churn_date) between g.effective_begin and g.effective_end
left join app_bi.app_bi_dw.vfact_merchant_revenue_summary m on m.merchant_token = c.merchant_token and m.product_category = 'Processing' and report_date between dateadd('day',-91,dateadd('day', -91, first_churn_date)) and dateadd('day', -91, first_churn_date)
group by 1,2,3,4,5,6,7,8,9,10,11
