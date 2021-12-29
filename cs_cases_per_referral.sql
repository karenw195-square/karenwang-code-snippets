with base as (
select ra.referrer_best_available_merchant_token, ra.referee_best_available_merchant_token
, count(distinct case when m1.issue_bucket = 'referrals' then c1.case_id else null end) as referrer_cases
, count(distinct case when m2.issue_bucket = 'referrals' then c2.case_id else null end) as referee_cases
from app_payments.app_payments_analytics.referee_attributions ra
left join app_support.app_support.cases c1 on ra.referrer_best_available_merchant_token = c1.square_merchant_token
left join app_support.app_support.case_macro_labels m1 on m1.case_id = c1.case_id 
left join app_support.app_support.cases c2 on ra.referee_best_available_merchant_token = c2.square_merchant_token
left join app_support.app_support.case_macro_labels m2 on m2.case_id = c2.case_id 
where ra.action_at > '2021-01-01'
group by 1,2
  )
  
select sum(referrer_cases + referee_cases), count(referrer_best_available_merchant_token || referee_best_available_merchant_token)
from base
