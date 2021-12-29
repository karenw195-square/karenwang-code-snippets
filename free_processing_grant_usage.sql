with granted_amount as (
select date_trunc('month',balance_changed_at) as grant_month
, currency_code
, case when balance_change_type in ('Grant - Square Campaign','Grant - Partner Campaign', 'Grant - System Referral') then balance_change_type
       when balance_change_type = 'Grant - Manual Referral' and balance_product_tag in ('Account Management','Customer Support') then balance_product_tag
       when balance_change_type = 'Grant - Manual Referral' and balance_product_tag not in ('Account Management','Customer Support') then 'Customer Support'
       when balance_change_type in ('Grant - Other', 'Grant - Rebate Program') then balance_product_tag
  else null end category
, count(*) as granted_rewards
, sum(amount_base_unit) as granted_amount_base_unit
, avg(to_usd_ratio) as to_usd_ratio
from app_payments.app_payments_analytics.balance_change_mapping
where balance_changed_at > '2021-01-01'
and balance_change_action = 'Grant'
and balance_type = 'FreeProcessingBalance'
--and balance_change_type <> 'Grant - Rebate Program'
group by 1,2,3
  )
  
, used_amount as (
select date_trunc('month',bc1.balance_changed_at) as use_month
, bc1.currency_code
, case when bc2.balance_change_type in ('Grant - Square Campaign','Grant - Partner Campaign', 'Grant - System Referral') then bc2.balance_change_type
       when bc2.balance_change_type = 'Grant - Manual Referral' and bc2.balance_product_tag in ('Account Management','Customer Support') then bc2.balance_product_tag
       when bc2.balance_change_type = 'Grant - Manual Referral' and bc2.balance_product_tag not in ('Account Management','Customer Support') then 'Customer Support'
       when bc2.balance_change_type in ('Grant - Other', 'Grant - Rebate Program') then bc2.balance_product_tag
  else null end category
, count(case when bc1.balance_change_action = 'Use' then 1 else null end) as uses
, 0 - sum(bc1.amount_base_unit) as used_amount_amount_base_unit
, sum(bc1.contra_revenue_amount_cents) as contra_revenue_amount_cents
, avg(bc1.to_usd_ratio) as to_usd_ratio
from app_payments.app_payments_analytics.balance_change_mapping bc1
  left join app_payments.app_payments_analytics.balance_change_mapping bc2 on bc1.balance_id = bc2.balance_id 
where bc1.balance_changed_at > '2021-01-01'
and bc1.balance_change_action in ('Use','Refund','Undo Refund')
and bc2.balance_change_action = 'Grant'
and bc1.balance_type = 'FreeProcessingBalance'
--and bc2.balance_change_type <> 'Grant - Rebate Program'
group by 1,2,3
)

select coalesce(ga.grant_month,ua.use_month) as report_month
, coalesce(ga.currency_code, ua.currency_code) as currency_code
, coalesce(ga.category, ua.category) as category
, sum(ga.granted_rewards) as granted_rewards
, sum(uses) as uses
, sum(ga.granted_amount_base_unit) as granted_amount_base_unit
, sum(ga.granted_amount_base_unit * ga.to_usd_ratio) as granted_amount_usd
, sum(ua.used_amount_amount_base_unit) as used_amount_base_unit
, sum(ua.used_amount_amount_base_unit * ua.to_usd_ratio) as used_amount_usd
, sum(ua.contra_revenue_amount_cents) as contra_revenue_base_unit
, sum(ua.contra_revenue_amount_cents * ua.to_usd_ratio) as contra_revenue_usd
from granted_amount ga
full outer join used_amount ua on ga.grant_month = ua.use_month and ga.currency_code = ua.currency_code and ga.category = ua.category
group by 1,2,3
;
