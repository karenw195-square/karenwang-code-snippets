/*
owner: karenwang
last updated: 2022-10-28
description: this is a quarterly refresh load to build the fact tables that power the acquisition and retention tiles in go/payprodsizing
*/

/*
table: app_payments.app_payments_analytics.autosizing_acquisition
description: this query builds a merchant level view for each funnel stage ("signup","activation","first_payment") within a pre-defined moving annual cohort (take the last day of the last complete quarter as report date, look back 91-456 days to get a full year cohort plus 3 months curing time for NVA). It appends a list of features for further segmentation before aggregating the total NVA for the impact estimate.

table columns:
    - merchant_token: merchants' unique identifiers
    - funnel_stage: including "signup","activation","first_payment" - each merchant might have multiple funnel stages depending on how far they've moved forward
    - business_category: the vertical each merchant self-reported during onboarding (e.g. professional_services, retail, etc.)
    - country_code: the country code each merchant self-reported during onboarding (e.g. US, AU, etc.)
    - npa_usd: first year net profit added (a mix of predicted and actual values, depending on if the merchant has been with Square for > 1 year)
    - nva_usd: first year net volume added (a mix of predicted and actual values, depending on if the merchant has been with Square for > 1 year)
    - source_application: the platforms a merchant created a Square account from (e.g. web, register-ios, etc.)
    - is_paid: binary indicator (0 or 1) of whether the merchant has any paid marketing touchpoint before ToF
    - is_organic: binary indicator (0 or 1) of whether the merchant has any organic marketing touchpoint before ToF
    - is_referral: binary indicator (0 or 1) of whether the merchant has any referral touchpoint before ToF
    - is_deactivated: binary indicator (0 or 1) of whether the merchant is currently deactivated (not allowed to process payments)
    - is_pmt: binary indicator (0 or 1) of whether the merchant has visited any of the /payments pages (including the child pages) before signing up
    - is_pmt_home: binary indicator (0 or 1) of whether the merchant has visited the /payments page before signing up
    - is_pmt_fee: binary indicator (0 or 1) of whether the merchant has visited the /payments/our-fees, or /payments/pricing, or /payments/fee-calculator page before signing up
    - bapi: best available product intent based on the merchant's onboarding responses
    - is_sales_closed: binary indicator (0 or 1) of whether the merchant is onboarded by a sales representative
    - is_am_managed: binary indicator (0 or 1) of whether the merchant is ever managed by an account manager
*/

create or replace table app_payments.app_payments_analytics.autosizing_acquisition as

with funnel_framework as (
select du.user_token as merchant_token
    , 'signup' as funnel_stage
    , du.business_category
    , du.receipt_country_code
    , du.npa_total / 100 as npa_usd
    , du.nva / 100 as nva_usd
    from app_bi.hexagon.vdim_user du
where du.user_type = 'MERCHANT'
    and du.user_created_at_date between dateadd('day',-456, dateadd('day',-1, date_trunc('quarter',current_date))) and dateadd('day',-91, dateadd('day',-1, date_trunc('quarter',current_date))) 
    -- use last day of last complete quarter as report date, trace back a year while leaving 3 months' curing time
    and du.receipt_country_code in ('US','CA','GB','AU','JP','IE','ES','FR')
    and du.source_user_type = 'SQUARE SELLER'  
    
union 

select du.user_token as merchant_token
    , 'activation' as funnel_stage
    , du.business_category 
    , du.receipt_country_code
    , du.npa_total / 100 as npa_usd
    , du.nva / 100 as nva_usd
    from app_bi.hexagon.vdim_user du
where du.user_type = 'MERCHANT'
    and du.first_successful_activation_request_created_at_date between dateadd('day',-456, dateadd('day',-1, date_trunc('quarter',current_date))) and dateadd('day',-91, dateadd('day',-1, date_trunc('quarter',current_date))) 
    -- use last day of last complete quarter as report date, trace back a year while leaving 3 months' curing time
    and du.receipt_country_code in ('US','CA','GB','AU','JP','IE','ES','FR')
    and du.source_user_type = 'SQUARE SELLER'  
    
union 

select du.user_token as merchant_token
    , 'first_payment' as funnel_stage
    , du.business_category
    , du.receipt_country_code
    , du.npa_total / 100 as npa_usd
    , du.nva / 100 as nva_usd
    from app_bi.hexagon.vdim_user du
    left join app_bi.hexagon.vagg_user_lifetime_summary uls on du.user_token = uls.user_token
where du.user_type = 'MERCHANT'
    and uls.first_card_payment_date between dateadd('day',-456, dateadd('day',-1, date_trunc('quarter',current_date))) and dateadd('day',-91, dateadd('day',-1, date_trunc('quarter',current_date))) 
    -- use last day of last complete quarter as report date, trace back a year while leaving 3 months' curing time
    and du.receipt_country_code in ('US','CA','GB','AU','JP','IE','ES','FR')
    and du.source_user_type = 'SQUARE SELLER'  
)

, platform as (
select best_available_merchant_token as merchant_token
    , case when source_application in ('desktop web','web') then 'web'
      when source_application in ('mobile web','mobile_web') then 'mobile_web'
      when source_application = 'onboard' and device_os in ('Windows',
                                                            'Mac OS',
                                                            'Desktop Browser',
                                                            'Linux',
                                                            'Ubuntu') then 'web'
      when source_application = 'onboard' and device_os is not null then 'mobile_web'
      when source_application = 'register-ios' then 'register-ios'
      when source_application = 'register-android' then 'register-android'
      when source_application = 'retail-pos-ios' then 'retail-ios'
      when source_application = 'restaurant-pos-ios' then 'restaurant-ios'
      when source_application = 'weebly-mobile-app' then 'weebly-app'
      when source_application = 'invoices-app-ios' then 'invoices-ios'
      when source_application = 'invoices-app-android' then 'invoices-android'
      when source_application = 'appointments-ios' then 'appointments-ios'
      when source_application = 'appointments-android' then 'appointments-android'
      when source_application = 'payroll' then 'payroll'
      else 'UNKNOWN' end as source_application
    from app_bi.pentagon.fact_signups
group by 1,2
)

, channel as (
select merchant_token
   , count(distinct case when channel in (
                           'online_native',
                           'online_video',
                           'streaming_video',
                           'online_display',
                           'paid_search_asa',
                           'online_affiliate',
                           'paid_search_brand',
                           'paid_search_nonbrand',
                           'mobile_uac',
                           'radio',
                           'drtv',
                           'paid_search_shopping',
                           'direct_mail',
                           'mobile_other',
                           'online_social',
                           'print') then merchant_token else null end) as is_paid
   , count(distinct case when channel in (
                           'organic_other',
                           'content',
                           'organic_search',
                           'organic_hard_connection') then merchant_token else null end) as is_organic
   , count(distinct case when channel in ('referral') then merchant_token else null end) as is_referral
   , count(distinct case when channel in ('deactivation') then merchant_token else null end) as is_deactivated
   from app_marketing.public.fact_unified_attribution_marketing
   where country_code in ('US','CA','GB','JP','AU','IE','FR','ES')
   and conversion_type = 'sq_tof'
   group by 1
)

, public_web as (
select best_available_merchant_token as merchant_token
    , count(distinct best_available_merchant_token) as is_pmt
    , count(distinct case when webpage_path ilike '%/payments' then best_available_merchant_token else null end) as is_pmt_home
    , count(distinct case when webpage_path ilike '%/payments/our-fees'
                            or webpage_path ilike '%/payments/pricing'
                            or webpage_path ilike '%/payments/fee-calculator'
                     then best_available_merchant_token else null end) as is_pmt_fee
    from app_mktg_web.attribution.public_web_pageview pw
where u_recorded_date >= '2018-01-01'
  and (webpage_path ilike '%/us/en/payments%' 
     or webpage_path ilike '%/us/es/payments%' 
     or webpage_path ilike '%/ca/en/payments%' 
     or webpage_path ilike '%/ca/fr/payments%' 
     or webpage_path ilike '%/gb/en/payments%' 
     or webpage_path ilike '%/au/en/payments%' 
     or webpage_path ilike '%/jp/ja/payments%'
     or webpage_path ilike '%/ie/en/payments%'
     or webpage_path ilike '%/fr/fr/payments%' 
     or webpage_path ilike '%/es/es/payments%')
  and pw.audience = 'Prospect'
group by 1
)

, bapi as (
select merchant_token
      , case when value in ('Ecommerce','eCommerce') then 'Ecommerce'
             when value in ('Restaurant POS','Restaurants') then 'Restaurants'
             when value in ('Retail POS','Retail') then 'Retail'
        else value end as bapi
    from precog.raw_oltp.signals s
where name in ('signup:best-available-product-intent-primary')
group by 1,2
)

, sales_closed as (
select du.merchant_token
    , count(distinct du.merchant_token) as is_sales_closed
    from app_sales.sales_comp.closed_deals cd
    join app_sales.app_sales_etl.sfdc_square_accounts sa
      on sa.sf_object_id = cd.opportunity_id
    join app_bi.hexagon.vdim_user du
      on du.user_token = sa.user_token
    where 1=1
      and cd.sales_segment in ('Sales','Strat', 'Enterprise', 'Field Sales')
    group by 1
)

, am_managed as (
select du.merchant_token
    , count(distinct du.merchant_token) as is_am_managed
    from app_merch_growth.app_merch_growth_etl.sbs_book_of_business bob
    join app_bi.hexagon.vdim_user du
      on bob.user_token = du.user_token
     and du.is_merchant = 1
where bob.program is not null
  and bob.treatment_control = 'Treatment'
  and bob.owner_name is not null
  and bob.owner_name != 'Jade Batstone'
  and bob.program not in ('MVS')
group by 1
)

select funnel_framework.merchant_token
, funnel_framework.funnel_stage
, funnel_framework.business_category
, funnel_framework.receipt_country_code as country_code
, coalesce(funnel_framework.npa_usd,0) as npa_usd
, coalesce(funnel_framework.nva_usd,0) as nva_usd
, coalesce(platform.source_application, 'UNKNOWN') as source_application
, coalesce(channel.is_paid,0) as is_paid
, coalesce(channel.is_organic,0) as is_organic
, coalesce(channel.is_referral,0) as is_referral
, coalesce(channel.is_deactivated,0) as is_deactivated
, coalesce(public_web.is_pmt,0) as is_pmt
, coalesce(public_web.is_pmt_home,0) as is_pmt_home
, coalesce(public_web.is_pmt_fee,0) as is_pmt_fee
, bapi.bapi
, coalesce(sales_closed.is_sales_closed,0) as is_sales_closed
, coalesce(am_managed.is_am_managed,0) as is_am_managed
from funnel_framework
left join platform on funnel_framework.merchant_token = platform.merchant_token
left join channel on funnel_framework.merchant_token = channel.merchant_token
left join public_web on funnel_framework.merchant_token = public_web.merchant_token
left join bapi on funnel_framework.merchant_token = bapi.merchant_token
left join sales_closed on funnel_framework.merchant_token = sales_closed.merchant_token
left join am_managed on funnel_framework.merchant_token = am_managed.merchant_token
;

/*
table: app_payments.app_payments_analytics.autosizing_retention
description: this query builds a merchant level view within a pre-defined moving annual cohort (take the last day of the last complete quarter as report date, look back 365 to 730 days - "prior phase" - to get a full year cohort of active sellers, and define their churn vs. retention based on their processing in the next 365 days - "current phase"). The GPV run rate per seller is defined as the highest trailing 91d GPV during the "prior phase". A list of features are also appended for further segmentation before aggregating the GPV run rate for impact estimate.

table columns:
    - merchant_token: merchants' unique identifiers
    - merchant_mcc: the vertical each merchant self-reported during onboarding (e.g. professional_services, retail, etc.)
    - merchant_country: the country code each merchant self-reported during onboarding (e.g. US, AU, etc.)
    - gpv_91d_run_rate: highest trailing 91d GPV of each merchants during their prior phase
    - is_retained: binary indicator (0 or 1) of whether a merchant has any processing during their current phase
    - gpv_segment: highest GPV segment during each merchant's prior phase
    - tenure_months: the rounded-up months since first payment date of each merchant by the end of the prior phase
    - tenure_years: the rounded-up years since first payment date of each merchant by the end of the prior phase
    - has_deact_units: binary indicator (0 or 1) of whether a merchant currently has deactivated units
    - ticket_size: average sales volume per merchant during the prior phase
    - take_rate: the percentage of sales volume that goes to the fee charged by Square during the prior phase
    - cnp_pct: percentage of card not present GPV during the prior phase
    - term_pct: percentage of Register Terminal GPV among total GPV during the prior phase
    - spos_pct: percentage of Register POS GPV among total GPV during the prior phase
    - vt_pct: percentage of Virtual Terminal GPV among total GPV during the prior phase
    - inv_pct: percentage of Invoices GPV among total GPV during the prior phase
    - ecom_pct: percentage of eCommerce API GPV among total GPV during the prior phase
    - other_pct: percentage of other GPV (none of the 5 above) among total GPV during the prior phase
    - has_term_attach: binary indicator (0 or 1) of whether a merchant processed any Register Terminal GPV during the prior phase
    - has_spos_attach: binary indicator (0 or 1) of whether a merchant processed any Register POS GPV during the prior phase
    - has_vt_attach: binary indicator (0 or 1) of whether a merchant processed any Virtual Terminal GPV during the prior phase
    - has_inv_attach: binary indicator (0 or 1) of whether a merchant processed any Invoices GPV during the prior phase
    - has_ecom_attach: binary indicator (0 or 1) of whether a merchant processed any eCommerce API GPV during the prior phase
    - has_other_attach: binary indicator (0 or 1) of whether a merchant processed any other GPV during the prior phase
    - has_risk_case: binary indicator (0 or 1) of whether a merchant has any risk case during the prior phase
    - has_credit_risk_case: binary indicator (0 or 1) of whether a merchant has any credit risk case during the prior phase
    - has_compliance_case: binary indicator (0 or 1) of whether a merchant has any compliance case during the prior phase
    - has_fraud_case: binary indicator (0 or 1) of whether a merchant has any fraud case during the prior phase
    - has_disputes: binary indicator (0 or 1) of whether a merchant has any dispute during the prior phase
    - has_reward_grant: binary indicator (0 or 1) of whether a merchant was granted any reward during the prior phase
    - has_referral_reward_grant: binary indicator (0 or 1) of whether a merchant was granted any reward from referrals during the prior phase
    - has_campaign_reward_grant: binary indicator (0 or 1) of whether a merchant was granted any reward from Square campaigns during the prior phase
    - has_partner_reward_grant: binary indicator (0 or 1) of whether a merchant was granted any reward from partner campaigns during the prior phase
    - has_other_reward_grant: binary indicator (0 or 1) of whether a merchant was granted any other reward during the prior phase
    - is_custom_priced: binary indicator (0 or 1) of whether a merchant was on custom price during the prior phase
*/
create or replace table app_payments.app_payments_analytics.autosizing_retention as

with funnel_framework as (
select merchant_token
    , merchant_mcc
    , merchant_country
    , max(gpv_net_var_usd_91d_52w) as gpv_91d_run_rate
    -- use the highest 91d trailing GPV during the "prior phase" to define each seller's GPV run rate (91d)
    , case when max(gpv_net_var_usd_91d) > 0 then 1 else 0 end is_retained
    -- use the highest 91d trailing GPV during the "current phase" to define if the seller is active at all during the "current phase"
    , max(case when gpv_segment_52_weeks_ago = 'Micro' then '[1] Micro'
               when gpv_segment_52_weeks_ago = 'SMB' then '[2] SMB'
               when gpv_segment_52_weeks_ago = 'Mid-Market' then '[3] Mid-Market'
               when gpv_segment_52_weeks_ago = 'Enterprise' then '[4] Enterprise'
          else '[0] Inactive'end) as gpv_segment
    from app_bi.hexagon.merchant_mature_cohort
where product_name = 'ALL'
    -- include all processing activities
    and report_date between dateadd('day',-364, dateadd('day',-1, date_trunc('quarter',current_date))) and dateadd('day',-1, date_trunc('quarter',current_date))
    -- report date is the last day of the last complete quarter
    -- filter on the annual cohort trailing from the report date; this will be framed as "current phase"
    and gpv_net_var_usd_91d_52w > 0
    -- filter on the sellers with GPV 52w prior to the "current phase" - indicating activeness in the "prior phase"
    group by 1,2,3
)

, tenure as (
select funnel_framework.merchant_token
    , ceil((datediff('day',uls.first_card_payment_date,dateadd('day',-365, dateadd('day',-1, date_trunc('quarter',current_date))))+1) / 30) as tenure_months
    , ceil((datediff('day',uls.first_card_payment_date,dateadd('day',-365, dateadd('day',-1, date_trunc('quarter',current_date))))+1) / 365) as tenure_years
    -- use the last day of the "prior phase" to calculate tenure, because all "active sellers" would already processed at least a payment at that point.
    from funnel_framework
    left join app_bi.hexagon.vagg_user_lifetime_summary uls on funnel_framework.merchant_token = uls.user_token and uls.user_type = 'MERCHANT'
)

, payments as (
select funnel_framework.merchant_token
    , max(du.is_currently_deactivated) as has_deact_units
    , sum(amount_base_unit_usd / 100) as gpv
    , count(distinct payment_token) as trx
    , sum(fee_amount_base_unit_usd / 100) as fee
    , sum(case when product_name = 'Register Terminal' then amount_base_unit_usd / 100 else 0 end) as term_gpv
    , sum(case when product_name = 'Register POS' then amount_base_unit_usd / 100 else 0 end) as spos_gpv
    , sum(case when product_name = 'Virtual Terminal' then amount_base_unit_usd / 100 else 0 end) as vt_gpv
    , sum(case when product_name = 'Invoices' then amount_base_unit_usd / 100 else 0 end) as inv_gpv
    , sum(case when product_name = 'eCommerce API' then amount_base_unit_usd / 100 else 0 end) as ecom_gpv
    , sum(case when product_name not in ('Register Terminal','Register POS','Virtual Terminal','Invoices','eCommerce API') then amount_base_unit_usd / 100 else 0 end) as other_gpv
    , sum(case when is_card_present = 0 then amount_base_unit_usd / 100 else 0 end) as cnp_gpv
    from app_bi.hexagon.vfact_payment_transactions fpt
    inner join app_bi.hexagon.vdim_user du on fpt.unit_token = du.user_token
    inner join funnel_framework on du.merchant_token = funnel_framework.merchant_token
where fpt.is_gpv = 1
    and fpt.payment_trx_recognized_date between dateadd('day',-820, dateadd('day',-1, date_trunc('quarter',current_date))) and dateadd('day',-365, dateadd('day',-1, date_trunc('quarter',current_date)))
    -- filter on the "prior phase" plus 91d trailing window
    group by 1
)

, risk_review as (
select du.merchant_token 
    , count(distinct du.merchant_token) as has_risk_case
    , count(distinct case when "group" in ('credit_risk') then du.merchant_token else null end) as has_credit_risk_case
    , count(distinct case when "group" in ('compliance') then du.merchant_token else null end) as has_compliance_case
    , count(distinct case when case_type IN ('suspicion','tr_general','ato_alert','ato_escalation') then du.merchant_token else null end) as has_fraud_case
    from app_risk.app_risk.fact_risk_cases c 
    left join app_bi.hexagon.vdim_user du on du.user_token = c.user_token
    where c.created_at between dateadd('day',-820, dateadd('day',-1, date_trunc('quarter',current_date))) and dateadd('day',-365, dateadd('day',-1, date_trunc('quarter',current_date))) 
    -- filter on the "prior phase" plus 91d trailing window
    and ("group" in ('credit_risk','compliance') or case_type IN ('suspicion','tr_general','ato_alert','ato_escalation'))
    group by 1
)

, disputes as (
select du.merchant_token
    , count(distinct du.merchant_token) as has_disputes
    FROM app_risk.app_risk.chargebacks c 
    left join app_bi.hexagon.vdim_user du on du.user_token = c.user_token
    where c.chargeback_date between dateadd('day',-820, dateadd('day',-1, date_trunc('quarter',current_date))) and dateadd('day',-365, dateadd('day',-1, date_trunc('quarter',current_date))) 
    -- filter on the "prior phase" plus 91d trailing window
    group by 1
)

, rewards_exposure as (
select merchant_token
    , count(distinct merchant_token) as has_reward_grant
    , count(distinct case when balance_change_type in ('Grant - System Referral','Grant - Manual Referral') then merchant_token else null end) as has_referral_reward_grant
    , count(distinct case when balance_change_type = 'Grant - Square Campaign' then merchant_token else null end) as has_campaign_reward_grant
    , count(distinct case when balance_change_type = 'Grant - Partner Campaign' then merchant_token else null end) as has_partner_reward_grant
    , count(distinct case when balance_change_type = 'Grant - Other' then merchant_token else null end) as has_other_reward_grant
    from app_payments.app_payments_analytics.balance_change_mapping
    where balance_changed_at::date between dateadd('day',-820, dateadd('day',-1, date_trunc('quarter',current_date))) and dateadd('day',-365, dateadd('day',-1, date_trunc('quarter',current_date)))
    -- filter on the "prior phase" plus 91d trailing window
    and balance_change_action = 'Grant'
    group by 1
)

, custom_pricing as (
select b.merchant_token
    , count(distinct b.merchant_token) as is_custom_priced
    from payments_dw.public.payment_revenue_fees a 
    inner join app_bi.hexagon.vdim_user b on a.unit_token = b.user_token 
    where a.fee_type = 'CAPTURE' 
       and a.feeplan_id ilike 'CUSTOM_%'-- not in any sort of custom plan 
       and fee_created_at::date between dateadd('day',-820, dateadd('day',-1, date_trunc('quarter',current_date))) and dateadd('day',-365, dateadd('day',-1, date_trunc('quarter',current_date)))
    -- filter on the "prior phase" plus 91d trailing window
    group by 1
)

select funnel_framework.merchant_token
    , funnel_framework.merchant_mcc
    , funnel_framework.merchant_country
    , funnel_framework.gpv_91d_run_rate
    , funnel_framework.is_retained
    , funnel_framework.gpv_segment
    , tenure.tenure_months
    , tenure.tenure_years
    , payments.has_deact_units
    , payments.gpv / nullif(payments.trx,0) as ticket_size
    , payments.fee / nullif(payments.gpv,0) as take_rate
    , payments.cnp_gpv / nullif(payments.gpv,0) as cnp_pct
    , payments.term_gpv / nullif(payments.gpv,0) as term_pct
    , payments.spos_gpv / nullif(payments.gpv,0) as spos_pct
    , payments.vt_gpv / nullif(payments.gpv,0) as vt_pct
    , payments.inv_gpv / nullif(payments.gpv,0) as inv_pct
    , payments.ecom_gpv / nullif(payments.gpv,0) as ecom_pct
    , payments.other_gpv / nullif(payments.gpv,0) as other_pct
    , case when payments.term_gpv > 0 then 1 else 0 end has_term_attach
    , case when payments.spos_gpv > 0 then 1 else 0 end has_spos_attach
    , case when payments.vt_gpv > 0 then 1 else 0 end has_vt_attach
    , case when payments.inv_gpv > 0 then 1 else 0 end has_inv_attach
    , case when payments.ecom_gpv > 0 then 1 else 0 end has_ecom_attach
    , case when payments.other_gpv > 0 then 1 else 0 end has_other_attach
    , coalesce(risk_review.has_risk_case,0) as has_risk_case
    , coalesce(risk_review.has_credit_risk_case,0) as has_credit_risk_case
    , coalesce(risk_review.has_compliance_case,0) as has_compliance_case
    , coalesce(risk_review.has_fraud_case,0) as has_fraud_case
    , coalesce(disputes.has_disputes,0) as has_disputes
    , coalesce(rewards_exposure.has_reward_grant,0) as has_reward_grant
    , coalesce(rewards_exposure.has_referral_reward_grant,0) as has_referral_reward_grant
    , coalesce(rewards_exposure.has_campaign_reward_grant,0) as has_campaign_reward_grant
    , coalesce(rewards_exposure.has_partner_reward_grant,0) as has_partner_reward_grant
    , coalesce(rewards_exposure.has_other_reward_grant,0) as has_other_reward_grant
    , coalesce(custom_pricing.is_custom_priced,0) as is_custom_priced
from funnel_framework
left join tenure on funnel_framework.merchant_token = tenure.merchant_token
left join payments on funnel_framework.merchant_token = payments.merchant_token
left join risk_review on funnel_framework.merchant_token = risk_review.merchant_token
left join disputes on funnel_framework.merchant_token = disputes.merchant_token
left join rewards_exposure on funnel_framework.merchant_token = rewards_exposure.merchant_token
left join custom_pricing on funnel_framework.merchant_token = custom_pricing.merchant_token
;
