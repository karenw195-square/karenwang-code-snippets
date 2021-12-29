create or replace table personal_karenwang.public.spine as
select DISTINCT du.user_token as merchant_token
, user_created_at as merchant_signup_at
, first_successful_activation_request_approved_at as merchant_activation_at
, coalesce(mls.first_card_payment_at, '9999-09-09 00:00:00') as first_card_payment_at
, coalesce(mls.latest_card_payment_at, '9999-09-09 00:00:00') as latest_card_payment_at
, ra.action_at as referred_at
, ra.referee_lifetime_gpv_net_usd
from app_bi.pentagon.dim_user du
left join app_payments.app_payments_analytics.referee_attributions ra on du.user_token = ra.referrer_best_available_merchant_token
left join app_bi.pentagon.aggregate_merchant_lifetime_summary mls on du.user_token = mls.best_available_merchant_token
where du.user_type = 'MERCHANT'
;
--check average referral rate & referral point 
select count(distinct merchant_token) as total
, count(distinct case when referred_at > '2019-01-01' then merchant_token else null end) / count(distinct merchant_token) as referral_rate_all
, count(distinct rt.best_available_merchant_token) / count(distinct merchant_token) as exposed_rate
, count(distinct case when channel in ('onboard_web','onboard_app') then rt.best_available_merchant_token else null end) / count(distinct merchant_token) as onboard_exposed_rate
, count(distinct case when channel in ('dashboard','app_android','app_ios') then rt.best_available_merchant_token else null end) / count(distinct merchant_token) as app_dash_exposed_rate
, count(distinct case when referred_at > action_at then rt.best_available_merchant_token else null end) / count(distinct rt.best_available_merchant_token) as referral_rate_exposed
, count(distinct case when channel in ('onboard_web','onboard_app') and referred_at > action_at then rt.best_available_merchant_token else null end) / count(distinct case when channel in ('onboard_web','onboard_app') then rt.best_available_merchant_token else null end) as referral_rate_exposed_onboard
, count(distinct case when channel in ('dashboard','app_android','app_ios') and referred_at > action_at then rt.best_available_merchant_token else null end) / count(distinct case when channel in ('dashboard','app_android','app_ios') then rt.best_available_merchant_token else null end) as referral_rate_exposed_app_dash
, count(distinct case when first_card_payment_at <> '9999-09-09 00:00:00' then merchant_token else null end) / count(distinct merchant_token ) as nna_rate
, count(distinct case when referred_at > first_card_payment_at and first_card_payment_at <> '9999-09-09 00:00:00' then merchant_token else null end) / count(distinct case when first_card_payment_at <> '9999-09-09 00:00:00' then merchant_token else null end) as referral_rate_nna
from personal_karenwang.public.spine s
left join app_payments.app_payments_analytics.referrer_touchpoints rt on s.merchant_token = rt.best_available_merchant_token 
where 1=1
and merchant_activation_at <> '9999-09-09 00:00:00'
;
--check referral quality at different point
with per_referral as (
  select merchant_token
, referred_at
, case when referred_at < first_card_payment_at then 'pre-NNA'
       when referred_at > first_card_payment_at then 'post-NNA'
  else null end referral_point
, referee_lifetime_gpv_net_usd
from personal_karenwang.public.spine s
where 1=1
and merchant_activation_at <> '9999-09-09 00:00:00'
and referred_at is not null
  )

select referral_point
, count(*) as count
, avg(coalesce(referee_lifetime_gpv_net_usd,0)) as per_referee_gpv
, count(referee_lifetime_gpv_net_usd) / count(*) as referee_nna_pct
, sum(coalesce(referee_lifetime_gpv_net_usd,0)) / count(distinct merchant_token) as avg_referee_gpv_per_referrer
, count(*) / count(distinct merchant_token) as avg_referees_per_referrer
from per_referral
group by 1
;

--build foundation tables for each moment

create or replace table personal_karenwang.public.referral_moment_community_all as 
select merchant_token
, created_at_utc as ts
, conversation_id as id
from app_support.app_support.seller_community_events
;

create or replace table personal_karenwang.public.referral_moment_community_op as 
with prep as (
select *, row_number() over (partition by conversation_id order by created_at_utc) as rn from app_support.app_support.seller_community_events
  where event_type in ('thread_authored','comment_authored','best_answer_authored','marked_best_answer')
)
select merchant_token
, created_at_utc as ts
, conversation_id as id
from prep
where rn = 1 and event_type in ('thread_authored','comment_authored')
;

create or replace table personal_karenwang.public.referral_moment_community_cmtr as 
with prep as (
select *, row_number() over (partition by conversation_id order by created_at_utc) as rn from app_support.app_support.seller_community_events
  where event_type in ('thread_authored','comment_authored','best_answer_authored','marked_best_answer')
)
select merchant_token
, created_at_utc as ts
, conversation_id as id
from prep
where rn > 1 and event_type in ('comment_authored')
;

create or replace table personal_karenwang.public.referral_moment_community_ba as 
with prep as (
select *, row_number() over (partition by conversation_id order by created_at_utc) as rn from app_support.app_support.seller_community_events
  where event_type in ('thread_authored','comment_authored','best_answer_authored','marked_best_answer')
)
select merchant_token
, min(created_at_utc) as ts
, conversation_id as id
from prep
where rn > 1 and event_type in ('best_answer_authored','marked_best_answer')
group by 1,3
;

create or replace table personal_karenwang.public.referral_moment_survey_onboarding as 
select contact_id as merchant_token
, to_timestamp(date||':00') as ts
, _row as id
from fivetran.app_payments.register_onboarding_survey_2020_q_4
where q_1 >= 6
;

create or replace table personal_karenwang.public.referral_moment_survey_brand_nps as 
select merchant_token as merchant_token
, to_timestamp(date) as ts
, _row as id
from fivetran.app_payments.nps_2020_q_4
where nps >= 9
;

create or replace table personal_karenwang.public.referral_moment_am_nps as 
select du.best_available_merchant_token as merchant_token
, response_date as ts
, REPLY_ID as id
from APP_MERCH_GROWTH.APP_MERCH_GROWTH_ETL.AM_FACT_CSAT sr 
LEFT JOIN app_merch_growth.app_merch_growth_etl.am_fact_opportunities op
ON sr.user_token = op.opportunity_id
LEFT JOIN app_merch_growth.app_merch_growth_etl.am_fact_activities fa
ON sr.user_token = fa.sfdc_task_id
left join app_bi.pentagon.dim_user du on du.user_token = COALESCE(op.merchant_token,fa.merchant_token,sr.user_token)
where nps >= 9
;

create or replace table personal_karenwang.public.referral_moment_cs_csat as
select du.best_available_merchant_token as merchant_token
, c.case_created_at as ts
, c.case_id as id
from app_support.app_support.cases c
left join APP_SUPPORT.APP_SUPPORT.SURVEY_RESULTS s on c.case_id = s.case_id
      and s.data_completeness_rank = 1
left join app_support.app_support.dim_queues q ON c.queue_id = q.queue_id
left join app_bi.pentagon.dim_user du on c.square_unit_token = du.user_token
where q.organization = 'Customer Success' 
and s.overall_experience_score >= 6
; 

create or replace table personal_karenwang.public.referral_moment_beta as
SELECT merchant_token
  , FEATURE_START_DATE as ts
  , merchant_token || b.BETA_FEATURE_NAME_ID as id
FROM app_support.scalable.fact_beta_token b
INNER JOIN app_support.scalable.dim_beta_programs p ON b.beta_feature_name_id = p.beta_feature_name_id
;

create or replace table personal_karenwang.public.referral_moment_android_prompt_click as
select du.best_available_merchant_token as merchant_token
, u_recorded_at as ts
, u_uuid as id
from EVENTSTREAM2.CATALOGS.mobile_click c
  left join app_bi.pentagon.dim_user du on c.subject_user_token = du.user_token
where mobile_click_description = 'Review Prompt: Rate Now'
;

create or replace table personal_karenwang.public.referral_moment_ios_prompt_show as
select subject_usertoken 
, RECORDED_AT as ts
, UUID as id
from eventstream1.events.all_events
where eventname = 'Action'
and eventvalue = 'In App Review Prompt: Requested Apple to Show Prompt'
and SOURCE_APPLICATION_TYPE = 'register-ios'
and RECORDED_DATE >= '2020-08-14'
;

create or replace table personal_karenwang.public.referral_moment_ios_prompt_show as
select du.best_available_merchant_token as merchant_token
, ts
, id
from personal_karenwang.public.referral_moment_ios_prompt_show e
left join app_bi.pentagon.dim_user du on e.subject_usertoken = du.user_token
;

create or replace table personal_karenwang.public.referral_moment_rev_growth as
with prep as (
select merchant_token
  , currency_code
  , effective_begin
  , effective_end
  , merchant_segment
  , lead(merchant_segment,1) over (partition by merchant_token, currency_code order by effective_begin) as next_gpv_segment
  from app_bi.app_bi_dw.dim_merchant_gpv_segment g
)
, grow_biz as (
select * 
, row_number() over (partition by merchant_token order by effective_begin) as rn
from prep
where merchant_segment = 'Micro' and NEXT_GPV_SEGMENT in ('SMB','Mid-Market','Enterprise')
  )
  
select merchant_token
, effective_begin as ts
, merchant_token || effective_begin as id
from grow_biz
where rn = 1
;

create or replace table personal_karenwang.public.cumulative_customers as
with prep as (
select s.merchant_token
, fpt.payment_token
, fpt.pan_fidelius_token
, payment_trx_recognized_at
, first_card_payment_at
, row_number() over (partition by s.merchant_token, payment_trx_recognized_date order by payment_trx_recognized_at desc) as rn_day
, row_number() over (partition by s.merchant_token, fpt.pan_fidelius_token order by payment_trx_recognized_at) as rn_pan
from personal_karenwang.public.spine s
JOIN app_bi.pentagon.dim_user du 
      ON du.best_available_merchant_token=s.merchant_token AND user_type='UNIT' 
    LEFT JOIN app_bi.pentagon_table.fact_payment_transactions_base fpt
      ON fpt.unit_token=du.user_token
     
where first_card_payment_at >= '2010-01-01'
AND fpt.payment_trx_recognized_date>='2010-01-01'
and merchant_activation_at <> '9999-09-09 00:00:00'
  and fpt.is_gpv = 1
)
, prep2 as (
select *
, sum(case when rn_pan = 1 then 1 else 0 end) over (partition by merchant_token order by payment_trx_recognized_at rows between unbounded preceding and current row) as cum_cnt_distinct
, count(*) over (partition by merchant_token order by payment_trx_recognized_at rows between unbounded preceding and current row) as cum_cnt
from prep
  )
  
select merchant_token
, payment_trx_recognized_at::date as date
, datediff('day',first_card_payment_at,payment_trx_recognized_at) as days_to_pmt
, cum_cnt_distinct as cum_customer
, cum_cnt as cum_pmt
from prep2
where rn_day = 1
;

create or replace table personal_karenwang.public.referral_moment_customer_growth as
with prep as (
  select merchant_token
, date
, row_number() over (partition by merchant_token order by date) as rn
from personal_karenwang.public.cumulative_customers
where CUM_CUSTOMER > 350
)
select merchant_token
, date as ts
, merchant_token || date as id
from prep
where rn = 1
;

create or replace table personal_karenwang.public.referral_moment_repeat_customer_growth as
with prep as (
  select merchant_token
, date
, row_number() over (partition by merchant_token order by date) as rn
from personal_karenwang.public.cumulative_customers
where (cum_pmt - cum_customer) / cum_pmt > 0.27
)
select merchant_token
, date as ts
, merchant_token || date as id
from prep
where rn = 1
;

create or replace table personal_karenwang.public.referral_moment_second_employee as
WITH base AS (
  SELECT *, row_number() over (PARTITION BY merchant_id ORDER BY created_at) AS rn 
  FROM roster.merchants.employees 
  WHERE person_id IS NOT null
)

SELECT merchant_id AS merchant_token
, MAX(CASE WHEN rn = 2 THEN created_at ELSE NULL END) AS ts
, merchant_id || ts as id
FROM base b
GROUP BY 1
;

create or replace table personal_karenwang.public.referral_moment_second_location as
WITH base AS (
  SELECT *, row_number() over (PARTITION BY merchant_id ORDER BY created_at) AS rn 
  FROM roster.merchants.locations 
  WHERE id IS NOT NULL
)

  SELECT merchant_id AS merchant_token
, MAX(CASE WHEN rn = 2 THEN created_at ELSE NULL END) AS ts
, merchant_id || ts as id
FROM base b
GROUP BY 1
;

create or replace table personal_karenwang.public.referral_moment_non_r4_hw as
WITH hardware AS
(
  SELECT
      su.merchant_token
    , hr.order_completed_date AS created_at
    , dp.product_name
  FROM app_bi.app_bi_dw.vfact_hardware_order_line_items hr
  JOIN solidshop.raw_oltp.sq_users su ON su.id = hr.user_id
  LEFT OUTER JOIN app_bi.app_bi_dw.dim_product dp ON hr.sku = dp.natural_key_product
  WHERE hr.order_state = 'complete'
  
  UNION ALL
  
  SELECT 
      merchant_token
    , touchpoint_at AS created_at
    , product_name
  FROM app_hardware.endpoints.merchant_touchpoints
)

  SELECT
    du.best_available_merchant_token as merchant_token
  , MIN(CASE WHEN hardware.product_name not ILIKE '%R4%' THEN hardware.created_at ELSE NULL END) AS ts
  , du.best_available_merchant_token || ts as id
FROM hardware
JOIN app_bi.pentagon.dim_user du
  ON hardware.merchant_token = du.user_token
GROUP BY du.best_available_merchant_token
;

create or replace table personal_karenwang.public.referral_moment_non_reg_attach as
with non_reg as (
select merchant_token
  , min(merchant_first_activity_date) as first_non_reg_date
from app_bi.app_bi_dw.dim_merchant_first_activity
where product_name not in ('Register POS', 'Register Terminal')
  group by 1
)

, reg as (
select merchant_token
  , min(merchant_first_activity_date) as first_reg_date
from app_bi.app_bi_dw.dim_merchant_first_activity
where product_name in ('Register POS', 'Register Terminal')
  group by 1
)

select reg.merchant_token
, first_non_reg_date as ts
, reg.merchant_token || first_non_reg_date as id
from reg
left join non_reg using (merchant_token)
where first_non_reg_date is not null
; 

create or replace table personal_karenwang.public.referral_moment_deposit as
with prep as (
  select du.best_available_merchant_token as merchant_token
, ATTEMPTED_AT as ts
, row_number() over (partition by du.best_available_merchant_token order by ATTEMPTED_AT) as rn
from app_payments.app_payments_analytics.fact_daily_deposits d
left join app_bi.pentagon.dim_user du on d.unit_token  = du.user_token
where is_successful = 1 and ATTEMPTED_AMOUNT > 100
  )
  
select merchant_token
, ts
, merchant_token || ts as id
from prep
where rn = 2
;
