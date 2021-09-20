--timestamped on 9/20/2021
create or replace table personal_karenwang.public.custom_amount_sellers as 
select distinct du.best_available_merchant_token
from app_bi.pentagon.fact_payment_transactions fpt
  left join app_bi.pentagon.dim_user du
  on fpt.unit_token = du.user_token
where fpt.product_name in ('Register Terminal', 'Register POS')
  and is_itemized = 0
  and amount_base_unit > 100
;
create or replace table personal_karenwang.public.custom_amount_sellers_91d_processing as 
select c.best_available_merchant_token
, sum(amount_base_unit_usd) as gpv_usd_91d
, sum(case when product_name in ('Register Terminal', 'Register POS') and is_itemized = 0 then amount_base_unit_usd else 0 end) as gpv_cust_usd_91d
, sum(case when product_name in ('Register Terminal', 'Register POS') and is_itemized = 1 then amount_base_unit_usd else 0 end) as gpv_item_usd_91d
, sum(case when product_name in ('Invoices') then amount_base_unit_usd else 0 end) as gpv_inv_usd_91d
, sum(case when product_name in ('Virtual Terminal') then amount_base_unit_usd else 0 end) as gpv_vt_usd_91d
, sum(case when product_name in ('eCommerce API') then amount_base_unit_usd else 0 end) as gpv_ecom_usd_91d
, sum(case when is_card_present = 1 then amount_base_unit_usd else 0 end) as gpv_cp_usd_91d
, sum(case when is_card_present = 1 and fpt.reader_type='R12' then amount_base_unit_usd else 0 end) as gpv_r12_usd_91d
, sum(case when is_card_present = 1 and fpt.reader_type='T2' then amount_base_unit_usd else 0 end) as gpv_t2_usd_91d
, sum(case when is_card_present = 1 and fpt.reader_type='X2' then amount_base_unit_usd else 0 end) as gpv_x2_usd_91d
, sum(case when is_card_present = 1 and fpt.reader_type='S1' then amount_base_unit_usd else 0 end) as gpv_s1_usd_91d
, sum(case when is_card_present = 1 and fpt.reader_type='M1' then amount_base_unit_usd else 0 end) as gpv_r4_usd_91d
, sum(case when is_card_present = 1 and fpt.reader_type='R41' then amount_base_unit_usd else 0 end) as gpv_r41_usd_91d
, sum(case when is_card_present = 1 and fpt.reader_type='R6' then amount_base_unit_usd else 0 end) as gpv_r6_usd_91d
, avg(amount_base_unit_usd) as ticket_size_91d
from app_bi.pentagon.fact_payment_transactions fpt
  left join app_bi.pentagon.dim_user du
  on fpt.unit_token = du.user_token
  inner join personal_karenwang.public.custom_amount_sellers c
  on du.best_available_merchant_token = c.best_available_merchant_token
where fpt.is_gpv = 1  
and fpt.payment_trx_recognized_date > current_date - 91
group by 1
;

create or replace table personal_karenwang.public.custom_amount_sellers_91d_52w_processing as 
select c.best_available_merchant_token
, sum(amount_base_unit_usd) as gpv_usd_91d_52w
, sum(case when product_name in ('Register Terminal', 'Register POS') and is_itemized = 0 then amount_base_unit_usd else 0 end) as gpv_cust_usd_91d_52w
, sum(case when product_name in ('Register Terminal', 'Register POS') and is_itemized = 1 then amount_base_unit_usd else 0 end) as gpv_item_usd_91d_52w
, sum(case when product_name in ('Invoices') then amount_base_unit_usd else 0 end) as gpv_inv_usd_91d_52w
, sum(case when product_name in ('Virtual Terminal') then amount_base_unit_usd else 0 end) as gpv_vt_usd_91d_52w
, sum(case when product_name in ('eCommerce API') then amount_base_unit_usd else 0 end) as gpv_ecom_usd_91d_52w
, sum(case when is_card_present = 1 then amount_base_unit_usd else 0 end) as gpv_cp_usd_91d_52w
, avg(amount_base_unit_usd) as ticket_size_91d_52w
from app_bi.pentagon.fact_payment_transactions fpt
  left join app_bi.pentagon.dim_user du
  on fpt.unit_token = du.user_token
  inner join personal_karenwang.public.custom_amount_sellers c
  on du.best_available_merchant_token = c.best_available_merchant_token
where fpt.is_gpv = 1  
and fpt.payment_trx_recognized_date between current_date - 91 - 365 and current_date - 365
group by 1
;

create or replace table personal_karenwang.public.custom_amount_sellers_processing as 
select p1.best_available_merchant_token
, p1.gpv_usd_91d
, p1.gpv_cust_usd_91d
, p1.gpv_item_usd_91d
, p1.gpv_inv_usd_91d
, p1.gpv_vt_usd_91d
, p1.gpv_ecom_usd_91d
, p1.gpv_cp_usd_91d
, p1.gpv_r4_usd_91d
, p1.gpv_r41_usd_91d
, p1.gpv_r6_usd_91d
, p1.gpv_r12_usd_91d
, p1.gpv_t2_usd_91d
, p1.gpv_x2_usd_91d
, p1.gpv_s1_usd_91d
, p1.ticket_size_91d
, p2.gpv_usd_91d_52w
, p2.gpv_cust_usd_91d_52w
, p2.gpv_item_usd_91d_52w
, p2.gpv_inv_usd_91d_52w
, p2.gpv_vt_usd_91d_52w
, p2.gpv_ecom_usd_91d_52w
, p2.gpv_cp_usd_91d_52w
, p2.ticket_size_91d_52w
from personal_karenwang.public.custom_amount_sellers_91d_processing p1
left join personal_karenwang.public.custom_amount_sellers_91d_52w_processing p2 on p1.best_available_merchant_token = p2.best_available_merchant_token
;
create or replace table personal_karenwang.public.custom_amount_sellers_features as 
select c.best_available_merchant_token
, mls.business_category
, mls.first_card_payment_date
, mls.latest_card_payment_date
, case when r.best_available_merchant_token is not null then 1 else 0 end is_risk_account
, coalesce(p.gpv_usd_91d/100, 0) as gpv_usd_91d
, coalesce(p.gpv_cust_usd_91d/100, 0) as gpv_cust_usd_91d
, coalesce(p.gpv_item_usd_91d/100, 0) as gpv_item_usd_91d
, coalesce(p.gpv_inv_usd_91d/100, 0) as gpv_inv_usd_91d
, coalesce(p.gpv_vt_usd_91d/100, 0) as gpv_vt_usd_91d
, coalesce(p.gpv_ecom_usd_91d/100, 0) as gpv_ecom_usd_91d
, coalesce(p.gpv_cp_usd_91d/100, 0) as gpv_cp_usd_91d
, coalesce(p.gpv_r4_usd_91d/100, 0) as gpv_r4_usd_91d
, coalesce(p.gpv_r41_usd_91d/100, 0) as gpv_r41_usd_91d
, coalesce(p.gpv_r6_usd_91d/100, 0) as gpv_r6_usd_91d
, coalesce(p.gpv_r12_usd_91d/100, 0) as gpv_r12_usd_91d
, coalesce(p.gpv_t2_usd_91d/100, 0) as gpv_t2_usd_91d
, coalesce(p.gpv_x2_usd_91d/100, 0) as gpv_x2_usd_91d
, coalesce(p.gpv_s1_usd_91d/100, 0) as gpv_s1_usd_91d
, p.ticket_size_91d / 100 as ticket_size_91d
, coalesce(p.gpv_usd_91d_52w/100, 0) as gpv_usd_91d_52w
, coalesce(p.gpv_cust_usd_91d_52w/100, 0) as gpv_cust_usd_91d_52w
, coalesce(p.gpv_item_usd_91d_52w/100, 0) as gpv_item_usd_91d_52w
, coalesce(p.gpv_inv_usd_91d_52w/100, 0) as gpv_inv_usd_91d_52w
, coalesce(p.gpv_vt_usd_91d_52w/100, 0) as gpv_vt_usd_91d_52w
, coalesce(p.gpv_ecom_usd_91d_52w/100, 0) as gpv_ecom_usd_91d_52w
, coalesce(p.gpv_cp_usd_91d_52w/100, 0) as gpv_cp_usd_91d_52w
, p.ticket_size_91d_52w / 100 as ticket_size_91d_52w
, count(distinct report_date) as active_days_365d
, avg(trx_count) as trx_per_active_day_356d
from personal_karenwang.public.custom_amount_sellers c
left join app_bi.pentagon.aggregate_merchant_lifetime_summary mls
     on c.best_available_merchant_token = mls.merchant_token
left join (select distinct best_available_merchant_token 
           , business_category
           from app_bi.pentagon.dim_user 
           where is_currently_frozen = 1 or is_currently_deactivated = 1) r 
     on mls.merchant_token = r.best_available_merchant_token
left join personal_karenwang.public.custom_amount_sellers_processing p
     on p.best_available_merchant_token = c.best_available_merchant_token
left join (select merchant_token
           , report_date
           , sum(volume_count) as trx_count
           from app_bi.app_bi_dw.vfact_merchant_revenue_summary
           where product_category = 'Processing'
           and report_date > current_date - 365
           group by 1,2) mrs 
     on mrs.merchant_token = mls.merchant_token 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28
;

create or replace table personal_karenwang.public.custom_amount_sellers_features_plus as 
with saas_capital as (
select merchant_token
  , max(case when product_category = 'SaaS' then 1 else 0 end) is_currently_saas
  , max(case when product_category = 'SaaS' and product_name ilike '%deposit%' then 1 else 0 end) is_currently_saas_deposit
  , max(case when product_category = 'SaaS' and product_name='Payroll' then 1 else 0 end) is_currently_saas_payroll
  , max(case when product_category = 'SaaS' and product_name='Square Online Store' then 1 else 0 end) is_currently_saas_sos
  , max(case when product_category = 'SaaS' and product_name='Appointments' then 1 else 0 end) is_currently_saas_appt
  , max(case when product_category = 'SaaS' and product_name='Marketing' then 1 else 0 end) is_currently_saas_mkt
  , max(case when product_category = 'SaaS' and product_name='Loyalty' then 1 else 0 end) is_currently_saas_lyt
  , max(case when product_category = 'SaaS' and product_name='Team Management' then 1 else 0 end) is_currently_saas_tm
  , max(case when product_category = 'SaaS' and product_name='Gift Cards' then 1 else 0 end) is_currently_saas_gc
  , max(case when product_category = 'SaaS' and product_name='Square for Retail' then 1 else 0 end) is_currently_saas_retail
  , max(case when product_category = 'SaaS' and product_name='Square for Restaurants' then 1 else 0 end) is_currently_saas_restaurant
  , max(case when product_category = 'Capital' then 1 else 0 end) is_currently_capital
from app_bi.app_bi_dw.dim_merchant_product_activity
  where effective_end = '9999-12-31'
group by 1
)

, employee as (
select merchant_id as merchant_token 
  , count(distinct person_id) AS employees
from roster.merchants.employees
where is_active = TRUE 
group by 1
)

, location as (
select merchant_id as merchant_token 
  , count(distinct id) AS locations
from roster.merchants.locations
where is_active = TRUE 
group by 1
)

, churn as (
select merchant_token
, effective_begin
, row_number() over (partition by merchant_token order by effective_begin desc) as rn
, lag(effective_begin, 1) over (partition by merchant_token order by effective_begin) as last_effective_begin
from app_bi.app_bi_dw.dim_merchant_gpv_segment
where merchant_segment = 'Inactive'
)

select f.*
  , s.is_currently_saas
  , s.is_currently_saas_deposit
  , s.is_currently_saas_payroll
  , s.is_currently_saas_sos
  , s.is_currently_saas_appt
  , s.is_currently_saas_mkt
  , s.is_currently_saas_lyt
  , s.is_currently_saas_tm
  , s.is_currently_saas_gc
  , s.is_currently_saas_retail
  , s.is_currently_saas_restaurant
  , s.is_currently_capital
  , e.employees
  , l.locations
  , af.is_business_seller
, max(g.effective_begin) as last_churn_date
, max(g.last_effective_begin) as second_last_churn_date
from personal_karenwang.public.custom_amount_sellers_features f
left join saas_capital s on f.best_available_merchant_token = s.merchant_token
left join employee e on f.best_available_merchant_token = e.merchant_token
left join location l on f.best_available_merchant_token = l.merchant_token
left join churn g on f.best_available_merchant_token = g.merchant_token
left join app_payments.app_payments_analytics.fact_activations_funnel af on f.best_available_merchant_token = af.best_available_merchant_token
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45
;


create or replace table app_payments.app_payments_analyticstemp.segmentation_framework_all_custom_amount_merchants as
select fpc.*
, gpv.merchant_sub_segment
, case when business_category in ('retail','food_and_drink','beauty_and_personal_care') then 1 else 0 end is_inperson_mcc
, case when business_category in ('home_and_repair','professional_services') then 1 else 0 end is_remote_mcc
, case when FIRST_CARD_PAYMENT_DATE > current_date - 456 or GPV_USD_91D <= 0 then null else GPV_CUST_USD_91D/GPV_USD_91D end as cust_rate
, case when FIRST_CARD_PAYMENT_DATE > current_date - 456 or GPV_USD_91D <= 0 then null else GPV_CP_USD_91D/GPV_USD_91D end as cp_rate
, case when FIRST_CARD_PAYMENT_DATE > current_date - 456 and GPV_USD_91D > 0 then 'new active' 
            when GPV_USD_91D <= 0 and IS_RISK_ACCOUNT = 1 then 'risk churn'
            when GPV_USD_91D <= 0 and IS_RISK_ACCOUNT = 0 and datediff('day',FIRST_CARD_PAYMENT_DATE, LATEST_CARD_PAYMENT_DATE) <= 7 then 'trialist churn'
            when GPV_USD_91D <= 0 and IS_RISK_ACCOUNT = 0 then 'other churn'
            when GPV_USD_91D > 0 then 'mature active'
       end seller_active_status
, case when GPV_USD_91D <= 0 then null
       when GPV_CUST_USD_91D = GPV_USD_91D then 'pure terminal'
       when GPV_CUST_USD_91D = 0 then 'pure non-terminal'
       when GPV_CUST_USD_91D / GPV_USD_91D >= 0.5 then 'mostly terminal'
       when GPV_CUST_USD_91D / GPV_USD_91D < 0.5 then 'mostly non-terminal'
       end product_mix
, case when FIRST_CARD_PAYMENT_DATE > current_date - 456 or GPV_USD_91D <= 0 then null
       when GPV_CP_USD_91D <= 0 then 'pure cnp'
       when GPV_CP_USD_91D = GPV_USD_91D then 'pure cp'
       when GPV_CP_USD_91D > 0 then 'hybrid'
       end card_presence_mix 
, case when product_mix = 'pure terminal' then 1 else 0 end is_pure_term
, case when product_mix = 'mostly terminal' then 1 else 0 end is_most_term
, case when product_mix = 'mostly non-terminal' then 1 else 0 end is_most_nonterm
, case when product_mix = 'pure non-terminal' then 1 else 0 end is_pure_nonterm
, case when card_presence_mix = 'pure cp' then 1 else 0 end is_pure_cp
, case when card_presence_mix = 'hybrid' then 1 else 0 end is_hybrid
, case when card_presence_mix = 'pure cnp' then 1 else 0 end is_pure_cnp
, gpv_usd_91d /nullif(gpv_usd_91d_52w,0) - 1 as gpv_usd_91d_yoy
, gpv_cust_usd_91d /nullif(gpv_cust_usd_91d_52w,0) - 1 as gpv_cust_usd_91d_yoy
from personal_karenwang.public.custom_amount_sellers_features_plus fpc
left join app_bi.app_bi_dw.dim_merchant_gpv_segment gpv on fpc.best_available_merchant_token = gpv.merchant_token
     and current_date between effective_begin and effective_end
;

create or replace table app_payments.app_payments_analyticstemp.segmentation_framework_label as
select m.best_available_merchant_token
, case when seller_active_status = 'mature active' 
            and product_mix in ('pure terminal','mostly terminal')
            and merchant_sub_segment  in ('SMB','Mid-Market','Enterprise')
       then 'High Value'
       when seller_active_status = 'mature active' 
            and product_mix in ('pure terminal','mostly terminal')
            and merchant_sub_segment not in ('SMB','Mid-Market','Enterprise')
            and (gpv_usd_91d_yoy >= 0 or gpv_usd_91d_yoy is null)
       then 'High Growth'
       when seller_active_status = 'new active' 
            and product_mix in ('pure terminal','mostly terminal')
       then 'New Active'
       when seller_active_status in ('new active' ,'mature active' )
            and product_mix not in ('pure terminal','mostly terminal')
       then 'Graduate Active'
       when seller_active_status = 'mature active' 
            and merchant_sub_segment not in ('SMB','Mid-Market','Enterprise')
            and gpv_usd_91d_yoy < 0
       then 'Diminishing Active'
       when seller_active_status ilike '%churn%' 
       then 'Churn'
   end segment_label
, m.business_category
, du.business_type
, du.country_code 
, du.preferred_language_code
, first_card_payment_date
, f.source_application
, case when is_business_seller = TRUE then 1 when is_business_seller = FALSE then 0 end is_business_seller
, case when employees > 1 then 1 else 0 end is_multi_employees
, case when locations > 1 then 1 else 0 end is_multi_locations
, case when gpv_inv_usd_91d > 0 then 1 else 0 end is_inv_91d
, case when gpv_vt_usd_91d > 0 then 1 else 0 end is_vt_91d
, case when gpv_item_usd_91d > 0 then 1 else 0 end is_item_91d
, case when gpv_ecom_usd_91d > 0 then 1 else 0 end is_ecom_91d
, gpv_cp_usd_91d / nullif(gpv_usd_91d,0) as cp_rate
, case when gpv_r12_usd_91d > 0 then 1 else 0 end is_r12_91d
, case when gpv_t2_usd_91d > 0 then 1 else 0 end is_t2_91d
, case when gpv_x2_usd_91d > 0 then 1 else 0 end is_x2_91d
, ticket_size_91d
, active_days_365d
, trx_per_active_day_356d
, is_currently_saas
, is_currently_saas_deposit
, is_currently_saas_payroll
, is_currently_saas_appt
, is_currently_saas_sos
, is_currently_saas_mkt
, is_currently_saas_lyt
, is_currently_saas_gc
, is_currently_saas_tm
, is_currently_saas_retail
, is_currently_saas_restaurant
, is_currently_capital
, is_risk_account
--, u.channel
--, u.campaign_initiative
--, u.frac_attribution
from app_payments.app_payments_analyticstemp.segmentation_framework_all_custom_amount_merchants m
left join app_bi.pentagon.fact_signups f on m.best_available_merchant_token = f.best_available_merchant_token
left join app_bi.pentagon.dim_user du on  m.best_available_merchant_token = du.user_token and du.user_type = 'MERCHANT'
--left join app_marketing.public.fact_unified_attribution_marketing u on u.merchant_token = m.best_available_merchant_token and u.conversion_type = 'sq_tof'
;
