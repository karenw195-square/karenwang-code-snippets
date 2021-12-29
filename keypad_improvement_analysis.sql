create or replace table personal_karenwang.public.spos_keypad_parsing as
select SUBJECT_USER_TOKEN
, SUBJECT_MERCHANT_TOKEN
, U_RECORDED_AT
, MOBILE_CLICK_DESCRIPTION 
, case when MOBILE_CLICK_DESCRIPTION like 'SPOS Keypad: Tap Charge Button%' then 'SPOS Keypad: Tap Charge Button'
       when MOBILE_CLICK_DESCRIPTION like 'SPOS Keypad: Tap Show Cart%' then 'SPOS Keypad: Tap Show Cart'
       when MOBILE_CLICK_DESCRIPTION like 'SPOS Keypad: Add Custom Amount to Cart%' then 'SPOS Keypad: Add Custom Amount to Cart'
       when MOBILE_CLICK_DESCRIPTION like 'SPOS Keypad: Clear Single Amount%' then 'SPOS Keypad: Clear Single Amount'
       when MOBILE_CLICK_DESCRIPTION like 'SPOS Keypad: Clear Cart Confirm%' then 'SPOS Keypad: Clear Cart Confirm'
       when MOBILE_CLICK_DESCRIPTION like 'SPOS Cart: Clear Cart%' then 'SPOS Cart: Clear Cart'
       when MOBILE_CLICK_DESCRIPTION like 'SPOS Keypad: Tap Backspace%' then 'SPOS Cart: Tap Backspace'
       else MOBILE_CLICK_DESCRIPTION end grouped_event
, case when MOBILE_CLICK_DESCRIPTION like 'SPOS Keypad: Tap Backspace%' then null
       when MOBILE_CLICK_DESCRIPTION like 'SPOS Keypad: Add Custom Amount to Cart: (null)' then null
       when MOBILE_CLICK_DESCRIPTION not like 'SPOS Keypad: Tap Show Cart%' 
        and MOBILE_CLICK_DESCRIPTION not like 'SPOS Keypad: Clear Cart Confirm%' 
        and MOBILE_CLICK_DESCRIPTION not like 'SPOS Cart: Clear Cart%' 
       then cast(substr(replace(MOBILE_CLICK_DESCRIPTION,'SPOS Keypad: ',''), charindex(':',replace(MOBILE_CLICK_DESCRIPTION,'SPOS Keypad: ',''))+2, 100) as integer)
       else cast(replace(substr(replace(MOBILE_CLICK_DESCRIPTION,' ',''),charindex('total:',replace(MOBILE_CLICK_DESCRIPTION,' ',''))+6,100),'}','') as integer)
       end total_amount
, case when MOBILE_CLICK_DESCRIPTION like 'SPOS Keypad: Tap Backspace%' then null
       when MOBILE_CLICK_DESCRIPTION not like 'SPOS Keypad: Tap Show Cart%' 
        and MOBILE_CLICK_DESCRIPTION not like 'SPOS Keypad: Clear Cart Confirm%' 
        and MOBILE_CLICK_DESCRIPTION not like 'SPOS Cart: Clear Cart%'
       then null 
       else cast(substr(replace(MOBILE_CLICK_DESCRIPTION,' ','')
                          ,charindex('custom:',replace(MOBILE_CLICK_DESCRIPTION,' ',''))+7
                          ,charindex('items:',replace(MOBILE_CLICK_DESCRIPTION,' ',''))-charindex('custom:',replace(MOBILE_CLICK_DESCRIPTION,' ',''))-8) 
                 as integer) end custom_quant
, case when MOBILE_CLICK_DESCRIPTION like 'SPOS Keypad: Tap Backspace%' then null
       when MOBILE_CLICK_DESCRIPTION not like 'SPOS Keypad: Tap Show Cart%' 
        and MOBILE_CLICK_DESCRIPTION not like 'SPOS Keypad: Clear Cart Confirm%' 
        and MOBILE_CLICK_DESCRIPTION not like 'SPOS Cart: Clear Cart%'
       then null 
       else cast(substr(replace(MOBILE_CLICK_DESCRIPTION,' ','')
                        ,charindex('items:',replace(MOBILE_CLICK_DESCRIPTION,' ',''))+6
                        ,charindex('total:',replace(MOBILE_CLICK_DESCRIPTION,' ',''))-charindex('items:',replace(MOBILE_CLICK_DESCRIPTION,' ',''))-7) 
                 as integer) end item_quant
from EVENTSTREAM2.CATALOGS.MOBILE_CLICK
where MOBILE_CLICK_DESCRIPTION ilike 'SPOS Keypad%' or MOBILE_CLICK_DESCRIPTION ilike 'SPOS Cart: Clear Cart%'
order by SUBJECT_USER_TOKEN, U_RECORDED_AT
;

CREATE OR REPLACE TABLE personal_karenwang.public.custom_amount_master AS 
SELECT fpt.payment_token
  , fpt.amount_base_unit_usd AS payment_amount_base_unit_usd
  , fpt.amount_base_unit AS payment_amount_base_unit
  , tip_amount_base_unit
  , fpt.product_name
  , fpt.payment_trx_recognized_at
  , i.transaction_completedat
  , fpt.pay_with_square_entry_method
  , fpt.is_card_present
  , fpt.is_cash_payment
  , fpt.other_tender_name
  , i.transaction_merchanttoken
  , i.name
  , i.quantity
  , i.amount
  , i.itemization_tokenpair_client_token
  , i.transaction_billtoken
  , fpt.external_bill_token
  , du.user_token
  , du.best_available_merchant_token
  , du.business_category
  , du.country_code
FROM app_bi.pentagon.fact_payment_transactions fpt
LEFT JOIN app_bi.pentagon_internal_table.beemo_bill_cart_lineitems_itemization i ON fpt.external_bill_token = i.transaction_billtoken 
LEFT JOIN app_bi.pentagon.dim_user du ON fpt.unit_token = du.user_token and du.user_type = 'UNIT'
LEFT JOIN roster.merchants.locations m ON m.id = du.user_token
WHERE fpt.payment_trx_recognized_at::DATE >= '2020-09-01'::DATE
  AND i.transaction_completedat::DATE >= '2020-09-01'::DATE
  AND (fpt.product_name in ('Register Terminal','Register POS','Square Online Checkout','Invoices') or fpt.is_gpv = 0)
ORDER BY 1,7
;


create or replace table personal_karenwang.public.charge_button_payments_mapping as
with payments_prep as (
select user_token
  , payment_token
  , coalesce(product_name,'Non-GPV') as product_name
  , pay_with_square_entry_method
  , payment_trx_recognized_at
  , avg(payment_amount_base_unit - tip_amount_base_unit) as charge_amount_base_unit
  , count(1) as total_count
  , count(case when name in ('UNKNOWN','','Custom Amount') then 1 else null end) as custom_count
  , count(case when name not in ('UNKNOWN','','Custom Amount') then 1 else null end) as item_count
from personal_karenwang.public.custom_amount_master
group by 1,2,3,4,5
  )
  
, payments as (
select *
  , lead(payment_trx_recognized_at,1) over (partition by user_token order by payment_trx_recognized_at) as next_pmt_at
  , lag(payment_trx_recognized_at,1) over (partition by user_token order by payment_trx_recognized_at) as last_pmt_at
  from payments_prep
)
  
, charge_event_seq as (
select *
  , min(u_recorded_at) over (partition by subject_user_token) as first_u_recorded_at 
  , lead(u_recorded_at,1) over (partition by subject_user_token order by u_recorded_at) as next_event_at
  , lag(u_recorded_at,1) over (partition by subject_user_token order by u_recorded_at) as last_event_at
  from personal_karenwang.public.spos_keypad_parsing
  where grouped_event = 'SPOS Keypad: Tap Charge Button'
)

select *
, row_number() over (partition by x.subject_user_token, x.u_recorded_at order by p.payment_trx_recognized_at) as rn_pmt
from charge_event_seq x
left join payments p
    on p.user_token = x.subject_user_token 
    and p.payment_trx_recognized_at >= x.first_u_recorded_at 
    and p.payment_trx_recognized_at between x.u_recorded_at and coalesce(x.next_event_at,'9999-09-09') 
    and p.charge_amount_base_unit = x.total_amount 
order by p.user_token, x.u_recorded_at, p.payment_trx_recognized_at

;

create or replace table personal_karenwang.public.charge_button_payments_mapping_w_clear_plus as
with add_clear_events as (
select m.*
, case when p.subject_user_token is not null then row_number() over (partition by m.user_token, m.u_recorded_at order by case when p.total_amount is null then '2000-01-01' else p.u_recorded_at end desc) else null end as rn_clear
, p.u_recorded_at as cleared_at
, p.total_amount as cleared_amount
, p.grouped_event as clear_event
from personal_karenwang.public.charge_button_payments_mapping m
left join (select * 
           from personal_karenwang.public.spos_keypad_parsing 
           where grouped_event in ('SPOS Keypad: Clear Single Amount'
                                   ,'SPOS Keypad: Clear Cart Confirm'
                                   ,'SPOS Cart: Clear Cart'
                                   ,'SPOS Cart: Tap Backspace')
          ) p 
    on m.subject_user_token = p.subject_user_token
    and p.u_recorded_at between coalesce(m.last_event_at,'2000-01-01') and m.u_recorded_at 
    and p.u_recorded_at > dateadd('minute',-5,m.u_recorded_at)
where rn_pmt = 1  --exclude redundant payments due to missing charge events 
)

, add_clear_count as (
select subject_user_token
  , subject_merchant_token
  , u_recorded_at as charge_event_at
  , last_event_at
  , mobile_click_description as charge_event_original
  , grouped_event
  , total_amount as event_amount
  , payment_token
  , product_name
  , pay_with_square_entry_method
  , payment_trx_recognized_at
  , charge_amount_base_unit
  , total_count as total_items
  , custom_count as custom_items
  , item_count as library_items
  , max(rn_clear) as clear_count
  , max(case when rn_clear = 1 then cleared_amount else null end) as last_cleared_amount
  , max(case when rn_clear = 1 then clear_event else null end) as last_clear_event
  , max(cleared_at) as last_cleared_at
  from add_clear_events
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
)

select m.*
  , count(p.u_recorded_at) as plus_count
  , max(p.u_recorded_at) as last_plus_at
from add_clear_count m
left join (select * 
           from personal_karenwang.public.spos_keypad_parsing 
           where grouped_event = 'SPOS Keypad: Add Custom Amount to Cart'
          ) p 
    on m.subject_user_token = p.subject_user_token
    and p.u_recorded_at between coalesce(m.last_event_at,'2000-01-01') and m.charge_event_at 
    and p.u_recorded_at > dateadd('minute',-5,m.charge_event_at)    
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
order by m.subject_user_token, m.charge_event_at 
;
 
select case when custom_items > 0 AND library_items = 0 then 'pure custom'
            when custom_items > 0 AND library_items > 0 then 'hybird'
            when custom_items = 0 AND library_items > 0 then 'pure item'
       else null end type
/* , case when pay_with_square_entry_method ilike '%EMV%' then 'EMV'
         when pay_with_square_entry_method ilike '%Contactless%' then 'Contactless'
         when pay_with_square_entry_method ilike '%Swipe%' then 'Swipe'
         when pay_with_square_entry_method ilike '%N/A%' then 'non-GPV'
         when pay_with_square_entry_method ilike '%MANUALLY_KEYED%' then 'MKE'
         when pay_with_square_entry_method ilike '%EXTERNAL_API%' then 'Online Checkout'
         when pay_with_square_entry_method ilike '%CARD_ON_FILE%' then 'CoF'
         when pay_with_square_entry_method ilike '%QR_CODE%' then 'QR Code'
         when pay_with_square_entry_method ilike '%INVOICE%' then 'Invoice'
     else pay_with_square_entry_method end method*/
--, du.country_code
--, du.business_category 
-- , m.product_name
, case when datediff('day',du2.first_successful_activation_request_created_at, charge_event_at) < 30 then 'new' else 'mature' end new_mature
 , count(*) as total_charge_events
 , count(*) / nullif(count(distinct subject_user_token),0) / 7 daily_charge_events_per_seller
 , count(case when plus_count = 0 then null else 1 end) / count(*)  as plus_rate
 , count(case when total_items > 1 then 1 else null end) / count(*)  as multi_item_amount_rate
 , count(clear_count) / count(*)  as clear_rate
 , count(case when last_clear_event = 'SPOS Keypad: Clear Single Amount' then clear_count else null end) / nullif(count(clear_count),0) as single_clear_rate
 , count(case when last_clear_event = 'SPOS Keypad: Clear Cart Confirm' then clear_count else null end) / nullif(count(clear_count),0) as confirm_clear_rate
 , count(case when last_clear_event = 'SPOS Cart: Clear Cart' then clear_count else null end) / nullif(count(clear_count),0) as clear_cart_rate
 , count(case when last_clear_event = 'SPOS Cart: Tap Backspace' then clear_count else null end) / nullif(count(clear_count),0) as backspace_rate
 from personal_karenwang.public.charge_button_payments_mapping_w_clear_plus m
 left join app_bi.pentagon.dim_user du on m.subject_user_token = du.user_token
 left join app_bi.pentagon.dim_user du2 on du2.best_available_merchant_token = du.best_available_merchant_token and du2.user_type = 'MERCHANT'
 where du.country_code in ('US','CA','JP','AU','GB')
 and date_trunc('day',charge_event_at) between '2020-10-09' and '2020-10-15'
 --and type = 'pure custom'
group by 1,2--,3
order by 1,2--,4
;

select 
case when custom_items > 0 AND library_items = 0 then 'pure custom'
            when custom_items > 0 AND library_items > 0 then 'hybird'
            when custom_items = 0 AND library_items > 0 then 'pure item'
       else null end type
--, c.country_code
--, c.business_category
, count(case when c.country_code = 'JP' or mod(amount,100) = 0 then 1 else null end) / count(*) as whole_numbers
, count(case when c.country_code <> 'JP' and mod(amount, 25) = 0 and mod(amount, 100) <> 0 then 1 else null end)  / count(*) as reg_frac_numbers
, count(case when c.country_code <> 'JP' and mod(amount, 25) <> 0 then 1 else null end)  / count(*) as irre_frac_numbers
  from personal_karenwang.public.charge_button_payments_mapping_w_clear_plus m
 left join personal_karenwang.public.custom_amount_master c on c.user_token = m.subject_user_token and c.payment_token = m.payment_token
 where c.country_code in ('US','CA','JP','AU','GB')
 and date_trunc('day',charge_event_at) between '2020-10-09' and '2020-10-15'
 and business_category not in ('food_and_drink','retail')
 group by 1--,2
