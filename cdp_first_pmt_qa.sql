create or replace table personal_karenwang.public.first_pmt as 
with base as (
    select du2.user_token as merchant_token
    , du2.user_created_at::date as user_created_date
    , fpt.payment_trx_recognized_at
    , fpt.payment_token
    , fpt.payment_entry_method
    , fpt.pay_with_square_entry_method
    , fpt.is_card_present
    , fpt.amount_base_unit
    , fpt.currency_code
    , case when fpt.is_debit = 1 then 'DEBIT' when fpt.is_credit = 1 then 'CREDIT' end tender_type 
    , fpt.reader_type
    , fpt.product_name
    , row_number() over (partition by du2.user_token order by payment_trx_recognized_at) as rn
from app_bi.pentagon.fact_payment_transactions fpt
left join app_bi.pentagon.dim_user du on fpt.unit_token = du.user_token
left join app_bi.pentagon.dim_user du2 on du.best_available_merchant_token = du2.user_token
where du.user_type = 'UNIT'
and du2.user_type = 'MERCHANT'
and fpt.payment_trx_recognized_date < current_date
and du2.user_created_at::date >= '2021-07-24'
and fpt.is_gpv = 1
and is_card_payment = 1
    )
select * from base where rn = 1 and payment_trx_recognized_at::date >= '2021-07-24'
;

create or replace table personal_karenwang.public.cdp_first_pmt as 
select f.entity_id as merchant_token
    , du.user_created_at::date as user_created_date
    , f.timestamp as payment_trx_recognized_at
    , f.properties_payment_token
    , f.properties_payment_method
    , f.properties_pay_with_square_entry_method
    , case when f.properties_is_card_present = 'TRUE' then 1 when f.properties_is_card_present = 'FALSE' then 0 end properties_is_card_present
    , f.properties_amount_base_unit
    , f.properties_currency_code
    , f.properties_tender_type
    , f.properties_reader_name
    from customer_data.cdp_event_logger.merchant_complete_first_payment f
    left join app_bi.pentagon.dim_user du on f.entity_id = du.user_token
    where du.user_type = 'MERCHANT'
    and f.timestamp::date >= '2021-07-24'
    and f.timestamp::date < current_date --'2021-08-05'
    and du.user_created_at::date >= '2021-07-24'
;

select coalesce(f.payment_trx_recognized_at::date,c.payment_trx_recognized_at::date) as user_created_date
, count(distinct c.properties_payment_token) as cdp_count
, count(distinct f.payment_token) as pentagon_count
, count(distinct case when c.properties_payment_token is not null and f.payment_token is not null then f.payment_token else null end) as overlap_count
--, count(distinct case when f.payment_entry_method = properties_payment_method then f.payment_token else null end) as method_overlap
, count(distinct case when f.pay_with_square_entry_method = properties_pay_with_square_entry_method then f.payment_token else null end) as pws_method_overlap
, count(distinct case when f.is_card_present = properties_is_card_present then f.payment_token else null end) as card_presence_overlap
, count(distinct case when f.amount_base_unit = properties_amount_base_unit then f.payment_token else null end) as amount_overlap
, count(distinct case when f.currency_code = properties_currency_code then f.payment_token else null end) as currency_overlap
, count(distinct case when f.tender_type = properties_tender_type then f.payment_token else null end) as tender_overlap
, count(distinct case when f.reader_type = properties_reader_name or (f.reader_type is null and properties_reader_name is null) then f.payment_token else null end) as reader_overlap
from personal_karenwang.public.first_pmt f
full outer join personal_karenwang.public.cdp_first_pmt c 
     on f.merchant_token = c.merchant_token
     and f.payment_token = c.properties_payment_token
 group by 1
 order by 1
;

select datediff('minute',f.payment_trx_recognized_at,c.payment_trx_recognized_at) as sec_diff
, count(distinct c.properties_payment_token) as pmt
, pmt / sum(pmt) over()
from personal_karenwang.public.first_pmt f
full outer join personal_karenwang.public.cdp_first_pmt c 
     on f.merchant_token = c.merchant_token
     and f.payment_token = c.properties_payment_token
 --where not f.amount_base_unit = properties_amount_base_unit
 group by 1
 order by 2 desc
