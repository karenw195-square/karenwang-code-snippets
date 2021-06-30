with base as (
select distinct du.best_available_merchant_token
from app_bi.pentagon.fact_payment_transactions fpt
  left join app_bi.pentagon.dim_user du
  on fpt.unit_token = du.user_token
where fpt.product_name in ('Register Terminal', 'Register POS') --used SPOS to process payments
  and fpt.is_itemized = 0 --custom amounts
  and fpt.is_gpv = 1  
  and fpt.payment_trx_recognized_date > current_date - 91
  and fpt.amount_base_unit > 100 --non-test payment
)
select c.best_available_merchant_token
, sum(amount_base_unit_usd) as gpv_usd_91d
, sum(case when product_name in ('Register Terminal', 'Register POS') and is_itemized = 0 then amount_base_unit_usd else 0 end) as gpv_cust_usd_91d
, gpv_cust_usd_91d / nullif(gpv_usd_91d,0) as custom_rate
from app_bi.pentagon.fact_payment_transactions fpt
  left join app_bi.pentagon.dim_user du
  on fpt.unit_token = du.user_token and du.user_type = 'UNIT'
  inner join base c
  on du.best_available_merchant_token = c.best_available_merchant_token
where fpt.is_gpv = 1  
and fpt.payment_trx_recognized_date > current_date - 91
group by 1
having custom_rate > 0.5
