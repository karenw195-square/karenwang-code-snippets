--size the attachment of terminal quick churns to non-processing products
with prep as (
select datediff('day',tmg.FIRST_product_DATE,m.latest_payment_date) as term_churn_lag
  , tmg.FIRST_product_DATE
  , latest_payment_date
  , tmg.MERCHANT_TOKEN
from personal_karenwang.public.terminal_migration_grouping3 tmg
  join app_bi.app_bi_dw.dim_merchant m on tmg.merchant_token = m.merchant_token
where seller_group = 'Pure Terminal'
  and latest_payment_date < dateadd('day',-91,'2020-05-27')
  )

, prep2 as (
select f.merchant_token, f.product_name, f.merchant_net_new_date 
  , row_number() over (partition by f.merchant_token order by f.merchant_net_new_date) as rn
from app_bi.app_bi_dw.dim_merchant_first_activity f
join prep p on f.merchant_token = p.merchant_token and term_churn_lag = 0

  )
  
, prep3 as (
select *, sum(case when product_name = 'Register Terminal' then rn else null end) over (partition by merchant_token) as term_rn
from prep2
  order by merchant_token,merchant_net_new_date
)
  
--sizing
select count(distinct merchant_token) as total
, count(distinct case when term_rn = 1 and rn > 1 then merchant_token else null end) as term_to_non
, count(distinct case when term_rn > 1 and rn > 1 then merchant_token else null end) as non_to_term
, count(distinct case when term_rn > 1 and rn > term_rn then merchant_token else null end) as non_to_term_to_non
, count(distinct case when term_rn = 1 and rn > 1 then merchant_token else null end) /count(distinct merchant_token) as term_to_non_rate
, count(distinct case when term_rn > 1 and rn > 1 then merchant_token else null end)  /count(distinct merchant_token) asnon_to_term_rate
, count(distinct case when term_rn > 1 and rn > term_rn then merchant_token else null end)  /count(distinct merchant_token) as non_to_term_to_non_rate
from prep3
;

--list common non-processing attachments
/*select product_name, count(distinct merchant_token) 
from prep3
where 1=1 and product_name <> 'Register Terminal'
--term_rn = 1 and rn > 1
--term_rn > 1 and rn = 1
--term_rn > 1 and rn > term_rn
group by 1
order by 2 desc;*/

-- trialist churn transaction pattern with mcc distro
CREATE OR REPLACE TABLE personal_karenwang.public.trialist_stats AS
select case when latest_payment_date < dateadd('day',-91,'2020-05-27') and datediff('day',tmg.FIRST_product_DATE,m.latest_payment_date) = 0 then 'trialist churns' 
            when latest_payment_date < dateadd('day',-91,'2020-05-27') and datediff('day',tmg.FIRST_product_DATE,m.latest_payment_date) > 0 then 'slower churns'
            when latest_payment_date >= dateadd('day',-91,'2020-05-27') then 'active'
            else null end churn_type
, tmg.merchant_token
, tmg.first_product_date
, m.merchant_business_category as mcc
, m.merchant_business_type as sub_mcc
, m.merchant_activation_address_country_code as country_code
, ufmf.source_application
, ufmf.channel
, datediff('day',tmg.FIRST_product_DATE,m.latest_payment_date) as days_before_churn
, case when ufmf.FROZEN = 1 or ufmf.DEACTIVATED = 1 then true else false end is_fraud_suspect
, case when coalesce(ufmf.BANK_ACCOUNT_LINKED_DATE,faf.BANK_LINKING_SUCCESS_AT) <= tmg.first_product_date then true else false end is_bank_linked
, case when faf.FIRST_HW_ORDER_DATE <= tmg.first_product_date then true else false end is_hw_ordered
, sum(transaction_count_net) as total_trx
, sum(gpv_net_local) as total_gpv
, sum(case when mrs.card_presence = 'CP' then transaction_count_net else null end) as cp_trx
, sum(case when mrs.card_presence = 'CP' then gpv_net_local else null end) as cp_gpv
, sum(case when rt.reader_type in ('R4','M1') then transaction_count_net else null end) as r4_trx
, sum(case when rt.reader_type in ('R4','M1') then gpv_net_local else null end) as r4_gpv
, sum(case when mrs.report_date = tmg.FIRST_product_DATE then transaction_count else null end) as first_day_trx
, sum(case when mrs.report_date = tmg.FIRST_product_DATE then gpv_gross_local else null end) as first_day_gpv
, sum(case when mrs.report_date = tmg.FIRST_product_DATE then transaction_count_net else null end) as first_day_trx_net
, sum(case when mrs.report_date = tmg.FIRST_product_DATE then gpv_net_local else null end) as first_day_gpv_net
, sum(case when mrs.report_date = tmg.FIRST_product_DATE and mrs.card_presence = 'CP' then transaction_count else null end) as first_day_cp_trx
, sum(case when mrs.report_date = tmg.FIRST_product_DATE and mrs.card_presence = 'CP' then gpv_gross_local else null end) as first_day_cp_gpv
, sum(case when mrs.report_date = tmg.FIRST_product_DATE and rt.reader_type in ('R4','M1') then transaction_count else null end) as first_day_r4_trx
, sum(case when mrs.report_date = tmg.FIRST_product_DATE and rt.reader_type in ('R4','M1') then gpv_gross_local else null end) as first_day_r4_gpv
from personal_karenwang.public.terminal_migration_grouping3 tmg
  left join app_bi.app_bi_dw.dim_merchant m on tmg.merchant_token = m.merchant_token
  left join APP_PAYMENTS.APP_PAYMENTS_ANALYTICS.UNIFYING_FUNNEL_MERCHANT_FACTS ufmf on tmg.merchant_token = ufmf.BEST_AVAILABLE_MERCHANT_TOKEN
  left join app_bi.app_bi_dw.dim_user du on m.key_merchant = du.key_merchant
  left join app_bi.app_bi_dw.vfact_daily_processing_summary mrs on mrs.key_user = du.key_user 
  left join app_bi.app_bi_dw.dim_product dp on dp.key_product = mrs.key_product 
  left join app_bi.app_bi_dw.dim_reader_type rt on rt.key_reader_type = mrs.key_reader_type
  left join app_payments.app_payments_analytics.fact_activations_funnel faf on faf.BEST_AVAILABLE_MERCHANT_TOKEN = tmg.merchant_token
where seller_group = 'Pure Terminal'
and dp.product_name = 'Register Terminal'
  --and latest_payment_date < dateadd('day',-91,'2020-05-27')
group by 1,2,3,4,5,6,7,8,9,10,11,12

;

--aggregation
select churn_type
, case when churn_type = 'trialist churns' and first_day_gpv <= 1 and first_day_trx <= 1 then 'test pay'
       when churn_type <> 'trialist churns' then null
       else 'non-test pay' end pay_type
--, mcc
--, sub_mcc
--, is_fraud_suspect
--, is_bank_linked
--, is_hw_ordered
--, country_code
--, source_application
--, channel
, avg(days_before_churn) as avg_days_before_churn
, count(distinct merchant_token) as sellers
, count(distinct case when is_fraud_suspect then merchant_token else null end) as is_fraud_sellers
, count(distinct case when is_bank_linked then merchant_token else null end) as bank_linked_sellers
, count(distinct case when is_hw_ordered then merchant_token else null end) as hw_ordered_sellers
, count(distinct case when first_day_cp_trx is not null then merchant_token else null end) as first_day_cp_sellers
, count(distinct case when first_day_r4_trx is not null then merchant_token else null end) as first_day_r4_sellers
, count(distinct case when is_fraud_suspect then merchant_token else null end) / count(distinct merchant_token) as is_fraud_rate
, count(distinct case when is_bank_linked then merchant_token else null end) / count(distinct merchant_token) as bank_linked_rate
, count(distinct case when is_hw_ordered then merchant_token else null end) / count(distinct merchant_token) as hw_ordered_rate
, count(distinct case when first_day_cp_trx is not null then merchant_token else null end) / count(distinct merchant_token) as first_day_cp_rate
, count(distinct case when first_day_r4_trx is not null then merchant_token else null end) / count(distinct merchant_token) as first_day_r4_rate
, sum(total_gpv/(days_before_churn+1) * 365)/count(distinct merchant_token) as annualized_gpv_per_sellers
, sum(total_trx/(days_before_churn+1) * 365)/count(distinct merchant_token) as annualized_trx_per_sellers
, sum(total_gpv)/sum(total_trx) as ticket_size
, sum(first_day_gpv)/count(distinct merchant_token) as first_day_gpv_per_sellers
, sum(first_day_trx)/count(distinct merchant_token) as first_day_trx_per_sellers
, sum(first_day_gpv)/sum(first_day_trx) as first_day_ticket_size
, 1- sum(case when churn_type = 'trialist churns' then total_gpv else first_day_gpv_net end)/sum(first_day_gpv) as first_day_refund_rate_gpv
, 1- sum(case when churn_type = 'trialist churns' then total_trx else first_day_trx_net end)/sum(first_day_trx) as first_day_refund_rate_trx
, sum(case when not is_fraud_suspect then first_day_gpv else 0 end)/count(distinct case when not is_fraud_suspect then merchant_token else null end) as first_day_gpv_per_valid_sellers
, sum(case when not is_fraud_suspect then first_day_trx else 0 end)/count(distinct case when not is_fraud_suspect then merchant_token else null end) as first_day_trx_per_valid_sellers
, sum(case when not is_fraud_suspect then first_day_gpv else 0 end)/sum(case when not is_fraud_suspect then first_day_trx else 0 end) as first_day_valid_ticket_size

from personal_karenwang.public.trialist_stats
group by 1,2
order by 1,2
;

--understand bank linking & hardware ordering as leading indicators
select 
churn_type
, case when churn_type = 'trialist churns' and first_day_gpv <= 1 and first_day_trx <= 1 then 'test pay'
       when churn_type <> 'trialist churns' then null
       else 'non-test pay' end pay_type
, mcc
--, sub_mcc
--, country_code
--, business_name
, is_bank_linked
--, is_hw_ordered
, count(distinct merchant_token) as sellers
, count(distinct case when is_fraud_suspect then merchant_token else null end)/count(distinct merchant_token) as fraud_rate
, sum(first_day_gpv)/count(distinct merchant_token) as first_day_gpv_per_sellers
--, sum(total_gpv)/count(distinct merchant_token) as first_day_net_gpv_per_sellers
, 1- sum(case when churn_type = 'trialist churns' then total_gpv else first_day_gpv_net end)/sum(first_day_gpv) as first_day_refund_rate_gpv
, sum(first_day_gpv)/sum(first_day_trx) as first_day_ticket_size

from personal_karenwang.public.trialist_stats ts
--left join app_payments.app_payments_analytics.adj_business_name bn on ts.merchant_token = bn.user_token
where 1=1--not is_bank_linked and churn_type = 'trialist churns' and not (first_day_gpv <= 1 and first_day_trx <= 1)
--and not IS_FRAUD_SUSPECT
group by 1,2,3,4

;

--understand if referrals bring in trialist churns.
select churn_type
, case when churn_type = 'trialist churns' and first_day_gpv <= 1 and first_day_trx <= 1 then 'test pay'
       when churn_type <> 'trialist churns' then null
       else 'non-test pay' end pay_type
, count(distinct ra.referee_best_available_merchant_token) as referees
, count(distinct ts.merchant_token) as sellers
, sum(case when ra.referee_best_available_merchant_token is not null then first_day_gpv else 0 end)
 / count(distinct ra.referee_best_available_merchant_token) as first_day_gpv_per_referees
, 1- sum(case when ra.referee_best_available_merchant_token is null then 0 else case when churn_type = 'trialist churns' then total_gpv else first_day_gpv_net end end)
/ sum(case when ra.referee_best_available_merchant_token is not null then first_day_gpv else 0 end) as first_day_refund_rate_referee
from personal_karenwang.public.trialist_stats ts
left join app_payments.app_payments_analytics.referee_attributions ra
on ts.merchant_token = ra.referee_best_available_merchant_token
group by 1,2

;

-- trialist CS contacts before churning
CREATE OR REPLACE TABLE personal_karenwang.public.trialist_cs_contacts AS
select ts.*
, cml.product, cml.issue_category, cml.issue_bucket,cml.issue, cml.macro_label
from personal_karenwang.public.trialist_stats ts
left join app_support.app_support.cases c on c.SQUARE_MERCHANT_TOKEN = ts.merchant_token and c.case_created_at::date between ts.first_product_date and dateadd('day',7,ts.first_product_date)
left join app_support.app_support.case_macro_labels cml ON cml.case_id = c.case_id
order by ts.merchant_token, c.case_created_at;

select churn_type
, case when churn_type = 'trialist churns' and first_day_gpv <= 1 and first_day_trx <= 1 then 'test pay'
       when churn_type <> 'trialist churns' then null
       else 'non-test pay' end pay_type
--, product
--, issue_category
, issue_bucket
--, issue
, macro_label
, count(distinct merchant_token) as sellers
--, count(distinct case when issue_category is not null then merchant_token else null end) as sellers_w_cs
from personal_karenwang.public.trialist_cs_contacts
where 1=1 --churn_type = 'trialist churns'
and is_fraud_suspect
and issue_bucket is not null
group by 1,2,3,4--,5
order by 2,5 desc
;

--acquisition optimization
select --churn_type
--, case when churn_type = 'trialist churns' and first_day_gpv <= 1 and first_day_trx <= 1 then 'test pay'
--       when churn_type <> 'trialist churns' then null
--       else 'non-test pay' end pay_type
 aa.channel
, aa.sub_channel
--, aa.CAMPAIGN
--, aa.detailed_channel
--, aa.partner_channel
, aa.offer_channel
, sum(aa.frac_sq_tof_conversions) as sellers
from personal_karenwang.public.trialist_stats ts
left join app_marketing.attribution.attribution5 aa
on ts.merchant_token = aa.best_available_merchant_token
where aa.country_code IN('US','CA','JP','GB','AU')
and aa.sub_channel is null
group by 1,2,3--,4,5
order by 1,2,4 desc
;
