---------get target segment sellers (2018-2019 to give the later cohorts time to cure)
CREATE OR REPLACE TABLE personal_karenwang.public.persona_target_sellers AS 
WITH base AS (
SELECT *
, row_number() over (PARTITION BY best_available_merchant_token ORDER BY first_payment_date) AS rn
FROM app_onboarding.product_intent.product_intent_merchant_facts 
WHERE signup_date >= '2019-01-01' 
  AND signup_date < '2020-01-01'
  AND product_intent_signal IN ('bapi-primary')
           ) 
, add_term_gpv AS (
SELECT best_available_merchant_token
, product_intent_value
, signup_date
, source_application
, country_code
, device_os
, activation_success_date
, bank_account_linked_date
, onboard_completion_date
, first_app_event_date
, payment_product
, first_payment_date
, SUM(volume_gross_local) AS gpv_term_local
FROM base b
JOIN app_bi.app_bi_dw.vfact_merchant_revenue_summary mrs 
  ON b.best_available_merchant_token = mrs.merchant_token AND mrs.product_name = 'Register Terminal'
WHERE b.rn = 1
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
)

SELECT * FROM add_term_gpv
WHERE (country_code = 'JP' AND gpv_term_local > 100) OR 
(country_code <> 'JP' AND gpv_term_local > 1)  --sellers who had terminal payments (excluding test pay)
;


-----------merchant attributes
CREATE OR REPLACE TABLE personal_karenwang.public.persona_merchant_attributes AS 
SELECT 
    s.best_available_merchant_token    
  , s.signup_date
  , s.source_application
  , s.country_code
  , s.device_os
  , s.product_intent_value
  , s.payment_product as first_payment_product
  , s.first_payment_date
  , datediff('day',s.signup_date,nullif(s.activation_success_date,'9999-09-09')) as days_to_activation
  , datediff('day',s.signup_date,nullif(s.bank_account_linked_date,'9999-09-09')) as days_to_bank_link
  , datediff('day',s.signup_date,nullif(s.onboard_completion_date,'9999-09-09')) as days_to_onboard_completion
  , datediff('day',s.signup_date,nullif(s.first_app_event_date,'9999-09-09')) as days_to_first_app_event
  , datediff('day',s.signup_date,nullif(s.first_payment_date,'9999-09-09')) as days_to_first_payment
  , du.num_units
  , du.business_category
  , du.total_times_unfrozen
  , du.total_times_deactivated
  , faf.is_business_seller
  , faf.is_cbd_merchant
  , datediff('day',s.signup_date,nullif(faf.first_hw_order_date,'9999-09-09')) as days_to_hardware_order
  , CASE WHEN ra.referee_best_available_merchant_token IS NOT NULL THEN TRUE ELSE FALSE END is_referred
FROM personal_karenwang.public.persona_target_sellers s
LEFT JOIN app_bi.pentagon.dim_user du 
  ON s.best_available_merchant_token=du.user_token
LEFT JOIN app_payments.app_payments_analytics.fact_activations_funnel faf 
  ON s.best_available_merchant_token=faf.best_available_merchant_token
LEFT JOIN app_payments.app_payments_analytics.referee_attributions ra
  ON s.best_available_merchant_token=ra.referee_best_available_merchant_token
WHERE du.has_successful_activation_request=1  
;

-----------payment summary  
CREATE OR REPLACE TABLE personal_karenwang.public.persona_payment_summary AS 
WITH daily AS (
SELECT 
    mb.best_available_merchant_token
  , mb.signup_date  
  , mb.first_payment_date
  , fpt.payment_trx_recognized_date AS pmt_date
  , COUNT(DISTINCT fpt.payment_token) AS payment
  , SUM(fpt.amount_base_unit_usd / 100) AS payment_amount
  , COUNT(DISTINCT CASE WHEN is_refunded = 1 THEN fpt.payment_token END) AS refunded_payment
  
  , COUNT(DISTINCT CASE WHEN fpt.product_name='Register POS' THEN fpt.payment_token END) AS spos_pmt
  , COUNT(DISTINCT CASE WHEN fpt.product_name='Register Terminal' THEN fpt.payment_token END) AS terminal_pmt
  , COUNT(DISTINCT CASE WHEN fpt.product_name='Virtual Terminal' THEN fpt.payment_token END) AS vt_pmt
  , COUNT(DISTINCT CASE WHEN fpt.product_name='Invoices' THEN fpt.payment_token END) AS invoice_pmt
  , COUNT(DISTINCT CASE WHEN fpt.product_name not in ('Register POS','Register Terminal','Virtual Terminal','Invoices') THEN fpt.payment_token END) AS misc_pmt
  , SUM(CASE WHEN fpt.product_name='Register POS' THEN fpt.amount_base_unit_usd / 100 ELSE 0 END) AS spos_gpv
  , SUM(CASE WHEN fpt.product_name='Register Terminal' THEN fpt.amount_base_unit_usd / 100 ELSE 0 END) AS terminal_gpv
  , SUM(CASE WHEN fpt.product_name='Virtual Terminal' THEN fpt.amount_base_unit_usd / 100 ELSE 0 END) AS vt_gpv
  , SUM(CASE WHEN fpt.product_name='Invoices' THEN fpt.amount_base_unit_usd / 100 ELSE 0 END) AS invoice_gpv
  , SUM(CASE WHEN fpt.product_name not in ('Register POS','Register Terminal','Virtual Terminal','Invoices') THEN fpt.amount_base_unit_usd / 100 ELSE 0 END) AS misc_gpv
  
  , COUNT(DISTINCT CASE WHEN fpt.reader_type IS NULL AND IS_CARD_PRESENT = 0 THEN fpt.payment_token END) AS cnp_pmt
  , COUNT(DISTINCT CASE WHEN fpt.reader_type IS NOT NULL AND IS_CARD_PRESENT = 1 THEN fpt.payment_token END) AS cp_pmt
  , COUNT(DISTINCT CASE WHEN fpt.reader_type='R12' THEN fpt.payment_token END) AS r12_pmt
  , COUNT(DISTINCT CASE WHEN fpt.reader_type='X2' THEN fpt.payment_token END) AS x2_pmt
  , COUNT(DISTINCT CASE WHEN fpt.reader_type='M1' THEN fpt.payment_token END) AS r4_pmt
  , COUNT(DISTINCT CASE WHEN fpt.reader_type='T2' THEN fpt.payment_token END) AS t2_pmt
  , COUNT(DISTINCT CASE WHEN fpt.reader_type='S1' THEN fpt.payment_token END) AS s1_pmt
  , COUNT(DISTINCT CASE WHEN fpt.reader_type='R41' THEN fpt.payment_token END) AS r41_pmt
  , COUNT(DISTINCT CASE WHEN fpt.reader_type='R6' THEN fpt.payment_token END) AS r6_pmt
  , SUM(CASE WHEN fpt.reader_type IS NULL AND IS_CARD_PRESENT = 0 THEN fpt.amount_base_unit_usd / 100 ELSE 0 END) AS cnp_gpv
  , SUM(CASE WHEN fpt.reader_type IS NOT NULL AND IS_CARD_PRESENT = 1 THEN fpt.amount_base_unit_usd / 100 ELSE 0 END) AS cp_gpv
  , SUM(CASE WHEN fpt.reader_type='R12' THEN fpt.amount_base_unit_usd / 100 ELSE 0 END) AS r12_gpv
  , SUM(CASE WHEN fpt.reader_type='X2' THEN fpt.amount_base_unit_usd / 100 ELSE 0 END) AS x2_gpv
  , SUM(CASE WHEN fpt.reader_type='M1' THEN fpt.amount_base_unit_usd / 100 ELSE 0 END) AS r4_gpv
  , SUM(CASE WHEN fpt.reader_type='T2' THEN fpt.amount_base_unit_usd / 100 ELSE 0 END) AS t2_gpv
  , SUM(CASE WHEN fpt.reader_type='S1' THEN fpt.amount_base_unit_usd / 100 ELSE 0 END) AS s1_gpv
  , SUM(CASE WHEN fpt.reader_type='R41' THEN fpt.amount_base_unit_usd / 100 ELSE 0 END) AS r41_gpv
  , SUM(CASE WHEN fpt.reader_type='R6' THEN fpt.amount_base_unit_usd / 100 ELSE 0 END) AS r6_gpv
FROM personal_karenwang.public.persona_merchant_attributes mb
JOIN app_bi.pentagon.dim_user du 
  ON du.best_available_merchant_token=mb.best_available_merchant_token 
  AND user_type='UNIT' 
LEFT JOIN app_bi.pentagon.fact_payment_transactions fpt
  ON fpt.unit_token=du.user_token
  AND fpt.payment_trx_recognized_date >= '2019-01-01'
  AND fpt.is_gpv=1 
  AND fpt.amount_base_unit > 100 --excluding test pay
GROUP BY 1,2,3,4
)
  
SELECT best_available_merchant_token
  , signup_date
  , first_payment_date
  , DATEDIFF('day',MAX(first_payment_date),MAX(pmt_date)) + 1 AS payment_tenure_days
  , COUNT(DISTINCT pmt_date) AS active_days
  , COUNT(DISTINCT CASE WHEN terminal_pmt = payment THEN pmt_date END) AS pure_term_days
  , COUNT(DISTINCT CASE WHEN terminal_pmt = 0 THEN pmt_date END) AS pure_non_term_days
  , COUNT(DISTINCT CASE WHEN terminal_pmt > 0 and payment > terminal_pmt THEN pmt_date END) AS hybrid_term_days
  , SUM(payment) AS total_payment
  , SUM(payment_amount) AS total_gpv
  , SUM(refunded_payment) / NULLIF(SUM(payment),0) AS refund_rate
  , SUM(payment_amount) / NULLIF(SUM(payment),0) AS ticket_size
  , SUM(CASE WHEN pmt_date BETWEEN signup_date AND DATEADD('day', 180, signup_date) THEN payment ELSE 0 END) AS payment_180d_since_signup
  , SUM(CASE WHEN pmt_date BETWEEN signup_date AND DATEADD('day', 180, signup_date) THEN payment_amount ELSE 0 END) as gpv_180d_since_signup
  , SUM(CASE WHEN pmt_date BETWEEN first_payment_date AND DATEADD('day', 180, first_payment_date) THEN payment ELSE 0 END) AS payment_180d_since_nna
  , SUM(CASE WHEN pmt_date BETWEEN first_payment_date AND DATEADD('day', 180, first_payment_date) THEN payment_amount ELSE 0 END) as gpv_180d_since_nna
  
  , SUM(spos_pmt) AS spos_pmt
  , SUM(terminal_pmt) AS terminal_pmt
  , SUM(vt_pmt) AS vt_pmt
  , SUM(invoice_pmt) AS invoice_pmt
  , SUM(misc_pmt) AS misc_pmt
  , SUM(spos_gpv) AS spos_gpv
  , SUM(terminal_gpv) AS terminal_gpv
  , SUM(vt_gpv) AS vt_gpv
  , SUM(invoice_gpv) AS invoice_gpv
  , SUM(misc_gpv) AS misc_gpv
  , SUM(spos_gpv) / NULLIF(SUM(spos_pmt),0) AS spos_ticket_size
  , SUM(terminal_gpv) / NULLIF(SUM(terminal_pmt),0) AS terminal_ticket_size
  , SUM(vt_gpv) / NULLIF(SUM(vt_pmt),0) AS vt_ticket_size
  , SUM(invoice_gpv) / NULLIF(SUM(invoice_pmt),0) AS invoice_ticket_size
  , SUM(misc_gpv) / NULLIF(SUM(misc_pmt),0) AS misc_ticket_size
  
  , SUM(cnp_pmt) AS cnp_pmt
  , SUM(r12_pmt) AS r12_pmt
  , SUM(x2_pmt) AS x2_pmt
  , SUM(r4_pmt) AS r4_pmt
  , SUM(t2_pmt) AS t2_pmt
  , SUM(s1_pmt) AS s1_pmt
  , SUM(r41_pmt) AS r41_pmt
  , SUM(r6_pmt) AS r6_pmt
  , SUM(cnp_gpv) AS cnp_gpv
  , SUM(r12_gpv) AS r12_gpv
  , SUM(x2_gpv) AS x2_gpv
  , SUM(r4_gpv) AS r4_gpv
  , SUM(t2_gpv) AS t2_gpv
  , SUM(s1_gpv) AS s1_gpv
  , SUM(r41_gpv) AS r41_gpv
  , SUM(r6_gpv) AS r6_gpv
  , SUM(cnp_gpv) / NULLIF(SUM(cnp_pmt),0) AS cnp_ticket_size
  , SUM(cp_gpv) / NULLIF(SUM(cp_pmt),0) AS cp_ticket_size
  
  , SUM(CASE WHEN pmt_date BETWEEN first_payment_date AND DATEADD('day', 90, first_payment_date) THEN payment ELSE 0 END) AS total_pmt_3m
  , SUM(CASE WHEN pmt_date BETWEEN first_payment_date AND DATEADD('day', 90, first_payment_date) THEN spos_pmt ELSE 0 END) AS spos_pmt_3m
  , SUM(CASE WHEN pmt_date BETWEEN first_payment_date AND DATEADD('day', 90, first_payment_date) THEN terminal_pmt ELSE 0 END) AS terminal_pmt_3m
  , SUM(CASE WHEN pmt_date BETWEEN first_payment_date AND DATEADD('day', 90, first_payment_date) THEN vt_pmt ELSE 0 END) AS vt_pmt_3m
  , SUM(CASE WHEN pmt_date BETWEEN first_payment_date AND DATEADD('day', 90, first_payment_date) THEN invoice_pmt ELSE 0 END) AS invoice_pmt_3m
  , SUM(CASE WHEN pmt_date BETWEEN first_payment_date AND DATEADD('day', 90, first_payment_date) THEN misc_pmt ELSE 0 END) AS misc_pmt_3m
  
  , SUM(CASE WHEN pmt_date BETWEEN DATEADD('day', 91, first_payment_date) AND DATEADD('day', 180, first_payment_date) THEN payment ELSE 0 END) AS total_pmt_3m_6m
  , SUM(CASE WHEN pmt_date BETWEEN DATEADD('day', 91, first_payment_date) AND DATEADD('day', 180, first_payment_date) THEN spos_pmt ELSE 0 END) AS spos_pmt_3m_6m
  , SUM(CASE WHEN pmt_date BETWEEN DATEADD('day', 91, first_payment_date) AND DATEADD('day', 180, first_payment_date) THEN terminal_pmt ELSE 0 END) AS terminal_pmt_3m_6m
  , SUM(CASE WHEN pmt_date BETWEEN DATEADD('day', 91, first_payment_date) AND DATEADD('day', 180, first_payment_date) THEN vt_pmt ELSE 0 END) AS vt_pmt_3m_6m
  , SUM(CASE WHEN pmt_date BETWEEN DATEADD('day', 91, first_payment_date) AND DATEADD('day', 180, first_payment_date) THEN invoice_pmt ELSE 0 END) AS invoice_pmt_3m_6m
  , SUM(CASE WHEN pmt_date BETWEEN DATEADD('day', 91, first_payment_date) AND DATEADD('day', 180, first_payment_date) THEN misc_pmt ELSE 0 END) AS misc_pmt_3m_6m
  
  , SUM(CASE WHEN pmt_date > DATEADD('day', 180, first_payment_date) THEN payment ELSE 0 END) AS total_pmt_6m
  , SUM(CASE WHEN pmt_date > DATEADD('day', 180, first_payment_date) THEN spos_pmt ELSE 0 END) AS spos_pmt_6m
  , SUM(CASE WHEN pmt_date > DATEADD('day', 180, first_payment_date) THEN terminal_pmt ELSE 0 END) AS terminal_pmt_6m
  , SUM(CASE WHEN pmt_date > DATEADD('day', 180, first_payment_date) THEN vt_pmt ELSE 0 END) AS vt_pmt_6m
  , SUM(CASE WHEN pmt_date > DATEADD('day', 180, first_payment_date) THEN invoice_pmt ELSE 0 END) AS invoice_pmt_6m
  , SUM(CASE WHEN pmt_date > DATEADD('day', 180, first_payment_date) THEN misc_pmt ELSE 0 END) AS misc_pmt_6m
FROM daily
GROUP BY 1,2,3
;  

------------gpv_segment
CREATE OR REPLACE TABLE personal_karenwang.public.persona_gpv_segment AS
WITH highest_segment AS (
SELECT merchant_token
, MAX(CASE WHEN merchant_segment = 'SMB' THEN '2 - SMB'
           WHEN merchant_segment = 'Micro' THEN '1 - Micro'
           WHEN merchant_segment = 'Mid-Market' THEN '3 - Mid-Market'
           WHEN merchant_segment = 'Enterprise' THEN '4 - Enterprise'
      ELSE '0 - Other' END) AS gpv_segment -- highest rev segment
FROM app_bi.app_bi_dw.dim_merchant_gpv_segment
GROUP BY 1
)  

SELECT ps.best_available_merchant_token
  , gpv_segment    
FROM personal_karenwang.public.persona_payment_summary ps
JOIN highest_segment gs
    ON ps.best_available_merchant_token=gs.merchant_token
;    


----------repeat card
CREATE OR REPLACE TABLE personal_karenwang.public.persona_card AS 
WITH card as (
    SELECT 
        mb.best_available_merchant_token
      , pan_fidelius_token
      , COUNT(DISTINCT fpt.payment_token) AS cnt    
    FROM personal_karenwang.public.persona_merchant_attributes mb
    JOIN app_bi.pentagon.dim_user du 
      ON du.best_available_merchant_token=mb.best_available_merchant_token 
      AND user_type='UNIT' 
    LEFT JOIN app_bi.pentagon_table.fact_payment_transactions_base fpt
      ON fpt.unit_token=du.user_token
      AND fpt.payment_trx_recognized_date>='2019-01-01'
      --AND fpt.payment_trx_recognized_date BETWEEN mb.signup_date AND DATEADD('day',180,signup_date)
      AND pan_fidelius_token IS NOT NULL
    GROUP BY 
        mb.best_available_merchant_token
      , pan_fidelius_token
    )

SELECT 
   best_available_merchant_token
 , COUNT(DISTINCT pan_fidelius_token) AS total_card
 , COUNT(DISTINCT CASE WHEN cnt>1 then pan_fidelius_token end) AS repeat_card
FROM card
GROUP BY best_available_merchant_token
;

----------saas product usage
CREATE OR REPLACE TABLE personal_karenwang.public.persona_saas_product_usage AS 
WITH 
  saas_product AS (  
    SELECT 
       mb.best_available_merchant_token
     , product_name
     , merchant_net_new_date  
     , RANK() OVER (PARTITION BY merchant_token ORDER BY merchant_net_new_date) AS rank
    FROM personal_karenwang.public.persona_merchant_attributes mb
    LEFT JOIN app_bi.app_bi_dw.dim_merchant_first_activity fa 
      ON mb.best_available_merchant_token=fa.merchant_token 
    WHERE product_category = 'SaaS' and merchant_net_new_date <> '9999-12-31'
    )

SELECT 
    best_available_merchant_token
  , MAX(CASE WHEN rank=1 THEN product_name END) AS first_saas_product
  , MAX(CASE WHEN rank=1 THEN merchant_net_new_date END) AS first_saas_date
  , MAX(CASE WHEN product_name='Payroll' THEN merchant_net_new_date END) AS payroll_net_new_date
  , MAX(CASE WHEN product_name='Appointments' THEN merchant_net_new_date END) AS appt_net_new_date
  , MAX(CASE WHEN product_name='Gift Cards' THEN merchant_net_new_date END) AS giftcard_net_new_date
  , MAX(CASE WHEN product_name='Marketing' THEN merchant_net_new_date END) AS marketing_net_new_date 
  , MAX(CASE WHEN product_name like '%Deposit%' THEN merchant_net_new_date END) AS deposit_net_new_date
  , MAX(CASE WHEN product_name='Team Management' THEN merchant_net_new_date END) AS tm_net_new_date
  , MAX(CASE WHEN product_name='Loyalty' THEN merchant_net_new_date END) AS loyalty_net_new_date
  , MAX(CASE WHEN product_name='Square Online Store' THEN merchant_net_new_date END) AS sos_net_new_date
  , MAX(CASE WHEN product_name NOT IN ('Payroll','Appointments','Gift Cards','Marketing','Team Management','Loyalty','Square Online Store') AND product_name NOT LIKE '%Deposit%' THEN merchant_net_new_date END) AS other_saas_net_new_date
FROM saas_product
GROUP BY 1
;

----------payment product usage
CREATE OR REPLACE TABLE personal_karenwang.public.persona_pmt_product_usage AS 
WITH 
  saas_product AS (  
    SELECT 
       mb.best_available_merchant_token
     , product_name
     , merchant_net_new_date  
     , RANK() OVER (PARTITION BY merchant_token ORDER BY merchant_net_new_date) AS rank
    FROM personal_karenwang.public.persona_merchant_attributes mb
    LEFT JOIN app_bi.app_bi_dw.dim_merchant_first_activity fa 
      ON mb.best_available_merchant_token=fa.merchant_token 
    WHERE product_category = 'Processing' and merchant_net_new_date <> '9999-12-31'
    )

SELECT 
    best_available_merchant_token
  , MAX(CASE WHEN rank=1 THEN product_name END) AS first_pmt_product
  , MAX(CASE WHEN product_name='Register Terminal' THEN merchant_net_new_date END) AS term_net_new_date
  , MAX(CASE WHEN product_name='Register POS' THEN merchant_net_new_date END) AS spos_net_new_date
  , MAX(CASE WHEN product_name='Invoices' THEN merchant_net_new_date END) AS invoices_net_new_date
  , MAX(CASE WHEN product_name='Virtual Terminal' THEN merchant_net_new_date END) AS vt_net_new_date 
  , MAX(CASE WHEN product_name NOT IN ('Register Terminal','Register POS','Invoices','Virtual Terminal') THEN merchant_net_new_date END) AS other_pmt_net_new_date
FROM saas_product
GROUP BY 1
;


-----------employee
CREATE OR REPLACE TABLE personal_karenwang.public.persona_employee AS 
SELECT 
    merchant_token AS best_available_merchant_token
  , COUNT(DISTINCT person_token) AS employees
FROM WEB.RAW_OLTP.EMPLOYEES
WHERE active = TRUE 
  AND deleted_at IS NULL
GROUP BY merchant_token
;

-----------applet usage
CREATE OR REPLACE TABLE personal_karenwang.public.persona_applet AS 
SELECT 
    mb.best_available_merchant_token
  , COUNT(CASE WHEN lower(data_rawdata:detail::string) = 'invoice' THEN 1 END) AS invoice_visits
  , COUNT(CASE WHEN lower(data_rawdata:detail::string) = 'transactionhistory' THEN 1 END) AS trx_visits
  , COUNT(CASE WHEN lower(data_rawdata:detail::string) = 'reports' THEN 1 END) AS reports_visits
  , COUNT(CASE WHEN lower(data_rawdata:detail::string) = 'balance' THEN 1 END) AS balance_visits
  , COUNT(CASE WHEN lower(data_rawdata:detail::string) = 'customers' THEN 1 END) AS customers_visits
  , COUNT(CASE WHEN lower(data_rawdata:detail::string) = 'items' THEN 1 END) AS items_visits
  , COUNT(CASE WHEN lower(data_rawdata:detail::string) = 'settings' THEN 1 END) AS settings_visits
  , COUNT(CASE WHEN lower(data_rawdata:detail::string) = 'help' THEN 1 END) AS support_visits
FROM personal_karenwang.public.persona_merchant_attributes mb
JOIN app_bi.pentagon.dim_user du
    ON mb.best_available_merchant_token=du.best_available_merchant_token    
JOIN eventstream1.events.all_events_alltime e
    ON du.user_token = e.subject_usertoken
    AND recorded_date >='2019-01-01'
    --AND recorded_date BETWEEN mb.signup_date AND DATEADD('day',180, mb.signup_date)
    AND eventvalue = 'Applet Switcher Selected Applet'
    AND eventname = 'Action'
GROUP BY 1
;

---------final table 
CREATE OR REPLACE TABLE personal_karenwang.public.persona_final AS 
SELECT 
    mb.best_available_merchant_token    
  , mb.signup_date
  , mb.source_application
  , mb.country_code
  , mb.device_os
  , mb.num_units
  , mb.business_category
  , mb.is_business_seller
  , mb.is_cbd_merchant
  , mb.is_referred
  , gpv_segment
  , employees
  , repeat_card/total_card                                                    AS repeat_card_pct
  , mb.product_intent_value
  , coalesce(mb.first_payment_product,first_pmt_product)                      AS first_payment_product
  , first_saas_product
  , mb.days_to_activation
  , mb.days_to_hardware_order
  , mb.days_to_bank_link
  , mb.days_to_onboard_completion
  , mb.days_to_first_app_event
  , mb.days_to_first_payment
  , DATEDIFF('DAY',signup_date,first_saas_date)                               AS days_to_saas  
  , DATEDIFF('DAY',signup_date,term_net_new_date)                             AS days_to_term
  , DATEDIFF('DAY',signup_date,spos_net_new_date)                             AS days_to_spos
  , DATEDIFF('DAY',signup_date,invoices_net_new_date)                         AS days_to_invoices
  , DATEDIFF('DAY',signup_date,vt_net_new_date)                               AS days_to_vt
  , DATEDIFF('DAY',signup_date,payroll_net_new_date)                          AS days_to_payroll
  , DATEDIFF('DAY',signup_date,marketing_net_new_date)                        AS days_to_marketing
  , DATEDIFF('DAY',signup_date,deposit_net_new_date)                          AS days_to_deposit
  , DATEDIFF('DAY',signup_date,giftcard_net_new_date)                         AS days_to_giftcard
  , DATEDIFF('DAY',signup_date,appt_net_new_date)                             AS days_to_appt
  , DATEDIFF('DAY',signup_date,tm_net_new_date)                               AS days_to_tm
  , DATEDIFF('DAY',signup_date,loyalty_net_new_date)                          AS days_to_loyalty
  , DATEDIFF('DAY',signup_date,sos_net_new_date)                              AS days_to_sos
  , CASE WHEN payment_tenure_days < 90 THEN '<3M'
         WHEN payment_tenure_days BETWEEN 90 AND 180 THEN '3M-6M'
         WHEN payment_tenure_days > 180 THEN '>6M'END                         AS tenure_group
  , active_days / NULLIF(payment_tenure_days,0)                               AS active_days_pct
  , pure_term_days / NULLIF(active_days,0)                                    AS pure_term_days_pct
  , hybrid_term_days / NULLIF(active_days,0)                                  AS hybrid_term_days_pct
  , total_payment
  , total_payment / NULLIF(active_days,0)                                     AS trx_per_active_day
  , payment_180d_since_signup
  , payment_180d_since_nna
  , ticket_size
  , refund_rate
  , spos_pmt / NULLIF(total_payment,0)                                        AS spos_pmt_pct
  , terminal_pmt / NULLIF(total_payment,0)                                    AS terminal_pmt_pct
  , vt_pmt / NULLIF(total_payment,0)                                          AS vt_pmt_pct
  , invoice_pmt / NULLIF(total_payment,0)                                     AS invoice_pmt_pct
  , cnp_pmt / NULLIF(total_payment,0)                                         AS cnp_pmt_pct
  , r12_pmt / NULLIF(total_payment,0)                                         AS r12_pmt_pct
  , x2_pmt / NULLIF(total_payment,0)                                          AS x2_pmt_pct
  , r4_pmt / NULLIF(total_payment,0)                                          AS r4_pmt_pct
  , t2_pmt / NULLIF(total_payment,0)                                          AS t2_pmt_pct
  , s1_pmt / NULLIF(total_payment,0)                                          AS s1_pmt_pct
  , r41_pmt / NULLIF(total_payment,0)                                         AS r41_pmt_pct
  , r6_pmt / NULLIF(total_payment,0)                                          AS r6_pmt_pct
  , terminal_pmt_3m / NULLIF(total_pmt_3m,0)                                  AS terminal_pmt_3m_pct
  , terminal_pmt_3m_6m / NULLIF(total_pmt_3m_6m,0)                            AS terminal_pmt_3m_6m_pct
  , terminal_pmt_6m / NULLIF(total_pmt_6m,0)                                  AS terminal_pmt_6m_pct
  , invoice_visits / NULLIF(days_to_first_payment + payment_tenure_days,0)    AS daily_invoice_visits
  , trx_visits / NULLIF(days_to_first_payment + payment_tenure_days,0)        AS daily_trx_visits
  , reports_visits / NULLIF(days_to_first_payment + payment_tenure_days,0)    AS daily_reports_visits
  , balance_visits / NULLIF(days_to_first_payment + payment_tenure_days,0)    AS daily_balance_visits
  , customers_visits / NULLIF(days_to_first_payment + payment_tenure_days,0)  AS daily_customers_visits
  , items_visits / NULLIF(days_to_first_payment + payment_tenure_days,0)      AS daily_items_visits
  , settings_visits / NULLIF(days_to_first_payment + payment_tenure_days,0)   AS daily_settings_visits
  , support_visits / NULLIF(days_to_first_payment + payment_tenure_days,0)    AS daily_support_visits

FROM personal_karenwang.public.persona_merchant_attributes mb
LEFT JOIN personal_karenwang.public.persona_card USING (best_available_merchant_token)
LEFT JOIN personal_karenwang.public.persona_gpv_segment USING (best_available_merchant_token)
LEFT JOIN personal_karenwang.public.persona_employee  USING (best_available_merchant_token)
LEFT JOIN personal_karenwang.public.persona_payment_summary  USING (best_available_merchant_token)
LEFT JOIN personal_karenwang.public.persona_saas_product_usage  USING (best_available_merchant_token)   
LEFT JOIN personal_karenwang.public.persona_pmt_product_usage USING (best_available_merchant_token)   
LEFT JOIN personal_karenwang.public.persona_applet USING (best_available_merchant_token) 
WHERE total_payment > 0
;
