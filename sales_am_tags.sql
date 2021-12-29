CREATE OR REPLACE TABLE personal_karenwang.public.merchant_with_am_sales_tag AS
WITH squareup_merchants AS (
    SELECT DISTINCT du.best_available_merchant_token
    FROM app_bi.pentagon.dim_user du
), am_managed AS (
    SELECT DISTINCT merchant_token
    FROM (
      SELECT
        du.merchant_token
      , bob.owner_name AS bob_owner_name
      , bob.program AS bob_program
      , quarter AS bob_quarter
      , at.appointments_link
      , ROW_NUMBER() OVER(PARTITION BY du.merchant_token ORDER BY quarter DESC) AS rn
      FROM app_merch_growth.app_merch_growth_etl.sbs_book_of_business bob
      JOIN app_bi.pentagon.dim_user du
        ON bob.user_token = du.user_token
        AND du.is_merchant = 1
      LEFT JOIN app_merch_growth.app_merch_growth_etl.am_table at
             ON bob.owner_id = at.sfdc_owner_id
      WHERE bob.quarter = YEAR(CURRENT_DATE()) || ' Q' || QUARTER(CURRENT_DATE()) /* Most up-to-date list */
        AND bob.program IS NOT NULL
        AND bob.treatment_control = 'Treatment'
        AND bob.owner_name IS NOT NULL
        AND bob.owner_name != 'Jade Batstone'
        AND bob.program NOT IN ('MVS')
    )
    WHERE rn = 1 -- dedupe / get the  most recent
), sales_closed AS (
    SELECT
      du.merchant_token
    , MIN(cd.close_date) AS sales_close_date
    FROM app_sales.sales_comp.closed_deals cd
    JOIN app_sales.app_sales_etl.sfdc_square_accounts sa
      ON sa.sf_object_id = cd.opportunity_id
    JOIN app_bi.app_bi_dw.dim_user du
      ON du.user_token = sa.user_token
    WHERE 1=1
      AND cd.sales_segment IN ('Sales','Strat', 'Enterprise', 'Field Sales')
    GROUP BY 1
)
SELECT s.BEST_AVAILABLE_MERCHANT_TOKEN
, am.MERCHANT_TOKEN as is_account_managed
, sc.MERCHANT_TOKEN as is_sales_closed
, sc.SALES_CLOSE_DATE
FROM squareup_merchants s
LEFT JOIN am_managed am
       ON s.best_available_merchant_token = am.merchant_token
LEFT JOIN sales_closed sc
       ON s.best_available_merchant_token = sc.merchant_token
;

CREATE OR REPLACE TABLE personal_karenwang.public.merchant_token_w_processing AS
select mast.*
, ufmf.business_category
, sum(case when mrs.report_date between dateadd('day',-91,current_date) and current_date then volume_net_var_usd else 0 end) as last_91d_gpv
, sum(case when mrs.report_date between dateadd('day',-30,current_date) and current_date then volume_net_var_usd else 0 end) as last_30d_gpv
, sum(case when mrs.product_name = 'Register Terminal' and mrs.report_date between dateadd('day',-91,current_date) and current_date then volume_net_var_usd else 0 end) as last_91d_term_gpv
, sum(case when mrs.product_name = 'Register Terminal' and mrs.report_date between dateadd('day',-30,current_date) and current_date then volume_net_var_usd else 0 end) as last_30d_term_gpv

from personal_karenwang.public.merchant_with_am_sales_tag mast
join APP_PAYMENTS.APP_PAYMENTS_ANALYTICS.unifying_funnel_merchant_facts ufmf on mast.BEST_AVAILABLE_MERCHANT_TOKEN = ufmf.BEST_AVAILABLE_MERCHANT_TOKEN
join app_bi.app_bi_dw.vfact_merchant_revenue_summary mrs on mast.BEST_AVAILABLE_MERCHANT_TOKEN = mrs.merchant_token and mrs.product_category = 'Processing'
group by 1,2,3,4,5
;

select mtp.*
, datediff('day',ufmf.ACTIVATION_SUCCESS_DATE,current_date) as days_since_activation
, mrs.product_name
, sum(case when mrs.report_date between dateadd('day',-91,current_date) and current_date then volume_net_var_usd else 0 end) as last_91d_non_processing_gpv
from personal_karenwang.public.merchant_token_w_processing mtp
join APP_PAYMENTS.APP_PAYMENTS_ANALYTICS.unifying_funnel_merchant_facts ufmf on mtp.BEST_AVAILABLE_MERCHANT_TOKEN = ufmf.BEST_AVAILABLE_MERCHANT_TOKEN
join app_bi.app_bi_dw.vfact_merchant_revenue_summary mrs on mtp.BEST_AVAILABLE_MERCHANT_TOKEN = mrs.merchant_token and mrs.product_category <> 'Processing'
where last_91d_gpv > 250000/4
and last_30d_term_gpv > 0
and last_91d_gpv = last_91d_term_gpv
and country_code = 'US'
group by 1,2,3,4,5,6,7,8,9,10,11

