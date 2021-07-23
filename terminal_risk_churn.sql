---------------------------------------------------------------------------------------------------------------
--v3 incorporating changes 7/22/2021
---------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE personal_karenwang.PUBLIC.TERMINAL_CASING_v2 AS
(
--Active Terminal Seller who has had 'Register Terminal' GPV in 2021.
--And has had no other transactions in the past 91 days
SELECT
dps.merchant_token
, du.business_category
, SUM(dps.volume_gross_var_usd) AS gpv_2021
, CASE WHEN dps.merchant_token NOT IN
        (
            SELECT DISTINCT dps.merchant_token
            FROM app_bi.app_bi_dw.vfact_merchant_revenue_summary dps
            WHERE dps.report_date >= '2021-1-1' --make sure sellers have no non-terminal payments in last 91 days either
            AND dps.product_name != 'Register Terminal'
            AND dps.product_category = 'Processing'
        )
        THEN true ELSE false
    END AS register_terminal_only
, MAX(dps.report_date) as last_trx_dt
FROM app_bi.app_bi_dw.vfact_merchant_revenue_summary dps
  LEFT JOIN app_bi.pentagon.dim_user du on dps.merchant_token = du.user_token and du.user_type = 'MERCHANT'
WHERE dps.product_name = 'Register Terminal'
AND dps.report_date >= '2021-1-1'
AND du.user_token NOT IN 
    (
    SELECT DISTINCT dps.merchant_token
    FROM app_bi.app_bi_dw.vfact_merchant_revenue_summary dps
    WHERE dps.report_date > CURRENT_DATE - 91
    )
GROUP BY 1, 2
HAVING gpv_2021 > 1
)
;

CREATE OR REPLACE TABLE personal_karenwang.PUBLIC.terminal_risk AS
WITH
usr AS --get user create date
(
    SELECT du.user_token as merchant_token
    , du.user_created_at_date as merchant_created_date
    , min(date(dufa.merchant_first_activity_date)) AS merchant_first_activity_date
    FROM app_bi.pentagon.dim_user du
    LEFT JOIN app_bi.app_bi_dw.dim_merchant_first_activity dufa
        ON du.user_token = dufa.merchant_token
    WHERE du.user_token IN 
        (
        SELECT temp.merchant_token
        FROM personal_karenwang.PUBLIC.TERMINAL_CASING_v2 temp
        )
    AND du.user_type = 'MERCHANT'
    GROUP BY du.user_token, du.user_created_at_date
),
gpv AS --get user segment at last trx date
(
    SELECT merchant_token, effective_begin, effective_end, merchant_segment
    FROM app_bi.app_bi_dw.dim_merchant_gpv_segment
    WHERE merchant_token IN 
        (
        SELECT merchant_token
        FROM personal_karenwang.PUBLIC.TERMINAL_CASING_v2
        )
),
has_any_case AS --get any case count per user during w/i 30 days from last trx, last case date, case type, and status
(
    SELECT du.best_available_merchant_token AS merchant_token
    , a.case_count_any
    , date(a.last_case_dt_any) AS last_case_dt_any
    , "group" AS last_case_group_any
    , case_type AS last_case_type_any
    , subroute AS last_case_subroute_any
    , deactivated AS last_case_deactivated_any
    , frozen AS last_case_frozen_any
    FROM app_risk.app_risk.fact_risk_cases frc
    LEFT JOIN app_bi.pentagon.dim_user du on frc.user_token = du.user_token
    LEFT JOIN
         (
         SELECT du.best_available_merchant_token
         , count(case_id) AS case_count_any
         , max(created_at) AS last_case_dt_any
         FROM app_risk.app_risk.fact_risk_cases frc
         LEFT JOIN app_bi.pentagon.dim_user du on frc.user_token = du.user_token
         LEFT JOIN personal_karenwang.PUBLIC.TERMINAL_CASING_v2 t ON du.best_available_merchant_token = t.merchant_token
         WHERE du.best_available_merchant_token IN 
            (
                SELECT merchant_token
                FROM personal_karenwang.PUBLIC.TERMINAL_CASING_v2
            )
         AND date(created_at) BETWEEN last_trx_dt - 30 AND last_trx_dt + 30
         GROUP BY du.best_available_merchant_token) AS a
    ON a.best_available_merchant_token = du.best_available_merchant_token AND a.last_case_dt_any = frc.created_at
),
has_risk_case AS --get risk only case count per user during w/i 30 days from last trx, last case date, case type, and status
(
    SELECT du.best_available_merchant_token AS merchant_token
    , b.case_count_risk
    , date(b.last_case_dt_risk) AS last_case_dt_risk
    , "group" AS last_case_group_risk
    , case_type AS last_case_type_risk
    , subroute AS last_case_subroute_risk
    , deactivated AS last_case_deactivated_risk
    , frozen AS last_case_frozen_risk
    FROM app_risk.app_risk.fact_risk_cases frc
    LEFT JOIN app_bi.pentagon.dim_user du on frc.user_token = du.user_token
    LEFT JOIN
         (
          SELECT du.best_available_merchant_token
          , count(case_id) AS case_count_risk
          , max(created_at) AS last_case_dt_risk
          FROM app_risk.app_risk.fact_risk_cases frc
          LEFT JOIN app_bi.pentagon.dim_user du on frc.user_token = du.user_token
          LEFT JOIN personal_karenwang.PUBLIC.TERMINAL_CASING_v2 t ON du.best_available_merchant_token = t.merchant_token
          WHERE du.best_available_merchant_token IN 
            (
                SELECT merchant_token
                FROM personal_karenwang.PUBLIC.TERMINAL_CASING_v2
            )
            AND date(created_at) BETWEEN last_trx_dt - 30 AND last_trx_dt + 30
            AND "group" NOT IN ('compliance', 'chargebacks', 'finance', 'underwriting') --ex non-risk cases
            AND subroute NOT LIKE ('%requeue%') --ex user review requeues
            GROUP BY du.best_available_merchant_token) AS b
    ON b.best_available_merchant_token = du.best_available_merchant_token AND b.last_case_dt_risk = frc.created_at
),
user_taxonomy AS --get cahrgeback count by type classification
(
    SELECT du.best_available_merchant_token AS merchant_token
        , SUM(CASE WHEN taxonomy_category_name = 'Fake Business Known Fraud' THEN 1 ELSE 0 END) AS fake_account
        , SUM(CASE WHEN taxonomy_category_name = 'Good Merchant Gone Bad' THEN 1 ELSE 0 END) AS gmgb
        , SUM(CASE WHEN taxonomy_category_name = 'ATO Fraud' THEN 1 ELSE 0 END) AS ato
        , SUM(CASE WHEN taxonomy_category_name = 'Buyer Fraud' THEN 1 ELSE 0 END) AS buyer_fraud
        , SUM(CASE WHEN taxonomy_category_name = 'Credit Risk' THEN 1 ELSE 0 END) AS credit_risk
        , SUM(CASE WHEN taxonomy_category_name = 'Chargeback In-Line' THEN 1 ELSE 0 END) AS chargeback_inline
        , SUM(CASE WHEN taxonomy_category_name is null THEN 1 ELSE 0 END) AS none
    FROM app_risk.app_risk.chargebacks c
    LEFT JOIN app_bi.pentagon.dim_user du on c.user_token = du.user_token
    WHERE du.best_available_merchant_token IN 
    (
        SELECT merchant_token
        FROM personal_karenwang.PUBLIC.TERMINAL_CASING_v2
    )
    AND chargeback_date >= '2021-01-01'
    GROUP BY du.best_available_merchant_token
)
SELECT primary.*
    , usr.merchant_created_date
    , usr.merchant_first_activity_date
    , CASE
        WHEN last_trx_dt - merchant_first_activity_date > 456 THEN true ELSE false
        END AS IS_MATURE
    , gpv.merchant_segment
    , has_any_case.case_count_any
    , has_any_case.last_case_dt_any
    , has_any_case.last_case_group_any
    , has_any_case.last_case_type_any
    , has_any_case.last_case_subroute_any
    , has_any_case.last_case_deactivated_any
    , has_any_case.last_case_frozen_any
    , has_risk_case.case_count_risk
    , has_risk_case.last_case_dt_risk
    , has_risk_case.last_case_group_risk
    , has_risk_case.last_case_type_risk
    , has_risk_case.last_case_subroute_risk
    , has_risk_case.last_case_deactivated_risk
    , has_risk_case.last_case_frozen_risk
    , user_taxonomy.fake_account
    , user_taxonomy.gmgb
    , user_taxonomy.ato
    , user_taxonomy.buyer_fraud
    , user_taxonomy.credit_risk
    , user_taxonomy.chargeback_inline
    , user_taxonomy.none
FROM personal_karenwang.PUBLIC.TERMINAL_CASING_v2 primary
LEFT JOIN usr ON primary.merchant_token = usr.merchant_token
LEFT JOIN gpv ON primary.merchant_token = gpv.merchant_token
    AND (last_trx_dt BETWEEN gpv.effective_begin AND gpv.effective_end)
LEFT JOIN has_any_case ON primary.merchant_token = has_any_case.merchant_token
LEFT JOIN has_risk_case ON primary.merchant_token = has_risk_case.merchant_token
LEFT JOIN user_taxonomy ON primary.merchant_token = user_taxonomy.merchant_token
ORDER BY gpv_2021 DESC
;
