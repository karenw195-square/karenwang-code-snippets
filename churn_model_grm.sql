/*this sample script is modified based on legacy code: https://docs.google.com/document/d/1zeO-sBny8D5fjPq_EdpHaE67O9XMI2kh1ub3OVidy5o/edit?usp=sharing*/
/*major changes*/
/* 1) change the training date from '2021-09-01' to '2022-05-01'*/
/* 2) change the country code inclusion rule from ('US', 'CA', 'JP') to US only*/
/* 3) get rid of the irrelevant subqueries "am_calls" and "churn_scores"*/
/* 4) change a few defining parameters (see comments in-line)*/

with merchant_sample_temp AS
(
SELECT
      du.best_available_unit_token as unit_token
    , du.merchant_token
    , du.country_code
    , lpd.user_token
    , lpd.first_payment_date
    , lpd.last_payment_date
    , TO_DATE('2022-05-01') AS training_date
    , lpd.day_count_to_date - 1 AS frequency
    , DATEDIFF('day',lpd.first_payment_date,lpd.last_payment_date) AS recency
    , DATEDIFF('day',lpd.first_payment_date,'2022-05-01'::DATE) AS time_active
    , DATEDIFF('day',lpd.last_payment_date,'2022-05-01'::DATE) AS days_since_last_payment
    , CASE WHEN fpd.future_payment_count > 0 THEN 0 ELSE 1 END AS is_churned
  FROM (
    SELECT
        asdps.unit_token AS user_token
      , MAX(asdps.payment_trx_recognized_date) AS last_payment_date
      , MIN(asdps.payment_trx_recognized_date) AS first_payment_date
      , SUM(asdps.card_payment_count) AS payment_count_to_date
      , COUNT(DISTINCT asdps.payment_trx_recognized_date) AS day_count_to_date
      , COUNT(DISTINCT DATE_TRUNC('week', payment_trx_recognized_date))
        / (DATEDIFF('week',MIN(DATE_TRUNC('week', payment_trx_recognized_date)),
        MAX(DATE_TRUNC('week', payment_trx_recognized_date))) + 1)
        AS f_weeks_active
    FROM APP_BI.PENTAGON.AGGREGATE_SELLER_DAILY_PAYMENT_SUMMARY asdps
    WHERE asdps.payment_trx_recognized_date <= '2022-05-01'::DATE
      AND asdps.card_payment_count > 0 -- consider only card payments
    GROUP BY asdps.unit_token
  ) lpd
  LEFT JOIN (
    SELECT
        asdps.unit_token AS user_token
      , SUM(asdps.card_payment_count) AS future_payment_count
    FROM APP_BI.PENTAGON.AGGREGATE_SELLER_DAILY_PAYMENT_SUMMARY asdps
    WHERE asdps.payment_trx_recognized_date > DATEADD('day', 28, '2022-05-01'::DATE)
      AND asdps.payment_trx_recognized_date <= DATEADD('day', 118, '2022-05-01'::DATE)
      AND asdps.card_payment_count > 0 -- consider only card payments
    GROUP BY asdps.unit_token
  ) fpd
  ON lpd.user_token = fpd.user_token
  JOIN APP_BI.PENTAGON.DIM_USER du
    ON lpd.user_token = du.user_token
  WHERE du.is_currently_frozen = 0
    AND du.is_currently_deactivated = 0
    AND du.country_code IN ('US')
    AND du.source_user_type = 'SQUARE SELLER'
    AND lpd.payment_count_to_date > 10 /*changed from 100 to 10*/
    AND lpd.first_payment_date < DATEADD( 'day', -14, '2022-05-01'::DATE) 
    AND lpd.first_payment_date >= DATEADD( 'day', -182, '2022-05-01'::DATE) /*added to only include younger cohorts*/
    AND lpd.f_weeks_active > 0.3 /*changed from 0.5 to 0.3 to include more CNP sellers who process less frequently*/
ORDER BY
  unit_token
)
-- Filter the above sample using a minimum 91-day profit threshold
SELECT
    ms.unit_token
  , ms.user_token
  , ms.merchant_token
  , ms.country_code
  , ms.first_payment_date
  , ms.last_payment_date
  , ms.training_date
  , ms.frequency
  , ms.recency
  , ms.time_active
  , ms.days_since_last_payment
  , ms.is_churned
  , profit.total_profit_91d AS profit_91d
FROM merchant_sample_temp ms
JOIN (
    SELECT
          COALESCE(rev.unit_token, fee.unit_token) AS unit_token
        , SUM(rev.net_revenue) - SUM(fee.total_fees) AS total_profit_91d
        , SUM(rev.net_gpv) AS total_net_gpv_91d
    FROM
        (
        SELECT
              du.unit_token
            , dps.report_date
            , SUM(dps.revenue_net_var_usd) AS net_revenue
            , SUM(dps.gpv_net_var_usd) AS net_gpv
        FROM merchant_sample_temp ms
        JOIN APP_BI.APP_BI_DW.DIM_UNIT du
          ON ms.unit_token = du.unit_token
        JOIN APP_BI.APP_BI_DW.VFACT_DAILY_PROCESSING_SUMMARY dps
          ON dps.key_unit = du.key_unit
        WHERE dps.report_date BETWEEN DATEADD('day', -91, ms.last_payment_date) AND ms.last_payment_date
        GROUP BY 1, 2
        ) rev
    FULL OUTER JOIN
        (
        SELECT
              du.unit_token
            , dfs.report_date
            , SUM(dfs.fee_total_var_usd) AS total_fees
        FROM merchant_sample_temp ms
        JOIN APP_BI.APP_BI_DW.DIM_UNIT du
          ON ms.unit_token = du.unit_token
        JOIN APP_BI.APP_BI_DW.VFACT_DAILY_FEE_SUMMARY dfs
          ON dfs.key_unit = du.key_unit
        WHERE dfs.report_date BETWEEN DATEADD('day', -91, ms.last_payment_date) AND ms.last_payment_date
        GROUP BY 1, 2
        ) fee
        ON rev.report_date = fee.report_date
        AND rev.unit_token = fee.unit_token
    GROUP BY 1
) profit 
ON ms.unit_token = profit.unit_token
WHERE profit.total_profit_91d > 20 /*changed from 200 to 20*/
  AND profit.total_net_gpv_91d * 4 > 5000 /*changed from 50000 to 5000*/
ORDER BY
  unit_token
;
