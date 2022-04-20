/*
owner: karenwang
last updated: 2022-04-15
description: this is a replacement of the deprecated ES1 table (last refresh on 2022-04-15).
*/

/*
table: app_payments.app_payments_analytics.es1_historical
description: this query builds a static table of all referral client-side event in ES1.
It is meant to be a replacement of the ES1 table that is deprecated soon:
    - subject_usertoken: ES1 user_token, could be a mix of merchant tokens and unit tokens.
    - eventvalue: ES1 eventvalue
    - eventname: ES1 eventname
    - source_application_type: ES1 app name (e.g. dashbaord, onboard, register-ios, register-android)
    - eventtype: curated type from eventvalue & eventname, to make the downstream query simpler
    - recorded_at: ES1 recorded_at. timestamp when the event was triggered.
*/
CREATE OR REPLACE TABLE app_payments.app_payments_analytics.es1_historical (
    subject_usertoken             VARCHAR(64)     NOT NULL
  , eventvalue                    VARCHAR(1000)
  , eventname                     VARCHAR(1000)
  , source_application_type       VARCHAR(64)
  , eventtype                     VARCHAR(64)
  , recorded_at                   TIMESTAMP
);


INSERT INTO app_payments.app_payments_analytics.es1_historical (
SELECT subject_usertoken
    , eventvalue
    , eventname
    , source_application_type
    , 'referral onboard web' as eventtype
    , recorded_at
FROM eventstream1.events.all_events_alltime
WHERE source_application_type = 'onboard'
AND (eventvalue = 'us_referrals'
      OR (eventname ILIKE '%Link%'
          AND (eventvalue ILIKE '%facebook%'
               OR eventvalue ILIKE '%twitter%'
               OR eventvalue ILIKE '%mail%'
              )
         )
    )
AND subject_usertoken IS NOT NULL
);

INSERT INTO app_payments.app_payments_analytics.es1_historical (
SELECT subject_usertoken
    , eventvalue
    , eventname
    , source_application_type
    , 'referral dashboard' as eventtype
    , recorded_at
FROM eventstream1.events.all_events_alltime
WHERE source_application_type = 'dashboard'
AND ((eventname = 'Click'
       AND eventvalue IN ('Checklist Action Item: Refer A Friend' /* -- check list click */
                          , 'Settings Navigation: Referrals' /* -- through settings */
                          , 'Global Navigation: Referrals' /* -- click through navbar */
                          , 'Settings Referrals: Twitter Share' /* -- and then share on Twitter */
                          , 'Settings Referrals: Facebook Share' /* -- and then shared on Facebook */
                          , 'Settings Referrals: Email Share' /* -- and then shared through Email client */
                         )
     )
   OR (eventname = 'View'
         AND eventvalue = 'Settings: Referrals'
      )
    )
AND subject_usertoken IS NOT NULL
);

INSERT INTO app_payments.app_payments_analytics.es1_historical (
SELECT subject_usertoken
    , eventvalue
    , eventname
    , source_application_type
    , 'referral onboard app' as eventtype
    , recorded_at
FROM eventstream1.events.all_events_alltime
WHERE source_application_type IN ('register-android','register-ios')
AND ((eventname = 'View'
      AND eventvalue = 'Onboard: Activation Flow Referrals'
     )
  OR (eventname = 'Action'
      AND eventvalue = 'Activation Referral Button'
     )
    )
AND subject_usertoken IS NOT NULL
);

INSERT INTO app_payments.app_payments_analytics.es1_historical (
SELECT subject_usertoken
    , eventvalue
    , eventname
    , source_application_type
    , 'referral app' as eventtype
    , recorded_at
FROM eventstream1.events.all_events_alltime
WHERE source_application_type IN ('register-android','register-ios')
AND ((eventname = 'View'
      AND eventvalue = 'Help Flow: Referral'
     )
  OR (eventname IN ('Tap','Action')
      AND eventvalue = 'Referral Button'
     )
    )
AND subject_usertoken IS NOT NULL
);
