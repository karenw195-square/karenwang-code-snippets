-- US; US-en or US-es
-- Made first payment 91-182 days ago
-- Not currently frozen or deactivated

with active_accounts as (
select distinct unit.best_available_merchant_token as merchant_token
, unit.user_token as unit_token
from app_bi.pentagon.dim_user unit
left join app_bi.pentagon.dim_user merchant 
     on unit.best_available_merchant_token = merchant.user_token 
     and merchant.user_type = 'MERCHANT'
where unit.user_type = 'UNIT'
    and unit.is_currently_frozen = 0
    and unit.is_currently_deactivated = 0
    and unit.unit_active_status = TRUE
    and merchant.is_deleted = 0
    and merchant.country_code = 'US'
    and merchant.preferred_language_code in ('en-US','es-US') 
)

select distinct amls.merchant_token
, aa.unit_token
from app_bi.pentagon.aggregate_merchant_lifetime_summary amls 
inner join active_accounts aa on amls.merchant_token = aa.merchant_token
where first_card_payment_date between current_date -182 and current_date - 91
