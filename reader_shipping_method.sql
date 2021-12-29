select page_click_description
, page_click_detail
, count(distinct subject_merchant_token) as clicks
, count(distinct case when m.first_card_payment_date <> '9999-09-09' then subject_merchant_token else null end) / clicks as nna_rate
from eventstream2.catalogs.page_click p
left join app_bi.pentagon.aggregate_merchant_lifetime_summary m on p.subject_merchant_token = m.merchant_token
where page_click_action = 'Signup'
and page_click_description = 'card_reader_option_selected'
--and page_click_detail = 'order-mail'
and u_recorded_date between current_date - 7 and current_date - 1
group by 1,2
order by 3 desc
;
