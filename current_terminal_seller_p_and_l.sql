/*payment attribution flow chart: https://docs.google.com/drawings/d/1j0z-W6zSG73oPrm41F-0djFGvmhhp0QUp99r_49wPk8/edit*/
select distinct merchant_token
from app_bi.app_bi_dw.vfact_merchant_revenue_summary
where product_name = 'Register Terminal'
and report_date >= current_date - 30
