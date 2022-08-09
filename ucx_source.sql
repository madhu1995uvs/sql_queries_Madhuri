USE EMTCQIData

DECLARE @Start Date
DECLARE @End Date

SET @Start = '01/01/21'
SET @End = '01/01/22'

--cte_purpose: create dummy variables for source, including 'CleanCatch', 'Cath', 'Bagged'
; with ucx_source as
	(
	select pt_fin as pt_fin_ucx_source
	, orig_order_dt_tm
	, CATALOG_TYPE
    , case when order_detail like '%clean%' then 'CleanCatch'
      when order_detail like '%cath%' Then 'Cath'
      when ORDER_DETAIL like '%bag%' then 'Bagged'
      else order_detail
      end Source
  from Adm_Orders_DC_Import
  where ORDER_MNEMONIC like '%urine culture%' and CATALOG_TYPE like '%lab%'
--and ORIG_ORDER_DT_TM between @start and @End
	)

select *
from ucx_source
where ORIG_ORDER_DT_TM between @start and @End