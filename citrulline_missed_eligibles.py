#citrulline eligibility data
#Kenneth McKinley author
#update 9.23.2022

import pandas as pd
import os
import time
#import datetime
from datetime import date, timedelta
from sql_server_conn import sql_server_conn
from dateutil.parser import parse
import email_jc as em


#error_recips = "kmckinley@childrensnational.org"


conn = sql_server_conn()
today = date.today()
start_date = '01/01/2021'  # str(today - timedelta(days=10))
end_date = str(today - timedelta(days=3))
date_range = parse(start_date).strftime("%m_%d_%Y") + '_to_' + \
    parse(end_date).strftime("%m_%d_%Y")


cit_sql = """
; with tat as 
	(
	SELECT distinct patient_fin
	, PATIENT_MRN
	, CHECKIN_DATE_TIME
	, datediff(month, pt_dob, checkin_date_time) age_months
	, TRACK_GROUP
	, PT_GENDER
	, PT_DISCH_DISPO
	, PT_DX1
    , PT_DX2
    , PT_DX3
    , REASON_FOR_VISIT

	from ED_TAT_MASTER
	)

, bill as
	(
	select distinct patient_identification, 
	concat (',',LTRIM(ICD1),',',LTRIM(ICD2),',',ltrim(ICD3),',',ltrim(ICD4),',',ltrim(ICD5),',',ltrim(ICD6),',',ltrim(ICD7),',',ltrim(ICD8),',',ltrim(ICD9),','
	,ltrim(ICD10),',',ltrim(ICD11),',',ltrim(ICD12),',',ltrim(ICD13),',',ltrim(ICD14),',',ltrim(ICD15)) ALLICD
	from ED_BILLING_DATA_Master
	)

, vs as
	(
	select PT_FIN, max(temperature_result) as max_temp
	from ED_vitals_import_master
	where TEMPERATURE_RESULT is not null
	group by pt_fin
	)

, opioids as
	(
	select *, opioid = 1
	from (
		select distinct pain_med.pt_fin, pain_med.ORDER_MNEMONIC, pain_med.ORDERED_AS_MNEMONIC, pain_med.ORDER_STATUS, pain_med.ORDERED_DT_TM
		, row_number () over (partition by pain_med.pt_fin order by pain_med.ordered_dt_tm) as rn
		from ED_Orders_Import_Master pain_med
		where order_status like '%complete%'
		and (ORDER_MNEMONIC like '%fentanyl%'
			or ORDER_MNEMONIC like '%morphine%'
			or ORDER_MNEMONIC like '%hydromorph%')
		) tb1
		where rn=1
	)

select *
, case when age_months between 72 and 264 then '1' else '0' end age_eligible
, case when max_temp > 38.4 then '0' else '1' end no_fever
, case 
	when datepart(hour,CHECKIN_DATE_TIME) < 8 then 'checkin_pre_8a' 
	when datepart(hour,CHECKIN_DATE_TIME) > 16 then 'checkin_post_5p'
	else 'arrival_during_working_hours'
	end arrival_block

from tat
--inner join BILL on BILL.PATIENT_IDENTIFICATION = TAT.PATIENT_FIN
left outer join vs on tat.patient_fin = vs.pt_fin
left outer join opioids on tat.patient_fin = opioids.pt_fin

where CHECKIN_DATE_TIME between @start and @end
and (REASON_FOR_VISIT like '%scd%'
	or REASON_FOR_VISIT like '%sickle%'
	or REASON_FOR_VISIT like '%sickel%'
	or REASON_FOR_VISIT like '%sikcle%'
	or REASON_FOR_VISIT like '%ssd%'
	or REASON_FOR_VISIT like '%sc pain%'
    or concat(pt_dx1,pt_dx2,pt_dx3) like '%sickle%' 
    or concat(pt_dx1,pt_dx2,pt_dx3) like '%occlusive%' 
    or concat(pt_dx1,pt_dx2,pt_dx3) like '%Hb-SS%')
and TRACK_GROUP not like 'edu%'
--and ALLICD like '%,D57%' and ALLICD not like '%,D57.3%'

order by CHECKIN_DATE_TIME
"""

cit = pd.read_sql(cit_sql, conn, params=[start_date, end_date])