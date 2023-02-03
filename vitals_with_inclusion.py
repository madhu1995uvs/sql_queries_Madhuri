# -*- coding: utf-8 -*-
"""
Created on Fri Feb  3 11:00:06 2023

@author: MUPPULURI
"""

# -*- coding: utf-8 -*-
"""
Created on Wed Jan 25 12:12:42 2023

@author: MUPPULURI
"""

# -*- coding: utf-8 -*-
"""
Created on Wed Jan 25 11:49:45 2023

@author: MUPPULURI
"""
# %%
import os 
direct=r"C:\Users\muppuluri\OneDrive - Children's National Hospital\Desktop\Madhuri\Current Projects\In Progress\My_Projects\AI Sepsis\sql\final\NEW"
os.chdir(direct)

#%%
"""IMPORT PACKAGES"""
import pandas as pd 
import pyodbc 
import numpy as np
import os
import re
from dateutil.parser import parse
from datetime import date
from sql_server_conn import sql_server_conn
import hashlib


# %%
conn = sql_server_conn()

start_date = '01/01/2021'
end_date = '02/01/2021'

date_range = parse(start_date).strftime("%m_%d_%Y") + '_to_' +     parse(end_date).strftime("%m_%d_%Y")
date_range

# %%
"""resp rate query"""
rr_sql  = """
-- update next line each new VS query
; with rr as (
	select PT_MRN as PatientId
	, PT_FIN as EncounterId
-- update next line each new VS query
	, RESP_RATE as VitalSignName
-- update next line each new VS query
	, RESP_RATE_RESULT as VitalSignValue
	, RESULT_DT_TM as CaptureTimestamp
	from ED_Vitals_Import_Master
-- update next line each new VS query
	where RESP_rate is not null
	and CHECKIN_DT_TM between ? and ?
	)
select * from rr
"""
rr = pd.read_sql(rr_sql,conn,params=[start_date,end_date])

# %%

pulse_sql  = """
; with pulse as (
	select PT_MRN as PatientId
	, PT_FIN as EncounterId
	, PULSE_RATE as VitalSignName
	, PULSE_RATE_RESULT as VitalSignValue
	, RESULT_DT_TM as CaptureTimestamp
	from ED_Vitals_Import_Master
	where pulse_rate is not null
	and CHECKIN_DT_TM between ? and ?
	)
select * from pulse
"""
pulse = pd.read_sql(pulse_sql,conn,params=[start_date,end_date])


# %%
"""temperature query"""
temp_sql  = """
; with temp as (
      select PT_MRN as PatientId
      ,PT_FIN as EncounterId
      ,[TEMPERATURE] as VitalSignName
      ,[TEMPERATURE_RESULT] as VitalSignValue
      ,[RESULT_DT_TM] as CaptureTimestamp
  from ED_Vitals_Import_Master
  where TEMPERATURE_RESULT is not null and CHECKIN_DT_TM between ? and ?
	)
select * from temp
"""
temp = pd.read_sql(temp_sql,conn,params=[start_date,end_date])

# %%
"""O2 sat query"""
O2sat_sql  = """
; with O2sat as (
      select PT_MRN as PatientId
      ,PT_FIN as EncounterId
      ,[OXYGEN_SAT] as VitalSignName
     ,[OXYGEN_SAT_RESULT] as VitalSignValue
      ,[RESULT_DT_TM] as CaptureTimestamp
  from ED_Vitals_Import_Master
  where OXYGEN_SAT_RESULT is not null and CHECKIN_DT_TM between ? and ?
	)
select * from O2sat
"""
O2sat = pd.read_sql(O2sat_sql,conn,params=[start_date,end_date])
# %%
"""SBP query"""
sbp_sql  = """
; with sbp as (
      select PT_MRN as PatientId
      ,PT_FIN as EncounterId
      ,[SYSBP] as VitalSignName
     ,[SYSBP_RESULT] as VitalSignValue
      ,[RESULT_DT_TM] as CaptureTimestamp
  from ED_Vitals_Import_Master
  where SYSBP_RESULT is not null and CHECKIN_DT_TM between ? and ?
	)
select * from sbp
"""
sbp = pd.read_sql(sbp_sql,conn,params=[start_date,end_date])
# %%
"""DBP query"""
dbp_sql  = """
; with dbp as (
      select PT_MRN as PatientId
      ,PT_FIN as EncounterId
      ,[DIABP] as VitalSignName
     ,[DIABP_RESULT] as VitalSignValue
      ,[RESULT_DT_TM] as CaptureTimestamp
  from ED_Vitals_Import_Master
  where DIABP_RESULT is not null and CHECKIN_DT_TM between ? and ?
	)
select * from dbp
"""
dbp = pd.read_sql(dbp_sql,conn,params=[start_date,end_date])
# %%
"""append other VS to pulse, then order by mrn, fin, then deidentify"""
all_vitals = pd.concat([rr,pulse],ignore_index=True)
all_vitals = pd.concat([all_vitals,temp],ignore_index=True)
all_vitals = pd.concat([all_vitals,O2sat],ignore_index=True)
all_vitals = pd.concat([all_vitals,sbp],ignore_index=True)
all_vitals = pd.concat([all_vitals,dbp],ignore_index=True)
# create new line of code to concat each new vs table
# all_vitals = pd.concat([all_vitals,newvs],ignore_index=True)

all_vitals = all_vitals.sort_values(by=['PatientId','EncounterId'])

# %%
"""inclusion criteria"""

"""use basic demographic data from tat table"""

inclusion_sql  = """
; with tat as 
	(
	SELECT distinct patient_fin
	,PATIENT_MRN
	, datediff(MONTH, pt_dob, checkin_date_time) age_mo
	,TRIAGE_DATE_TIME
    from ED_TAT_MASTER
    where checkin_date_time between ? and ?)


--cte_purpose: create a variable for transfers from another hospital

, notes as (
       select pt_fin as notes_fin
       , osh = '1'
       from ED_NOTES_MASTER
       where RESULT_TITLE_TEXT like '%prearriva%'
       and (result like '%Referring Facility%Referring Facility%'
              or result not like '%Referring Facility (unlisted)%'
              )
       )
--cte_purpose: create variable for sepsis_treatment from bolus
,bolus as (select
  PT_FIN as bolus_pt_fin
  ,bolus_GIVEN_DT_TM_1 = max(case when RN=1 then [GIVEN_DT_TM] end)
  ,bolus_GIVEN_DT_TM_2 = max(case when RN=2 then [GIVEN_DT_TM] end)
  ,bolus_GIVEN_DT_TM_3 = max(case when RN=3 then [GIVEN_DT_TM] end)
  From  (Select *,RN=Row_Number() over (Partition By PT_FIN Order by [GIVEN_DT_TM],ORDERED_AS_MNEMONIC)
	From ED_Orders_Import_Master
  where GIVEN_DT_TM is not null 
  and ORDERED_AS_MNEMONIC like '%bolus%') bolus
  Group by PT_FIN) 

--cte_purpose: create variable for 2 IV boluses
,boluses as ( select *
  , two_bolus = '1'
   from bolus
  where (bolus_GIVEN_DT_TM_2 is not null and bolus_GIVEN_DT_TM_3 is null))

--cte_purpose: create variable for sepsis_treatment from antibiotics
,antibiotics as (select
  PT_FIN as ab_pt_fin
  ,GIVEN_DT_TM as ab_GIVEN_DT_TM
  ,abx = '1'
  From ED_Orders_Import_Master
  where GIVEN_DT_TM is not null 
  and (ORDERED_AS_MNEMONIC like '%ceftriaxone%' or ORDERED_AS_MNEMONIC like '%vancomycin%' or ORDERED_AS_MNEMONIC like '%ampicillin%'
   or ORDERED_AS_MNEMONIC like '%ceftazidime%' or ORDERED_AS_MNEMONIC like '%cefepime%' or ORDERED_AS_MNEMONIC like '%levofloxacin%'
    or ORDERED_AS_MNEMONIC like '%meropenem%' or ORDERED_AS_MNEMONIC like '%tobramycin%' or ORDERED_AS_MNEMONIC like '%gentamicin%' 
	or ORDERED_AS_MNEMONIC like '%metronidazole%' or ORDERED_AS_MNEMONIC like '%azithromycin%' or ORDERED_AS_MNEMONIC like '%acyclovir%'
		or ORDERED_AS_MNEMONIC like '%clindamycin%' or ORDERED_AS_MNEMONIC like '%doxycycline%')
		and (CATALOG_TYPE='Pharmacy') AND (ORDER_MNEMONIC Not Like '%ointment%' AND ORDERED_AS_MNEMONIC Not Like '%ointment%') AND (ORIG_ORD_AS 
Not Like '%Home Meds%' And ORIG_ORD_AS Not Like '%Pharmacy Charge%') AND (RX_ROUTE Not Like '%oph%' And RX_ROUTE 
Not Like '%top%' And RX_ROUTE Not Like '%eye%' And RX_ROUTE Not Like '%ear%' And RX_ROUTE Not Like '%otic%'))

--cte_purpose: create variable for sepsis_treatment from two bolus and antibiotics
,sepsis_treatment as (
select * 
,seps_tx='1'
from boluses
left outer join antibiotics on boluses.bolus_pt_fin=antibiotics.ab_pt_fin
where two_bolus = '1' and abx = '1')

--cte_purpose: create variable for blood culture
,blood_culture as (select
  PT_FIN as cx_fin
  ,bcx = '1'
  ,ORIG_ORDER_DT_TM as culture_dt_tm
  From ED_Orders_Import_Master
       where ORIG_ORDER_DT_TM is not null
		and  ORDER_MNEMONIC like '%blood cul%'
		and (ORDER_STATUS not like '%canceled%'
			and ORDER_STATUS not like '%deleted%'
			and ORDER_STATUS not like '%on hold%')
       )

--cte_purpose: create variable for Positive sepsis screen
, screen as (
       select fin as screen_fin
       , screen_pos = '1'
       from sepsis_order_import
       where POTENTIAL_SEPSIS_ORDER_STATUS like '%comp%'
       )


,final as (
		select *
        --,redcap_repeat_instance=Row_Number() over (Partition By patient_fin Order by [RESULT_DT_TM])
		from tat
			left outer join notes on tat.patient_fin = notes.notes_fin 
			left outer join sepsis_treatment on tat.patient_fin = sepsis_treatment.bolus_pt_fin 
			left outer join blood_culture on tat.patient_fin = blood_culture.cx_fin 
			left outer join screen on  tat.patient_fin = screen_fin
				where datediff (hour,TRIAGE_DATE_TIME,culture_dt_tm)<6
                and datediff (hour,TRIAGE_DATE_TIME,bolus_GIVEN_DT_TM_2)<6
                and datediff (hour,TRIAGE_DATE_TIME,ab_GIVEN_DT_TM)<6
				and osh is null
				and (seps_tx='1' Or (screen_pos = '1' and bcx = '1'))
				and age_mo between 3 and 216)

select *
from final
"""

inclusion= pd.read_sql(inclusion_sql,conn,params=[start_date,end_date])

# %%
"""merge vital sign data with included patients"""
# see documentation here: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.merge.html
final_vitals = inclusion.merge(all_vitals,how='left',left_on='patient_fin',right_on='EncounterId')



# %%
#de-ID FIN and mrn
def shash(val):
    h = hashlib.sha256()
    vb = bytes(val, 'utf-8')
    h.update(vb)
    return h.digest()

final_vitals['PatientId_v'] = final_vitals['PatientId'].astype(str)
final_vitals['PatientId_v'] = final_vitals['PatientId_v'].apply(shash)

final_vitals['EncounterId_v'] = final_vitals['EncounterId'].astype(str)
final_vitals['EncounterId_v'] = final_vitals['EncounterId_v'].apply(shash)

# %%
from pandas.tseries.offsets import DateOffset
final_vitals['CaptureTimestamp1']=final_vitals['CaptureTimestamp']+DateOffset(days=14)

# %%
#"""create date offset for deidentification"""
#final_vitals['CaptureTimestamp'] = 'rq_date_offset'
final_vitals = final_vitals[['PatientId_v', 'EncounterId_v', 'VitalSignName',
       'VitalSignValue', 'CaptureTimestamp1']]

# %%

csv_path = os.getcwd()
csv_filename = f'vitals_{date_range}.csv'
csv_pathfile = csv_path + "\\" + csv_filename

# %%
final_vitals.to_csv(f'vitals_{date_range}.csv',index=False)

# %%
filename = f"vitals_trial.xlsx"
writer = pd.ExcelWriter(filename, engine='xlsxwriter')
final_vitals.to_excel(writer,float_format="%.0f", index=False)
writer.save()
# %%