# -*- coding: utf-8 -*-
"""
Created on Wed Sep  7 10:13:51 2022

@author: kmckinley
"""

#%%
"""IMPORT PACKAGES"""
import pandas as pd 
import pyodbc 
import numpy as np
import os
from dateutil.parser import parse
from datetime import date

#from sql_server_conn import sql_server_conn
#conn = sql_server_conn()


conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                      'Server=ENTSQL01LSNR;'
                      'Database=EMTCQIData;'
                      'Trusted_Connection=yes;')

"""
start_date = '01/01/2018'
end_date = '04/01/2021'

date_range = parse(start_date).strftime("%m_%d_%Y") + '_to_' +     parse(end_date).strftime("%m_%d_%Y")
date_range
"""


#%%
"""QUERY ED REGISTRY TO OBTAIN DEMOGRAPHIC AND CLINICAL DATA FOR PATIETNS WITH UTI DIAGNOSIS"""
uti_sql = """

DECLARE @Start Date
DECLARE @End Date

SET @Start = '01/01/18'
SET @End = '04/01/21'

/* Patient, provider charecterstics and visist variables for measuring Adherence 
to First-Line Antimicrobial Treatment for Presumed Urinary Tract Infections in
the Emergency Department

Written by ED Data Analytics Team
Last updated 09/07/22 */

--Common table expressions

; with TAT as 
       (
       select distinct 
              PATIENT_FIN
              ,PATIENT_MRN
              , PT_GENDER
              ,PT_RACE
              ,PT_ETHNICITY
			  ,PT_DOB
              ,datediff(month,PT_DOB, CHECKIN_DATE_TIME) age_months
              ,FIRST_MD_SEEN
              ,LAST_ASSIGNED_MD
              ,REASON_FOR_VISIT
              ,PT_DX1
              ,PT_DX2
              ,PT_DX3
              , left(PT_ACUITY,1) ESI
              ,dispo_date_time
              ,CHECKIN_DATE_TIME
		
		
		,case when PT_gender like '%M%' then '0'
		when PT_gender like '%F%' then '1'
		end gender_cat

		,case when PT_RACE like '%hispanic%' then '0'
		when PT_RACE like '%Indian%' then '1'
		when (PT_RACE like '%asian%') and (PT_RACE not like '%Caucasian%') then '2'
		when PT_RACE like '%black%' then '3'
		when PT_RACE like '%Hawaiian%' then '4'
		when PT_RACE like '%Caucasian%' then '5'
		else '6' end race_cat
		
/*
0=Hispanic or Latino
1=American Indian or Alaska Native
2=Asian
3=Black or African American
4=Native Hawaiian or Other Pacific Islander
5=White
6=Other/Unknown
*/
		, case when (PT_ETHNICITY like '%hisp%' or PT_ETHNICITY like '%Lat%')
		and (PT_ETHNICITY not like '%not%') and (PT_ETHNICITY not like '%non%') then '1'
		else '0'
		end Ethnicity_cat

	
		, case when FIRST_MD_SEEN like '%Abdulrahman,E%' or FIRST_MD_SEEN like '%Abo,A%' or FIRST_MD_SEEN like '%Agrawal,D%' 
		or FIRST_MD_SEEN like '%Ahluwalia,T%' or FIRST_MD_SEEN like '%Atabaki,S%' or FIRST_MD_SEEN like '%Batabyal,R%' 
		or FIRST_MD_SEEN like '%Berkowitz,D%' or FIRST_MD_SEEN like '%Breslin,K%' or FIRST_MD_SEEN like '%Brown,K%' 
		or FIRST_MD_SEEN like '%Button,K%' or FIRST_MD_SEEN like '%Chamberlain,J%' or FIRST_MD_SEEN like '%Chapman,J%' 
		or FIRST_MD_SEEN like '%Combs,S%' or FIRST_MD_SEEN like '%Donnelly,K%' or FIRST_MD_SEEN like '%Freishtat�,R%' 
		or FIRST_MD_SEEN like '%Goodwin,T%' or FIRST_MD_SEEN like '%Goyal,M%' or FIRST_MD_SEEN like '%Guse,S%' 
		or FIRST_MD_SEEN like '%Gutierrez,C%' or FIRST_MD_SEEN like '%Hill,E%' or FIRST_MD_SEEN like '%Jarvis,L%' 
		or FIRST_MD_SEEN like '%Kendi,S%' or FIRST_MD_SEEN like '%Kline,J%' or FIRST_MD_SEEN like '%Koutroulis,I%' 
		or FIRST_MD_SEEN like '%Lawson,S%' or FIRST_MD_SEEN like '%Lecuyer,M%' or FIRST_MD_SEEN like '%Lindgren,C%' 
		or FIRST_MD_SEEN like '%Magilner,D%' or FIRST_MD_SEEN like '%Maxwell,A%' or FIRST_MD_SEEN like '%McIver,M%' 
		or FIRST_MD_SEEN like '%McKinley,K%' or FIRST_MD_SEEN like '%Morrison,S%' or FIRST_MD_SEEN like '%Connell,K%' 
		or FIRST_MD_SEEN like '%Patel,S%' or FIRST_MD_SEEN like '%Payne,A%' or FIRST_MD_SEEN like '%Pershad,J%' 
		or FIRST_MD_SEEN like '%Quinn,M%' or FIRST_MD_SEEN like '%Root,J%' or FIRST_MD_SEEN like '%Rucker,A%' 
		or FIRST_MD_SEEN like '%Shaukat,H%' or FIRST_MD_SEEN like '%Simpson,J%' or FIRST_MD_SEEN like '%Teach,S%' 
		or FIRST_MD_SEEN like '%Trigylidas,T%' or FIRST_MD_SEEN like '%Thomas-Mohtat,R%' or FIRST_MD_SEEN like '%Ward,C%' 
		or FIRST_MD_SEEN like '%Wiener,A%' or FIRST_MD_SEEN like '%Willner,E%' or FIRST_MD_SEEN like '%Zaveri,P%' 
		or FIRST_MD_SEEN like '%Zhao,S%' or FIRST_MD_SEEN like '%Cohen,J%' or FIRST_MD_SEEN like '%Wiener,A%' 
		or FIRST_MD_SEEN like '%LaJoie,J%' or FIRST_MD_SEEN like '%Claiborne,M%' or FIRST_MD_SEEN like '%Ng,C%' 
		or FIRST_MD_SEEN like '%Good,D%' or FIRST_MD_SEEN like '%Kendi,S%' or FIRST_MD_SEEN like '%Falk,M%' 
				then '1' 
		end seen_by_pem

, case when FIRST_RESIDENT_SEEN is null then '0' else '1' end seen_by_res_pa

       from ED_TAT_MASTER
       where CHECKIN_DATE_TIME between @Start and @End
       )

--cte_purpose: create level of training variable with 0=resident/PA, 1=non-PEM LIP, 2=PEM provider
, level_training as (
	select 
	PATIENT_FIN as PATIENT_FIN_level_training
	
	,case when seen_by_res_pa like '1' then '1'
	when seen_by_pem like '1' then '3'
	else '2'
	end level_of_training

	from tat
	)

--cte_purpose: create variables for insurance, language, ICD
, bill as
       (
       select distinct patient_identification, language as pt_language, nachri_types as pt_insurance
                      , concat (',',LTRIM(ICD1),',',LTRIM(ICD2),',',ltrim(ICD3),',',ltrim(ICD4),',',ltrim(ICD5),',',ltrim(ICD6),',',ltrim(ICD7),',',ltrim(ICD8),',',ltrim(ICD9),','
              ,ltrim(ICD10),',',ltrim(ICD11),',',ltrim(ICD12),',',ltrim(ICD13),',',ltrim(ICD14),',',ltrim(ICD15)) ALLICD
		 ,case 
		when ed_billing_data_master.language like '%english%' or ed_billing_data_master.language = 'eng' then '0'
		when ed_billing_data_master.language like '%spanish%'  then '1'
		else '2'
		end Language_cat

		,case when nachri_types like '%commercial%' or nachri_types like '%medi%'
		or nachri_types like '%other%'
		or nachri_types like '%tri%'
		or nachri_types like '%work%' then '1'
		else '0'
		end Insurance_cat

		,case when nachri_types = 'other' then'1' 
		else '0' end insurance_other

	  from ED_BILLING_DATA_Master )

--cte_purpose: create dummy variables for source, including 'CleanCatch', 'Cath', 'Bagged'
--adds about 5% redundant rows, complete duplicates, no need for order by
, urine_culture_source as
       (
       select pt_fin as pt_fin_urine_culture_source
	   , case when first_value(order_detail) 
				OVER (
					PARTITION BY PT_FIN
					ORDER BY orig_order_dt_tm ASC
					ROWS UNBOUNDED PRECEDING) like '%clean%' then 'CleanCatch'
			when first_value(order_detail)
				over (
					PARTITION BY PT_FIN
					ORDER BY orig_order_dt_tm ASC
					ROWS UNBOUNDED PRECEDING) like '%cath%' Then 'Cath'
			when first_value(ORDER_DETAIL)
				over (
					PARTITION BY PT_FIN
					ORDER BY orig_order_dt_tm ASC
					ROWS UNBOUNDED PRECEDING) like '%bag%' then 'Bagged'
			else Null
			end ucx_source
  from Adm_Orders_DC_Import
  where ORDER_MNEMONIC like '%urine culture%'
  and ORDER_STATUS = 'Completed'
       )

--cte_purpose: create Bounceback within 72 hours
, tat_bb as 
	(
       select distinct bb =1
		   , patient_fin as patient_fin_bb
		   , PATIENT_MRN as patient_mrn_bb
		   , CHECKIN_DATE_TIME as checkin_date_time_bb
		from ED_TAT_MASTER
       )

--cte_purpose: create LE, nitrite, and wbc variables for UA
, ua_results as
	(
	select distinct
	pt_fin as pt_fin_ua
	--,RESULT_MNEMONIC as urine_result_ua
	--,result as result_ua
	,case when (RESULT_MNEMONIC like '%Urine Leukocyte Esterase') and 
		(result like '%[1-3]%') then '1'
		else '0'
		end ua_LE
	,case when (RESULT_MNEMONIC like '%urine nitrite') and 
		(result like '%pos%') then '1'
		else '0'
		end ua_nitrite
	, case when (RESULT_MNEMONIC like '%urine wbc') and 
		(result like '%[5-10000]%' or result like '%[5-9]%')
		and (result not like '0-%') and (result not like '0') and (result not like '3-5') then '1'
		else '0'
		end ua_wbc

	from LAB_DATA_IMPORT_Master
	WHERE (ORDER_MNEMONIC like 'complete%') 
		and (RESULT_MNEMONIC like '%Urine Leukocyte Esterase'
			or RESULT_MNEMONIC like '%urine wbc' 
			or RESULT_MNEMONIC like '%urine nitrite') 
		and (result like '%[0-9]%' 
			and (result not like '%[a-z]%')
				or (result like '%large%' 
				or result like '%moderate%' 
				or result like '%small%' 
				or result like '%ref range%')
			)
	)	

--cte_purpose: report only the max value from each UA parameter, and use this for final UA categorization
, ua_final as
	(
	select pt_fin_ua
	, max(ua_LE) as ua_LE_max
	, max(ua_nitrite) as ua_nitrite_max
	, max(ua_wbc) as ua_wbc_max
	, case 
		when max(ua_LE) = 1
		or max(ua_nitrite) = 1 
		or max(ua_wbc) = 1
		then '1'
		else '0'
		end ua_final_cat
	from ua_results
	group by pt_fin_ua
	)

--cte_purpose: create variable of udip POC results
,udip as  --gets Urinalysis result
       (select distinct
              pt_fin as pt_fin_udip
              , RESULT_DT_TM
              ,RESULT_MNEMONIC as udip_result_name
              , ORDER_MNEMONIC as udip_order
              ,result as udip_result
                      --,try_convert(integer,result) as numeric_result
              from LAB_DATA_IMPORT_Master
              WHERE ORDER_MNEMONIC like '%urine%' and ORDER_MNEMONIC like '%poc%'
              and (RESULT_MNEMONIC = 'urine wbc' or RESULT_MNEMONIC = 'urine nitrite')
              and result not like '%test%'
              and result not like '%cancel%'
              and result not like '%duplicate%'
              and result not like '%no result%'
              and result not like '%no sample%'
              and result not like '%wrong%'
              and result not like '%computer%'
              )


--cte_purpose: create variable for positive udip nitrite
, udip_nit as (
       select pt_fin_udip as pt_fin_udip_nit
       , RESULT_DT_TM as result_dt_tm_nit
       , case when udip_result like '%pos%' then '1'
              --when udip_result_name like '%nit%' and udip_result like '%neg%' then '0'
              else '0'
       end udip_nit_result
       from udip
       where udip_result_name like '%nit%'
       )

--cte_purpose: create variable for positive udip wbc
, udip_wbc as (
       select distinct pt_fin_udip as pt_fin_udip_wbc
       , result_dt_tm as RESULT_DT_TM_wbc
       , case when udip_result like '%[1-3]%' then '1'
              --when udip_result_name like '%wbc%' and (udip_result like '%neg%' or udip_result like '%trace%') then '0'
              else '0'
       end udip_wbc_result
       from udip
       where udip_result_name like '%wbc%'
       )

--cte_purpose: create categorical variable for udip result
, udip_cat as	   (select distinct pt_fin_udip_nit as pt_fin_udip_cat
       , case when udip_wbc_result = 0 and udip_nit_result = 0       then '0'
              when udip_wbc_result = 1 and udip_nit_result = 0 then '1'
              when udip_wbc_result = 0 and udip_nit_result = 1  then '2'
              else '3'
       end udip_result_cat
     
       from udip_nit
inner join udip_wbc on udip_nit.pt_fin_udip_nit=udip_wbc.pt_fin_udip_wbc and udip_nit.result_dt_tm_nit=udip_wbc.RESULT_DT_TM_wbc)

 

--cte_purpose: create Urine culture results,Urine culture positivity
--adds about 6% redundant rows, no differences in redundant rows but will order by positive_culture desc as precaution
,urine_culture as  
(
	select distinct 
		PT_FIN as PT_FIN_urine_culture
		--, order_mnemonic
		, result as urine_culture_result
--case below is redundant and probably underperforms compared to ucx source
/*
, case
		when result like '%cathet%'
		then '1'
		else '0'
end urine_cath
*/
		, case 
			when result like '%[5-9]0,000%'
			or result like '%00,000%'
			then '1'
			else '0'
end positive_culture
	from LAB_DATA_IMPORT_Master
	where RESULT_DT_TM between @Start and @end
		and ORDER_MNEMONIC like '%urine culture%')
	

--cte_purpose: Create variable for Antibiotics prescribed
 --Note to Kenny: confirm line 100

 ,any_antibiotic_prescribed as (
 select distinct
 PT_FIN as PT_FIN_any_antibiotic_prescribed
 ,any_abx_prescribed='1'
--, COMPLETED_DT_TM
--, ORDER_STATUS
from ED_Orders_Import_Master
where ORIG_ORD_AS like 'Prescription'
              and (ORDER_MNEMONIC like '%cefprozil%'
                      or ORDER_MNEMONIC like '%cephalex%'
                      or ORDER_MNEMONIC like '%trimethoprim%'
                      or ORDER_MNEMONIC like '%nitrofurantoin%'
                      or ORDER_MNEMONIC like '%cefdinir%'
                      or ORDER_MNEMONIC like '%amoxicillin%'
                      or ORDER_MNEMONIC like '%ciprofloxacin%'
                    or  ORDERED_AS_MNEMONIC Like '%cillin%' 
					Or ORDERED_AS_MNEMONIC Like '%mycin%'
					Or ORDERED_AS_MNEMONIC like '%micin%'
					Or ORDERED_AS_MNEMONIC Like '%cef%'
				/* avoid acephen rectal tylenol by excluding % */
					Or ORDERED_AS_MNEMONIC Like 'ceph%' 
					Or ORDERED_AS_MNEMONIC Like 'roceph%' 
					Or ORDERED_AS_MNEMONIC Like '%augmentin%' 
					Or ORDERED_AS_MNEMONIC Like '%penem%'
					Or ORDERED_AS_MNEMONIC like '%oxacin%'
					Or ORDERED_AS_MNEMONIC like '%zolid%'
                      )
              and (RX_ROUTE is null
                      or (RX_ROUTE Not Like '%oph%'      
                      And RX_ROUTE Not Like '%top%' 
                      And RX_ROUTE Not Like '%eye%' 
                      And RX_ROUTE Not Like '%ear%' 
                      And RX_ROUTE Not Like '%otic%'))
              and (ORDER_STATUS = 'Ordered'
			  or ORDER_STATUS = 'Completed'))
			  --AND CHECKIN_DT_TM BETWEEN @Start and @End

 ,any_antibiotic_start as (
 select distinct
 PT_FIN as PT_FIN_any_antibiotic_start
 ,any_abx_start='1'
 ,case when ORDER_MNEMONIC like '%ceftri%'
	then '1'
	else '0'
	end Ceftriaxone_ED
--, COMPLETED_DT_TM
--, ORDER_STATUS
from ED_Orders_Import_Master
where ORIG_ORD_AS like 'Normal Order'
and (CATALOG_TYPE = 'pharmacy')
              and (ORDER_MNEMONIC like '%cefprozil%'
                      or ORDER_MNEMONIC like '%cephalex%'
                      or ORDER_MNEMONIC like '%trimethoprim%'
                      or ORDER_MNEMONIC like '%nitrofurantoin%'
                      or ORDER_MNEMONIC like '%cefdinir%'
                      or ORDER_MNEMONIC like '%amoxicillin%'
                      or ORDER_MNEMONIC like '%ciprofloxacin%'
						or ORDERED_AS_MNEMONIC Like '%cillin%' 
						Or ORDERED_AS_MNEMONIC Like '%mycin%'
						Or ORDERED_AS_MNEMONIC like '%micin%'
						Or ORDERED_AS_MNEMONIC Like '%cef%'
					/* avoid acephen rectal tylenol by excluding % */
						Or ORDERED_AS_MNEMONIC Like 'ceph%' 
						Or ORDERED_AS_MNEMONIC Like 'roceph%' 
						Or ORDERED_AS_MNEMONIC Like '%augmentin%' 
						Or ORDERED_AS_MNEMONIC Like '%penem%'
						Or ORDERED_AS_MNEMONIC like '%oxacin%'
						Or ORDERED_AS_MNEMONIC like '%zolid%'
)
              and (RX_ROUTE is null
                      or (RX_ROUTE Not Like '%oph%'      
                      And RX_ROUTE Not Like '%top%' 
                      And RX_ROUTE Not Like '%eye%' 
                      And RX_ROUTE Not Like '%ear%' 
                      And RX_ROUTE Not Like '%otic%'))
              and (ORDER_STATUS = 'Ordered'
			  or ORDER_STATUS = 'Completed'))
			  --AND CHECKIN_DT_TM BETWEEN @Start and @End



--cte_purpose: create dose, unit, frequency, duration variables for prescribed antibiotics
, antibiotic_prescibed as (
       SELECT distinct
         PT_FIN as abx_fin
       , ORDER_MNEMONIC as antibiotic_name
       , DOSE AS antibiotic_dose
       , DOSE_UNIT AS antibiotic_unit
       , FREQUENCY AS antibiotic_freq
       , DURATION AS antibiotic_duration
       , DURATION_UNIT AS antibiotic_dur_unit
       , ORDER_STATUS

       FROM ED_Orders_Import_Master

       WHERE ORIG_ORD_AS like 'Prescription'
              and (ORDER_MNEMONIC like '%cefprozil%'
                      or ORDER_MNEMONIC like '%cephalex%'
                      or ORDER_MNEMONIC like '%trimethoprim%'
                      or ORDER_MNEMONIC like '%nitrofurantoin%'
                      or ORDER_MNEMONIC like '%cefdinir%'
                      or ORDER_MNEMONIC like '%amoxicillin%'
                      or ORDER_MNEMONIC like '%ciprofloxacin%'
                      )
              and (RX_ROUTE is null
                      or (RX_ROUTE Not Like '%oph%'      
                      And RX_ROUTE Not Like '%top%' 
                      And RX_ROUTE Not Like '%eye%' 
                      And RX_ROUTE Not Like '%ear%' 
                      And RX_ROUTE Not Like '%otic%'))
              and ORDER_STATUS not like '%cancel%'
              AND CHECKIN_DT_TM BETWEEN @Start and @End
       )

/*
--cte_purpose: create variable for Ceftriaxone in ED

,Ceftriaxone as (
       SELECT distinct
         PT_FIN as cef_fin
       ,case when ORIG_ORD_AS like 'Normal Order'
              and (ORDER_MNEMONIC like '%Ceftriaxone%')
              and (RX_ROUTE is null
                      or (RX_ROUTE Not Like '%oph%'      
                      And RX_ROUTE Not Like '%top%' 
                      And RX_ROUTE Not Like '%eye%' 
                      And RX_ROUTE Not Like '%ear%' 
                      And RX_ROUTE Not Like '%otic%'))
              and ORDER_STATUS not like '%cancel%' then '1'
			  else '0'
			  end Ceftriaxone_ED
             
     FROM ED_Orders_Import_Master 
	 where CHECKIN_DT_TM BETWEEN @Start and @End)*/
	   
--cte_purpose: create variable first temperature result
, temp_first as 
	(
	select distinct
	PT_FIN as temp_pt_fin
	--,RESULT_DT_TM
	--,TEMPERATURE_RESULT
	,FIRST_VALUE(TEMPERATURE_RESULT) OVER (
				PARTITION BY PT_FIN
				ORDER BY RESULT_DT_TM ASC
				ROWS UNBOUNDED PRECEDING
			) AS first_temp
	from ED_Vitals_Import_Master

	where ED_Vitals_Import_Master.CHECKIN_DT_TM between @start and @end
	and TEMPERATURE_RESULT is not null
	and TEMPERATURE_RESULT like '%[0-9]%'
	)

--cte_purpose: alternative cte strategy to create variable for first temperature result
--note, this cte was previously included as a left outer join
--use of max(case when RN=1... produces the same number of rows as temp_first cte
, temp1 as
	(
	select
	PT_FIN as temp_pt_fin
	,TEMP_1 = max(case when RN=1 then [TEMPERATURE_RESULT] end)
	From  
		(
		Select *,RN=Row_Number() over (Partition By PT_FIN Order by [result_Dt_tm],TEMPERATURE_RESULT)
		From [EMTCQIData].[dbo].[ED_Vitals_Import_Master]
		where TEMPERATURE_RESULT is not null 
		) temp_tab
	Group by PT_FIN 
	)


--end of common table expressions


select patient_fin, patient_mrn, PT_DOB,age_months, gender_cat, race_cat ,Ethnicity_cat
,Insurance_cat, insurance_other,Language_cat
,allicd,FIRST_MD_SEEN,LAST_ASSIGNED_MD,seen_by_res_pa, level_of_training
,REASON_FOR_VISIT,ESI,PT_DX1,PT_DX2,PT_DX3
, bb--,patient_mrn_bb, checkin_date_time_bb
, first_temp
, udip_result_cat
, ua_LE_max, ua_nitrite_max, ua_wbc_max, ua_final_cat
, ucx_source
, urine_culture_result, positive_culture
,any_abx_prescribed, any_abx_start
,antibiotic_name, antibiotic_dose, antibiotic_unit, antibiotic_freq, antibiotic_duration, antibiotic_dur_unit
,Ceftriaxone_ED


----,orig_order_dt_tm
----, urine_result_ua_le,result_ua_le,ua_Le,urine_result_ua_nit,result_ua_nit
----,ua_nitrite,urine_result_ua_wbc,result_ua_wbc,ua_wbc,ua_final
----,TEMP_1
----,urine_cath


--,FIRST_MD_SEEN,LAST_ASSIGNED_MD,seen_by_pem, seen_by_res_pa, udip_result_name, udip_order, udip_result, result_dt_tm_nit, udip_nit_result, RESULT_DT_TM_wbc
--,udip_wbc_result, pt_language, pt_insurance,PT_ETHNICITY, PT_GENDER , PT_RACE, dispo_date_time,CHECKIN_DATE_TIME

from TAT 

inner join bill on tat.PATIENT_FIN = BILL.PATIENT_IDENTIFICATION 
left outer join level_training on tat.PATIENT_FIN = level_training.PATIENT_FIN_level_training 
left outer join urine_culture_source on tat.PATIENT_FIN = urine_culture_source.pt_fin_urine_culture_source
left outer join tat_bb on tat.patient_mrn = tat_bb.patient_mrn_bb and datediff(hour,tat.DISPO_DATE_TIME,tat_bb.checkin_date_time_bb) between 8 and 72
left outer join ua_final on tat.PATIENT_FIN = ua_final.pt_fin_ua
left outer join udip_cat on tat.PATIENT_FIN = udip_cat.pt_fin_udip_cat
left outer join urine_culture on tat.PATIENT_FIN = urine_culture.PT_FIN_urine_culture
left outer join any_antibiotic_prescribed on tat.PATIENT_FIN = any_antibiotic_prescribed.PT_FIN_any_antibiotic_prescribed
left outer join any_antibiotic_start on tat.PATIENT_FIN = any_antibiotic_start.PT_FIN_any_antibiotic_start
left outer join antibiotic_prescibed on tat.PATIENT_FIN = antibiotic_prescibed.abx_fin
left outer join temp_first on tat.PATIENT_FIN = temp_first.temp_pt_fin


----left outer join temp1 on tat.PATIENT_FIN = temp1.temp_pt_fin
----left outer join ua_final_cat on tat.PATIENT_FIN = ua_final_cat.pt_fin_ua_le
----left outer join Ceftriaxone on tat.PATIENT_FIN = Ceftriaxone.cef_fin

where age_months between 0 and 228
--AND (temp_first.RESULT_DT_tm = TEMP-FIRST.FIRST_TEMP)
and (allicd like '%,N30.00%' 
or allicd like '%,N30.01%' 
or allicd like '%,N30.10%' 
or allicd like '%,N30.11%' 
or allicd like '%,N30.20%' 
or allicd like '%,N30.21%' 
or allicd like '%,N30.30%' 
or allicd like '%,N30.31%' 
or allicd like '%,N30.40%' 
or allicd like '%,N30.41%' 
or allicd like '%,N30.80%' 
or allicd like '%,N30.81%' 
or allicd like '%,N30.90%' 
or allicd like '%,N30.91%'
or allicd like '%,N34.0%' 
or allicd like '%,N34.1%' 
or allicd like '%,N34.2%' 
or allicd like '%,N34.3%' 
or allicd like '%,N39.0%' 
or allicd like '%,N39.9%')


order by patient_fin asc, udip_result_cat desc, positive_culture desc, antibiotic_duration desc, Ceftriaxone_ED desc



"""

#uti = pd.read_sql(uti_sql,conn, params=[start_date,end_date])
uti = pd.read_sql(uti_sql,conn)

#%%
"""DELETE IDENTICAL ROWS, KEEP REDUNDANT FINS WITH DIFFERENCE IN ROWS FOR MALEK TO REVIEW"""
uti1 = uti.drop_duplicates()

#%%
"""CREATE LIST OF FINS TO USE IN SUBSEQUENT QUERY"""
fin_list = uti.patient_fin.unique().tolist()
fin_slist = [str(x) for x in fin_list]
seperator = "','"
fin_string = "'" + seperator.join(fin_slist) + "'"

fin_tuple = tuple(fin_slist)


#%%
"""QUERY NOTES TO OBTAIN ALLERGY DATA"""
allergies_sql = """

--cte_purpose: create allergy_str variable, substring starting with pattern before allergies are listed in notes, ending after 1000 characters
; with notes as 
	(
       select pt_fin as fin_notes
       , RESULT_TITLE_TEXT
       , RESULT_DT_TM
       , SUBSTRING(result,charindex('allergies (active)',result),1000) as allergy_str
       from ED_NOTES_MASTER
       where result like '%allergies (active%'
--       and RESULT_DT_TM between @Start and @End
	   and pt_fin in {}
    )

 --cte_purpose: create allergy_substr variable, a substring of allergy_str that ends with pattern for next part of note OR ends after 1000 characters.
 --not every note has the following section, which starts with 'Medication list', hence need for 2 CTEs
, small_notes as 
	(
    select *
    , case
        when allergy_str like '%Medication list%'
        then left(allergy_str,charindex('Medication list',allergy_str)-1)
--		when allergy_str like '%pretreat%'
--		then left(allergy_str,charindex('pretreat',allergy_str)-1)
--		when allergy_str like '%pre-treat%'
--		then left(allergy_str,charindex('pre-treat',allergy_str)-1)
        else allergy_str
        end allergy_substr
        from notes
	)

--cte_purpose: create categorical variable Medication_allergy, using list of medications that are frequently listed as an allergy
, allergy_notes as 
	(
       select *
		,case when allergy_substr like '%Abilify%' then 'Abilify'when allergy_substr like '%acetaminophen%' then 'acetaminophen'
		when allergy_substr like '%Adacel%' then 'Adacel'when allergy_substr like '%Adderall%' then 'Adderall'
		when allergy_substr like '%Advil%' then 'Advil'when allergy_substr like '%albuterol%' then 'albuterol'
		when allergy_substr like '%Allegra%' then 'Allegra'when allergy_substr like '%Altabax%' then 'Altabax'
		when allergy_substr like '%amitriptyline%' then 'amitriptyline'when allergy_substr like '%amoxicillin%' then 'amoxicillin'
		when allergy_substr like '%Amoxil%' then 'Amoxil'when allergy_substr like '%amphotericin%' then 'amphotericin'
		when allergy_substr like '%ampicillin%' then 'ampicillin'when allergy_substr like '%Ancef%' then 'Ancef'
		when allergy_substr like '%ANESTHESIA%' then 'ANESTHESIA'when allergy_substr like '%Antibiotic%' then 'Antibiotic'
		when allergy_substr like '%Apidra%' then 'Apidra'when allergy_substr like '%Aquaphor%' then 'Aquaphor'
		when allergy_substr like '%arginine%' then 'arginine'when allergy_substr like '%ASA,%' then 'ASA,'
		when allergy_substr like '%aspirin%' then 'aspirin'when allergy_substr like '%Ativan%' then 'Ativan'
		when allergy_substr like '%atropine%' then 'atropine'when allergy_substr like '%Atrovent%' then 'Atrovent'
		when allergy_substr like '%Augmentin%' then 'Augmentin'when allergy_substr like '%azithromycin%' then 'azithromycin'
		when allergy_substr like '%aztreonam%' then 'aztreonam'when allergy_substr like '%bacitracin%' then 'bacitracin'
		when allergy_substr like '%Bactrim%' then 'Bactrim'when allergy_substr like '%Bactroban%' then 'Bactroban'
		when allergy_substr like '%Basaglar%' then 'Basaglar'when allergy_substr like '%Benadryl%' then 'Benadryl'
		when allergy_substr like '%Benedryl%' then 'Benedryl'when allergy_substr like '%Betadine%' then 'Betadine'
		when allergy_substr like '%Biaxin%' then 'Biaxin'when allergy_substr like '%Blistex%' then 'Blistex'
		when allergy_substr like '%calcium%' then 'calcium'when allergy_substr like '%carBAMazepine%' then 'carBAMazepine'
		when allergy_substr like '%CARBOplatin%' then 'CARBOplatin'when allergy_substr like '%Ceclor%' then 'Ceclor'
		when allergy_substr like '%CeFAZolin%' then 'CeFAZolin'when allergy_substr like '%cefdinir%' then 'cefdinir'
		when allergy_substr like '%cefepime%' then 'cefepime'when allergy_substr like '%cefixime%' then 'cefixime'
		when allergy_substr like '%ceftazidime%' then 'ceftazidime'when allergy_substr like '%cefTRIAXone%' then 'cefTRIAXone'
		when allergy_substr like '%cefuroxime%' then 'cefuroxime'when allergy_substr like '%Cefzil%' then 'Cefzil'
		when allergy_substr like '%cephalexin%' then 'cephalexin'when allergy_substr like '%cephalosporins%' then 'cephalosporins'
		when allergy_substr like '%Chlorhexidine%' then 'Chlorhexidine'when allergy_substr like '%Cipro%' then 'Cipro'
		when allergy_substr like '%ciprofloxacin%' then 'ciprofloxacin'when allergy_substr like '%Claritin%' then 'Claritin'
		when allergy_substr like '%clindamycin%' then 'clindamycin'when allergy_substr like '%clonidine%' then 'clonidine'
		when allergy_substr like '%codeine%' then 'codeine'when allergy_substr like '%Colace%' then 'Colace'
		when allergy_substr like '%Colloidal%' then 'Colloidal'when allergy_substr like '%Compazine%' then 'Compazine'
		when allergy_substr like '%Concerta%' then 'Concerta'when allergy_substr like '%Contrast%' then 'Contrast'
		when allergy_substr like '%corticosteroids%' then 'corticosteroids'when allergy_substr like '%Decadron%' then 'Decadron'
		when allergy_substr like '%Delsym%' then 'Delsym'when allergy_substr like '%Depakote%' then 'Depakote'
		when allergy_substr like '%dexamethasone%' then 'dexamethasone'when allergy_substr like '%dextromethorphan%' then 'dextromethorphan'
		when allergy_substr like '%Dextrose%' then 'Dextrose'when allergy_substr like '%Diflucan%' then 'Diflucan'
		when allergy_substr like '%Dilantin%' then 'Dilantin'when allergy_substr like '%Dilaudid%' then 'Dilaudid'
		when allergy_substr like '%Dimetapp%' then 'Dimetapp'when allergy_substr like '%Diphendramine%' then 'Diphendramine'
		when allergy_substr like '%diphenhydrAMINE%' then 'diphenhydrAMINE'when allergy_substr like '%dolutegravir%' then 'dolutegravir'
		when allergy_substr like '%doxycycline%' then 'doxycycline'when allergy_substr like '%emollients,%' then 'emollients,'
		when allergy_substr like '%erythromycin%' then 'erythromycin'when allergy_substr like '%Erythromycin,%' then 'Erythromycin,'
		when allergy_substr like '%etoposide%' then 'etoposide'when allergy_substr like '%Excedrin%' then 'Excedrin'
		when allergy_substr like '%Flonase%' then 'Flonase'when allergy_substr like '%Focalin%' then 'Focalin'
		when allergy_substr like '%fosfomycin%' then 'fosfomycin'when allergy_substr like '%fosphenytoin%' then 'fosphenytoin'
		when allergy_substr like '%Haldol%' then 'Haldol'when allergy_substr like '%heparin%' then 'heparin'
		when allergy_substr like '%Humira%' then 'Humira'when allergy_substr like '%hydralazine%' then 'hydralazine'
		when allergy_substr like '%hydrocodone%' then 'hydrocodone'when allergy_substr like '%hydrocortisone%' then 'hydrocortisone'
		when allergy_substr like '%hydroxyurea%' then 'hydroxyurea'when allergy_substr like '%ibuprofen%' then 'ibuprofen'
		when allergy_substr like '%iodine%' then 'iodine'when allergy_substr like '%isoniazid%' then 'isoniazid'
		when allergy_substr like '% K %' then 'Potassium'when allergy_substr like '%Keppra%' then 'Keppra'
		when allergy_substr like '%ketamine%' then 'ketamine'when allergy_substr like '%LaMICtal%' then 'LaMICtal'
		when allergy_substr like '%lamoTRIgine%' then 'lamoTRIgine'when allergy_substr like '%Levaquin%' then 'Levaquin'
		when allergy_substr like '%lisinopril%' then 'lisinopril'when allergy_substr like '%Lortab%' then 'Lortab'
		when allergy_substr like '%Macrobid%' then 'Macrobid'when allergy_substr like '%Magnesium%' then 'Magnesium'
		when allergy_substr like '%melatonin%' then 'melatonin'when allergy_substr like '%Mestinon%' then 'Mestinon'
		when allergy_substr like '%midazolam%' then 'midazolam'when allergy_substr like '%morphine%' then 'morphine'
		when allergy_substr like '%Motrin%' then 'Motrin'when allergy_substr like '%naproxen%' then 'naproxen'
		when allergy_substr like '%narcotic%' then 'narcotic'when allergy_substr like '%nitric%' then 'nitric'
		when allergy_substr like '%nitrofurantoin%' then 'nitrofurantoin'when allergy_substr like '%nitrous%' then 'nitrous'
		when allergy_substr like '%nonsteroidal%' then 'nonsteroidal'when allergy_substr like '%NSAIDs%' then 'NSAIDs'
		when allergy_substr like '%nystatin%' then 'nystatin'when allergy_substr like '%olopatadine%' then 'olopatadine'
		when allergy_substr like '%Omnicef%' then 'Omnicef'when allergy_substr like '%oxyCODONE%' then 'oxyCODONE'
		when allergy_substr like '%Pediacare%' then 'Pediacare'when allergy_substr like '%pegaspargase%' then 'pegaspargase'
		when allergy_substr like '%penicillin%' then 'penicillin'when allergy_substr like '%penicillins%' then 'penicillins'
		when allergy_substr like '%Percocet%' then 'Percocet'when allergy_substr like '%PHENobarbital%' then 'PHENobarbital'
		when allergy_substr like '%Polytrim%' then 'Polytrim'when allergy_substr like '%predniSONE%' then 'predniSONE'
		when allergy_substr like '%procainamide%' then 'procainamide'when allergy_substr like '%propofol%' then 'propofol'
		when allergy_substr like '%raNITIdine%' then 'raNITIdine'when allergy_substr like '%Reglan%' then 'Reglan'when allergy_substr like '%Retin-A%' then 'Retin-A'
		when allergy_substr like '%RisperDAL%' then 'RisperDAL'when allergy_substr like '%riTUXimab%' then 'riTUXimab'when allergy_substr like '%Robitussin%' then 'Robitussin'
		when allergy_substr like '%Rocephin%' then 'Rocephin'when allergy_substr like '%Similac%' then 'Similac'when allergy_substr like '%sulfa%' then 'sulfa'
		when allergy_substr like '%sulfamethoxazole%' then 'sulfamethoxazole'when allergy_substr like '%sulfonamides%' then 'sulfonamides'
		when allergy_substr like '%Tamiflu%' then 'Tamiflu'when allergy_substr like '%temsirolimus%' then 'temsirolimus'
		when allergy_substr like '%tobramycin%' then 'tobramycin'when allergy_substr like '%Trileptal%' then 'Trileptal'
		when allergy_substr like '%Tylenol%' then 'Tylenol'when allergy_substr like '%vancomycin%' then 'vancomycin'when allergy_substr like '%Vicks%' then 'Vicks'
		when allergy_substr like '%Zantac%' then 'Zantac'when allergy_substr like '%Zithromax%' then 'Zithromax'when allergy_substr like '%Zofran%' then 'Zofran'
		when allergy_substr like '%Zonegran%' then 'Zonegran'when allergy_substr like '%Zosyn%' then 'Zosyn'when allergy_substr like '%ZyrTEC%' then 'ZyrTEC'
		else '0'
		end medication_allergy
		from small_notes
	)

, med_allergy_flattened as
	(
	select fin_notes
	, allergy1 = max(case when rn=1 then medication_allergy end)
	, allergy2 = max(case when rn=2 then medication_allergy end)
	, allergy3 = max(case when rn=3 then medication_allergy end)
	, allergy4 = max(case when rn=4 then medication_allergy end)
	, allergy5 = max(case when rn=5 then medication_allergy end)
	, max(allergy_substr) as allergy_text
	from 
		(select *
		, rn = ROW_NUMBER() over (partition by fin_notes order by medication_allergy)
		from allergy_notes
		where medication_allergy != '0'
		) allergy_tab
	group by fin_notes
	)

select * from med_allergy_flattened
""".format(fin_tuple)

allergies = pd.read_sql(allergies_sql,conn)

#%%
"""MERGE UTI AND ALLERGIES DF"""
allergies1 = allergies.drop_duplicates()
mu = pd.merge(uti1,allergies1,how="left",left_on='patient_fin',right_on='fin_notes')

#%%
"""SAVE SPREADSHEET"""
#path = r"C:\Users\kmckinley\OneDrive - Children's National Hospital\Documents\Colleagues\fellows\Malek"
#os.chdir(path)

today = date.today()
filename = f"UTI_{today}.xlsx"
writer = pd.ExcelWriter(filename, engine='xlsxwriter')
mu.to_excel(writer, sheet_name='UTI_data',float_format="%.0f")
writer.save()
