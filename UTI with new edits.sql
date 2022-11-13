


USE EMTCQIData

DECLARE @Start Date
DECLARE @End Date

SET @Start = '01/01/18'
SET @End = '04/01/21'

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
			  ,pt_disch_dispo 

		
		
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
       and pt_disch_dispo not like '%IP%' and pt_disch_dispo not like '%inpatient%')

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

	from ED_LAB_DATA_Master
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
              from ED_LAB_DATA_Master
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
	from ED_LAB_DATA_Master
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
 , COMPLETED_DT_TM as antibiotic_date_time
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
                      And RX_ROUTE Not Like '%otic%'
					   And RX_ROUTE Not Like 'bucc'
					   And RX_ROUTE Not Like 'IM'
						And RX_ROUTE Not Like 'IV'
						And RX_ROUTE Not Like 'Nasal'
					  ))
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
					  or ORDER_MNEMONIC like '%Cefuroxime%'
                      or ORDERED_AS_MNEMONIC Like '%cillin%' 
						Or ORDERED_AS_MNEMONIC Like '%cef%'
						Or ORDERED_AS_MNEMONIC Like '%augmentin%' 
						Or ORDERED_AS_MNEMONIC like '%oxacin%')
              and (RX_ROUTE is null
                      or (RX_ROUTE Not Like '%oph%'      
                      And RX_ROUTE Not Like '%top%' 
                      And RX_ROUTE Not Like '%eye%' 
                      And RX_ROUTE Not Like '%ear%' 
                      And RX_ROUTE Not Like '%otic%'
					  )
					and ORDER_MNEMONIC not like '% otic%'  
					and ORDER_MNEMONIC not like '% oph%' 
					 )
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


select patient_fin, patient_mrn, CHECKIN_DATE_TIME, PT_DOB,age_months, gender_cat, race_cat ,Ethnicity_cat
,Insurance_cat, insurance_other,Language_cat
,allicd,FIRST_MD_SEEN,LAST_ASSIGNED_MD,seen_by_res_pa, level_of_training
,REASON_FOR_VISIT,ESI,PT_DX1,PT_DX2,PT_DX3
, bb--,patient_mrn_bb, checkin_date_time_bb
, first_temp
, udip_result_cat
, ua_LE_max, ua_nitrite_max, ua_wbc_max, ua_final_cat
, ucx_source
, urine_culture_result, positive_culture
,any_abx_prescribed, any_abx_start, antibiotic_date_time
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
--or allicd like '%,N30.10%' 
--or allicd like '%,N30.11%' 
--or allicd like '%,N30.20%' 
--or allicd like '%,N30.21%' 
--or allicd like '%,N30.30%' 
--or allicd like '%,N30.31%' 
--or allicd like '%,N30.40%' 
--or allicd like '%,N30.41%' 
or allicd like '%,N30.80%' 
or allicd like '%,N30.81%' 
or allicd like '%,N30.90%' 
or allicd like '%,N30.91%'
--or allicd like '%,N34.0%' 
--or allicd like '%,N34.1%' 
--or allicd like '%,N34.2%' 
--or allicd like '%,N34.3%' 
or allicd like '%,N39.0%')
--or allicd like '%,N39.9%')


order by patient_fin asc, udip_result_cat desc, positive_culture desc, antibiotic_duration desc, Ceftriaxone_ED desc