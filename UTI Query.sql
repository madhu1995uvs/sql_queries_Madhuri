

USE EMTCQIData
DECLARE @Start Date
DECLARE @End Date

SET @Start = '01/01/18'
SET @End = '04/01/21';
	
	
/* Patient, provider charecterstics and visist variables for measuring Adherence 
to First-Line Antimicrobial Treatment for Presumed Urinary Tract Infections in
the Emergency Department

Written by ED Data Analytics Team
Last updated 07/06/22 */

--Common table expressions


--cte_purpose: create variable of Urinalysis result
with ua_results as 
	(select distinct
		pt_fin
		,RESULT_MNEMONIC as urine_result_ua
		,result as result_ua
			--,try_convert(integer,result) as numeric_result
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
				or result like '%ref range%'))
			)
			


--cte_purpose: create variable of udip POC results
,udip as  --gets Urinalysis result
       (select distinct
              pt_fin
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
       select pt_fin as pt_fin_udip_nit
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
       select pt_fin as pt_fin_udip_wbc
       , result_dt_tm as RESULT_DT_TM_wbc
       , case when udip_result like '%[1-3]%' then '1'
              --when udip_result_name like '%wbc%' and (udip_result like '%neg%' or udip_result like '%trace%') then '0'
              else '0'
       end udip_wbc_result
       from udip
       where udip_result_name like '%wbc%'
       )



--cte_purpose: create dose, unit, frequency, duration variables for prescribed antibiotics
, antibiotic_prescibed as (
       SELECT 
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



--cte_purpose: create FIN,MRN,DOB,age,gender,PT_race,PT_ethnicity,first_MD_seen,last_MD_assigned,Reasonforvisit,PT_ACUITY,ED_Diagnosis1,2,3 (insurance,language,ICD)
, EDTAT as 
	(
	select distinct 
		PATIENT_FIN
		,PATIENT_MRN
		, PT_GENDER
		,PT_RACE
		,PT_ETHNICITY
		,datediff(month,PT_DOB, CHECKIN_DATE_TIME) age_months
		,FIRST_MD_SEEN
		,LAST_ASSIGNED_MD
		,REASON_FOR_VISIT
		,PT_DX1
		,PT_DX2
		,PT_DX3
		, left(tat.PT_ACUITY,1) ESI
		,bill.language
		, bill.nachri_types
		,dispo_date_time
		,CHECKIN_DATE_TIME

	from ED_TAT_MASTER tat


INNER JOIN --gets insurance,language,ICD
	(
	select distinct patient_identification, language, nachri_types, 
			concat (',',LTRIM(ICD1),',',LTRIM(ICD2),',',ltrim(ICD3),',',ltrim(ICD4),',',ltrim(ICD5),',',ltrim(ICD6),',',ltrim(ICD7),',',ltrim(ICD8),',',ltrim(ICD9),','
			,ltrim(ICD10),',',ltrim(ICD11),',',ltrim(ICD12),',',ltrim(ICD13),',',ltrim(ICD14),',',ltrim(ICD15)) ALLICD
		from ED_BILLING_DATA_Master) BILL 
		on BILL.PATIENT_IDENTIFICATION = tat.PATIENT_FIN ) 



 --cte_purpose: create Bounceback within 72 hours
, tat_bb as 
	(
       select bb =1
		   , patient_fin as patient_fin_bb
		   , PATIENT_MRN as patient_mrn_bb
		   , CHECKIN_DATE_TIME as checkin_date_time_bb
		from ED_TAT_MASTER
       )
 
 --cte_purpose: create Urine culture results,Urine culture positivity
,urine_culture as  
(
	select distinct 
		PT_FIN
		, order_mnemonic
		, result
	, case
		when result like '%cathet%'
		then '1'
		else '0'
end urine_cath
		, case 
			when result like '%[5-9]0,000%'
			or result like '%00,000%'
			then '1'
			else '0'
end positive_culture
	from LAB_DATA_IMPORT_Master
	where RESULT_DT_TM between @Start and @end
		and ORDER_MNEMONIC like '%urine culture%')
	, notes as (
		select pt_fin
		, RESULT_TITLE_TEXT
		, RESULT_DT_TM
		, SUBSTRING(result,charindex('allergies (active)',result),1000) as allergy_str
		from ED_NOTES_MASTER
		where result like '%allergies (active%'
		and RESULT_DT_TM between @Start and @end
			   )
 
 --cte_purpose: create allergy
, small_notes as (
		select *
		, case
			when allergy_str like '%Medication list%'
			then left(allergy_str,charindex('Medication list',allergy_str)-1)
			else allergy_str
			end allergy_substr
			from notes
				)




--end of common table expression


select * from EDTAT

INNER JOIN
	ua_results ON ua_results.PT_FIN = edtat.PATIENT_FIN

left outer join 
       (select pt_fin
       , FIRST_VALUE(allergy_substr) over (partition by pt_fin order by result_dt_tm asc rows unbounded preceding) as first_allergy
       from small_notes
       )
       overall_allergy on EDTAT.patient_fin=overall_allergy.pt_fin


left outer join 
	urine_culture
	on ua_results.PT_FIN = urine_culture.PT_FIN

left outer join
       (select *
       from tat_bb) 
       bounceback on EDTAT.patient_mrn = bounceback.patient_mrn_bb
       and datediff(Hour,EDTAT.dispo_date_time,bounceback.checkin_date_time_bb)>8
       and datediff(Hour,EDTAT.dispo_date_time,bounceback.checkin_date_time_bb)<72
 
where EDTAT.CHECKIN_DATE_TIME between @start and @end


/*select distinct pt_fin_udip_nit
       , case when udip_wbc_result = 0 and udip_nit_result = 0       then '0'
              when udip_wbc_result = 1 and udip_nit_result = 0 then '1'
              when udip_wbc_result = 0 and udip_nit_result = 1  then '2'
              else '3'
       end udip_result_cat
       , antibiotic_prescibed.*
       from udip_nit
inner join udip_wbc on udip_nit.pt_fin_udip_nit=udip_wbc.pt_fin_udip_wbc and udip_nit.result_dt_tm_nit=udip_wbc.RESULT_DT_TM_wbc
left outer join antibiotic_prescibed on udip_nit.pt_fin_udip_nit = antibiotic_prescibed.abx_FIN
where RESULT_DT_TM_wbc between @Start and @End*/
	
