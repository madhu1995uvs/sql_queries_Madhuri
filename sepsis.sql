USE EMTCQIData

DECLARE @Start Date
DECLARE @End Date

SET @Start = '01/01/21'
SET @End = '02/01/21'


--cte_purpose: use basic demographic data from tat table
; with tat as 
	(
	SELECT distinct patient_fin
	, patient_mrn
	, datediff(year, pt_dob, checkin_date_time) age_yrs
	, pt_gender
	, pt_race
	, pt_disch_dispo
	, track_group
	from ED_TAT_MASTER
	)

--cte_purpose: create a variable 'allicd' for all icds associated with the visit, from the billing table
, bill as 
	(
	select distinct patient_identification, 
	concat (',',LTRIM(ICD1),',',LTRIM(ICD2),',',ltrim(ICD3),',',ltrim(ICD4),',',ltrim(ICD5),',',ltrim(ICD6),',',ltrim(ICD7),',',ltrim(ICD8),',',ltrim(ICD9),','
	,ltrim(ICD10),',',ltrim(ICD11),',',ltrim(ICD12),',',ltrim(ICD13),',',ltrim(ICD14),',',ltrim(ICD15)) allicd
	from ED_BILLING_DATA_Master
	)

--cte_purpose: create variable for weight_result
, pt_weight as
	(
	select distinct pt_fin as fin_weight
	, weight_result
	from ED_Vitals_Import_Master
	where WEIGHT_RESULT is not null
	)

--cte_purpose: create variable for temperature_result
, pt_temp as
	(
	select distinct pt_fin as fin_temp
	, temperature_result
	from ED_Vitals_Import_Master
	where TEMPERATURE_RESULT is not null
	)

--cte_purpose: create variable for max_temp
, max_temp as
	(	
	select PT_FIN as fin_max_temp, max(temperature_result) as max_temp_result
	from ED_vitals_import_master
	where TEMPERATURE_RESULT is not null
	group by pt_fin
	)

--cte_purpose: create variable for O2 sat
, pt_sat as
	(
	select distinct pt_fin as fin_sat
	, OXYGEN_SAT_RESULT
	from ED_Vitals_Import_Master
	where OXYGEN_SAT_RESULT is not null
	)

--cte_purpose: create variable for fiO2
, pt_fio2 as
	(
	select distinct pt_fin as fin_fio2
	, FIO2_RESULT
	from ED_Vitals_Import_Master
	where fiO2_result is not null
	)

--cte_purpose: create variable for respiratory rate, 'rr'
, pt_rr as
	(
	select distinct pt_fin as fin_rr
	, RESP_RATE_RESULT
	from ED_Vitals_Import_Master
	where RESP_RATE_RESULT is not null
	)

--cte_purpose: create variable for heart rate, 'hr'
, pt_hr as
	(
	select distinct pt_fin as fin_hr
	, PULSE_RATE_RESULT
	from ED_Vitals_Import_Master
	where PULSE_RATE_RESULT is not null
	)

--cte_purpose: create variable for systolic blood pressure, 'sbp'
, pt_sbp as
	(
	select distinct pt_fin as fin_sbp
	, try_cast(SYSBP_RESULT as numeric) as sbp
	, RESULT_DT_TM as sys_dt_tm
	from ED_Vitals_Import_Master
	where SYSBP_RESULT is not null
	)

--cte_purpose: create variable for diastolic blood pressure, 'dbp'
, pt_dbp as
	(
	select distinct pt_fin as fin_dbp
	, try_cast(DIABP_RESULT as numeric) as dbp
	, RESULT_DT_TM as diabp_dt_tm
	from ED_Vitals_Import_Master
	where DIABP_RESULT is not null
	)

--cte_purpose: create a variable for mean arterial blood pressure, 'map'
, pt_map as
	(
	select distinct fin_sbp as fin_map
	, sbp*0.33 + dbp*0.67 as map_result
	from pt_sbp
	left outer join pt_dbp on pt_sbp.fin_sbp=pt_dbp.fin_dbp and pt_sbp.sys_dt_tm=pt_dbp.diabp_dt_tm
	)

--cte_purpose: create variable for gcs
, pt_gcs as
	(
	select distinct pt_fin as fin_gcs
	, GLASGOW_RESULT
	from ED_Vitals_Import_Master
	where glasgow_RESULT is not null
	)

--cte_purpose: create variable for wbc
, pt_wbc as
	(
	select distinct pt_fin as fin_wbc
	, result as wbc
	from LAB_DATA_IMPORT_Master
	where RESULT_MNEMONIC like '%WBC%'
	and SPECIMEN_TYPE like '%blood%' 
	and result like '[0-9]%'
	)

--cte_purpose: create variable for wbc1, first wbc result
, pt_wbc1 as
	(
	select fin_wbc1, wbc1
	from (
		select distinct wbc1.pt_fin as fin_wbc1, wbc1.result_mnemonic, wbc1.specimen_type, wbc1.result as wbc1, wbc1.result_dt_tm
		, row_number () over (partition by wbc1.pt_fin order by wbc1.result_dt_tm) as rnWBC
		from LAB_DATA_IMPORT_Master wbc1
		where RESULT_MNEMONIC like '%WBC%'
		and SPECIMEN_TYPE like '%blood%' 
		and result like '[0-9]%' 
		) wbc1_tab
		where rnWBC=1
	)

--cte_purpose: create variable for wbc
, pt_bands_percent as
	(
	select distinct pt_fin as fin_bands, RESULT as bands_percent
	from LAB_DATA_IMPORT_Master
	where RESULT_MNEMONIC like '%band%'
	and RESULT_MNEMONIC not like '%oligo%'
	and SPECIMEN_TYPE like '%blood%' 
	and result like '[0-9]%'
	and result is not null
	)

select *
from tat

inner join bill on tat.patient_fin = bill.PATIENT_IDENTIFICATION 
left outer join pt_weight on tat.patient_fin = pt_weight.fin_weight
--left outer join pt_temp on tat.patient_fin = pt_temp.fin_temp
--left outer join max_temp on tat.patient_fin = max_temp.fin_max_temp
--left outer join pt_sat on tat.patient_fin = pt_sat.fin_sat
--left outer join pt_fio2 on tat.patient_fin = pt_fio2.fin_fio2
--left outer join pt_rr on tat.patient_fin = pt_rr.fin_rr
left outer join pt_hr on tat.patient_fin = pt_hr.fin_hr
left outer join pt_map on tat.PATIENT_FIN = pt_map.fin_map
left outer join pt_gcs on tat.PATIENT_FIN = pt_gcs.fin_gcs
left outer join pt_wbc on tat.PATIENT_FIN = pt_wbc.fin_wbc
--left outer join pt_wbc1 on tat.PATIENT_FIN = pt_wbc1.fin_wbc1
left outer join pt_bands_percent on tat.PATIENT_FIN = pt_bands_percent.fin_bands

where allicd like '%,a41.%'
and TRACK_GROUP = 'ED Tracking Group'

order by patient_fin