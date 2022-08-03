USE EMTCQIData

DECLARE @Start Date
DECLARE @End Date

SET @Start = '01/01/20'
SET @End = '01/01/22'

--cte_purpose: create variable age_months, mov, yov
; with tat as
	(
	SELECT distinct patient_fin
	, PATIENT_MRN
	, CHECKIN_DATE_TIME
	, datediff(month, pt_dob, checkin_date_time) age_months
	, PT_AGE
	, TRACK_GROUP
	, PT_GENDER
	, PT_DISCH_DISPO
	, REASON_FOR_VISIT
	, Year(CHECKIN_DATE_TIME) as YOV
	, Month(CHECKIN_DATE_TIME) as MOV
	from ED_TAT_MASTER
	)

--cte_purpose: create variable 'ALLICD'
, bill as 
	(
	select distinct patient_identification, 
	concat (',',LTRIM(ICD1),',',LTRIM(ICD2),',',ltrim(ICD3),',',ltrim(ICD4),',',ltrim(ICD5),',',ltrim(ICD6),',',ltrim(ICD7),',',ltrim(ICD8),',',ltrim(ICD9),','
	,ltrim(ICD10),',',ltrim(ICD11),',',ltrim(ICD12),',',ltrim(ICD13),',',ltrim(ICD14),',',ltrim(ICD15)) ALLICD
	from ED_BILLING_DATA_Master
	)

--cte_purpose: include only patients from tat with a SCD ICD10, using 'ALLICD'	
, scd_pt as
	(
	select *
	from tat inner join bill on tat.PATIENT_FIN=bill.PATIENT_IDENTIFICATION
	where ALLICD like '%,D57.%' and ALLICD not like '%,D57.3%'
	)

select distinct yov, mov
--, patient_fin
--, age_months
--, pt_age
--, REASON_FOR_VISIT
, count(patient_fin) as number_scd_visits
from scd_pt
where checkin_date_time between @start and @end
and age_months <36
group by yov, mov
order by yov, mov