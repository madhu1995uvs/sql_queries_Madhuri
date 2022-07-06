USE EMTCQIData

DECLARE @Start Date
DECLARE @End Date

SET @Start = '01/01/21'
SET @End = '12/31/2021';


select distinct 
	PATIENT_FIN
	,PATIENT_MRN
	, PT_GENDER
	,PT_RACE
	,PT_ETHNICITY
	,datediff(YEAR,PT_DOB, CHECKIN_DATE_TIME) age_years
	,FIRST_MD_SEEN
	,LAST_ASSIGNED_MD
	,REASON_FOR_VISIT
	,PT_DX1
	,PT_DX2
	,PT_DX3
	, left(tat.PT_ACUITY,1) ESI
	,PT_DISCH_DISPO
	,dispo_date_time
	,CHECKIN_DATE_TIME
	,TRACK_GROUP
	,CASE 
		WHEN PT_DISCH_DISPO LIKE '%IP' THEN 'ADMIT'
		WHEN PT_DISCH_DISPO LIKE 'Admit%' THEN 'ADMIT'
		WHEN PT_DISCH_DISPO LIKE 'Expired%' THEN 'ADMIT'
		ELSE 'DISCHARGE'
	END
	As Disposition

	from ED_TAT_MASTER tat

	where (CHECKIN_DATE_TIME between @start and @end) and (TRACK_GROUP like '%ED Track%')

	
