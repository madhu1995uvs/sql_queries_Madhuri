USE EMTCQIData

DECLARE @Start Date
DECLARE @End Date

SET @Start = '01/01/2021'
SET @End = '12/31/2021';

with tat as
	(
	select distinct 
		PATIENT_FIN
		,PATIENT_MRN
		,datediff(YEAR,PT_DOB, CHECKIN_DATE_TIME) age_years
		,FIRST_MD_SEEN
		,LAST_ASSIGNED_MD
		,REASON_FOR_VISIT
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

	where (CHECKIN_DATE_TIME between @start and @end) and (TRACK_GROUP like '%ED Track%'))

	

, notes as (
       select pt_fin
       , RESULT_TITLE_TEXT
       , RESULT_DT_TM
       from ED_NOTES_MASTER
	   where RESULT_TITLE_TEXT like '%event note - death%'
       and RESULT_DT_TM between @Start and @end
       )

	   select * from tat

LEFT OUTER JOIN
	notes ON notes.PT_FIN = tat.PATIENT_FIN