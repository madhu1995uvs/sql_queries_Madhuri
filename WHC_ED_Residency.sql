USE EMTCQIData

DECLARE @Start Date
DECLARE @End Date

SET @Start = '01/01/19'
SET @End = '01/01/22'

; with tat as (
	select distinct 
	PATIENT_FIN
	,PATIENT_MRN
	, PT_GENDER
	,PT_RACE
	,PT_ETHNICITY
	,datediff(YEAR,PT_DOB, CHECKIN_DATE_TIME) age_years
	,DATEPART(year,checkin_date_time) as yov
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
	, case 
		when CHECKIN_DATE_TIME between '07/01/19' and '07/01/20' then '2019-2020'
		when CHECKIN_DATE_TIME between '07/01/20' and '07/01/21' then '2020-2021'
		when CHECKIN_DATE_TIME between '07/01/21' and '07/01/22' then '2021-2022'
	end academic_year
	, CASE WHEN PT_DISCH_DISPO LIKE '%IP' or PT_DISCH_DISPO LIKE 'Admit%' THEN '1' end admission
	, case  when pt_disch_dispo like 'DISCHARGE' or PT_DISCH_DISPO like '%home%' then '1' end discharge
	, case WHEN PT_DISCH_DISPO LIKE 'Expired%' or pt_disch_dispo like 'died%' or pt_disch_dispo like 'dead%' THEN '1' end deceased
	, case when datediff(YEAR,PT_DOB, CHECKIN_DATE_TIME) < 18 then '1' end under_18y
	, case when FIRST_RESIDENT_SEEN like '%ierardo, aly%' 
		or FIRST_RESIDENT_SEEN like '%johnson, matthew%'
		or FIRST_RESIDENT_SEEN like '%migdal, talia%'
		or FIRST_RESIDENT_SEEN like '%orinda, miriam%'
		then '1' 
		end seen_by_em_res
	
	from ED_TAT_MASTER tat

	where (CHECKIN_DATE_TIME between @start and @end) and (TRACK_GROUP like '%ED Track%')
	and PT_ACUITY is not null
	)

--below doesn't work yet. work on this later
select academic_year
, count(PATIENT_FIN) as total_ed_patients
, count(admission) as admitted_from_ed
, count(under_18y) as pediatric_0_to_17
, count(discharge) as discharged_patients
, count(deceased) as deaths_in_ed
, count(seen_by_em_res) as patients_seen_by_em_res
from tat
group by academic_year

