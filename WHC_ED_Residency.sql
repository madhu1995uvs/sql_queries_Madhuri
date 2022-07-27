USE EMTCQIData

DECLARE @Start Date
DECLARE @End Date

SET @Start = '07/01/18'
SET @End = '07/01/22'

--cte_purpose: create binary variables for bed request to icu and bed request to operating room
; with notes as (
	select distinct PT_FIN
	, case when result like '%critical%' then '1' end destination_icu
	, case when result like '%OR (Operating Room)%' then '1' end destination_or
	from ED_NOTES_MASTER
	where RESULT_TITLE_TEXT like '%ed bed%'
	and RESULT_DT_TM between @start and @end
	and (result like '%critical%' or result like '%Destination Requested :   OR (Operating Room)%')
	and result not like '%Care Team :   Psychiatry%'
	)

--cte_purpose: create variables for patients first seen by different types of providers
, tat as (
	select distinct 
	PATIENT_FIN
	,PATIENT_MRN
	, destination_icu
	, destination_or
	,datediff(YEAR,PT_DOB, CHECKIN_DATE_TIME) age_years
	, datepart(hour, checkin_date_time) as hov
	, datepart(DAYOFYEAR, checkin_date_time) as dov
	,DATEPART(year,checkin_date_time) as yov
	, datepart(WEEKDAY, checkin_date_time) as weekday_number
	, datename(weekday, checkin_date_time) as weekday_name
	,FIRST_MD_SEEN
	,LAST_ASSIGNED_MD
	,REASON_FOR_VISIT
	, left(tat.PT_ACUITY,1) ESI
	,PT_DISCH_DISPO
	,dispo_date_time
	,CHECKIN_DATE_TIME
	,TRACK_GROUP
	, case 
		when CHECKIN_DATE_TIME between '07/01/18' and '07/01/19' then '2018-2019'
		when CHECKIN_DATE_TIME between '07/01/19' and '07/01/20' then '2019-2020'
		when CHECKIN_DATE_TIME between '07/01/20' and '07/01/21' then '2020-2021'
		when CHECKIN_DATE_TIME between '07/01/21' and '07/01/22' then '2021-2022'
	end academic_year
	, CASE WHEN PT_DISCH_DISPO LIKE '%IP' or PT_DISCH_DISPO LIKE 'Admit%' THEN '1' end admission
	, case  when pt_disch_dispo like 'DISCHARGE' or PT_DISCH_DISPO like '%home%' then '1' end discharge
	, case WHEN PT_DISCH_DISPO LIKE 'Expired%' or pt_disch_dispo like 'died%' or pt_disch_dispo like 'dead%' THEN '1' end deceased
	, case when datediff(YEAR,PT_DOB, CHECKIN_DATE_TIME) < 18 then '1' end under_18y
	, case when PT_ACUITY like '%4%' or pt_acuity like '%5%' then '1' end low_acuity
	
	, case when FIRST_RESIDENT_SEEN is null  /*patients seen by EM faculty*/
		and (PT_ACUITY like '%1%' or pt_acuity like '%2%' or pt_acuity like '%3%')
		then '1'
		end seen_by_EM_Faculty

	, case when FIRST_RESIDENT_SEEN is null  /*patients seen by non EM faculty*/
		and (PT_ACUITY like '%4%' or pt_acuity like '%5%')
		and First_MD_seen not like '%PA-C' 
		and First_MD_seen not like '%NP' 
		then '1'
		end seen_by_non_EM_Faculty

	, case when FIRST_RESIDENT_SEEN not like '%PA-C' /*substract patients seen by EM residents to get seen_by_non_EM_Residents*/
		and First_RESIDENT_seen not like '%NP' 
		then '1' 
		end seen_by_ANY_Resident

	, case when FIRST_RESIDENT_SEEN like '%ierardo, aly%' 
		or FIRST_RESIDENT_SEEN like '%johnson, matthew%'
		or FIRST_RESIDENT_SEEN like '%migdal, talia%'
		or FIRST_RESIDENT_SEEN like '%orinda, miriam%'
		or FIRST_RESIDENT_SEEN like '%Scanlin, Anna%'
		or FIRST_RESIDENT_SEEN like '%Seidel, Randi%'
		or FIRST_RESIDENT_SEEN like '%Simpson, Damion%'
		or FIRST_RESIDENT_SEEN like '%Templeton, Melissa%'
		or FIRST_RESIDENT_SEEN like '%Bracy, Connor%'
		or FIRST_RESIDENT_SEEN like '%Castillo, Glennette%'
		or FIRST_RESIDENT_SEEN like '%Chan, Virginia%'
		or FIRST_RESIDENT_SEEN like '%Emily, Harrington%'
		or FIRST_RESIDENT_SEEN like '%Lew, Kelly%'
		or FIRST_RESIDENT_SEEN like '%Peragine, Brian%'
		or FIRST_RESIDENT_SEEN like '%Sheng, Bowen%'
		or FIRST_RESIDENT_SEEN like '%Trinh, Nam%'
		or FIRST_RESIDENT_SEEN like '%Weisbein, Alexa%'
		or FIRST_RESIDENT_SEEN like '%Williams, Brandon%'
		then '1' 
		end seen_by_em_res

	, case when FIRST_MD_SEEN like '%NP'
		or FIRST_MD_SEEN like '%PA'
		or FIRST_MD_SEEN like '%PA-C'
	then '1' 
		end seen_by_PA_NP

	from ED_TAT_MASTER tat
	left outer join notes
	on tat.PATIENT_FIN=notes.PT_FIN

	where (CHECKIN_DATE_TIME between @start and @end) and (TRACK_GROUP like '%ED Track%')
	and PT_ACUITY is not null
	)

--cte_purpose: create variable for weekend
, daily_phys_hrs as (
	select *
	, case when weekday_number=1 or weekday_number=7 then '1' else '0' end weekend
	from tat
	where FIRST_MD_SEEN not like '%np'
	and first_md_seen not like '%pa-c'
	)

--cte_purpose: create variable for number of LIP physicians each weekday hour of the academic year
, provider_wkday as (
	select hov, dov, academic_year
	, count(distinct first_md_seen) as unique_providers_wkday
	from daily_phys_hrs
	where weekend=0
	group by hov, dov, academic_year
	)

--cte_purpose: create variable for number of LIP physicians each weekend hour of the academic year
, provider_wkend as (
	select hov, dov, academic_year
	, count(distinct first_md_seen) as unique_providers_wkend
	from daily_phys_hrs
	where weekend=1
	group by hov, dov, academic_year
	)

--cte_purpose: sum total LIP physician-hours during weekdays each academic year
, provider_wkday_sum as (
	select academic_year
	, sum(unique_providers_wkday) as wkday_prov_hrs
	from provider_wkday
	group by academic_year
	)

--cte_purpose: sum total LIP physician-hours during weekends each academic year
, provider_wkend_sum as (
	select academic_year as academic_year1
	, sum(unique_providers_wkend) as wkend_prov_hrs
	from provider_wkend
	group by academic_year
	)

--cte_purpose: average LIP physician-hours during weekdays and weekends each academic year
, provider_hr_avg as (
	select *
	, wkday_prov_hrs/261 as avg_wkday_prov_hrs --average year with 365 days total, 5/7 are weekdays
	, wkend_prov_hrs/104 as avg_wkend_prov_hrs
	from provider_wkday_sum	
	inner join provider_wkend_sum on provider_wkday_sum.academic_year=provider_wkend_sum.academic_year1
	)

--cte_purpose: count total patients from variety of administrative categories for multiple academic years
, patient_counts as (
	select academic_year
	, count(PATIENT_FIN) as total_ed_patients
	, count(admission) as admitted_from_ed
	, count (destination_icu) as admits_to_icu
	, count (destination_or) as dispo_to_or
	, count(under_18y) as pediatric_0_to_17
	, count(low_acuity) as fast_track_possible
	, count(discharge) as discharged_patients
	, count(deceased) as deaths_in_ed
	, count(seen_by_em_res) as patients_seen_by_em_res
	, count (seen_by_PA_NP) as patients_seen_by_PA_N
	, count (seen_by_EM_Faculty) as patients_seen_by_EM_Faculty
	, count (seen_by_non_EM_Faculty) as patients_seen_by_non_EM_Faculty
	, count (seen_by_ANY_Resident) as patients_seen_by_ANY_Resident

	from tat
	group by academic_year
	)


select * 
from patient_counts
inner join provider_hr_avg 
on patient_counts.academic_year=provider_hr_avg.academic_year1
