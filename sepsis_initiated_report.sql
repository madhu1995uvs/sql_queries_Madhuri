; with ct as
	(
	select *
	from COVID_TAT
	where CHECKIN_DATE_TIME between '01/01/2022' and '08/26/2022'
	)

--cte_purpose: capture patients with sepsis pathway initiated
, seps as
	(
	select fin, SEPSIS_PATHWAY_INITIATED_ORDER_DATE
	--, format(SEPSIS_PATHWAY_INITIATED_ORDER_DATE, 'yyyy-MM-dd') as seps_date
	--, left(SEPSIS_PATHWAY_INITIATED_ORDER_DATE, 11) as seps_date
	--, SEPSIS_PATHWAY_INITIATED_ORDERED, SEPSIS_PATHWAY_INITIATED_ORDER_STATUS
	from sepsis_order_import
	where SEPSIS_PATHWAY_INITIATED_ORDER_DATE is not null
	)

--cte_purpose: review the column names from the sepsis_order_import table
, seps_cols as
	(
	select column_name, ordinal_position, data_type
	from INFORMATION_SCHEMA.columns
	where table_name = 'sepsis_order_import'
	)

--cte_purpose: create variables for fin, mrn, track_group, age_months, to join with sepsis data
, tat as
	(
	select patient_fin, patient_mrn, track_group, checkin_date_time, datediff(month,pt_dob,checkin_date_time) as age_months
	from ED_TAT_MASTER
	where checkin_date_time between '01/01/2022' and '08/27/2022'
	)

--select * from tat inner join seps on tat.patient_fin = seps.fin
select * from seps left outer join tat on seps.fin = tat.PATIENT_FIN