# -*- coding: utf-8 -*-
"""
Created on Tue Sep 20 15:24:45 2022

@author: muppuluri
"""

#import packages
import pandas as pd
import os
import time
from datetime import date, timedelta
from sql_server_conn import sql_server_conn
from dateutil.parser import parse
import email_jc as em

conn = sql_server_conn()

#recipients below will receive a notification if there is an error in automated report, separate address with semicolin ';'
error_recips = "kmckinley@childrensnational.org;muppuluri@childrensnational.org"
#recepients below will receive automated report, separate address with semicolin ';'
recipients_seps = "edresearchdirect@childrensnational.org;ikoutrouli@childrensnational.org"

try:
    
# set inclusion dates from start_date through end_date. Today is listed as end_date because SQL is not inclusive of final end date.
#if this report is automated to run on Wednesday each week, it should capture Sun-Sat of the previous week
    today = date.today()
    start_date = str(today - timedelta(days=10))
    end_day = today - timedelta(days=3)
    end_date = str(end_day)
    date_range = parse(start_date).strftime("%m_%d_%Y") + '_to_' + \
        parse(str(end_day - timedelta(days=1))).strftime("%m_%d_%Y")
    

	# define sql query
    sql = """

	; with ct as
		(
		select *
		from COVID_TAT
		)

		
	--cte_purpose: capture patients with sepsis pathway initiated
	, seps as
		(
		select fin, SEPSIS_PATHWAY_INITIATED_ORDER_DATE, POTENTIAL_SEPSIS_ORDER_STATUS
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
		)


	--cte_purpose: create variables for blood culture order
	, culture as
		(
		select distinct
				PT_FIN as PT_FIN_blood_culture
				, order_mnemonic
				, result as blood_culture_result

		from LAB_DATA_IMPORT_Master
		where ORDER_MNEMONIC like '%blood culture%'
		and result not like '%Cancel%'
		)



	--select * from tat inner join seps on tat.patient_fin = seps.fin
	select * from seps 
	left outer join culture on seps.fin =culture.PT_FIN_blood_culture
	left outer join tat on seps.fin = tat.PATIENT_FIN
	where  tat.age_months between '6' and '216'
	and tat.checkin_date_time between ? and ?


	--and seps.POTENTIAL_SEPSIS_ORDER_STATUS like '%complete%'

	"""

	# import data from sql as a pandas dataframe using the sql connection and sql query defined above
    seps_init = pd.read_sql(sql, conn, params=[start_date, end_date])
	#clean up the dataframe and eliminate redundant columns
    seps = seps_init[['fin', 'SEPSIS_PATHWAY_INITIATED_ORDER_DATE',
       'POTENTIAL_SEPSIS_ORDER_STATUS', 'order_mnemonic', 
	   'blood_culture_result', 'track_group', 'checkin_date_time', 'age_months']]
	#send seps dataframe as an email to recipients, listed above
    em.email_results_html(seps, 'Daily Sepsis', date_range, recipients_seps)

#notify Kenny and Madhuri if the automated report throws an error
except:
    em.email_failed(subject='Sepsis report failed', recipients=error_recips)