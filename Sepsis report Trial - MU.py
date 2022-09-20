# -*- coding: utf-8 -*-
"""
Created on Tue Sep 20 15:24:45 2022

@author: muppuluri
"""

import pandas as pd
import os
import time
#import datetime
from datetime import date
from sql_server_conn import sql_server_conn

conn = sql_server_conn()

error_recips = "kmckinley@childrensnational.org"

try:

    
# set inclusion dates to yesterday.
# I need help understanding the lines 24 to 30 - muppuluri
    today = datetime.date.today()
    start_date = str(today - timedelta(days=1))
    end_date = str(today)
    start_date2 = str(today - timedelta(days=3))
    date_range = parse(start_date).strftime("%m_%d_%Y") + '_to_' + \
        parse(str(today - timedelta(days=1))).strftime("%m_%d_%Y")
    #yesterday = parse(str(today - timedelta(days=1))).strftime("%m/%d/%Y")
    
"""conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=ENTSQL01LSNR;'
                      'Database=EMTCQIData;'
                      'Trusted_Connection=yes;')
"""

# define sql query
sql = """

; with ct as
	(
	select *
	from COVID_TAT
	where CHECKIN_DATE_TIME between '08/28/2022' and '08/29/2022'
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
	where checkin_date_time between '08/25/2022' and '08/26/2022'
	)


--cte_purpose: create variables for blood culture order
, culture as
	(
	select distinct 
			PT_FIN as PT_FIN_blood_culture
             , order_mnemonic
             , result as blood_culture_result

             , case
             when ORDER_MNEMONIC like '%blood culture%'
             then '1'
             else '0'
end Blood_culture

       from LAB_DATA_IMPORT_Master
       where result not like '%Cancel%'
       and RESULT_DT_TM between '08/20/2022' and '08/26/2022')



--select * from tat inner join seps on tat.patient_fin = seps.fin
select * from seps 
left outer join culture on seps.fin =culture.PT_FIN_blood_culture
left outer join tat on seps.fin = tat.PATIENT_FIN
where  tat.age_months between '6' and '216'


--and seps.POTENTIAL_SEPSIS_ORDER_STATUS like '%complete%'

"""

# import data from sql as a pandas dataframe using the sql connection and sql query defined above
seps = pd.read_sql(sql, conn, params=[startdate, enddate])

except:
    em.email_failed(
        subject='Sepsis report failed', recipients=error_recips)
