"""
created on 16 nov 2021
update with date_range and email_jc modules on 30 dec 2021

@author: kmck

automated report to lab for children with head trauma who had labs drawn
"""

import pandas as pd 
import pyodbc 
import numpy as np

import os
import xlsxwriter
import win32com.client as win32


import datetime
from datetime import datetime, timedelta
from dateutil.parser import parse


# load sql and email modules needed functions
path = r"c:\temp1"
os.chdir(path)
from sql_server_conn import sql_server_conn
conn = sql_server_conn()
from email_jc import email_results_html
from date_range import date_range 

start_date, end_date, date_range = date_range(delta = 5) # use function with default days delta = 3


# define and perform sql query
tat_sql= """
DECLARE @Start Date = ?
DECLARE @End Date = ?


select
PATIENT_FIN
--, TRACK_GROUP
--, REASON_FOR_VISIT
, PATIENT_MRN
--, tat.PT_DOB
--, PT_ACUITY
, PT_DX1
, PT_DX2
, PT_DX3
--, CHECKIN_DATE_TIME
--, catalog_type
, ordered_as_mnemonic
, order_status
, ORDER_ID
, completed_dt_tm


FROM COVID_TAT TAT 


inner join
       (
       select *
       from COVID_Orders
       where 
           (ordered_as_mnemonic like 'ALT' 
           or ordered_as_mnemonic like 'AST'
           or ordered_as_mnemonic like 'BMP'
           or ordered_as_mnemonic like 'CMP'
           or ordered_as_mnemonic like 'Amylase Level'
           or ordered_as_mnemonic like 'Electrolytes'
           or ordered_as_mnemonic like 'Lipase Level'
           or ordered_as_mnemonic like 'LFT'
           or ordered_as_mnemonic like 'Basic Metabolic Panel'
           or ordered_as_mnemonic like '%CBC%'
           or ordered_as_mnemonic like '%HGB%'
           or ordered_as_mnemonic like '%Hemoglobin%'
           or ordered_as_mnemonic like '%Type%'
           )
       and order_status = 'Completed'
       and catalog_type = 'Laboratory'
       )
lab on tat.PATIENT_FIN=lab.PT_FIN



WHERE tat.checkin_date_time between @Start and @End
and (DATEDIFF(year,tat.PT_DOB,CHECKIN_DATE_TIME)  >= 10) 
and (DATEDIFF(year,tat.PT_DOB,CHECKIN_DATE_TIME)  < 19)
AND tat.TRACK_GROUP like '%ED T%'
and (pt_dx1 like '%subarachnoid hem%'
                        or pt_dx2 like '%subarachnoid hem%'
                        or pt_dx3 like '%subarachnoid hem%'
                    or pt_dx1 like '%epidural hem%'
                        or pt_dx2 like '%epidural hem%'
                        or pt_dx3 like '%epidural hem%'
                    or pt_dx1 like '%subdural hem%'
                        or pt_dx2 like '%subdural hem%'
                        or pt_dx3 like '%subdural hem%'
                    or pt_dx1 like '%intracranial hem%'
                        or pt_dx2 like '%intracranial hem%'
                        or pt_dx3 like '%intracranial hem%'                    
                    or pt_dx1 like '%fracture of base of skull%'
                        or pt_dx2 like '%fracture of base of skull%'
                        or pt_dx3 like '%fracture of base of skull%'
                    or pt_dx1 like '%fracture of vault of skull%'
                        or pt_dx2 like '%fracture of vault of skull%'
                        or pt_dx3 like '%fracture of vault of skull%'
                    or pt_dx1 like '%fracture of skull%'
                        or pt_dx2 like '%fracture of skull%'
                        or pt_dx3 like '%fracture of skull%'
                    or pt_dx1 like '%Concussion without loss of consciousness%'
                        or pt_dx2 like '%Concussion without loss of consciousness%'
                        or pt_dx3 like '%Concussion without loss of consciousness%'
                    or pt_dx1 like '%Concussion with loss of consciousness%'
                        or pt_dx2 like '%Concussion with loss of consciousness%'
                        or pt_dx3 like '%Concussion with loss of consciousness%'
                    or pt_dx1 like 'Unspecified injury of head, initial encounter'
                        or pt_dx2 like 'Unspecified injury of head, initial encounter'
                        or pt_dx3 like 'Unspecified injury of head, initial encounter'
                    or pt_dx1 like '%specified injuries of head%'
                        or pt_dx2 like '%specified injuries of head%'
                        or pt_dx3 like '%specified injuries of head%'
                    or pt_dx1 like '%intracranial injury%'
                        or pt_dx2 like '%intracranial injury%'
                        or pt_dx3 like '%intracranial injury%'
                        )                        
--'compression of brain' not typically trauma related   


order by patient_fin, ordered_as_mnemonic
"""
bbox = pd.read_sql(tat_sql,conn,params=[start_date, end_date])
bbox = bbox.drop_duplicates(subset=['PATIENT_FIN'])
num = len(bbox)
num_df0 = {f'Number of patients identified since {str(start_date)}':[num]}
num_df1 = pd.DataFrame(data=num_df0)


# convert to html and email patient info to JC, if functioning can switch to labmed_research email as recipient
email_results_html(bbox,'Brainbox eligible lab sample',date_range,'jchamber@childrensnational.org')#;labmed_research@childrensnational.org')

# email to JC that says that it ran and number of eligible patients, if functioning can switch to edresearchdirect email as recipient
email_results_html(num_df1,'Brainbox eligibles report was run',date_range,'jchamber@childrensnational.org')#;edresearchdirect@childrensnational.org')
