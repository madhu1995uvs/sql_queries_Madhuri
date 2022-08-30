# -*- coding: utf-8 -*-
"""
Pulls eligibles for BEEPER Study
Modifed 2021_05_20

[x] notemp
[X] confirmed_function_post_notemp
"""



#Import Packages
# import pyodbc
import pandas as pd
import os
from datetime import datetime, timedelta
import xlsxwriter
import win32com.client as win32

#path = r"c:\temp"
# path = r"C:\Users\jchamber\Downloads"
#os.chdir(path)
from sql_server_conn import sql_server_conn
conn = sql_server_conn()
# set directory for output files
os.chdir(r"C:\Users\jchamber\OneDrive - Children's National Hospital\HSR\Research Eligibles")
# define SQL connectoin
"""conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=ENTSQL01LSNR;'
                      'Database=EMTCQIData;'
                      'Trusted_Connection=yes;')
"""

sql = """

DECLARE @Start Date
DECLARE @End Date

Set @Start = cast(getdate() - 9 as date)
Set @End = cast(getdate() as date)

SELECT distinct PATIENT_FIN as FIN, PATIENT_MRN as MRN, Format(TRIAGE_DATE_TIME,'MM/dd/yyyy HH:mm:ss') as Triage_Date_Time, 
TRACK_GROUP, PATIENT_NAME_FULL_FORMATTED as Pt_Name, Format(PT_DOB,'MM/dd/yyyy') as DOB , DateDiff(year,ed_tat_master.PT_DOB, ed_tat_master.CHECKIN_DATE_Time) as Age_Years_G,
REASON_FOR_VISIT, PT_DX1, PT_DX2, PT_DX3,  rad.Radiology, orders.ddimer

FROM ED_TAT_MASTER

LEFT OUTER JOIN
(
Select PT_FIN, 
	CASE 
       		WHEN RESULT_MNEMONIC IS NOT NULL THEN  '1'
       		WHEN RESULT_MNEMONIC IS  NULL THEN  ''
        END	Radiology	 
       FROM Rad_Results_Import_Master
       WHERE (RESULT_MNEMONIC like '%CT Chest Angio%' or RESULT_MNEMONIC like '%CT Angiography  Chest%' or
  RESULT_MNEMONIC like '%CTANGCH%' or RESULT_MNEMONIC like '%pulmonary%')

)
rad on ED_TAT_MASTER.PATIENT_FIN = rad.PT_FIN

 LEFT OUTER JOIN
       (
       select ED_Orders_Import_Master.ORDER_MNEMONIC, 
       ED_Orders_Import_Master.PT_FIN,
       	CASE 
       		WHEN ORDER_MNEMONIC IS NOT NULL THEN  '1'
       		WHEN ORDER_MNEMONIC IS  NULL THEN  ''
        END	ddimer 
       FROM ED_Orders_Import_Master
       WHERE (ORDER_MNEMONIC like '%dimer%')
       
       ) orders on ED_TAT_MASTER.patient_fin = orders.PT_FIN

WHERE  (ED_TAT_MASTER.checkin_date_time >= @start and ED_TAT_MASTER.checkin_date_time < @end) and ((DATEDIFF(month,PT_DOB,CHECKIN_DATE_TIME)  >= 48) and (DATEDIFF(month,PT_DOB,CHECKIN_DATE_TIME)  < 216)) and
(rad.Radiology=1 or orders.ddimer=1)
 """
beeper = pd.read_sql(sql, conn)
Start = datetime.strftime(datetime.now() - timedelta(9), '%m_%d_%y')
End = datetime.strftime(datetime.now() - timedelta(3), '%m_%d_%y')
filename = 'beeper_' + Start + '_' + End + '.xlsx'
writer = pd.ExcelWriter(filename, engine='xlsxwriter')

# Convert the dataframe to an XlsxWriter Excel object.
beeper.to_excel(writer, sheet_name='Potential beeper Patients', index=False)
writer.save()
#Send automated email to study team
outlook = win32.Dispatch('outlook.application')  # Connects to your CNMC email
mail = outlook.CreateItem(0)
mail.To = "jchamber@cnmc.org;gbadolat@cnmc.org; btparrish@childrensnational.org"  
mail.Subject = 'beeper Missed Eligibles Report'
mail.HTMLbody = (r"""A new beeper Missed Eligibles Report is now available: <br><br>
    <a href='https://cnmc-my.sharepoint.com/:f:/g/personal/jchamber_childrensnational_org/EofPGs-3PotOkawieBRKTa8Bdi06NNk83FoOIdM_SPllnw?e=2zgXH8'>
    https://cnmc-my.sharepoint.com/:f:/g/personal/jchamber_childrensnational_org/EofPGs-3PotOkawieBRKTa8Bdi06NNk83FoOIdM_SPllnw?e=2zgXH8</a> 
    """)
mail.Send()