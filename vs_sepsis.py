#%%
"""IMPORT PACKAGES"""
import pandas as pd 
import pyodbc 
import numpy as np
import os
import re
from dateutil.parser import parse
from datetime import date
from sql_server_conn import sql_server_conn
import hashlib


# %%
conn = sql_server_conn()

start_date = '01/01/2021'
end_date = '02/01/2021'

date_range = parse(start_date).strftime("%m_%d_%Y") + '_to_' +     parse(end_date).strftime("%m_%d_%Y")
date_range

# %%
"""resp rate query"""
rr_sql  = """
-- update next line each new VS query
; with rr as (
	select PT_MRN as PatientId
	, PT_FIN as EncounterId
-- update next line each new VS query
	, RESP_RATE as VitalSignName
-- update next line each new VS query
	, RESP_RATE_RESULT as VitalSignValue
	, RESULT_DT_TM as CaptureTimestamp
	from ED_Vitals_Import_Master
-- update next line each new VS query
	where RESP_rate is not null
	and CHECKIN_DT_TM between ? and ?
	)

select * from rr
"""
rr = pd.read_sql(rr_sql,conn,params=[start_date,end_date])

# %%

pulse_sql  = """
; with pulse as (
	select PT_MRN as PatientId
	, PT_FIN as EncounterId
	, PULSE_RATE as VitalSignName
	, PULSE_RATE_RESULT as VitalSignValue
	, RESULT_DT_TM as CaptureTimestamp
	from ED_Vitals_Import_Master
	where pulse_rate is not null
	and CHECKIN_DT_TM between ? and ?
	)

select * from pulse
"""
pulse = pd.read_sql(pulse_sql,conn,params=[start_date,end_date])


# %%
"""temperature query"""

# %%
"""O2 sat query"""

# %%
"""SBP query"""

# %%
"""DBP query"""

# %%
"""append other VS to pulse, then order by mrn, fin, then deidentify"""
all_vitals = pd.concat([rr,pulse],ignore_index=True)
# create new line of code to concat each new vs table
# all_vitals = pd.concat([all_vitals,newvs],ignore_index=True)

all_vitals = all_vitals.sort_values(by=['PatientId','EncounterId'])


# %%
#de-ID FIN and mrn
def shash(val):
    h = hashlib.sha256()
    vb = bytes(val, 'utf-8')
    h.update(vb)
    return h.digest()

all_vitals['PatientId'] = all_vitals['PatientId'].astype(str)
all_vitals['PatientId'] = all_vitals['PatientId'].apply(shash)

all_vitals['EncounterId'] = all_vitals['EncounterId'].astype(str)
all_vitals['EncounterId'] = all_vitals['EncounterId'].apply(shash)

# %%
all_vitals['CaptureTimestamp'] = 'rq_date_offset'
# %%
