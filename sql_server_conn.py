# -*- coding: utf-8 -*-
"""
Created on Fri Jul 16 08:31:51 2021

@author: JCHAMBER
"""

def sql_server_conn():
       """This provides connection to SQL Server using pyodbc with ODBC Driver 17"""
       import pyodbc
       conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                             'Server=ENTSQL01LSNR;'
                             'Database=EMTCQIData;'
                             'Trusted_Connection=yes;')
       return conn