# -*- coding: utf-8 -*-
"""
Created on Wed Aug 24 14:20:53 2022

@author: kmckinley
"""

import pandas as pd 
import pyodbc 
import numpy as np
import os
from dateutil.parser import parse


conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                      'Server=ENTSQL01LSNR;'
                      'Database=EMTCQIData;'
                      'Trusted_Connection=yes;')

start_date = '01/01/2018'
end_date = '02/01/2018'

date_range = parse(start_date).strftime("%m_%d_%Y") + '_to_' +     parse(end_date).strftime("%m_%d_%Y")
date_range

uti_sql = """

#TASK 1, FINISH THE NON-ALLERGY QUERY AND PLOP INTO PYTHON FILE


"""

uti = pd.read_sql(uti_sql,conn, params=[start_date,end_date])


#TASK 2, TOUCH UP THE ALLERGY DATA MANIPULATION TO ENSURE ONE ROW EACH PATIENT WITH MULTIPLE LISTED ALLERGIES
allergies_sql = """

; with notes as (
       select pt_fin as Patient_FIN_notes
       , RESULT_TITLE_TEXT
       , RESULT_DT_TM
       , SUBSTRING(result,charindex('allergies (active)',result),1000) as allergy_str
       from ED_NOTES_MASTER
       where result like '%allergies (active%'
       and RESULT_DT_TM between ? and ?
                       )

 --cte_purpose: create allergy
, small_notes as (
             select *
             , case
                    when allergy_str like '%Medication list%'
                    then left(allergy_str,charindex('Medication list',allergy_str)-1)
                    else allergy_str
                    end allergy_substr
                    from notes
                    )

, allergy_notes as (
       select *
,case when allergy_substr like '%Abilify%' then 'Abilify'when allergy_substr like '%acetaminophen%' then 'acetaminophen'
when allergy_substr like '%Adacel%' then 'Adacel'when allergy_substr like '%Adderall%' then 'Adderall'
when allergy_substr like '%Advil%' then 'Advil'when allergy_substr like '%albuterol%' then 'albuterol'
when allergy_substr like '%Allegra%' then 'Allegra'when allergy_substr like '%Altabax%' then 'Altabax'
when allergy_substr like '%amitriptyline%' then 'amitriptyline'when allergy_substr like '%amoxicillin%' then 'amoxicillin'
when allergy_substr like '%Amoxil%' then 'Amoxil'when allergy_substr like '%amphotericin%' then 'amphotericin'
when allergy_substr like '%ampicillin%' then 'ampicillin'when allergy_substr like '%Ancef%' then 'Ancef'
when allergy_substr like '%ANESTHESIA%' then 'ANESTHESIA'when allergy_substr like '%Antibiotic%' then 'Antibiotic'
when allergy_substr like '%Apidra%' then 'Apidra'when allergy_substr like '%Aquaphor%' then 'Aquaphor'
when allergy_substr like '%arginine%' then 'arginine'when allergy_substr like '%ASA,%' then 'ASA,'
when allergy_substr like '%aspirin%' then 'aspirin'when allergy_substr like '%Ativan%' then 'Ativan'
when allergy_substr like '%atropine%' then 'atropine'when allergy_substr like '%Atrovent%' then 'Atrovent'
when allergy_substr like '%Augmentin%' then 'Augmentin'when allergy_substr like '%azithromycin%' then 'azithromycin'
when allergy_substr like '%aztreonam%' then 'aztreonam'when allergy_substr like '%bacitracin%' then 'bacitracin'
when allergy_substr like '%Bactrim%' then 'Bactrim'when allergy_substr like '%Bactroban%' then 'Bactroban'
when allergy_substr like '%Basaglar%' then 'Basaglar'when allergy_substr like '%Benadryl%' then 'Benadryl'
when allergy_substr like '%Benedryl%' then 'Benedryl'when allergy_substr like '%Betadine%' then 'Betadine'
when allergy_substr like '%Biaxin%' then 'Biaxin'when allergy_substr like '%Blistex%' then 'Blistex'
when allergy_substr like '%calcium%' then 'calcium'when allergy_substr like '%carBAMazepine%' then 'carBAMazepine'
when allergy_substr like '%CARBOplatin%' then 'CARBOplatin'when allergy_substr like '%Ceclor%' then 'Ceclor'
when allergy_substr like '%CeFAZolin%' then 'CeFAZolin'when allergy_substr like '%cefdinir%' then 'cefdinir'
when allergy_substr like '%cefepime%' then 'cefepime'when allergy_substr like '%cefixime%' then 'cefixime'
when allergy_substr like '%ceftazidime%' then 'ceftazidime'when allergy_substr like '%cefTRIAXone%' then 'cefTRIAXone'
when allergy_substr like '%cefuroxime%' then 'cefuroxime'when allergy_substr like '%Cefzil%' then 'Cefzil'
when allergy_substr like '%cephalexin%' then 'cephalexin'when allergy_substr like '%cephalosporins%' then 'cephalosporins'
when allergy_substr like '%Chlorhexidine%' then 'Chlorhexidine'when allergy_substr like '%Cipro%' then 'Cipro'
when allergy_substr like '%ciprofloxacin%' then 'ciprofloxacin'when allergy_substr like '%Claritin%' then 'Claritin'
when allergy_substr like '%clindamycin%' then 'clindamycin'when allergy_substr like '%clonidine%' then 'clonidine'
when allergy_substr like '%codeine%' then 'codeine'when allergy_substr like '%Colace%' then 'Colace'
when allergy_substr like '%Colloidal%' then 'Colloidal'when allergy_substr like '%Compazine%' then 'Compazine'
when allergy_substr like '%Concerta%' then 'Concerta'when allergy_substr like '%Contrast%' then 'Contrast'
when allergy_substr like '%corticosteroids%' then 'corticosteroids'when allergy_substr like '%Decadron%' then 'Decadron'
when allergy_substr like '%Delsym%' then 'Delsym'when allergy_substr like '%Depakote%' then 'Depakote'
when allergy_substr like '%dexamethasone%' then 'dexamethasone'when allergy_substr like '%dextromethorphan%' then 'dextromethorphan'
when allergy_substr like '%Dextrose%' then 'Dextrose'when allergy_substr like '%Diflucan%' then 'Diflucan'
when allergy_substr like '%Dilantin%' then 'Dilantin'when allergy_substr like '%Dilaudid%' then 'Dilaudid'
when allergy_substr like '%Dimetapp%' then 'Dimetapp'when allergy_substr like '%Diphendramine%' then 'Diphendramine'
when allergy_substr like '%diphenhydrAMINE%' then 'diphenhydrAMINE'when allergy_substr like '%dolutegravir%' then 'dolutegravir'
when allergy_substr like '%doxycycline%' then 'doxycycline'when allergy_substr like '%emollients,%' then 'emollients,'
when allergy_substr like '%erythromycin%' then 'erythromycin'when allergy_substr like '%Erythromycin,%' then 'Erythromycin,'
when allergy_substr like '%etoposide%' then 'etoposide'when allergy_substr like '%Excedrin%' then 'Excedrin'
when allergy_substr like '%Flonase%' then 'Flonase'when allergy_substr like '%Focalin%' then 'Focalin'
when allergy_substr like '%fosfomycin%' then 'fosfomycin'when allergy_substr like '%fosphenytoin%' then 'fosphenytoin'
when allergy_substr like '%Haldol%' then 'Haldol'when allergy_substr like '%heparin%' then 'heparin'
when allergy_substr like '%Humira%' then 'Humira'when allergy_substr like '%hydralazine%' then 'hydralazine'
when allergy_substr like '%hydrocodone%' then 'hydrocodone'when allergy_substr like '%hydrocortisone%' then 'hydrocortisone'
when allergy_substr like '%hydroxyurea%' then 'hydroxyurea'when allergy_substr like '%ibuprofen%' then 'ibuprofen'
when allergy_substr like '%iodine%' then 'iodine'when allergy_substr like '%isoniazid%' then 'isoniazid'
when allergy_substr like '% K %' then 'Potassium'when allergy_substr like '%Keppra%' then 'Keppra'
when allergy_substr like '%ketamine%' then 'ketamine'when allergy_substr like '%LaMICtal%' then 'LaMICtal'
when allergy_substr like '%lamoTRIgine%' then 'lamoTRIgine'when allergy_substr like '%Levaquin%' then 'Levaquin'
when allergy_substr like '%lisinopril%' then 'lisinopril'when allergy_substr like '%Lortab%' then 'Lortab'
when allergy_substr like '%Macrobid%' then 'Macrobid'when allergy_substr like '%Magnesium%' then 'Magnesium'
when allergy_substr like '%melatonin%' then 'melatonin'when allergy_substr like '%Mestinon%' then 'Mestinon'
when allergy_substr like '%midazolam%' then 'midazolam'when allergy_substr like '%morphine%' then 'morphine'
when allergy_substr like '%Motrin%' then 'Motrin'when allergy_substr like '%naproxen%' then 'naproxen'
when allergy_substr like '%narcotic%' then 'narcotic'when allergy_substr like '%nitric%' then 'nitric'
when allergy_substr like '%nitrofurantoin%' then 'nitrofurantoin'when allergy_substr like '%nitrous%' then 'nitrous'
when allergy_substr like '%nonsteroidal%' then 'nonsteroidal'when allergy_substr like '%NSAIDs%' then 'NSAIDs'
when allergy_substr like '%nystatin%' then 'nystatin'when allergy_substr like '%olopatadine%' then 'olopatadine'
when allergy_substr like '%Omnicef%' then 'Omnicef'when allergy_substr like '%oxyCODONE%' then 'oxyCODONE'
when allergy_substr like '%Pediacare%' then 'Pediacare'when allergy_substr like '%pegaspargase%' then 'pegaspargase'
when allergy_substr like '%penicillin%' then 'penicillin'when allergy_substr like '%penicillins%' then 'penicillins'
when allergy_substr like '%Percocet%' then 'Percocet'when allergy_substr like '%PHENobarbital%' then 'PHENobarbital'
when allergy_substr like '%Polytrim%' then 'Polytrim'when allergy_substr like '%predniSONE%' then 'predniSONE'
when allergy_substr like '%procainamide%' then 'procainamide'when allergy_substr like '%propofol%' then 'propofol'
when allergy_substr like '%raNITIdine%' then 'raNITIdine'when allergy_substr like '%Reglan%' then 'Reglan'when allergy_substr like '%Retin-A%' then 'Retin-A'
when allergy_substr like '%RisperDAL%' then 'RisperDAL'when allergy_substr like '%riTUXimab%' then 'riTUXimab'when allergy_substr like '%Robitussin%' then 'Robitussin'
when allergy_substr like '%Rocephin%' then 'Rocephin'when allergy_substr like '%Similac%' then 'Similac'when allergy_substr like '%sulfa%' then 'sulfa'
when allergy_substr like '%sulfamethoxazole%' then 'sulfamethoxazole'when allergy_substr like '%sulfonamides%' then 'sulfonamides'
when allergy_substr like '%Tamiflu%' then 'Tamiflu'when allergy_substr like '%temsirolimus%' then 'temsirolimus'
when allergy_substr like '%tobramycin%' then 'tobramycin'when allergy_substr like '%Trileptal%' then 'Trileptal'
when allergy_substr like '%Tylenol%' then 'Tylenol'when allergy_substr like '%vancomycin%' then 'vancomycin'when allergy_substr like '%Vicks%' then 'Vicks'
when allergy_substr like '%Zantac%' then 'Zantac'when allergy_substr like '%Zithromax%' then 'Zithromax'when allergy_substr like '%Zofran%' then 'Zofran'
when allergy_substr like '%Zonegran%' then 'Zonegran'when allergy_substr like '%Zosyn%' then 'Zosyn'when allergy_substr like '%ZyrTEC%' then 'ZyrTEC'
else '0'
end Medication_allergy
from small_notes)


       select patient_fin_notes,
       Medication_allergy
       from allergy_notes
"""
allergies = pd.read_sql(allergies_sql,conn, params=[start_date,end_date])
allergies = allergies.sort_values('patient_fin_notes')
allg = allergies.drop_duplicates()

"""
#ALL THIS BELOW IS A WORK IN PROGRESS!
allg['Medication_allergy2'] = allg[allg.patient_fin_notes==allg.shift(-1).patient_fin_notes].shift(-1).Medication_allergy
allg['Medication_allergy3'] = allg[allg.patient_fin_notes==allg.shift(-2).patient_fin_notes].shift(-2).Medication_allergy
allg['Medication_allergy4'] = allg[allg.patient_fin_notes==allg.shift(-3).patient_fin_notes].shift(-3).Medication_allergy
allg['Medication_allergy5'] = allg[allg.patient_fin_notes==allg.shift(-4).patient_fin_notes].shift(-4).Medication_allergy

allerg = allg.drop_duplicates('patient_fin_notes')

#TASK 3, MAKE SURE QUERIES CAN BE MERGED AND PRODUCE REASONABLE TABLE, WITH ONE ROW FOR EACH VISIT
km = pd.merge(uti,allerg,how="left",left_on='PATIENT_FIN',right_on='patient_fin_notes')


#TASK 4, FIND THE DIRECTORY WHERE THE SPREADSHEET GETS SAVED
filename = f"UTI_{date_range}.xlsx"
writer = pd.ExcelWriter(filename, engine='xlsxwriter')
lwot.to_excel(writer, sheet_name='UTI_data',float_format="%.0f")
writer.save()
"""

