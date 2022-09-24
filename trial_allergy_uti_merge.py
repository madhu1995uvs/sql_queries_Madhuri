#%%
"""import packages"""
import pandas as pd
import os

# %%
"""import files to merge"""
allergies = pd.read_csv(r"C:\Users\kmckinley\OneDrive - Children's National Hospital\Documents\Colleagues\fellows\Malek\allergies_2018-2021.csv")
uti =  pd.read_excel(r"C:\Users\kmckinley\OneDrive - Children's National Hospital\Documents\Colleagues\fellows\Malek\UTI_2022-09-07.xlsx")


# %%
"""merge df"""
uti_allerg = pd.merge(uti, allergies, how = 'left', left_on= 'patient_fin', right_on = 'Financial Number')
# %%
#NOT YET DONE! NEED TO APPLY FUNCTIONS
#make entire df lowercase, including column names
abx=abx.applymap(lambda s: s.lower() if type(s) == str else s)
abx.columns=abx.columns.str.lower()

abx_scd=abx_scd.applymap(lambda s: s.lower() if type(s) == str else s)
abx_scd.columns=abx_scd.columns.str.lower()

def cat_abx(drug):
    """IMPORTANT TO LIST ABX WITH BETA LACTAMASE INHIBITORS BEFORE THE RELEVANT BETA LACTAM CATEGORIES IN THE IF/THEN STATEMENTS"""
    cat_aminoglycoside = re.compile(r'(amikacin|tobramycin|neomyc|gentamicin)')
    cat_penicillin = re.compile(r'(penicillin|pen g benz|pen g pot|benzathine penic)')
    cat_penicillin_beta_lactamase_resistant =re.compile(r'(nafcillin|oxacillin)')
    cat_aminopcn_or_carboxypcn_or_ureidopcn_w_beta_lactamase_inhibitor = re.compile(r'((amox|ampic|ticarc|piperac).*(clav|bactam|tazo)|augmentin|unasyn|zosyn)')
    cat_aminopenicillin = re.compile(r'am(oxi|pi)cillin')
    cat_cephalosporin_w_beta_lactamase_inhibitor = re.compile(r'(cef|ceph).+bactam')
    cat_first_gen_cephalosporin= re.compile(r'(ancef|cefa[^c]|cephalexin)')
    cat_second_gen_cephalosporin = re.compile(r'(cefoxi|cef[up]ro[xz]|cefotetan)')
    cat_third_gen_cephalosporin = re.compile(r'(ceftri|cefttri|cefix|cefdin|cefotax|ceftaz|cefpodox)')
    cat_fourth_gen_cephalosporin = re.compile(r'cefepime')
    cat_fifth_gen_cephalosporin = re.compile(r'ceftaroline')
    cat_lincosamide = re.compile(r'clinda')
    cat_nitroimidazole =re.compile(r'(metronidazole|flagyl)')
    cat_triazinane = re.compile(r'methenamine')
    cat_glycopeptide =re.compile(r'(vancomyc|vancomyin|dalbavanc)')
    cat_antimalarial =re.compile(r'atovaquone')
    cat_macrolide =re.compile(r'([yi]thromycin|e.e.s|fidaxomicin)')
    cat_monobactam =re.compile(r'aztreonam')
    cat_other_topical =re.compile(r'(bacitracin|mafenide|mupirocin|topical|ointment|ophthalmic|(top|opht|otic).+(soln|oint|susp)|dexameth|benzoyl peroxide|antibiotic irrigation mixture|silver)')
    cat_sulfonamide =re.compile(r'(bactrim|sulf.+tri|sulfadiazine)')
    cat_fluoroquinolone =re.compile(r'[oi]floxacin')
    cat_polypeptide =re.compile(r'colistimethate')
    cat_antimycobacterial =re.compile(r'(dapsone|rifampin|rifapentine|rifaximin|rifamixin|rifabutin|isoniazid|ethambutol|xifaxan)')
    cat_cyclic_lipopeptide =re.compile(r'daptomycin')
    cat_tetracycline = re.compile(r'(doxy|mino|tetra|tige)cycline')
    cat_carbapenem = re.compile(r'(erta|mero|imi)penem')
    cat_oxazolidinone = re.compile(r'linezolid')
    cat_nitrofuran = re.compile(r'nitrofuran')
    cat_phosphonic = re.compile(r'fosfomycin')
    cat_antiprotozoal = re.compile(r'pentamidine')
    cat_folate_syn_inhibitor = re.compile(r'trimethoprim')


    if bool(cat_aminoglycoside.search(drug))==True:
        return ('aminoglycoside')
    elif bool(cat_penicillin.search(drug))==True:
        return ('penicillin')
    elif bool(cat_penicillin_beta_lactamase_resistant.search(drug))==True:
        return ('penicillin_beta_lactamase_resistant')
    elif bool(cat_aminopcn_or_carboxypcn_or_ureidopcn_w_beta_lactamase_inhibitor.search(drug))==True:
        return ('aminopcn_or_carboxypcn_or_ureidopcn_w_beta_lactamase_inhibitor')
    elif bool(cat_aminopenicillin.search(drug))==True:
        return ('aminopenicillin')
    elif bool(cat_cephalosporin_w_beta_lactamase_inhibitor.search(drug))==True:
        return ('cephalosporin_w_beta_lactamase_inhibitor')
    elif bool(cat_first_gen_cephalosporin.search(drug))==True:
        return ('first_gen_cephalosporin')
    elif bool(cat_second_gen_cephalosporin.search(drug))==True:
        return ('second_gen_cephalosporin')
    elif bool(cat_third_gen_cephalosporin.search(drug))==True:
        return ('third_gen_cephalosporin')
    elif bool(cat_fourth_gen_cephalosporin.search(drug))==True:
        return ('fourth_gen_cephalosporin')
    elif bool(cat_fifth_gen_cephalosporin.search(drug))==True:
        return ('fifth_gen_cephalosporin')
    elif bool(cat_lincosamide.search(drug))==True:
        return ('lincosamide')
    elif bool(cat_nitroimidazole.search(drug))==True:
        return ('nitroimidazole')
    elif bool(cat_triazinane.search(drug))==True:
        return ('triazinane')
    elif bool(cat_glycopeptide.search(drug))==True:
        return ('glycopeptide')
    elif bool(cat_antimalarial.search(drug))==True:
        return ('antimalarial')       
    elif bool(cat_macrolide.search(drug))==True:
        return ('macrolide')
    elif bool(cat_monobactam.search(drug))==True:
        return ('monobactam')
    elif bool(cat_sulfonamide.search(drug))==True:
        return ('sulfonamide')
    elif bool(cat_fluoroquinolone.search(drug))==True:
        return ('fluoroquinolone')
    elif bool(cat_polypeptide.search(drug))==True:
        return ('polypeptide')
    elif bool(cat_antimycobacterial.search(drug))==True:
        return ('antimycobacterial')
    elif bool(cat_cyclic_lipopeptide.search(drug))==True:
        return ('cyclic_lipopeptide')
    elif bool(cat_tetracycline.search(drug))==True:
        return ('tetracycline')
    elif bool(cat_carbapenem.search(drug))==True:
        return ('carbapenem')
    elif bool(cat_oxazolidinone.search(drug))==True:
        return ('oxazolidinone')
    elif bool(cat_nitrofuran.search(drug))==True:
        return ('nitrofuran')
    elif bool(cat_phosphonic.search(drug))==True:
        return ('phosphonic')
    elif bool(cat_antiprotozoal.search(drug))==True:
        return ('antiprotozoal')
    elif bool(cat_folate_syn_inhibitor.search(drug))==True:
        return ('folate_syn_inhibitor')
#might be better to run with other_topical before aminoglycoside and sulfonamide to capture bacitracin/neomycin/polymyxin triple abx before they are labeled inappropriately as aminoglycoside and/or polytrim eye drops labeled as sulfonamides
    elif bool(cat_other_topical.search(drug))==True:
        return ('other_topical')

def cat_ceftriaxone(drug):
    cat_ceftri = re.compile(r'(ceftri|cefttri)')
    if bool(cat_ceftri.search(drug))==True:
        return (1)
    else:
        return (0)

abx['new_abx_category']=abx.medname.apply(cat_abx)
abx['ceftriaxone']=abx.medname.apply(cat_ceftriaxone)

abx_scd['new_abx_category']=abx_scd.medname.apply(cat_abx)
abx_scd['ceftriaxone']=abx_scd.medname.apply(cat_ceftriaxone)