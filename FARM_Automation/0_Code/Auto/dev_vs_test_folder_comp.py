print("--------------------------------------------------Executing dev_vs_test_folder_comp.py--------------------------------------------------")
import pandas as pd
from Paths import*
from folder_creation import*
from condition import*
import os
import os
root, dirs, files = os.walk(dev_op+"PAGEGEN").__next__()

for x in dirs:
    s=x.split("_")
    if(s[2]=="CLM"):
        dev_CLMS_fol=x
        dev_clms_folder_check=s[0]+"_"+s[1]+"_"+s[2]+"_"+s[3]
    elif(s[2]=="EIPS"):
        dev_EIPS_fol=x
        dev_eips_folder_check=s[0]+"_"+s[1]+ "_"+ s[2]+"_"+s[3]

test_clms_folder_check=coverage_fol+'_'+state_alpha+'_'+'CLM'+'_'+class_plan_fol
test_eips_folder_check=coverage_fol+'_'+state_alpha+'_'+'EIPS'+'_'+class_plan_fol


if(dev_eips_folder_check!=test_eips_folder_check and dev_clms_folder_check!=test_clms_folder_check):
    exit("folder names are wrong or load the right dev op")

if(dev_eips_folder_check==test_eips_folder_check and dev_clms_folder_check==test_clms_folder_check):
   clms_root,clms_dirs,clms_files=os.walk(dev_op+"PAGEGEN"+"\\"+dev_CLMS_fol).__next__()
   eips_root,eips_dirs,eips_files=os.walk(dev_op+"PAGEGEN"+"\\"+dev_EIPS_fol).__next__()


for x in clms_files:
    if(x=='LIABCL.TXT' or x=='LIABCL.txt'):
        clms_data_file=x
    elif(x=='HEAD.txt' or x=='HEAD.txt'):
        clms_head_file=x
for x in eips_files:
    if(x=='LIABLC.TXT' or x=='LIABLC.txt'):
        eips_data_file=x
    elif(x=='HEAD.TXT' or x=='HEAD.txt'):
        eips_head_file=x

fullpath_clms_data_file=clms_root+"\\"+clms_data_file
fullpath_clms_head_file=clms_root+"\\"+clms_head_file
fullpath_eips_data_file=eips_root+"\\"+eips_data_file
fullpath_eips_head_file=eips_root+"\\"+eips_head_file

print(fullpath_eips_head_file,"\n",fullpath_eips_data_file,"\n",fullpath_clms_head_file,"\n",fullpath_clms_data_file)
flag_48=0
flag_78=0
for x in range(len(ms_48)):
    if(ms_48[x]==state_alpha):
        state_category='MS48'+"_"+coverage+"_"+class_plan
        flag_48=1
        Sheet_name='MS48'
for x in range(len(ms_78)):
    if(ms_78[x]==state_alpha):
        state_category='MS78'+"_"+coverage+"_"+class_plan
        flag_78=1
        Sheet_name = 'MS78'
if(flag_78==0 and flag_48==0):
    state_category=state_alpha+"_"+coverage+"_"+class_plan
    Sheet_name = state_alpha

print(state_category)

    
