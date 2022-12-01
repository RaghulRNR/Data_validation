print("--------------------------------------------------pagination.py--------------------------------------------------")

from copying_to_data import*
import pandas as pd
from lookup import*
from API_read import*
import numpy as np
filter_pagination=pd.DataFrame()
class_plan = str(class_plan)
coverage = str(coverage)
if ((class_plan == 'old_sim' or class_plan == "legacy" or class_plan == "ocp" or class_plan == "sim") and coverage == 'liability'):
    DCSRPS_CLASS_TYPE_IND = 100
elif ((class_plan == 'old_sim' or class_plan == "legacy" or class_plan == "ocp" or class_plan == "sim") and coverage == 'physicaldamage'):
    DCSRPS_CLASS_TYPE_IND = 200;
elif ((class_plan == 'old_sim' or class_plan == "legacy" or class_plan == "sim") and coverage == 'autodealers and garagekeepers'):
    DCSRPS_CLASS_TYPE_IND = 300;
if ((class_plan == 'old_sim' or class_plan == "legacy" or class_plan == "ocp" or class_plan == "sim") and coverage == 'liability' and state_alpha=='DC'):
    DCSRPS_CLASS_TYPE_IND = 100
    dc_support = 110
if (state_alpha != 'DC'):
    filter_pagination = pagination[
        (pagination['DCSRPS_STATE'] == state_alpha) & (pagination['DCSRPS_CLASS_TYPE_IND'] == DCSRPS_CLASS_TYPE_IND)]
    print(filter_pagination)
else:
    filter_pagination = pagination[
        (pagination['DCSRPS_STATE'] == state_alpha) & (pagination['DCSRPS_CLASS_TYPE_IND'] == DCSRPS_CLASS_TYPE_IND) &
        (pagination['DCSRPS_CLASS_TYPE_IND'] == dc_support)]
    print(filter_pagination)

terr_count=len(filter_pagination.index)
print('Territory_count=',terr_count)
filter_pagination['rate_terr']=filter_pagination['DCSRPS_TERRITORY']
terrtiory_list=filter_pagination['DCSRPS_TERRITORY'].astype(int)
terrtiory_list=terrtiory_list.sort_values()
terrtiory_list=terrtiory_list.reset_index(drop=True)
print(terrtiory_list)
filter_pagination['rate_terr']=filter_pagination['rate_terr'].astype(int)
ex['rate_terr']=ex['rate_terr'].astype(int)
ex['Value']=ex['Value'].astype(float)
ex=ex.round({"Value":2})
#print(ex['Value'])

ex['int_value']=ex['Value'].astype(int)
ex['float_value']=ex['Value'].astype(float)
ex['con_value']=np.where((ex['int_value']==ex['float_value']),ex['int_value'].astype(str),ex['float_value'])
ex['con_value']=ex['con_value'].astype(str)
merged=pd.merge(filter_pagination, ex, on='rate_terr')
#print(filter_pagination)
merged.to_excel("C:\\Users\\i30691\\OneDrive - Verisk Analytics\\Documents\\py\\Auto\\data\\merged_"+data_file_name)


record_len=pd.DataFrame()
len_lkup=pd.read_excel(length_fol)
len_lkup=len_lkup.astype(str)
space_temp=len_lkup[len_lkup['state']==state_alpha]
temp_len_record=len_lkup[len_lkup['state']==state_alpha]
for x in space_temp['spaces_at_end']:
    space_at_end=x
for x in temp_len_record['length']:
    len_record=x
print('space_at_end=',space_at_end)
print('length_of each_recoed=',len_record)
space_at_end=int(space_at_end)-1
len_record=int(len_record)