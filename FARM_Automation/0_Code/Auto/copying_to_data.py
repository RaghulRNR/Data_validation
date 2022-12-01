print("--------------------------------------------------Executing copying_to_data.py----------------------------------------")
import os
from Read import variables
from Read import*
from Paths import *

import shutil
import pandas as pd
import requests
ts='======================================Time-Stamp=%s========================================'%timestamp
print(ts)
destination=data_folder
#spiliting_LOB_from_input_filename
split_lob=split_param_name.split("_")
lob=split_lob[1]
#print("LOB=",lob)
lob=lob.strip()
#Renaming with Time_Stamp
data_param_name=split_param_name+"_"+timestamp+"."+split_param_exten
data_pagination_name=split_pagination_name+"_"+timestamp+"."+split_pagination_exten
dev_op_name=str(dirs[0])+"_"+timestamp
#final_destination path
data_param_path=destination+data_param_name
data_pagination_path=destination+data_pagination_name
dev_op_path=dev_op+dev_op_name
#Copying_to data and dev_op
shutil.copyfile(input_txt,data_param_path)
shutil.copyfile(input_xl,data_pagination_path)
shutil.copytree(dev_folder,dev_op_path)

#Removing Files and folder in input folder:-
r,d,f=os.walk(dev_folder).__next__()
'''os.remove(input_txt)
os.remove(input_xl)
for x in d:
    g=dev_folder+"\\"+x
    shutil.rmtree(g)'''

with open(data_param_path) as f:
    for line in f:
        name, value = line.split("=")
        variables[name] = str(value)
if(lob=="CA"):
    state=variables["state"]
    coverage=variables["coverage"]
    class_plan=variables["class_plan"]
    cars=variables["cars"]
    ppt=variables["ppt"]
    gar=variables["gar"]
    user_id=variables["user_id"]
    host_job=variables["host_job"]

    split_cars=cars.split("A")
    split_ppt=ppt.split("A")
    split_gar=gar.split("A")
    A_ttt=cars[0]
    A_pubu=cars[0]
    A_ppt=ppt[0]
    A_deal=gar[0]
    A_keep=gar[0]

    lc_vrsn_ttt = split_cars[1].rstrip('\n')
    lc_vrsn_pubu = split_cars[1].rstrip('\n')
    lc_vrsn_ppt = split_ppt[1].rstrip('\n')
    lc_vrsn_keep = split_gar[1].rstrip('\n')
    lc_vrsn_deal = split_gar[1].rstrip('\n')
    state=state.rstrip('\n')
    class_plan=class_plan.rstrip('\n')
    coverage=coverage.rstrip('\n')
    host_job=host_job.rstrip('\n')
elif(lob=="FR"):
    state = variables["state"]
    user_id = variables["user_id"]
    param_lob=variables["lob"]
    filingDesignation=variables["filingDesignation"]
    filingVersion=variables["filingVersion"]

    state=state.rstrip('\n')
    user_id=user_id.rstrip('\n')
    param_lob=param_lob.rstrip('\n')
    filingDesignation=filingDesignation.rstrip('\n')
    filingVersion=filingVersion.rstrip('\n')

state=state.zfill(2)
global pagination
pagination=pd.read_excel(data_pagination_path,engine='openpyxl')
#print(pagination)
print('Input files are moves to Data_folder')
#Api_Test:-
print('---------------------------------------API TEST---------------------------------------')
empty='{}'
url='http://ratingapi-internal-losscostrepositoryt.iso.com/api/GetImportIdsFromExpandedSearch'
headers={'content-type':'application/json', 'Accept':'application/json'}
response=requests.request("POST",url,data=empty,headers=headers)
#print(response)
response_code=response.status_code
#print(response_code)
if(response_code==200):
   print('API is Up and Running')
else:
    exit('API is Down')

