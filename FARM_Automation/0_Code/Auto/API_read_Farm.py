print("--------------------------------------------------Executing API_read_Farm.py----------")
import numpy as np
import json
from Paths import *
from urllib.request import urlopen
from lookup import*

import pandas as pd

def apicall(res,filingDesignation,filingVersion):
    result2 = pd.DataFrame()
    filterdata = pd.DataFrame()
    for x in range(len(res)):
        print(res[x])
        url_id = 'http://ratingapi-internal-losscostrepositoryt.iso.com/api/Imports/' + str(res[x])
        json_url = urlopen(url_id)
        data = json.loads(json_url.read())
        imports_data = pd.json_normalize(data)
        imports_data.at[:,"merge"] = 1
        rates_data = pd.json_normalize(data, 'rates')
        rates_data.at[:,"merge"] = 1
        result = pd.merge(imports_data, rates_data, on='merge').drop("merge", 1)
        result2 = result2.append(result)
        #filterdata = result[(result['imports.filingDesignation'] == filingDesignation) & (result['imports.filingVersion']==filingVersion)]
    return result2

def api_link(json_request):
    url='http://ratingapi-internal-losscostrepositoryt.iso.com/api/GetImportIdsFromExpandedSearch'
    headers={'content-type':'application/json', 'Accept':'application/json'}
    response=requests.request("POST",url,data=json_request,headers=headers)
    #print('----------------------------------------------------------------------------------------------------------------------------------------------')
    print('response=',response)
    res= response.json()
    print(res)
    return res

state=state.zfill(2)
request='{ "LineOfBusiness": ["'+lob+'"], "State": ["' + state + '"],"FilingDesignation":["'+filingDesignation+'"],"FilingVersion":["'+filingVersion+'"]}'
print(request)
id=api_link(request)
data=apicall(id,filingDesignation,filingVersion)

ex=data.drop(columns=['importComments','metadata','rates','audit'])
data_file_name=lob+"_"+state_alpha+'_'+timestamp+'.xlsx'
#ex.to_excel("C:\\Users\\i30691\OneDrive - Verisk Analytics\\Documents\\py\\Auto\\troubleshoot\\MA_data.xlsx")
for x in zero_zoned:
    if(x==state_alpha ):
        ex['zone']='0'
if(state_alpha=="MA"):
    ex['zone']=ex['zone'].fillna(0)
#ex.to_excel("C:\\Users\\i30691\OneDrive - Verisk Analytics\\Documents\\py\\Auto\\troubleshoot\\API_Read.xlsx")

ex['Value']=ex['Value'].fillna(0)
ex=ex.reset_index(drop=True)
for x,y,index in zip(ex['Value'],ex['ValuePrecision'],ex.index):
    ex.loc[index,'ex_value']=x
    ex.loc[index,'ex_pre']=y
    temp=eval("'."+str(y)+"f'")
    ex.loc[index,'Value']=format(float(x),temp)
ex['Value']=ex['Value'].astype(str)
table_lkup=pd.read_excel(lkup_folder+"table.xlsx")
table_merge=pd.merge(ex,table_lkup)

group_table=pd.read_excel(lkup_folder+'group_table.xlsx')
state_group=pd.read_excel(lkup_folder+"state_group.xlsx")

temp_group=state_group[state_group['State']==state_alpha]
for x in temp_group['State_group']:
    find_group = x
#print(find_group)
group_table=pd.read_excel(lkup_folder+'group_table.xlsx')
#print(group_table)
filter_group=group_table[group_table['Group']==find_group]
#filter_group['zone_mapping']=filter_group['zone_mapping'].astype(int)

data_location=data_folder_fr + data_file_name
table_merge.to_excel(data_folder_fr + data_file_name)
