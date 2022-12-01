import numpy as np
import pandas as pd
conditions=pd.read_excel("C:\\Users\\i30691\\OneDrive - Verisk Analytics\\Documents\\py\Auto\\conditions\\testcase.xlsx")
input=pd.read_excel("C:\\Users\\i30691\\OneDrive - Verisk Analytics\\Documents\\py\\Auto\\troubleshoot\\AC_Data.xlsx")
conditions=conditions.fillna('|##|')
input=input.fillna('|##|')
input['Value']=input['Value'].replace(['|##|'],0)
input['Value']=input['Value'].astype(float)
conditions=conditions.add_prefix("con_")
conditions.at[:,'merge']='1'
input.at[:,'merge']='1'
merged=pd.merge(conditions,input,on='merge')
merged['con_O/P']=np.where((merged['TableDesignation']==merged['con_TableDesignation'])&(merged['causes_of_loss']==merged['con_causes_of_loss'])&
                           (merged['con_zone_y']==merged['con_zone_x'])&(merged['pol_lim_dols']==merged['con_pol_lim_dols']),merged['Value'],0)
co=merged[['con_S_no',"con_TC_NO",'con_TableDesignation','con_causes_of_loss','con_zone_x','con_pol_lim_dols','con_O/P']].copy()
temp=co.groupby(["con_TC_NO",'con_TableDesignation','con_causes_of_loss','con_zone_x','con_pol_lim_dols'],as_index=False).sum()
temp=temp.sort_values(by='con_S_no')
temp=temp.drop('con_S_no',axis=1)
temp.to_excel('C:\\Users\\i30691\\OneDrive - Verisk Analytics\\Documents\\py\\Auto\\troubleshoot\\Conditions_op.xlsx')
trans=temp[['con_TC_NO','con_O/P']].copy()

trans=trans.reset_index(drop=True)
trans=trans.set_index('con_TC_NO')
#trans=trans.reset_index(drop=True)
print(trans.transpose())