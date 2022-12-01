import pandas as pd
import numpy as np
input=pd.read_excel("C:\\Users\\i30691\\OneDrive - Verisk Analytics\\Documents\\py\\Auto\\troubleshoot\\testcase_check.xlsx")
dev_list=list()
re=list()
#print(input)
for x in range(1,156):
    a='tc'+str(x)
    dev_list.append(a)
#print(dev_list)
for x in range(9,156):
    a='tc'+str(x)
    re.append(a)
merged=pd.DataFrame()
merged=input[dev_list].copy()
print(merged)
merged[re]=merged[re].astype(float)

#pd.options.display.float_format = '{:,.2f}'.format-not working
#merged=merged.style.format("{:.2f%}")-not working
#merged[re]=pd.set_option('display.float_format','{:.2f}'.format)-not working
#merged.to_csv('C:\\Users\\i30691\\OneDrive - Verisk Analytics\\Documents\\py\\Auto\\troubleshoot\\test.txt')

'''for x in range(9,156):
    a='tc'+str(x)
    merged[a].round(decimals=2)'''
merged2=pd.DataFrame()
merged3=pd.DataFrame()


#merged[re]=np.round(merged[re],decimals=2)
#merged2=merged[re].round(decimals=2)

merged=merged.groupby(['tc1','tc2','tc3','tc4','tc5','tc6','tc7','tc8'],as_index=False).sum()
first_8=merged[['tc1','tc2','tc3','tc4','tc5','tc6','tc7','tc8']].copy()
next_all=merged[re].copy()
next_all.astype(float)
for x in next_all:
    inx=0
    for y in next_all[x]:
        #print(format(y,".2f"))
        merged3.at[inx,x]=format(y,".2f")
        merged3[x]=merged3[x].str.pad(7,side='left')
        inx=inx+1
final_op=pd.concat([first_8,merged3],axis=1)
final_op[['tc1','tc2','tc3','tc4','tc5','tc6','tc7','tc8']].astype(str)
final_op['tc3']=final_op['tc3'].str.pad(25,side='right')
final_op['tc4']=final_op['tc4'].str.pad(8,side='right')
final_op['tc6']=final_op['tc6'].str.pad(3,side='right')
final_op['tc7']=final_op['tc7'].str.pad(18,side='right')
final_op['tc8']=final_op['tc8'].str.pad(10,side='right')


final_op.to_excel('C:\\Users\\i30691\\OneDrive - Verisk Analytics\\Documents\\py\\Auto\\troubleshoot\\test.xlsx')
np.savetxt('C:\\Users\\i30691\\OneDrive - Verisk Analytics\\Documents\\py\\Auto\\troubleshoot\\final.txt',final_op.values,fmt="%s",delimiter='')