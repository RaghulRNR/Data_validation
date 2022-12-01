print("--------------------------------------------------Executing lookup.py------------------")

from copying_to_data import *
from Paths import*

state=int(state)
state=str(state)
#print(state)
lkup=pd.read_excel(lkup_folder+"state_2.xlsx")
lkup['State_code']=lkup['State_code'].apply(str)
#print(lkup['state_cd'].value)
temp=lkup[lkup['State_code']==state]
#state_alpha=temp[temp['state']]

for x in temp['state']:
    state_alpha=x
for x in temp['state_name']:
    state_name=x

#print(lkup)
print(temp)
#print(state_alpha)
#print(state_name)
