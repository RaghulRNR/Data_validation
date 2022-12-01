from API_read_Farm import *
from lookup import*
from fram_dev_op_comparison import*
import importlib
import os
print('----------------------------------------State_table_mapping--------------------------------------')
print(filter_group)
#'x=='AC' or x=='D' or x=='EF' or x=='G' or
for x,y,z in zip(filter_group['Map_table'],filter_group['zone_mapping'],filter_group['Sorting_order']):
    #if(x=='N'):
        r="==================================Table=%s,Zone=%s======================================="% (x,y)
        print(r)
        zone=str(y)
        Sorting=z
        temp=importlib.import_module(x)
        fun='temp.'+x+'(zone,Sorting)'
        clms,eips=eval(fun)
        print('--------------------Executing Farm_dev_op_comparision.py-------------------')
        comp_main(clms,eips,x,zone)




