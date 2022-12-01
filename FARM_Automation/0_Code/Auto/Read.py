print("--------------------------------------------------Executing Read.py--------------------------------------------------")
import os
from datetime import datetime
global paramfile_name
from Paths import*

#time stamp
global timestamp
t= datetime.now() # current date and time
timestamp = t.strftime("%Y%m%d_ %H%M%S")
variables = {}
root, dirs, file_name = os.walk(input_folder).__next__()
txt_flag=0
xl_flag=0
for x in range(len(file_name)):
   split = file_name[x].split('.')
   print(x)
   if(split[1]=='txt'):
       txt_flag=1
       paramfile_name=file_name[x]
       split_param_name=split[0]
       split_param_exten=split[1]
   elif(split[1]=='xlsx'):
       xl_flag=1
       pagination_name=file_name[x]
       split_pagination_name = split[0]
       split_pagination_exten = split[1]

if(txt_flag ==1 and xl_flag==1):
    print("reading input file")
elif(txt_flag !=1 and xl_flag!=1):
    exit('Upload the right input files:')

input_txt=input_folder+paramfile_name
input_xl=input_folder+pagination_name
dev_folder=input_folder+str(dirs[0])


