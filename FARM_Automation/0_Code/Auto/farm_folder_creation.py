print('------------------------------------------Farm_folder_creation-------------------------------------')
from API_read_Farm import*
from Paths import*
import os
#folder_creation
clms_fol_name='CAA17372_CLMS_'+lob+'_'+state_alpha+"_"+timestamp
eips_fol_name='CAA17372_EIPS_'+lob+'_'+state_alpha+"_"+timestamp

#folder_Comparison
dev_op_root,dev_op_dirs,dev_op_files=os.walk(dev_op_path).__next__()


for x in dev_op_dirs:
    s=x.split("_")
    if(s[1]=='CLMS'):
        dev_clms_folder=x
        dev_folder_check_clms=s[0]+"_"+s[1]+"_"+s[2]+"_"+s[3]
    if(s[1]=="EIPS"):
        dev_eips_folder=x
        dev_folder_check_eips=s[0]+"_"+s[1]+"_"+s[2]+"_"+s[3]

test_folder_check_clms='CAA17372_CLMS_'+lob+'_'+state_alpha
test_folder_check_eips='CAA17372_EIPS_'+lob+'_'+state_alpha


if(test_folder_check_clms!=dev_folder_check_clms and test_folder_check_eips!=dev_folder_check_eips):
    print('folder names are wrong or load the right dev op')

#print(dev_op_root+"\\"+dev_clms_folder)

#if(dev_folder_check_eips==test_folder_check_eips and dev_folder_check_clms==test_folder_check_clms):
eips_root,eips_dirs,eips_files=os.walk(dev_op_root+"\\"+dev_eips_folder).__next__()
clms_root,clms_dirs,clms_files=os.walk(dev_op_root+"\\"+dev_clms_folder).__next__()

#print(clms_files)

for x in clms_files:
    if(x=='FARMSLCC.TXT' or x=='FARMSLCC.txt'):
        clms_data_file=x
    elif(x=='HEAD.txt' or x=='HEAD.txt'):
        clms_head_file=x
for x in eips_files:
    if(x=='FARMSLCE.TXT' or x=='FARMSLCE.txt'):
        eips_data_file=x
    elif(x=='HEAD.TXT' or x=='HEAD.txt'):
        eips_head_file=x

fullpath_clms_data_file=clms_root+"\\"+clms_data_file
fullpath_clms_head_file=clms_root+"\\"+clms_head_file
fullpath_eips_data_file=eips_root+"\\"+eips_data_file
fullpath_eips_head_file=eips_root+"\\"+eips_head_file

#print(fullpath_eips_head_file,"\n",fullpath_eips_data_file,"\n",fullpath_clms_head_file,"\n",fullpath_clms_data_file)
#Executed Testcase folder creation
testcase_dir=executed_tc+'\\'+lob+'\\'+state_alpha+"_"+timestamp
os.mkdir(testcase_dir)
os.mkdir(testcase_dir+'\\'+'CLMS')
os.mkdir(testcase_dir +'\\'+'EIPS')
print('CLMS and Eips folders are created')
