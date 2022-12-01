print("--------------------------------------------------dev_data_load.py--------------------------------------------------")

import pandas as pd
from pagination import*
from condition import*
import numpy as np
from Paths import*
from dev_vs_test_folder_comp import*
compare = pd.DataFrame()
#compare_temp = pd.DataFrame()
def s(a,compare_temp):
    flag=1
    terr=""
    #print(compare_temp)
    for x in compare_temp[a]:
        if (x == "Fail"):
            flag = 0
            #terr=terr+','+compare_temp['DCSRPS_TERRITORY'][x].astype(str)
    if (flag == 0):
        k = 'fail'
    elif (flag == 1):
         k = 'pass'

    return k,terr
#Function for spliting positions from testcases:
def sep(a):
    split = a.split("-")
    start = int(split[0])-1
    end = int(split[1])
    return start,end


def dev_load(dev_op, test_op,program):
    dev_sin_clms_data_load = dev_op
    load_test_clms_op = test_op

    start = 0
    end = len_record - 1
    # Spliting the output into multiple lines
    for x in range(terr_count):
        dev_sin_clms_data_load[x] = dev_sin_clms_data_load['tc0'].map(lambda x: x[start:end])
        start = end + 1
        end = end + len_record

    final_dev_clms_data_load = dev_sin_clms_data_load = dev_sin_clms_data_load.transpose()

    # renaming the header of loaded dev_op
    dev_sin_clms_data_load.columns = ['tc0']
    dev_sin_clms_data_load = dev_sin_clms_data_load.drop(['tc0'][0], axis=1)

    length = pd.read_excel(Testcase_fol + state_category + '.xlsx', sheet_name='Len')

    # merging the dev_op and positions(length)
    final_dev_clms_data_load = final_dev_clms_data_load.astype(str)
    length = length.astype(str)
    final_dev_clms_data_load['merge'] = 1
    length['merge'] = 1
    merged_clms_op = pd.merge(final_dev_clms_data_load, length, on='merge')
    final_dev_clms_data_load = final_dev_clms_data_load.astype(str)
    length = length.astype(str)
    # merged_clms_op.to_excel('C:\\Users\\i30691\\OneDrive - Verisk Analytics\\Documents\\py\\Auto\\dev_merged.xlsx')
#loading_testcases:-
    Testcase = pd.read_excel(Testcase_fol + state_category + ".xlsx", sheet_name=Sheet_name)
    print(Testcase)
    testcase_count = len(Testcase.index)
    print('Testcase_count=', testcase_count)
#spliting_According to position:-
    length_ic = 1
    index = 0
    dev_list = []
    for x in range(testcase_count):
        length_tc = 'tc'
        tc = 'dev_tc'
        tc = tc + str(length_ic)
        length_tc = length_tc + str(length_ic)
        start, end = sep(merged_clms_op[length_tc][index])
        merged_clms_op[tc] = merged_clms_op['tc0'].map(lambda x: x[start:end])
        dev_list.append(tc)
        length_ic = int(length_ic) + 1

    merged_clms_op.to_excel(test_check + 'splited' + timestamp + '.xlsx')
    # merging test_op and dev_op into single table:
    mergedd_dev_test_op = pd.DataFrame()
    dev_eips_op = merged_clms_op[dev_list].copy()
    # dropping the single line data:-
    dev_eips_op = dev_eips_op.iloc[1:, :]

    load_test_clms_op = load_test_clms_op.reset_index(drop=True)
    dev_eips_op = dev_eips_op.reset_index(drop=True)
    load_test_clms_op.to_excel(test_check + 'test_op' + timestamp + '.xlsx')
    dev_eips_op.to_excel(test_check + 'dev_op' + timestamp + '.xlsx')
    #merging test and dev op:-
    mergedd_dev_test_op = pd.concat([load_test_clms_op, dev_eips_op], axis=1)
    mergedd_dev_test_op.to_excel(test_check + 'test and dev' + timestamp + '.xlsx')
    # comparing Test_op and dev_op

    ic = 1
    for y in range(testcase_count):
        tc = 'tc'
        dev_tc = 'dev_tc'
        tc = tc + str(ic)
        dev_tc = dev_tc + str(ic)
        compare[tc] = np.where(mergedd_dev_test_op[tc] == mergedd_dev_test_op[dev_tc], 'Pass', 'Fail')
        ic = int(ic) + 1
    compare_temp=pd.concat([compare,terrtiory_list], axis=1)
    compare_temp.to_excel(test_check + 'compare' + timestamp + '.xlsx')
    final_result = pd.DataFrame()
    ic = 1
    for x in range(testcase_count):
        tc = 'tc'
        a = tc + str(ic)
        final_result.at[1, a],final_result.at[2, a] = s(a,compare_temp)
        ic = int(ic) + 1
    # print(final_result)
    # transpose
    ex = final_result.transpose().reset_index()
    ex['tc_no'] = ex["index"]
    ex['res'] = ex[1]
    ex['terr']=ex[2]
    print(ex)
    # merging Testcase and final op
    merged_testcase_and_final = pd.concat([ex, Testcase], axis=1)
    executed_clms_testcase = pd.DataFrame()
    executed_clms_testcase = Testcase
    ic = 1
    for x in range(testcase_count):
        tc = 'tc'
        tc = tc + str(ic)
        executed_clms_testcase['Pass/Fail'] = np.where(
            merged_testcase_and_final['Test_Case_ No'] == merged_testcase_and_final['index'],
            merged_testcase_and_final['res'], 'A')
        ic = int(ic) + 1
    executed_clms_testcase.to_excel(executed_tc + Sheet_name +"_"+program+ "_Testcase_execution_" + timestamp + ".xlsx")
    print(Testcase)


dev_clms = pd.read_csv(fullpath_clms_data_file, delimiter='\n', names=['tc0'])
dev_eips = pd.read_csv(fullpath_eips_data_file, delimiter='\n', names=['tc0'])
dev_load(dev_clms, test_clms_op,"CLMS")
dev_load(dev_eips, test_eips_op,"EIPS")