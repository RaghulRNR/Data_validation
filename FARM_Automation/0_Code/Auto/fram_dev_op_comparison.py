import os
import pandas as pd
import importlib
from farm_folder_creation import *
from API_read_Farm import *
from Paths import *


def comp_main(clms, eips, table, zone):
    def sep(a):
        split = a.split("-")
        start = int(split[0]) - 1
        end = int(split[1])
        return start, end

    def eips_clms(dev_op, program,test_op):
        # splitting dev_op according to tables:-
        start = 0
        end = 1593
        dev_op_sep = pd.DataFrame()
        for x,y in zip(filter_group['Map_table'],filter_group['zone_mapping']):
            temp=str(x)+"_"+str(y)
            dev_op_sep[temp] = dev_op['tc0'].map(lambda x: x[start:end])
            start = end
            end = end + 1593
        trans_dev_op = dev_op_sep.transpose()
        #trans_dev_op.to_excel('C:\\Users\\i30691\\OneDrive - Verisk Analytics\\Documents\\py\\Auto\\troubleshoot\\multiple_line_dev.xlsx')
        map_table(trans_dev_op, program, table,test_op)  # hardcodeed value convert into variable

    def map_table(trans_dev_op, program, table,test_op):
        dev_table = pd.DataFrame()
        temp='test_'+program
        test_table = test_op
        temp=str(table)+"_"+str(zone)
        dev_table.at[temp, 0] = trans_dev_op.at[temp, 0]
        dev_test_comp(dev_table, test_table, table, program, zone)

    def dev_test_comp(dev_table, test_table, table, program, zone):
        # loading positions from testcase:-
        positions = pd.read_excel(Testcase_fol + "\\" + lob + "\\Table_" + table + '.xlsx', sheet_name='Len')
        # merging dev_op and positiond:-
        dev_table = dev_table.astype(str)
        positions = positions.astype(str)
        dev_table['merge'] = '1'
        positions['merge'] = '1'
        merged_dev_positions = pd.merge(dev_table, positions, on='merge')
        # loading Testcases:-
        Tetscase = pd.read_excel(Testcase_fol + "\\" + lob + "\\Table_" + table + '.xlsx')
        #merged_dev_positions.to_excel('C:\\Users\\i30691\\OneDrive - Verisk Analytics\\Documents\\py\\Auto\\troubleshoot\\merged_dev_positions.xlsx')
        testcase_count = len(Tetscase.index)
        print('Testcase_count', testcase_count)
        # spliting DEV OP to positions:-
        length_ic = 1
        index = 0
        dev_list = []
        for x in range(testcase_count):
            length_tc = 'tc'
            tc = 'dev_tc'
            tc = tc + str(length_ic)
            length_tc = length_tc + str(length_ic)
            start, end = sep(merged_dev_positions[length_tc][index])
            merged_dev_positions[tc] = merged_dev_positions[0].map(lambda x: x[start:end])
            dev_list.append(tc)
            length_ic = int(length_ic) + 1
        #merged_dev_positions.to_excel('C:\\Users\\i30691\\OneDrive - Verisk Analytics\\Documents\\py\\Auto\\troubleshoot\\merged_dev_positions_splitted.xlsx')
        # merging Test and dev OP:-
        dev_split_op = merged_dev_positions[dev_list].copy()
        merged_dev_test = pd.concat((dev_split_op, test_table), axis=1)

        q=test_table.add_prefix('dev_')
        A = pd.concat((dev_split_op, q), axis=0)
        #merged_dev_test.to_excel('C:\\Users\\i30691\\OneDrive - Verisk Analytics\\Documents\\py\\Auto\\troubleshoot\\merged_dev_test_op.xlsx')
        c=A.transpose()
        c.reset_index(inplace=True)
        c.columns = ['index', 'Actual_op','Expected_op']
        c["TC_No"]=c['index'].str.slice(4)
        del c['index']
        #c.to_excel('C:\\Users\\i30691\\OneDrive - Verisk Analytics\\Documents\\py\\Auto\\troubleshoot\\merged_dev_test_op_trans.xlsx')


        # comparing Test and Dev OP:-
        compare = pd.DataFrame()
        ic = 1
        for y in range(testcase_count):
            tc = 'tc'
            dev_tc = 'dev_tc'
            tc = tc + str(ic)
            dev_tc = dev_tc + str(ic)
            compare[tc] = np.where(merged_dev_test[tc] == merged_dev_test[dev_tc], 'Pass', 'Fail')
            ic = int(ic) + 1
        #compare.to_excel('C:\\Users\\i30691\\OneDrive - Verisk Analytics\\Documents\\py\\Auto\\troubleshoot\\dev_test_op_compare.xlsx')
        # Transposing  the commparision result:-
        final_compare = pd.DataFrame()
        inx = 0
        for x in compare:
            final_compare.at[inx, 'TC_No'] = x
            final_compare.at[inx, 'Result'] = compare.at[0, x]
            inx = inx + 1
        #final_compare.to_excel('C:\\Users\\i30691\\OneDrive - Verisk Analytics\\Documents\\py\\Auto\\troubleshoot\\Final_compare.xlsx')
        executed_testcase = pd.DataFrame()
        executed_testcase = pd.merge(Tetscase, c, on='TC_No')
        executed_testcase = pd.merge(executed_testcase,final_compare, on='TC_No')
        # Writing Final OP to the Folder:-
        print(' Test Execution for Table-',table,'_Zone',zone,'Completed')
        executed_testcase.to_excel(testcase_dir + "\\" + str(program.upper()) + '\\' + table + "_" + zone + "_" + program + '_Executed_testcase.xlsx',index=False)

    # -->Program starts here:-
    # loading the dev_op:-
    dev_clms = pd.read_csv(fullpath_clms_data_file, delimiter='\n', names=['tc0'])
    dev_eips = pd.read_csv(fullpath_eips_data_file, delimiter='\n', names=['tc0'])
    print('DEV CLMS and EIPS O/P are loaded to Dataframe')
    # calling the function
    test_clms = clms
    test_eips = eips
    eips_clms(dev_clms, 'clms',test_clms)
    eips_clms(dev_eips, 'eips',test_eips)
