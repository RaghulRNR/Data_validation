from API_read_Farm import *
from pagination_farm import *
from state_table_mapping import *
from farm_folder_creation import *


def F(zone, Sorting):
    import numpy as np
    import pandas as pd
    import warnings
    warnings.filterwarnings("ignore")
    table = 'EF'
    data = pd.DataFrame()
    merged = pd.DataFrame()
    if (zone == '123'):
        z1, z2, z3 = 1, 2, 3
    elif (zone == '0'):
        z1, z2, z3 = 0, 2, 3
    elif (zone == '456'):
        z1, z2, z3 = 4, 5, 6
    elif (zone == '234'):
        z1, z2, z3 = 2, 3, 4
    path = data_location
    zone = zone
    # pagiantion:-
    Sorting = Sorting - 1
    fil_pagination = pd.DataFrame()

    fil_pagination = filter_pagination[filter_pagination['index'] == Sorting]
    fil_pagination.at[:,'merge'] = 1
    # API_DATA:-
    data = pd.read_excel(path)
    AC_data = data[(data['Map_table'] == table) & ((data['zone'] == z1) | (data['zone'] == z2) | (data['zone'] == z3)) & (data['prop_cov']=='f')]
    AC_data.at[:,'merge'] = 1
    # MERGED_API and PAGINATION:-
    merged = pd.merge(AC_data, fil_pagination, on='merge')
    input = merged
    merged['tc1'] = 'H'
    merged['tc2'] = 'FARMMCRO'
    merged['tc3'] = state_alpha + state_name + " (" + state.zfill(2) + ")"
    merged['tc4'] = merged['DCSRPS_CLMS_TP_NAME'].str.strip()
    merged['tc5'] = merged['DCSRPS_PAGE_IND'].str.strip()
    merged['tc6'] = merged['DCSRPS_PAGE_NO'].str.zfill(3)
    merged['tc7'] = (merged['DCSRPS_EDITION_DAY'] +" "+merged['DCSRPS_EDITION'] + " " + merged[
        'DCSRPS_MMYY_EFF_DT']).str.strip()
    merged['tc8'] = merged['DCSRPS_EFF_CENTURY_YEAR']
    # Data_conditions;-
    merged['tc9'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                merged['zone'] == z1) & (merged['prop_cov'] == 'f') & (merged['cov_f_lim_dols_floor'] == 0),
                             merged['Value'], 0.00)
    merged['tc10'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                merged['zone'] == z2) & (merged['prop_cov'] == 'f') & (merged['cov_f_lim_dols_floor'] == 0),
                              merged['Value'], 0.00)
    merged['tc11'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                merged['zone'] == z3) & (merged['prop_cov'] == 'f') & (merged['cov_f_lim_dols_floor'] == 0),
                              merged['Value'], 0.00)
    merged['tc12'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                merged['zone'] == z1) & (merged['prop_cov'] == 'f') & (merged['cov_f_lim_dols_floor'] == 0),
                              merged['Value'], 0.00)
    merged['tc13'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                merged['zone'] == z2) & (merged['prop_cov'] == 'f') & (merged['cov_f_lim_dols_floor'] == 0),
                              merged['Value'], 0.00)
    merged['tc14'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                merged['zone'] == z3) & (merged['prop_cov'] == 'f') & (merged['cov_f_lim_dols_floor'] == 0),
                              merged['Value'], 0.00)
    merged['tc15'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z1) & (merged['prop_cov'] == 'f') & (merged['cov_f_lim_dols_floor'] == 0),
        merged['Value'], 0.00)
    merged['tc16'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z2) & (merged['prop_cov'] == 'f') & (merged['cov_f_lim_dols_floor'] == 0),
        merged['Value'], 0.00)
    merged['tc17'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z3) & (merged['prop_cov'] == 'f') & (merged['cov_f_lim_dols_floor'] == 0),
        merged['Value'], 0.00)
    merged['tc18'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                merged['zone'] == z1) & (merged['prop_cov'] == 'f') & (merged['cov_f_lim_dols_floor'] == 25001),
                              merged['Value'], 0.00)
    merged['tc19'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                merged['zone'] == z2) & (merged['prop_cov'] == 'f') & (merged['cov_f_lim_dols_floor'] == 25001),
                              merged['Value'], 0.00)
    merged['tc20'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                merged['zone'] == z3) & (merged['prop_cov'] == 'f') & (merged['cov_f_lim_dols_floor'] == 25001),
                              merged['Value'], 0.00)
    merged['tc21'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                merged['zone'] == z1) & (merged['prop_cov'] == 'f') & (merged['cov_f_lim_dols_floor'] == 25001),
                              merged['Value'], 0.00)
    merged['tc22'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                merged['zone'] == z2) & (merged['prop_cov'] == 'f') & (merged['cov_f_lim_dols_floor'] == 25001),
                              merged['Value'], 0.00)
    merged['tc23'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                merged['zone'] == z3) & (merged['prop_cov'] == 'f') & (merged['cov_f_lim_dols_floor'] == 25001),
                              merged['Value'], 0.00)
    merged['tc24'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z1) & (merged['prop_cov'] == 'f') & (merged['cov_f_lim_dols_floor'] == 25001),
        merged['Value'], 0.00)
    merged['tc25'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z2) & (merged['prop_cov'] == 'f') & (merged['cov_f_lim_dols_floor'] == 25001),
        merged['Value'], 0.00)
    merged['tc26'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z3) & (merged['prop_cov'] == 'f') & (merged['cov_f_lim_dols_floor'] == 25001),
        merged['Value'], 0.00)
    merged['tc27'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                merged['zone'] == z1) & (merged['prop_cov'] == 'f') & (merged['cov_f_lim_dols_floor'] == 100001),
                              merged['Value'], 0.00)
    merged['tc28'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                merged['zone'] == z2) & (merged['prop_cov'] == 'f') & (merged['cov_f_lim_dols_floor'] == 100001),
                              merged['Value'], 0.00)
    merged['tc29'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                merged['zone'] == z3) & (merged['prop_cov'] == 'f') & (merged['cov_f_lim_dols_floor'] == 100001),
                              merged['Value'], 0.00)
    merged['tc30'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                merged['zone'] == z1) & (merged['prop_cov'] == 'f') & (merged['cov_f_lim_dols_floor'] == 100001),
                              merged['Value'], 0.00)
    merged['tc31'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                merged['zone'] == z2) & (merged['prop_cov'] == 'f') & (merged['cov_f_lim_dols_floor'] == 100001),
                              merged['Value'], 0.00)
    merged['tc32'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                merged['zone'] == z3) & (merged['prop_cov'] == 'f') & (merged['cov_f_lim_dols_floor'] == 100001),
                              merged['Value'], 0.00)
    merged['tc33'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z1) & (merged['prop_cov'] == 'f') & (merged['cov_f_lim_dols_floor'] == 100001),
        merged['Value'], 0.00)
    merged['tc34'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z2) & (merged['prop_cov'] == 'f') & (merged['cov_f_lim_dols_floor'] == 100001),
        merged['Value'], 0.00)
    merged['tc35'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z3) & (merged['prop_cov'] == 'f') & (merged['cov_f_lim_dols_floor'] == 100001),
        merged['Value'], 0.00)
    merged['tc36'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                merged['zone'] == z1) & (merged['prop_cov'] == 'f') & (merged['cov_f_lim_dols_floor'] == 150001),
                              merged['Value'], 0.00)
    merged['tc37'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                merged['zone'] == z2) & (merged['prop_cov'] == 'f') & (merged['cov_f_lim_dols_floor'] == 150001),
                              merged['Value'], 0.00)
    merged['tc38'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                merged['zone'] == z3) & (merged['prop_cov'] == 'f') & (merged['cov_f_lim_dols_floor'] == 150001),
                              merged['Value'], 0.00)
    merged['tc39'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                merged['zone'] == z1) & (merged['prop_cov'] == 'f') & (merged['cov_f_lim_dols_floor'] == 150001),
                              merged['Value'], 0.00)
    merged['tc40'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                merged['zone'] == z2) & (merged['prop_cov'] == 'f') & (merged['cov_f_lim_dols_floor'] == 150001),
                              merged['Value'], 0.00)
    merged['tc41'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                merged['zone'] == z3) & (merged['prop_cov'] == 'f') & (merged['cov_f_lim_dols_floor'] == 150001),
                              merged['Value'], 0.00)
    merged['tc42'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z1) & (merged['prop_cov'] == 'f') & (merged['cov_f_lim_dols_floor'] == 150001),
        merged['Value'], 0.00)
    merged['tc43'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z2) & (merged['prop_cov'] == 'f') & (merged['cov_f_lim_dols_floor'] == 150001),
        merged['Value'], 0.00)
    merged['tc44'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z3) & (merged['prop_cov'] == 'f') & (merged['cov_f_lim_dols_floor'] == 150001),
        merged['Value'], 0.00)
    merged['tc45'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                merged['zone'] == z1) & (merged['prop_cov'] == 'f') & (merged['cov_f_lim_dols_floor'] == 200001),
                              merged['Value'], 0.00)
    merged['tc46'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                merged['zone'] == z2) & (merged['prop_cov'] == 'f') & (merged['cov_f_lim_dols_floor'] == 200001),
                              merged['Value'], 0.00)
    merged['tc47'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                merged['zone'] == z3) & (merged['prop_cov'] == 'f') & (merged['cov_f_lim_dols_floor'] == 200001),
                              merged['Value'], 0.00)
    merged['tc48'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                merged['zone'] == z1) & (merged['prop_cov'] == 'f') & (merged['cov_f_lim_dols_floor'] == 200001),
                              merged['Value'], 0.00)
    merged['tc49'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                merged['zone'] == z2) & (merged['prop_cov'] == 'f') & (merged['cov_f_lim_dols_floor'] == 200001),
                              merged['Value'], 0.00)
    merged['tc50'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                merged['zone'] == z3) & (merged['prop_cov'] == 'f') & (merged['cov_f_lim_dols_floor'] == 200001),
                              merged['Value'], 0.00)
    merged['tc51'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z1) & (merged['prop_cov'] == 'f') & (merged['cov_f_lim_dols_floor'] == 200001),
        merged['Value'], 0.00)
    merged['tc52'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z2) & (merged['prop_cov'] == 'f') & (merged['cov_f_lim_dols_floor'] == 200001),
        merged['Value'], 0.00)
    merged['tc53'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z3) & (merged['prop_cov'] == 'f') & (merged['cov_f_lim_dols_floor'] == 200001),
        merged['Value'], 0.00)

    merged['tc54']=0

    merged_eips =merged.copy()
    merged_eips['tc4'] = merged_eips['DCSRPS_EIPS_TP_NAME']
    all_list = list()
    first_8 = list()
    last_all = list()
    for x in range(1, 55):
        tc = 'tc' + str(x)
        all_list.append(tc)
        if (x <= 8):
            first_8.append(tc)
        else:
            last_all.append(tc)

    temp_test_clms_table_AC = merged[all_list].copy()
    temp_test_eips_table_AC = merged_eips[all_list].copy()

    def format_align(temp_table):
        test_table_AC = temp_table.groupby(first_8, as_index=False).sum()
        first_8_data_clms = test_table_AC[first_8]
        last_all_data_clms = test_table_AC[last_all]
        temp = pd.DataFrame()
        for x in last_all_data_clms:
            inx = 0
            for y in last_all_data_clms[x]:
                temp.at[inx, x] = format(y, ".2f")
                temp[x] = temp[x].str.pad(7, side='left')
                inx = inx + 1
        test_table_AC.truncate()
        test_table_AC = pd.concat([first_8_data_clms, temp], axis=1)
        test_table_AC['tc54'] = " "
        test_table_AC['tc3'] = test_table_AC['tc3'].str.pad(25, side='right')
        # test_table_AC['tc4'] =test_table_AC['tc4'].str.pad(8, side='left')
        test_table_AC['tc5'] = test_table_AC['tc5'].str.pad(1, side='right')
        test_table_AC['tc6'] = test_table_AC['tc6'].str.pad(3, side='right')
        test_table_AC['tc7'] = test_table_AC['tc7'].str.pad(18, side='left')
        test_table_AC['tc8'] = test_table_AC['tc8'].str.pad(10, side='right')
        test_table_AC['tc54'] = test_table_AC["tc54"].str.pad(1204, side='left')
        return test_table_AC

    #
    global test_clms_table_D
    global test_eips_table_D
    test_clms_table_D = format_align(temp_test_clms_table_AC)
    test_eips_table_D = format_align(temp_test_eips_table_AC)

    try:
        os.mkdir(farm_op + clms_fol_name)
        os.mkdir(farm_op + eips_fol_name)
    except FileExistsError:
        pass

    np.savetxt(farm_op + clms_fol_name + '\\TABLE_F_' + zone + '.txt', test_clms_table_D.values, fmt="%s",
               delimiter='')
    np.savetxt(farm_op + eips_fol_name + '\\TABLE_F_' + zone + '.txt', test_eips_table_D.values, fmt="%s",
               delimiter='')

    return test_clms_table_D, test_eips_table_D
