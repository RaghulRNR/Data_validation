from API_read_Farm import *
from pagination_farm import *
from state_table_mapping import *
from farm_folder_creation import *
def Value_ValueAlpha(x,y):
    if(x==0):
        return y
    else:
        return x
def N(zone, Sorting):
    import numpy as np
    import pandas as pd
    import warnings
    warnings.filterwarnings("ignore")
    table = 'N'
    data = pd.DataFrame()
    merged = pd.DataFrame()
    if (zone == '123'):
        z1, z2, z3 = 1, 2, 3
    elif (zone == '234'):
        z1, z2, z3 = 2, 3, 4
    else:
        z1, z2, z3 = 4, 5, 6
    path = data_location
    zone = zone
    # pagiantion:-
    Sorting = Sorting - 1
    fil_pagination = pd.DataFrame()

    fil_pagination = filter_pagination[filter_pagination['index'] == Sorting]
    fil_pagination.at[:,'merge'] = 1
    # API_DATA:-
    data = pd.read_excel(path)
    AC_data = data[(data['Map_table'] == table)]
    AC_data.at[:,'merge'] = 1
    AC_data['Value']=AC_data['Value'].fillna(0)
    #AC_data.to_excel("C:\\Users\\i30691\\OneDrive - Verisk Analytics\\Documents\\py\\Auto\\N_data.xlsx")
    # MERGED_API and PAGINATION:-
    merged = pd.merge(AC_data, fil_pagination, on='merge')
    merged['con_class'] = merged['class'].fillna(0)
    merged['con_class']=merged['con_class'].astype(int)
    merged['con_class']=merged['con_class'].astype(str)
    #merged['con_class'] = merged['con_class'].str.zfill(5)
    merged['tc1'] = 'H'
    merged['tc2'] = 'FARMMCRO'
    merged['tc3'] = state_alpha + state_name + " (" + state.zfill(2) + ")"
    merged['tc4'] = merged['DCSRPS_CLMS_TP_NAME'].str.strip()
    merged['tc5'] = merged['DCSRPS_PAGE_IND'].str.strip()
    merged['tc6'] = merged['DCSRPS_PAGE_NO'].str.zfill(3)
    merged['tc7'] = (merged['DCSRPS_EDITION_DAY'] +" "+ merged['DCSRPS_EDITION'] + " " + merged['DCSRPS_MMYY_EFF_DT']).str.strip()
    merged['tc8'] = merged['DCSRPS_EFF_CENTURY_YEAR']
    # Data_conditions;-
    merged['tc9'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9301'),
                             merged['class'], ' ')
    merged['tc10'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9301'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1), ' ')
    merged['tc11'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9302'),
                              merged['class'], ' ')
    merged['tc12'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9302'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1), ' ')
    merged['tc13'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9303'),
                              merged['class'], ' ')
    merged['tc14'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9303'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1), ' ')
    merged['tc15'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9305'),
                              merged['class'], ' ')
    merged['tc16'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9305'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1), ' ')
    merged['tc17'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9306'),
                              merged['class'], ' ')
    merged['tc18'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9306'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1), ' ')
    merged['tc19'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9307'),
                              merged['class'], ' ')
    merged['tc20'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9307'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1), ' ')
    merged['tc21'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9309'),
                              merged['class'], ' ')
    merged['tc22'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9309'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1), ' ')
    merged['tc23'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9310'),
                              merged['class'], ' ')
    merged['tc24'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9310'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1), ' ')
    merged['tc25'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9311'),
                              merged['class'], ' ')
    merged['tc26'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9311'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1), ' ')
    merged['tc27'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9313'),
                              merged['class'], ' ')
    merged['tc28'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9313'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1), ' ')
    merged['tc29'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9314'),
                              merged['class'], ' ')
    merged['tc30'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9314'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1), ' ')
    merged['tc31'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9315'),
                              merged['class'], ' ')
    merged['tc32'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9315'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1), ' ')
    merged['tc33'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9317'),
                              merged['class'], ' ')
    merged['tc34'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9317'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1), ' ')
    merged['tc35'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9318'),
                              merged['class'], ' ')
    merged['tc36'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9318'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1), ' ')
    merged['tc37'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9319'),
                              merged['class'], ' ')
    merged['tc38'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9319'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1), ' ')
    merged['tc39'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9321'),
                              merged['class'], ' ')
    merged['tc40'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9321'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1), ' ')
    merged['tc41'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9322'),
                              merged['class'], ' ')
    merged['tc42'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9322'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1), ' ')
    merged['tc43'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9323'),
                              merged['class'], ' ')
    merged['tc44'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9323'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1), ' ')
    merged['tc45'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9325'),
                              merged['class'], ' ')
    merged['tc46'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9325'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1), ' ')
    merged['tc47'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9326'),
                              merged['class'], ' ')
    merged['tc48'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9326'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1), ' ')
    merged['tc49'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9327'),
                              merged['class'], ' ')
    merged['tc50'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9327'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1), ' ')
    merged['tc51'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9329'),
                              merged['class'], ' ')
    merged['tc52'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9329'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1), ' ')
    merged['tc53'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9330'),
                              merged['class'], ' ')
    merged['tc54'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9330'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1), ' ')
    merged['tc55'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9331'),
                              merged['class'], ' ')
    merged['tc56'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9331'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1), ' ')
    merged['tc57'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9333'),
                              merged['class'], ' ')
    merged['tc58'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9333'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1), ' ')
    merged['tc59'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9334'),
                              merged['class'], ' ')
    merged['tc60'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9334'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1), ' ')
    merged['tc61'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9335'),
                              merged['class'], ' ')
    merged['tc62'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9335'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1), ' ')
    merged['tc63'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9337'),
                              merged['class'], ' ')
    merged['tc64'] = np.where((merged['TableDesignation'] == '37.BB.3.c.(LC)') & (merged['con_class'] == '9337'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1), ' ')

    merged['tc65'] =" "
    #merged.to_excel("C:\\Users\\i30691\\OneDrive - Verisk Analytics\\Documents\\py\\Auto\\N_after_condition.xlsx")
    merged=merged.astype(str)
    merged = merged.applymap(lambda x: x.strip())
    merged_eips = merged.copy()
    merged_eips['tc4'] = merged_eips['DCSRPS_EIPS_TP_NAME']
    all_list = list()
    first_8 = list()
    last_all = list()
    for x in range(1, 66):
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
        u = 9
        for x in last_all_data_clms:
            inx = 0
            for y in last_all_data_clms[x]:
                #print(dec)
                if (u % 2 != 0 and u<=64):
                    q = float(y)
                    q = int(q)
                    q = str(q).zfill(5)
                    q = q.rjust(7, " ")
                    temp.at[inx, x] = q
                else:
                    a='(a)'
                    star='*'
                    d_star='**'
                    if ((y != a and y != '')and (y != star and y != '')and (y != d_star and y != '')):
                        q=float(y)
                        temp.at[inx, x] = str(q).center(7, " ")
                    else:
                        #print('tc',u,'=',y)
                        temp.at[inx, x] = str(y).center(7, " ")
            inx = inx + 1
            u = u + 1
            temp['65']=''
        test_table_AC.truncate()
        test_table_AC = pd.concat([first_8_data_clms, temp], axis=1)

        test_table_AC['tc65'] = " "
        test_table_AC['tc3'] = test_table_AC['tc3'].str.pad(25, side='right')
        # test_table_AC['tc4'] =test_table_AC['tc4'].str.pad(8, side='left')
        test_table_AC['tc5'] = test_table_AC['tc5'].str.pad(1, side='right')
        test_table_AC['tc6'] = test_table_AC['tc6'].str.pad(3, side='right')
        test_table_AC['tc7'] = test_table_AC['tc7'].str.pad(18, side='left')
        test_table_AC['tc8'] = test_table_AC['tc8'].str.pad(10, side='right')
        test_table_AC['tc65'] = test_table_AC["tc65"].str.pad(1127, side='left')
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

    np.savetxt(farm_op + clms_fol_name + '\\TABLE_N'+ '.txt', test_clms_table_D.values, fmt="%s",
               delimiter='')
    np.savetxt(farm_op + eips_fol_name + '\\TABLE_N'+ '.txt', test_eips_table_D.values, fmt="%s",
               delimiter='')

    return test_clms_table_D, test_eips_table_D
