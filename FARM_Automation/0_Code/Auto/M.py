from API_read_Farm import *
from pagination_farm import *
from state_table_mapping import *
from farm_folder_creation import *
def Value_ValueAlpha(x,y):
    if(x==0):
        return y
    else:
        return x
def M(zone, Sorting):
    import numpy as np
    import pandas as pd
    import warnings
    warnings.filterwarnings("ignore")
    table = 'M'
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
    # MERGED_API and PAGINATION:-
    merged = pd.merge(AC_data, fil_pagination, on='merge')
    merged['con_class'] = merged['class'].fillna(0)
    merged['con_class']=merged['con_class'].astype(int)
    merged['con_class']=merged['con_class'].astype(str)

    merged['tc1'] = 'H'
    merged['tc2'] = 'FARMMCRO'
    merged['tc3'] = state_alpha + state_name + " (" + state.zfill(2) + ")"
    merged['tc4'] = merged['DCSRPS_CLMS_TP_NAME'].str.strip()
    merged['tc5'] = merged['DCSRPS_PAGE_IND'].str.strip()
    merged['tc6'] = merged['DCSRPS_PAGE_NO'].str.zfill(3)
    merged['tc7'] = (merged['DCSRPS_EDITION_DAY'] +" "+ merged['DCSRPS_EDITION'] + " " + merged['DCSRPS_MMYY_EFF_DT']).str.strip()
    merged['tc8'] = merged['DCSRPS_EFF_CENTURY_YEAR']
    # Data_conditions;-
    merged['tc9'] = np.where(
        (merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '52002') & (merged['Value'] != '0'),
        merged['class'],' ')
    merged['tc10'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '52002'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1),' ')
    merged['tc11'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '1901'),
                              merged['class'],' ')
    merged['tc12'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '1901'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1),' ')
    merged['tc13'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '53001'),
                              merged['class'],' ')
    merged['tc14'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '53001'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1),' ')
    merged['tc15'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '12583'),
                              merged['class'],' ')
    merged['tc16'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '12583'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1),' ')
    merged['tc17'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '53374'),
                              merged['class'],' ')
    merged['tc18'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '53374'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1),' ')
    merged['tc19'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '53375'),
                              merged['class'],' ')
    merged['tc20'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '53375'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1),' ')
    merged['tc21'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '53376'),
                              merged['class'],' ')
    merged['tc22'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '53376'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1),' ')
    merged['tc23'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '53377'),
                              merged['class'],' ')
    merged['tc24'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '53377'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1),' ')
    merged['tc25'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '13111'),
                              merged['class'],' ')
    merged['tc26'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '13111'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1),' ')
    merged['tc27'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '13112'),
                              merged['class'],' ')
    merged['tc28'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '13112'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1),' ')
    merged['tc29'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '53565'),
                              merged['class'],' ')
    merged['tc30'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '53565'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1),' ')
    merged['tc31'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '55371'),
                              merged['class'],' ')
    merged['tc32'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '55371'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1),' ')
    merged['tc33'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '56758'),
                              merged['class'],' ')
    merged['tc34'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '56758'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1),' ')
    merged['tc35'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '56759'),
                              merged['class'],' ')
    merged['tc36'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '56759'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1),' ')
    merged['tc37'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '56760'),
                              merged['class'],' ')
    merged['tc38'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '56760'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1),' ')
    merged['tc39'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '57001'),
                              merged['class'],' ')
    merged['tc40'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '57001'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1),' ')
    merged['tc41'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '57002'),
                              merged['class'],' ')
    merged['tc42'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '57002'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1),' ')
    merged['tc43'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '57913'),
                              merged['class'],' ')
    merged['tc44'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '57913'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1),' ')
    merged['tc45'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '16604'),
                              merged['class'],' ')
    merged['tc46'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '16604'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1),' ')
    merged['tc47'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '16892'),
                              merged['class'],' ')
    merged['tc48'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '16892'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1),' ')
    merged['tc49'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '16891'),
                              merged['class'],' ')
    merged['tc50'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '16891'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1),' ')
    merged['tc51'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '16890'),
                              merged['class'],' ')
    merged['tc52'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '16890'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1),' ')
    merged['tc53'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '59647'),
                              merged['class'],' ')
    merged['tc54'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '59647'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1),' ')
    merged['tc55'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '59963'),
                              merged['class'],' ')
    merged['tc56'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '59963'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1),' ')
    merged['tc57'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '59964'),
                              merged['class'],' ')
    merged['tc58'] = np.where((merged['TableDesignation'] == '37.AA.2.b.(LC)') & (merged['con_class'] == '59964'),
                              merged.apply(lambda x: Value_ValueAlpha(x['Value'], x['ValueAlpha']), axis=1),' ')
    merged['tc59'] =" "

    merged=merged.astype(str)
    merged = merged.applymap(lambda x: x.strip())
    merged_eips = merged.copy()
    merged_eips['tc4'] = merged_eips['DCSRPS_EIPS_TP_NAME']
    all_list = list()
    first_8 = list()
    last_all = list()
    for x in range(1, 60):
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
                if (u % 2 != 0 and u<=58):
                    q = float(y)
                    q = int(q)
                    q = str(q).zfill(5)
                    q = q.rjust(7, " ")
                    temp.at[inx, x] = q
                else:
                    a='(a)'
                    star='*'
                    d_star='**'
                    if((y != a and y != '')and (y != star and y != '')and (y != d_star and y != '')):
                        q=float(y)
                        q = format(q, ".3f")
                        temp.at[inx, x] = str(q).rjust(7, " ")
                    else:
                        temp.at[inx, x] = str(y).center(7, " ")
            inx = inx + 1
            u = u + 1
            temp['59']=''
        test_table_AC.truncate()
        test_table_AC = pd.concat([first_8_data_clms, temp], axis=1)

        test_table_AC['tc59'] = " "
        test_table_AC['tc3'] = test_table_AC['tc3'].str.pad(25, side='right')
        # test_table_AC['tc4'] =test_table_AC['tc4'].str.pad(8, side='left')
        test_table_AC['tc5'] = test_table_AC['tc5'].str.pad(1, side='right')
        test_table_AC['tc6'] = test_table_AC['tc6'].str.pad(3, side='right')
        test_table_AC['tc7'] = test_table_AC['tc7'].str.pad(18, side='left')
        test_table_AC['tc8'] = test_table_AC['tc8'].str.pad(10, side='right')
        test_table_AC['tc59'] = test_table_AC["tc59"].str.pad(1057+112, side='left')
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

    np.savetxt(farm_op + clms_fol_name + '\\TABLE_M'+ '.txt', test_clms_table_D.values, fmt="%s",
               delimiter='')
    np.savetxt(farm_op + eips_fol_name + '\\TABLE_M'+ '.txt', test_eips_table_D.values, fmt="%s",
               delimiter='')

    return test_clms_table_D, test_eips_table_D
