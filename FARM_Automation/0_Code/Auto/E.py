from API_read_Farm import *
from pagination_farm import *
from state_table_mapping import *
from farm_folder_creation import *


def E(zone, Sorting):
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
    AC_data = data[(data['Map_table'] == table) & ((data['zone'] == z1) | (data['zone'] == z2) | (data['zone'] == z3)) & (data['prop_cov']=='e_and_farm_inland_marine')]
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
#conditions:-
    merged['tc9'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                         merged['prop_type'] == 'grain_in_metal_structures'), merged['Value'], 0.00)
    merged['tc10'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'grain_in_metal_structures'), merged['Value'], 0.00)
    merged['tc11'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'grain_in_metal_structures'), merged['Value'], 0.00)
    merged['tc12'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'grain_in_metal_structures'), merged['Value'], 0.00)
    merged['tc13'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'grain_in_metal_structures'), merged['Value'], 0.00)
    merged['tc14'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'grain_in_metal_structures'), merged['Value'], 0.00)
    merged['tc15'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'grain_in_metal_structures'), merged['Value'], 0.00)
    merged['tc16'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'grain_in_metal_structures'), merged['Value'], 0.00)
    merged['tc17'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'grain_in_metal_structures'), merged['Value'], 0.00)
    merged['tc18'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'grain_in_other_structures'), merged['Value'], 0.00)
    merged['tc19'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'grain_in_other_structures'), merged['Value'], 0.00)
    merged['tc20'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'grain_in_other_structures'), merged['Value'], 0.00)
    merged['tc21'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'grain_in_other_structures'), merged['Value'], 0.00)
    merged['tc22'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'grain_in_other_structures'), merged['Value'], 0.00)
    merged['tc23'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'grain_in_other_structures'), merged['Value'], 0.00)
    merged['tc24'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'grain_in_other_structures'), merged['Value'], 0.00)
    merged['tc25'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'grain_in_other_structures'), merged['Value'], 0.00)
    merged['tc26'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'grain_in_other_structures'), merged['Value'], 0.00)
    merged['tc27'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'grain_in_the_open'), merged['Value'], 0.00)
    merged['tc28'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'grain_in_the_open'), merged['Value'], 0.00)
    merged['tc29'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'grain_in_the_open'), merged['Value'], 0.00)
    merged['tc30'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'grain_in_the_open'), merged['Value'], 0.00)
    merged['tc31'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'grain_in_the_open'), merged['Value'], 0.00)
    merged['tc32'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'grain_in_the_open'), merged['Value'], 0.00)
    merged['tc33'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'grain_in_the_open'), merged['Value'], 0.00)
    merged['tc34'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'grain_in_the_open'), merged['Value'], 0.00)
    merged['tc35'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'grain_in_the_open'), merged['Value'], 0.00)
    merged['tc36'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'hay_straw_fodder_forage_crops_in_structures'),
                              merged['Value'], 0.00)
    merged['tc37'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'hay_straw_fodder_forage_crops_in_structures'),
                              merged['Value'], 0.00)
    merged['tc38'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'hay_straw_fodder_forage_crops_in_structures'),
                              merged['Value'], 0.00)
    merged['tc39'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'hay_straw_fodder_forage_crops_in_structures'),
                              merged['Value'], 0.00)
    merged['tc40'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'hay_straw_fodder_forage_crops_in_structures'),
                              merged['Value'], 0.00)
    merged['tc41'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'hay_straw_fodder_forage_crops_in_structures'),
                              merged['Value'], 0.00)
    merged['tc42'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'hay_straw_fodder_forage_crops_in_structures'), merged['Value'], 0.00)
    merged['tc43'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'hay_straw_fodder_forage_crops_in_structures'), merged['Value'], 0.00)
    merged['tc44'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'hay_straw_fodder_forage_crops_in_structures'), merged['Value'], 0.00)
    merged['tc45'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'hay_straw_fodder_forage_crops_in_open'),
                              merged['Value'], 0.00)
    merged['tc46'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'hay_straw_fodder_forage_crops_in_open'),
                              merged['Value'], 0.00)
    merged['tc47'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'hay_straw_fodder_forage_crops_in_open'),
                              merged['Value'], 0.00)
    merged['tc48'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'hay_straw_fodder_forage_crops_in_open'),
                              merged['Value'], 0.00)
    merged['tc49'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'hay_straw_fodder_forage_crops_in_open'),
                              merged['Value'], 0.00)
    merged['tc50'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'hay_straw_fodder_forage_crops_in_open'),
                              merged['Value'], 0.00)
    merged['tc51'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'hay_straw_fodder_forage_crops_in_open'), merged['Value'], 0.00)
    merged['tc52'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'hay_straw_fodder_forage_crops_in_open'), merged['Value'], 0.00)
    merged['tc53'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'hay_straw_fodder_forage_crops_in_open'), merged['Value'], 0.00)
    merged['tc54'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'livestock_blanket'), merged['Value'], 0.00)
    merged['tc55'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'livestock_blanket'), merged['Value'], 0.00)
    merged['tc56'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'livestock_blanket'), merged['Value'], 0.00)
    merged['tc57'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'livestock_blanket'), merged['Value'], 0.00)
    merged['tc58'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'livestock_blanket'), merged['Value'], 0.00)
    merged['tc59'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'livestock_blanket'), merged['Value'], 0.00)
    merged['tc60'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'livestock_blanket'), merged['Value'], 0.00)
    merged['tc61'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'livestock_blanket'), merged['Value'], 0.00)
    merged['tc62'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'livestock_blanket'), merged['Value'], 0.00)
    merged['tc63'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'livestock_scheduled'), merged['Value'], 0.00)
    merged['tc64'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'livestock_scheduled'), merged['Value'], 0.00)
    merged['tc65'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'livestock_scheduled'), merged['Value'], 0.00)
    merged['tc66'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'livestock_scheduled'), merged['Value'], 0.00)
    merged['tc67'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'livestock_scheduled'), merged['Value'], 0.00)
    merged['tc68'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'livestock_scheduled'), merged['Value'], 0.00)
    merged['tc69'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'livestock_scheduled'), merged['Value'], 0.00)
    merged['tc70'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'livestock_scheduled'), merged['Value'], 0.00)
    merged['tc71'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'livestock_scheduled'), merged['Value'], 0.00)
    merged['tc72'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'machinery_vehicles_and_implements_blanket'),
                              merged['Value'], 0.00)
    merged['tc73'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'machinery_vehicles_and_implements_blanket'),
                              merged['Value'], 0.00)
    merged['tc74'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'machinery_vehicles_and_implements_blanket'),
                              merged['Value'], 0.00)
    merged['tc75'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'machinery_vehicles_and_implements_blanket'),
                              merged['Value'], 0.00)
    merged['tc76'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'machinery_vehicles_and_implements_blanket'),
                              merged['Value'], 0.00)
    merged['tc77'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'machinery_vehicles_and_implements_blanket'),
                              merged['Value'], 0.00)
    merged['tc78'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'machinery_vehicles_and_implements_blanket'), merged['Value'], 0.00)
    merged['tc79'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'machinery_vehicles_and_implements_blanket'), merged['Value'], 0.00)
    merged['tc80'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'machinery_vehicles_and_implements_blanket'), merged['Value'], 0.00)
    merged['tc81'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'machinery_vehicles_and_implements_scheduled'),
                              merged['Value'], 0.00)
    merged['tc82'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'machinery_vehicles_and_implements_scheduled'),
                              merged['Value'], 0.00)
    merged['tc83'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'machinery_vehicles_and_implements_scheduled'),
                              merged['Value'], 0.00)
    merged['tc84'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'machinery_vehicles_and_implements_scheduled'),
                              merged['Value'], 0.00)
    merged['tc85'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'machinery_vehicles_and_implements_scheduled'),
                              merged['Value'], 0.00)
    merged['tc86'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'machinery_vehicles_and_implements_scheduled'),
                              merged['Value'], 0.00)
    merged['tc87'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'machinery_vehicles_and_implements_scheduled'), merged['Value'], 0.00)
    merged['tc88'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'machinery_vehicles_and_implements_scheduled'), merged['Value'], 0.00)
    merged['tc89'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'machinery_vehicles_and_implements_scheduled'), merged['Value'], 0.00)
    merged['tc90'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (merged[
                                                                                                  'prop_type'] == 'machinery_vehicles_and_implements_cotton_pickers_&_combines'),
                              merged['Value'], 0.00)
    merged['tc91'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (merged[
                                                                                                  'prop_type'] == 'machinery_vehicles_and_implements_cotton_pickers_&_combines'),
                              merged['Value'], 0.00)
    merged['tc92'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (merged[
                                                                                                  'prop_type'] == 'machinery_vehicles_and_implements_cotton_pickers_&_combines'),
                              merged['Value'], 0.00)
    merged['tc93'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (merged[
                                                                                                  'prop_type'] == 'machinery_vehicles_and_implements_cotton_pickers_&_combines'),
                              merged['Value'], 0.00)
    merged['tc94'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (merged[
                                                                                                  'prop_type'] == 'machinery_vehicles_and_implements_cotton_pickers_&_combines'),
                              merged['Value'], 0.00)
    merged['tc95'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (merged[
                                                                                                  'prop_type'] == 'machinery_vehicles_and_implements_cotton_pickers_&_combines'),
                              merged['Value'], 0.00)
    merged['tc96'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'machinery_vehicles_and_implements_cotton_pickers_&_combines'),
        merged['Value'], 0.00)
    merged['tc97'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'machinery_vehicles_and_implements_cotton_pickers_&_combines'),
        merged['Value'], 0.00)
    merged['tc98'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'machinery_vehicles_and_implements_cotton_pickers_&_combines'),
        merged['Value'], 0.00)
    merged['tc99'] = np.where((merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                                          merged['prop_type'] == 'turkeys_in_and_outside_of_buildings'),
                              merged['Value'], 0.00)
    merged['tc100'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                    merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'turkeys_in_and_outside_of_buildings'), merged['Value'], 0.00)
    merged['tc101'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                    merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'turkeys_in_and_outside_of_buildings'), merged['Value'], 0.00)
    merged['tc102'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                    merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'turkeys_in_and_outside_of_buildings'), merged['Value'], 0.00)
    merged['tc103'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                    merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'turkeys_in_and_outside_of_buildings'), merged['Value'], 0.00)
    merged['tc104'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                    merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'turkeys_in_and_outside_of_buildings'), merged['Value'], 0.00)
    merged['tc105'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'turkeys_in_and_outside_of_buildings'), merged['Value'], 0.00)
    merged['tc106'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'turkeys_in_and_outside_of_buildings'), merged['Value'], 0.00)
    merged['tc107'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'turkeys_in_and_outside_of_buildings'), merged['Value'], 0.00)
    merged['tc108'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                    merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'poultry_in_and_outside_of_buildings'), merged['Value'], 0.00)
    merged['tc109'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                    merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'poultry_in_and_outside_of_buildings'), merged['Value'], 0.00)
    merged['tc110'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                    merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'poultry_in_and_outside_of_buildings'), merged['Value'], 0.00)
    merged['tc111'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                    merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'poultry_in_and_outside_of_buildings'), merged['Value'], 0.00)
    merged['tc112'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                    merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'poultry_in_and_outside_of_buildings'), merged['Value'], 0.00)
    merged['tc113'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                    merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'poultry_in_and_outside_of_buildings'), merged['Value'], 0.00)
    merged['tc114'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'poultry_in_and_outside_of_buildings'), merged['Value'], 0.00)
    merged['tc115'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'poultry_in_and_outside_of_buildings'), merged['Value'], 0.00)
    merged['tc116'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'poultry_in_and_outside_of_buildings'), merged['Value'], 0.00)
    merged['tc117'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                    merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'trays_boxes_and_box_shook'), merged['Value'], 0.00)
    merged['tc118'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                    merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'trays_boxes_and_box_shook'), merged['Value'], 0.00)
    merged['tc119'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                    merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'trays_boxes_and_box_shook'), merged['Value'], 0.00)
    merged['tc120'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                    merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'trays_boxes_and_box_shook'), merged['Value'], 0.00)
    merged['tc121'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                    merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'trays_boxes_and_box_shook'), merged['Value'], 0.00)
    merged['tc122'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                    merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'trays_boxes_and_box_shook'), merged['Value'], 0.00)
    merged['tc123'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'trays_boxes_and_box_shook'), merged['Value'], 0.00)
    merged['tc124'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'trays_boxes_and_box_shook'), merged['Value'], 0.00)
    merged['tc125'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'trays_boxes_and_box_shook'), merged['Value'], 0.00)
    merged['tc126'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                    merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'wool'), merged['Value'], 0.00)
    merged['tc127'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                    merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'wool'), merged['Value'], 0.00)
    merged['tc128'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                    merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'wool'), merged['Value'], 0.00)
    merged['tc129'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                    merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'wool'), merged['Value'], 0.00)
    merged['tc130'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                    merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'wool'), merged['Value'], 0.00)
    merged['tc131'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                    merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'wool'), merged['Value'], 0.00)
    merged['tc132'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'wool'), merged['Value'], 0.00)
    merged['tc133'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'wool'), merged['Value'], 0.00)
    merged['tc134'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'wool'), merged['Value'], 0.00)
    merged['tc135'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                    merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'products_farm_&_dairy_noc'), merged['Value'], 0.00)
    merged['tc136'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                    merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'products_farm_&_dairy_noc'), merged['Value'], 0.00)
    merged['tc137'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                    merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'products_farm_&_dairy_noc'), merged['Value'], 0.00)
    merged['tc138'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                    merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'products_farm_&_dairy_noc'), merged['Value'], 0.00)
    merged['tc139'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                    merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'products_farm_&_dairy_noc'), merged['Value'], 0.00)
    merged['tc140'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                    merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'products_farm_&_dairy_noc'), merged['Value'], 0.00)
    merged['tc141'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'products_farm_&_dairy_noc'), merged['Value'], 0.00)
    merged['tc142'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'products_farm_&_dairy_noc'), merged['Value'], 0.00)
    merged['tc143'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'products_farm_&_dairy_noc'), merged['Value'], 0.00)
    merged['tc144'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                    merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'farm_property_noc_blanket'), merged['Value'], 0.00)
    merged['tc145'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                    merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'farm_property_noc_blanket'), merged['Value'], 0.00)
    merged['tc146'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                    merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'farm_property_noc_blanket'), merged['Value'], 0.00)
    merged['tc147'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                    merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'farm_property_noc_blanket'), merged['Value'], 0.00)
    merged['tc148'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                    merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'farm_property_noc_blanket'), merged['Value'], 0.00)
    merged['tc149'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                    merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'farm_property_noc_blanket'), merged['Value'], 0.00)
    merged['tc150'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'farm_property_noc_blanket'), merged['Value'], 0.00)
    merged['tc151'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'farm_property_noc_blanket'), merged['Value'], 0.00)
    merged['tc152'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'farm_property_noc_blanket'), merged['Value'], 0.00)
    merged['tc153'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                    merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'farm_property_noc_scheduled'), merged['Value'], 0.00)
    merged['tc154'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                    merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'farm_property_noc_scheduled'), merged['Value'], 0.00)
    merged['tc155'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                    merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'farm_property_noc_scheduled'), merged['Value'], 0.00)
    merged['tc156'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                    merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'farm_property_noc_scheduled'), merged['Value'], 0.00)
    merged['tc157'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                    merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'farm_property_noc_scheduled'), merged['Value'], 0.00)
    merged['tc158'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                    merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'farm_property_noc_scheduled'), merged['Value'], 0.00)
    merged['tc159'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'farm_property_noc_scheduled'), merged['Value'], 0.00)
    merged['tc160'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'farm_property_noc_scheduled'), merged['Value'], 0.00)
    merged['tc161'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'farm_property_noc_scheduled'), merged['Value'], 0.00)
    merged['tc162'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                    merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'threshed_grain_and_seed_in_all_metal_tanks'), merged['Value'], 0.00)
    merged['tc163'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                    merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'threshed_grain_and_seed_in_all_metal_tanks'), merged['Value'], 0.00)
    merged['tc164'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                    merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'threshed_grain_and_seed_in_all_metal_tanks'), merged['Value'], 0.00)
    merged['tc165'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                    merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'threshed_grain_and_seed_in_all_metal_tanks'), merged['Value'], 0.00)
    merged['tc166'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                    merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'threshed_grain_and_seed_in_all_metal_tanks'), merged['Value'], 0.00)
    merged['tc167'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                    merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'threshed_grain_and_seed_in_all_metal_tanks'), merged['Value'], 0.00)
    merged['tc168'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'threshed_grain_and_seed_in_all_metal_tanks'), merged['Value'], 0.00)
    merged['tc169'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'threshed_grain_and_seed_in_all_metal_tanks'), merged['Value'], 0.00)
    merged['tc170'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'threshed_grain_and_seed_in_all_metal_tanks'), merged['Value'], 0.00)
    merged['tc171'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                    merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'office_contents'), merged['Value'], 0.00)
    merged['tc172'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                    merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'office_contents'), merged['Value'], 0.00)
    merged['tc173'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                    merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'office_contents'), merged['Value'], 0.00)
    merged['tc174'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                    merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'office_contents'), merged['Value'], 0.00)
    merged['tc175'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                    merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'office_contents'), merged['Value'], 0.00)
    merged['tc176'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                    merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'office_contents'), merged['Value'], 0.00)
    merged['tc177'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'office_contents'), merged['Value'], 0.00)
    merged['tc178'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'office_contents'), merged['Value'], 0.00)
    merged['tc179'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'office_contents'), merged['Value'], 0.00)
    merged['tc180'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                    merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'contents_of_fish_hatcheries'), merged['Value'], 0.00)
    merged['tc181'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                    merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'contents_of_fish_hatcheries'), merged['Value'], 0.00)
    merged['tc182'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'basic') & (
                    merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'contents_of_fish_hatcheries'), merged['Value'], 0.00)
    merged['tc183'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                    merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'contents_of_fish_hatcheries'), merged['Value'], 0.00)
    merged['tc184'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                    merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'contents_of_fish_hatcheries'), merged['Value'], 0.00)
    merged['tc185'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'broad') & (
                    merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'contents_of_fish_hatcheries'), merged['Value'], 0.00)
    merged['tc186'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z1) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'contents_of_fish_hatcheries'), merged['Value'], 0.00)
    merged['tc187'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z2) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'contents_of_fish_hatcheries'), merged['Value'], 0.00)
    merged['tc188'] = np.where(
        (merged['TableDesignation'] == '36.C.5.a.(2)') & (merged['causes_of_loss'] == 'special') & (
                    merged['zone'] == z3) & (merged['prop_cov'] == 'e_and_farm_inland_marine') & (
                    merged['prop_type'] == 'contents_of_fish_hatcheries'), merged['Value'], 0.00)
    merged['tc189']=0

    merged_eips = merged.copy()
    merged_eips['tc4'] = merged_eips['DCSRPS_EIPS_TP_NAME']
    all_list = list()
    first_8 = list()
    last_all = list()
    for x in range(1, 190):
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
        test_table_AC['tc189'] = " "
        test_table_AC['tc3'] = test_table_AC['tc3'].str.pad(25, side='right')
        # test_table_AC['tc4'] =test_table_AC['tc4'].str.pad(8, side='left')
        test_table_AC['tc5'] = test_table_AC['tc5'].str.pad(1, side='right')
        test_table_AC['tc6'] = test_table_AC['tc6'].str.pad(3, side='right')
        test_table_AC['tc7'] = test_table_AC['tc7'].str.pad(18, side='left')
        test_table_AC['tc8'] = test_table_AC['tc8'].str.pad(10, side='right')
        test_table_AC['tc189'] = test_table_AC["tc189"].str.pad(259, side='left')
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

    np.savetxt(farm_op + clms_fol_name + '\\TABLE_E_' + zone + '.txt', test_clms_table_D.values, fmt="%s",
               delimiter='')
    np.savetxt(farm_op + eips_fol_name + '\\TABLE_E_' + zone + '.txt', test_eips_table_D.values, fmt="%s",
               delimiter='')

    return test_clms_table_D, test_eips_table_D
