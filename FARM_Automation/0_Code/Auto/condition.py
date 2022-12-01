import pandas as pd
from lookup import*
import numpy as np
from API_read import*
from copying_to_data import*
from folder_creation import*
from pagination import*
clms=pd.DataFrame()
eips=clms=pd.DataFrame()
op=pd.DataFrame()
merged=merged.astype(str)
#TC1:
merged['tc1']='H'
#TC2:
merged['tc2']='L'
#TC3:
merged['tc3']="CARMCROE"
#TC4:
merged['tc4']=state_alpha+state_name+' ('+state.zfill(2)+')'
#TC5:
merged['rate_terr']=merged['rate_terr'].astype(str)
merged['tc5']=merged['rate_terr'].str.zfill(3)
#TC6:
merged['tc6']=merged['DCSRPS_PAGE_IND']
#TC7:
merged['tc7']=merged['DCSRPS_CLMS_TP_NAME']
#TC8:
merged['tc8']=merged['DCSRPS_PAGE_NO'].str.zfill(3)
#TC9:
date=merged['DCSRPS_EDITION_DAY']
edition=merged['DCSRPS_EDITION']
month=merged['DCSRPS_MMYY_EFF_DT']
final=date+" "+edition+" "+month
merged['tc9']=final
#TC10:
merged['tc10']=merged['DCSRPS_EFF_CENTURY_YEAR']
#TC11:
merged['tc11']='100,000'
#TC12:
merged['tc12']='500'
#TC13:
merged['tc13']='1000'
#TC14:
merged['tc14']='2000'
#TC15:
merged['tc15']='5000'
#TC16:
merged['tc16']="00"
#TC17:
merged['tc17']="00"
#TC18:
merged['tc18']="00"
#TC19:
merged['tc19']="00"
#TC20:
merged['tc20']="00"
#TC21:
merged['tc21']="00"
#TC22:
merged['tc22']="D"
#TC23:
merged['tc23']=np.where((merged['cov']=="liability")&(merged['pol_lim_dols']=='100000')&(merged['rule_num']=='23'),merged['con_value']," ")
#TC24:
merged['tc24']=np.where((merged['cov']=="medical_payments")&(merged['pol_lim_dols']=='500')&(merged['rule_num']=='23'),merged['con_value']," ")
#TC25:
merged['tc25']=np.where((merged['cov']=="medical_payments")&(merged['pol_lim_dols']=='1000')&(merged['rule_num']=='23'),merged['con_value']," ")
#TC26:
merged['tc26']=np.where((merged['cov']=="medical_payments")&(merged['pol_lim_dols']=='2000')&(merged['rule_num']=='23'),merged['con_value']," ")
#TC27:
merged['tc27']=np.where((merged['cov']=="medical_payments")&(merged['pol_lim_dols']=='5000')&(merged['rule_num']=='23'),merged['con_value']," ")
#TC28:
merged['tc28']=" ";
#TC29:
merged['tc29']=" ";
#TC30:
merged['tc30']=np.where((merged['cov']=="liability")&(merged['pol_lim_dols']=='100000')&(merged['rule_num']=='32'),merged['con_value']," ")
#TC31:
merged['tc31']=np.where((merged['cov']=="medical_payments")&(merged['pol_lim_dols']=='500')&(merged['rule_num']=='32'),merged['con_value']," ")
#TC32:
merged['tc32']=np.where((merged['cov']=="medical_payments")&(merged['pol_lim_dols']=='1000')&(merged['rule_num']=='32'),merged['con_value']," ")
#TC33:
merged['tc33']=np.where((merged['cov']=="medical_payments")&(merged['pol_lim_dols']=='2000')&(merged['rule_num']=='32'),merged['con_value']," ")
#TC34:
merged['tc34']=np.where((merged['cov']=="medical_payments")&(merged['pol_lim_dols']=='5000')&(merged['rule_num']=='32'),merged['con_value']," ")
#TC35:
merged['tc35']=" ";
#TC36:
merged['tc36']=" ";
#TC37:
merged['tc37']=np.where((merged['cov']=="liability")&(merged['pol_lim_dols']=='100000')&(merged['rule_num']=='40')&(merged['size_class']=="taxicabs_and_limousines"),merged['con_value']," ")
#TC38:
merged['tc38']=np.where((merged['cov']=="medical_payments")&(merged['pol_lim_dols']=='500')&(merged['rule_num']=='40')&(merged['size_class']=="taxicabs_and_limousines"),merged['con_value']," ")
#TC39:
merged['tc39']=np.where((merged['cov']=="medical_payments")&(merged['pol_lim_dols']=='1000')&(merged['rule_num']=='40')&(merged['size_class']=="taxicabs_and_limousines"),merged['con_value']," ")
#TC40:
merged['tc40']=np.where((merged['cov']=="medical_payments")&(merged['pol_lim_dols']=='2000')&(merged['rule_num']=='40')&(merged['size_class']=="taxicabs_and_limousines"),merged['con_value']," ")
#TC41:
merged['tc41']=np.where((merged['cov']=="medical_payments")&(merged['pol_lim_dols']=='5000')&(merged['rule_num']=='40')&(merged['size_class']=="taxicabs_and_limousines"),merged['con_value']," ")
merged['tc42']=" ";
#TC43:
merged['tc43']=" ";
#TC44:
merged['tc44']=np.where((merged['cov']=="liability")&(merged['pol_lim_dols']=='100000')&(merged['rule_num']=='40')&(merged['size_class']=="school_and_church_buses"),merged['con_value']," ")
#TC45:
merged['tc45']=np.where((merged['cov']=="medical_payments")&(merged['pol_lim_dols']=='500')&(merged['rule_num']=='40')&(merged['size_class']=="school_and_church_buses"),merged['con_value']," ")
#TC46:
merged['tc46']=np.where((merged['cov']=="medical_payments")&(merged['pol_lim_dols']=='1000')&(merged['rule_num']=='40')&(merged['size_class']=="school_and_church_buses"),merged['con_value']," ")
#TC47:
merged['tc47']=np.where((merged['cov']=="medical_payments")&(merged['pol_lim_dols']=='2000')&(merged['rule_num']=='40')&(merged['size_class']=="school_and_church_buses"),merged['con_value']," ")
#TC48:
merged['tc48']=np.where((merged['cov']=="medical_payments")&(merged['pol_lim_dols']=='5000')&(merged['rule_num']=='40')&(merged['size_class']=="school_and_church_buses"),merged['con_value']," ")
#TC49:
merged['tc49']=" ";
#TC50:
merged['tc50']=" ";
#TC51:
merged['tc51']=np.where((merged['cov']=="liability")&(merged['pol_lim_dols']=='100000')&(merged['rule_num']=='40')&(merged['size_class']=="other_buses"),merged['con_value']," ")
#TC52:
merged['tc52']=np.where((merged['cov']=="medical_payments")&(merged['pol_lim_dols']=='500')&(merged['rule_num']=='40')&(merged['size_class']=="other_buses"),merged['con_value']," ")
#TC53:
merged['tc53']=np.where((merged['cov']=="medical_payments")&(merged['pol_lim_dols']=='1000')&(merged['rule_num']=='40')&(merged['size_class']=="other_buses"),merged['con_value']," ")
#TC54:
merged['tc54']=np.where((merged['cov']=="medical_payments")&(merged['pol_lim_dols']=='2000')&(merged['rule_num']=='40')&(merged['size_class']=="other_buses"),merged['con_value']," ")
#TC55:
merged['tc55']=np.where((merged['cov']=="medical_payments")&(merged['pol_lim_dols']=='5000')&(merged['rule_num']=='40')&(merged['size_class']=="other_buses"),merged['con_value']," ")
#TC56:
merged['tc56']=" ";
#TC57:
merged['tc57']=" ";
#TC58:
merged['tc58']=np.where((merged['cov']=="liability")&(merged['pol_lim_dols']=='100000')&(merged['rule_num']=='40')&(merged['size_class']=="van_pools"),merged['con_value']," ")
#TC59:
merged['tc59']=np.where((merged['cov']=="medical_payments")&(merged['pol_lim_dols']=='500')&(merged['rule_num']=='40')&(merged['size_class']=="van_pools"),merged['con_value']," ")
#TC60:
merged['tc60']=np.where((merged['cov']=="medical_payments")&(merged['pol_lim_dols']=='1000')&(merged['rule_num']=='40')&(merged['size_class']=="van_pools"),merged['con_value']," ")
#TC61:
merged['tc61']=np.where((merged['cov']=="medical_payments")&(merged['pol_lim_dols']=='2000')&(merged['rule_num']=='40')&(merged['size_class']=="van_pools"),merged['con_value']," ")
#TC62:
merged['tc62']=np.where((merged['cov']=="medical_payments")&(merged['pol_lim_dols']=='5000')&(merged['rule_num']=='40')&(merged['size_class']=="van_pools"),merged['con_value']," ")
#TC63:
merged['tc63']=" ";
#TC64:
merged['tc64']=" ";
#TC65:
merged['tc65']=np.where((merged['cov']=="liability")&(merged['rule_num']=='49'),merged['con_value']," ")
#TC66:
#TC64:
merged['tc66']=" ";

merged=merged.astype(str)
merged=merged.applymap(lambda x: x.strip())


merged=merged.groupby(['tc1','tc2','tc3','tc4','tc6','tc7','tc8','tc9','tc10','tc11','tc12','tc13','tc14','tc15','tc16','tc17','tc18','tc19','tc20','tc21','tc22'],as_index=False).agg(lambda x: ''.join(x.drop_duplicates())).reset_index()



merged['tc4']=merged['tc4'].str.pad(29,side='right')
merged['tc6']=merged['tc6'].str.pad(1,side='right')
merged['tc7']=merged['tc7'].str.pad(8,side='right')
merged['tc8']=merged['tc8'].str.pad(3,side='right')
merged['tc9']=merged['tc9'].str.pad(18,side='right')
merged['tc10']=merged['tc10'].str.pad(4,side='right')
merged['tc11']=merged['tc11'].str.pad(7,side='left')
merged['tc12']=merged['tc12'].str.pad(5,side='left')
merged['tc13']=merged['tc13'].str.pad(5,side='left')
merged['tc14']=merged['tc14'].str.pad(5,side='left')
merged['tc15']=merged['tc15'].str.pad(5,side='left')
merged['tc23']=merged['tc23'].str.pad(6,side="left")
merged['tc24']=merged['tc24'].str.pad(6,side="right")
merged['tc25']=merged['tc25'].str.pad(6,side="right")
merged['tc26']=merged['tc26'].str.pad(6,side="right")
merged['tc27']=merged['tc27'].str.pad(6,side="right")
merged['tc28']=merged['tc28'].str.pad(6,side="right")
merged['tc29']=merged['tc29'].str.pad(6,side="right")
merged['tc30']=merged['tc30'].str.pad(6,side="left")
merged['tc31']=merged['tc31'].str.pad(6,side="right")
merged['tc32']=merged['tc32'].str.pad(6,side="right")
merged['tc33']=merged['tc33'].str.pad(6,side="right")
merged['tc34']=merged['tc34'].str.pad(6,side="right")
merged['tc35']=merged['tc35'].str.pad(6,side="right")
merged['tc36']=merged['tc36'].str.pad(6,side="right")
merged['tc37']=merged['tc37'].str.pad(6,side="left")
merged['tc38']=merged['tc38'].str.pad(6,side="right")
merged['tc39']=merged['tc39'].str.pad(6,side="right")
merged['tc40']=merged['tc40'].str.pad(6,side="right")
merged['tc41']=merged['tc41'].str.pad(6,side="right")
merged['tc42']=merged['tc42'].str.pad(6,side="right")
merged['tc43']=merged['tc43'].str.pad(6,side="right")
merged['tc44']=merged['tc44'].str.pad(6,side="left")
merged['tc45']=merged['tc45'].str.pad(6,side="right")
merged['tc46']=merged['tc46'].str.pad(6,side="right")
merged['tc47']=merged['tc47'].str.pad(6,side="right")
merged['tc48']=merged['tc48'].str.pad(6,side="right")
merged['tc49']=merged['tc49'].str.pad(6,side="right")
merged['tc50']=merged['tc50'].str.pad(6,side="right")
merged['tc51']=merged['tc51'].str.pad(6,side="left")
merged['tc52']=merged['tc52'].str.pad(6,side="right")
merged['tc53']=merged['tc53'].str.pad(6,side="right")
merged['tc54']=merged['tc54'].str.pad(6,side="right")
merged['tc55']=merged['tc55'].str.pad(6,side="right")
merged['tc56']=merged['tc56'].str.pad(6,side="right")
merged['tc57']=merged['tc57'].str.pad(6,side="right")
merged['tc58']=merged['tc58'].str.pad(6,side="left")
merged['tc59']=merged['tc59'].str.pad(6,side="right")
merged['tc60']=merged['tc60'].str.pad(6,side="right")
merged['tc61']=merged['tc61'].str.pad(6,side="right")
merged['tc62']=merged['tc62'].str.pad(6,side="right")
merged['tc63']=merged['tc63'].str.pad(6,side="right")
merged['tc64']=merged['tc64'].str.pad(6,side="right")
merged['tc65']=merged['tc65'].str.pad(6,side="left")
merged['tc66']=merged['tc66'].str.pad(int(space_at_end),side="left")

merged.sort_values(by=['tc5'],inplace=True)

test_clms_op=merged[['tc1','tc2','tc3','tc4','tc5','tc6','tc7','tc8','tc9','tc10','tc11','tc12','tc13','tc14','tc15','tc16','tc17','tc18','tc19','tc20','tc21','tc22','tc23','tc24','tc25','tc26','tc27','tc28','tc29','tc30','tc31','tc32','tc33','tc34','tc35','tc36','tc37','tc38','tc39','tc40','tc41','tc42','tc43','tc44','tc45','tc46','tc47','tc48','tc49','tc50','tc51','tc52','tc53','tc54','tc55','tc56','tc57','tc58','tc59','tc60','tc61','tc62','tc63','tc64','tc65','tc66']].copy()

merged['tc7']=merged['DCSRPS_EIPS_TP_NAME']
merged['tc7']=merged['tc7'].str.pad(8,side='right')

test_eips_op=merged[['tc1','tc2','tc3','tc4','tc5','tc6','tc7','tc8','tc9','tc10','tc11','tc12','tc13','tc14','tc15','tc16','tc17','tc18','tc19','tc20','tc21','tc22','tc23','tc24','tc25','tc26','tc27','tc28','tc29','tc30','tc31','tc32','tc33','tc34','tc35','tc36','tc37','tc38','tc39','tc40','tc41','tc42','tc43','tc44','tc45','tc46','tc47','tc48','tc49','tc50','tc51','tc52','tc53','tc54','tc55','tc56','tc57','tc58','tc59','tc60','tc61','tc62','tc63','tc64','tc65','tc66']].copy()

os.mkdir(eips_folder)
os.mkdir(CLMS_folder)
#test_clms_op.to_excel('C:\\Users\\i30691\\OneDrive - Verisk Analytics\\Documents\\py\\Auto\\test_op.xlsx')
clms_op=np.savetxt(CLMS_folder+"\\"+op_filename+'CL'+".txt",test_clms_op.values,fmt="%s",delimiter='')
eips_op=np.savetxt(eips_folder+"\\"+op_filename+'LC'+".txt",test_eips_op.values,fmt="%s",delimiter='')

#print(ret)