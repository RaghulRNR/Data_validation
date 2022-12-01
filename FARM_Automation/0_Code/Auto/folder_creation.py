print("--------------------------------------------------Executing folder_creation.py--------------------------------------------------")

import os
from Paths import*
from lookup import *
from copying_to_data import *

if (class_plan == 'old_sim'):
    class_plan_fol = 'SIM'
elif (class_plan == 'ocp'):
    class_plan_fol = 'OCP'
elif (class_plan == 'legacy'):
    class_plan_fol = 'LEG'
elif (coverage == 'sim'):
    class_plan_fol = 'LEG'

if (coverage == 'liability'):
    coverage_fol = 'LIAB'
    op_filename='LIAB'
elif (coverage == 'physicaldamage'):
    coverage_fol = 'PD'
    op_filename='PHYSDM'
elif (coverage == 'autodealers and garagekeepers'):
    coverage_fol = 'PDGA'
    op_filename='GARAGE'



eips_folder=output_folder+'\\'+coverage_fol+'_'+state_alpha+'_'+'EIPS'+'_'+class_plan_fol+'_'+host_job+'_'+timestamp
CLMS_folder=output_folder+'\\'+coverage_fol+'_'+state_alpha+'_'+'CLM'+'_'+class_plan_fol+'_'+host_job+'_'+timestamp
print(eips_folder)
print(CLMS_folder)

