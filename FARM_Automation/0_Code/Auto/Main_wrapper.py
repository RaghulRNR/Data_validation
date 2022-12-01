import time
import traceback
import sys
global timestamp
from datetime import datetime
t= datetime.now() # current date and time
timestamp = t.strftime("%Y%m%d_ %H%M%S")
with open("C:\\Users\\i30691\\OneDrive - Verisk Analytics\\Documents\\FARM_Automation\\Log\\Logg_"+timestamp+".txt", 'a') as fh:
    try:
        from Paths import *
        from Read import *
        from copying_to_data import *
        from lookup import *
        from lob_split import *
        print("****************************************Main_Wrapper_completed*******************************************************")
        time.sleep(5.0)
    except Exception as e:
        print(e)
        e_type, e_val, e_tb = sys.exc_info()
        traceback.print_exception(e_type, e_val, e_tb, file=fh)
        exit()
