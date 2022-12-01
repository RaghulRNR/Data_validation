import pandas as pd

from Paths import *
from copying_to_data import *
from folder_creation import op_filename, CLMS_folder, eips_folder
from pagination import *
import os


def fun_header(folder_name, pro,user_id,host_job,terr_count):
    Header = pd.DataFrame()
    Header2 = pd.DataFrame()
    a = 0
    user_id = user_id.strip("\n")
    Header.at[a, 'tc1'] = '0'
    Header['tc2'] = user_id.upper()
    Header['tc3'] = 'RATEPAGE'
    Header['tc4'] = 'EXAUTONW'
    Header['tc5'] = host_job.upper()
    Header['tc6'] = '001'
    Header['tc7'] = pro
    Header['tc8'] = terr_count + 1
    Header['tc9'] = 'S1' + state_alpha + 'R***.DOC'
    Header['tc10'] = '0'
    Header['tc11'] = '0'
    Header['tc12'] = '00'
    Header["tc13"] = "WINWORD.EXE "
    Header["tc14"] = ";"
    Header["tc15"] = "EXAUTONW"
    Header["tc16"] = ";"
    Header["tc17"] = "0"
    Header["tc18"] = ";"
    Header["tc19"] = "0"
    Header["tc20"] = ";"
    Header["tc21"] = "1"
    Header["tc22"] = "0"
    Header["tc23"] = "ISONB02"
    Header["tc24"] = "SYS"
    Header["tc25"] = "\TEST\\28\EIPS"

    Header=Header.astype(str)

    Header["tc1"] = Header["tc1"].str.pad(1, side="right")
    Header["tc2"] = Header["tc2"].str.pad(8, side="right")
    Header["tc3"] = Header["tc3"].str.pad(8, side="right")
    Header["tc4"] = Header["tc4"].str.pad(8, side="right")
    Header["tc5"] = Header["tc5"].str.pad(8, side="right")
    Header["tc6"] = Header["tc6"].str.pad(3, side="right")
    Header["tc7"] = Header["tc7"].str.pad(12, side="right")
    Header["tc8"] = Header["tc8"].str.pad(3, side="right")
    Header["tc9"] = Header["tc9"].str.pad(12, side="right")
    Header["tc10"] = Header["tc10"].str.pad(1, side="right")
    Header["tc11"] = Header["tc11"].str.pad(1, side="right")
    Header["tc12"] = Header["tc12"].str.pad(2, side="right")
    Header["tc13"] = Header["tc13"].str.pad(12, side="right")
    Header["tc14"] = Header["tc14"].str.pad(1, side="right")
    Header["tc15"] = Header["tc15"].str.pad(8, side="right")
    Header["tc16"] = Header["tc16"].str.pad(1, side="right")
    Header["tc17"] = Header["tc17"].str.pad(1, side="right")
    Header["tc18"] = Header["tc18"].str.pad(1, side="right")
    Header["tc19"] = Header["tc19"].str.pad(1, side="right")
    Header["tc20"] = Header["tc20"].str.pad(1, side="right")
    Header["tc21"] = Header["tc21"].str.pad(1, side="right")
    Header["tc22"] = Header["tc22"].str.pad(1, side="right")
    Header["tc23"] = Header["tc23"].str.pad(8, side="right")
    Header["tc24"] = Header["tc24"].str.pad(8, side="right")
    Header["tc25"] = Header["tc25"].str.pad(13, side="right")

    # Line2:-
    Header2.at[a, 'tc1'] = "1"
    Header2['tc2'] = pro
    Header2['tc3'] = "2"

    Header2["tc1"] = Header2["tc1"].str.pad(1, side="right")
    Header2["tc2"] = Header2["tc2"].str.pad(12, side="right")
    Header2["tc3"] = Header2["tc3"].str.pad(1, side="right")

    np.savetxt(folder_name + "\\HEAD" + ".txt", Header.values, fmt="%s", delimiter='')
    np.savetxt(folder_name + "\\HEAD1" + ".txt", Header2.values, fmt="%s", delimiter='')
    with open(folder_name + "\\HEAD1" + ".txt", 'r') as file:
        line2 = file.read()
    with open(folder_name + "\\HEAD" + ".txt", "a") as f:
        f.write(line2)
    os.remove(folder_name + "\\HEAD1" + ".txt")


fun_header(CLMS_folder, op_filename + "CL.TXT",user_id,host_job,terr_count)
fun_header(eips_folder, op_filename + "LC.TXT",user_id,host_job,terr_count)


