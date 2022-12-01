import pandas as pd
import numpy as np
op=pd.DataFrame()
input=pd.read_excel("C:\\Users\\i30691\\OneDrive - Verisk Analytics\\Documents\\py\\Auto\\data+123.xlsx")

conditions=pd.read_excel("Z:\\EVERYONE\\LCE\\01_FARM\\KTR\\Table_AC\\Input_files\\TABLE_AC_zone_123_State _AL.xlsx")
nan_replace='#'
conditions=conditions.fillna(nan_replace)
print(conditions)
Header=list(conditions)
print(Header)
print('header[0]=',Header[0])
print('header[1]=',Header[1])
print('header[2]=',Header[2])
print('header[3]=',Header[3])

condition_count=len(conditions.index)
input_count=len(input.index)
a =0
b=0
for x in range(condition_count):
    b=0
    #print('a=',a)
    c1=conditions.at[a, Header[0]]
    c2=conditions.at[a, Header[1]]
    c3=conditions.at[a, Header[2]]
    c4=conditions.at[a, Header[3]]

    #print(c1,'\n',c2,'\n',c3,'\n',c4,'\n')

    for y in range(input_count):
        if(c1=='#'):
            in1='#'
        else:
            in1=input.at[b,Header[0]]
        if(c2=='#'):
            in2='#'
        else:
            in2=print(input.at[b,Header[1]])
        if (c3 == '#'):
            in3 = '#'
        else:
            in3=input.at[b,Header[2]]
        if (c4 == '#'):
            in4 = '#'
        else:
            in4=input.at[b,Header[3]]
        print(in1,'==',c1)
        print(in2,'==',c2)
        print(in3,'==',c3)
        print(in4,'==',c4)
        op['tc'] = np.where((in1 == c1) or (in1 == '#')
                         and (in2 == c2) or (in2 == '#')
                         and (in3 == c3) or (in3 == '#')
                         and (in4 == c4) or (in4 == '#'),
                             input['Value'], '   0.0')
        b=b+1
    a=a+1



print(input)

op.to_excel("C:\\Users\\i30691\\OneDrive - Verisk Analytics\\Documents\\py\\Auto\\conditions\\final.xlsx")