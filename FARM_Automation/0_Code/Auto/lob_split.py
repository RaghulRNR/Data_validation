print('----------------------------------Executing lob_split--------------------------------')

from copying_to_data import *
print('LOB=',lob)
if(lob=="CA"):
    from wrapper_CA import*
elif(lob=="FR"):
    from Wrapper_Farm import*