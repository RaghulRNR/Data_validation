import pandas as pandas
import pandas as pd

decimal=pd.read_excel("C:\\Users\\i30691\\OneDrive - Verisk Analytics\\Documents\\decimal.xlsx")
value=pd.read_excel("C:\\Users\\i30691\\OneDrive - Verisk Analytics\\Documents\\value.xlsx")
for y,x in zip(decimal['decimal'],value['value']):
    temp='.'+str(y)+'f'
    print(format(x,temp))
    #print(x,y)