import requests
import time
json_request='{}'
url='http://ratingapi-internal-losscostrepositoryt.iso.com/api/GetImportIdsFromExpandedSearch'
headers={'content-type':'application/json', 'Accept':'application/json'}
response=requests.request("POST",url,data=json_request,headers=headers)
#print(response)
response_code=response.status_code
if(response_code==200):
   print('\n\n\n*******************************API is Up and Running************************************************')
else:
    exit('\n\n\n*******************************API is Down**********************************************************')
time.sleep(5.0)
