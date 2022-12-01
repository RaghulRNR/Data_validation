print("--------------------------------------------------Executing API_read.py--------------------------------------------------")

import json
from urllib.request import urlopen
from lookup import*

import pandas as pd

def apicall(res,lc_vrsn_all,mjr_cls_all):
    result2 = pd.DataFrame()
    filterdata = pd.DataFrame()
    for x in range(len(res)):
        print(res[x])
        url_id = 'http://ratingapi-internal-losscostrepositoryt.iso.com/api/Imports/' + str(res[x])
        json_url = urlopen(url_id)
        data = json.loads(json_url.read())
        imports_data = pd.json_normalize(data)
        imports_data["merge"] = 1
        rates_data = pd.json_normalize(data, 'rates')
        rates_data["merge"] = 1
        result = pd.merge(imports_data, rates_data, on='merge').drop("merge", 1)
        result2 = result2.append(result)
        filterdata = result2[(result2['lc_vrsn'] == lc_vrsn_all) & (result2['mjr_class']==mjr_cls_all)]
    return filterdata

def api_link(json_request):
    url='http://ratingapi-internal-losscostrepositoryt.iso.com/api/GetImportIdsFromExpandedSearch'
    headers={'content-type':'application/json', 'Accept':'application/json'}
    response=requests.request("POST",url,data=json_request,headers=headers)
    print('----------------------------------------------------------------------------------------------------------------------------------------------')
    print('response=',response)
    res= response.json()
    print(res)
    return res

state=state.zfill(2)
if (( class_plan == 'old_sim' or class_plan == "legacy" or class_plan == "ocp" or class_plan == "sim") and coverage == 'liability'):
    if (class_plan == 'old_sim'):
        Tabledesgination = "liability(lc)"
    elif (class_plan == "legacy"):
        Tabledesgination = "liability(lc)(leg)"
    elif (class_plan == "ocp"):
        Tabledesgination = "liability(lc)(ocp)"
    elif (class_plan == 'sim'):
        Tabledesgination = "liability(lc)(reg)"
    # ttt:
    request_ttt = '{ "LineOfBusiness": ["'+lob+'"], "State": ["' + state + '"],"ApprovalStatus": ["'+A_ttt+'"],"class_plan":["' + class_plan + '"],"TableDesignation": ["' + Tabledesgination + '"], "lc_vrsn":  ["' + lc_vrsn_ttt + '"], "mjr_class" : ["ttt"] }'
    print(request_ttt)
    ttt = api_link(request_ttt)
    data_ttt = apicall(ttt, lc_vrsn_ttt, "ttt")
    # pubu:
    request_pubu = '{ "LineOfBusiness": ["'+lob+'"], "State": ["' + state + '"],"ApprovalStatus": ["'+A_pubu+'"],"class_plan":["' + class_plan + '"],"TableDesignation": ["' + Tabledesgination + '"], "lc_vrsn":  ["' + lc_vrsn_pubu + '"], "mjr_class" : ["pubu"] }'
    print(request_pubu)
    pubu = api_link(request_pubu)
    data_pubu = apicall(pubu, lc_vrsn_pubu, "pubu")
    # ppt:
    request_ppt = '{ "LineOfBusiness": ["'+lob+'"], "State": ["' + state + '"],"ApprovalStatus": ["'+A_ppt+'"],"class_plan":["' + class_plan + '"],"TableDesignation": ["' + Tabledesgination + '"], "lc_vrsn":  ["' + lc_vrsn_ppt + '"], "mjr_class" : ["ppt"] }'
    print(request_ppt)
    ppt = api_link(request_ppt)
    data_ppt = apicall(ppt, lc_vrsn_ppt, "ppt")
    # Deal:
    request_deal = '{ "LineOfBusiness": ["'+lob+'"], "State": ["' + state + '"],"ApprovalStatus": ["'+A_deal+'"],"class_plan":["' + class_plan + '"],"TableDesignation": ["' + Tabledesgination + '"], "lc_vrsn":  ["' + lc_vrsn_deal + '"], "mjr_class" : ["deal"] }'
    print(request_deal)
    deal = api_link(request_deal)
    data_deal = apicall(deal, lc_vrsn_deal, "deal")
    # Appending_all_data:-
    appended_data = data_ttt.append([data_pubu, data_ppt, data_deal])
elif ((class_plan == 'old_sim' or class_plan == "legacy" or class_plan == "ocp" or class_plan == "sim") and coverage == 'physicaldamage'):
    if (class_plan == 'old_sim'):
        Tabledesgination = "liability(lc)"
    elif (class_plan == "legacy"):
        Tabledesgination = "liability(lc)(leg)"
    elif (class_plan == "ocp"):
        Tabledesgination = "liability(lc)(ocp)"
    elif (class_plan == 'sim'):
        Tabledesgination = "liability(lc)(reg)"
    # ttt
    request_ttt = '{ "LineOfBusiness": ["'+lob+'"], "State": ["' + state + '"],"ApprovalStatus": ["'+A_ttt+'"],"class_plan":["' + class_plan + '"],"TableDesignation": ["' + Tabledesgination + '"], "lc_vrsn":  ["' + lc_vrsn_ttt + '"], "mjr_class" : ["ttt"] }'
    print(request_ttt)
    ttt = api_link(request_ttt)
    data_ttt = apicall(ttt, lc_vrsn_ttt, "ttt")
    # pubu:
    request_pubu = '{ "LineOfBusiness": ["'+lob+'"], "State": ["' + state + '"],"ApprovalStatus": ["'+A_pubu+'"],"class_plan":["' + class_plan + '"],"TableDesignation": ["' + Tabledesgination + '"], "lc_vrsn":  ["' + lc_vrsn_pubu + '"], "mjr_class" : ["pubu"] }'
    print(request_pubu)
    pubu = api_link(request_pubu)
    data_pubu = apicall(pubu, lc_vrsn_pubu, "pubu")
    # ppt:
    request_ppt = '{ "LineOfBusiness": ["'+lob+'"], "State": ["' + state + '"],"ApprovalStatus": ["'+A_ppt+'"],"class_plan":["' + class_plan + '"],"TableDesignation": ["' + Tabledesgination + '"], "lc_vrsn":  ["' + lc_vrsn_ppt + '"], "mjr_class" : ["ppt"] }'
    print(request_ppt)
    ppt = api_link(request_ppt)
    data_ppt = apicall(ppt, lc_vrsn_ppt, "ppt")
    # Appending_all_data
    appended_data = data_ttt.append([data_pubu, data_ppt])
elif((class_plan == 'old_sim' or class_plan == "legacy" or class_plan == "sim") and coverage == 'autodealers and garagekeepers'):
    if (class_plan == 'old_sim'):
        Tabledesignation_49 = '49.(lc)'
        Tabledesgination_55 = '55.(lc)'
    elif (class_plan == 'legacy'):
        Tabledesignation_49 = '49.(lc)(leg)'
        Tabledesgination_55 = '55.(lc)(leg)'
    elif (class_plan == 'legacy'):
        Tabledesignation_49 = '249.(lc)(reg)'
        Tabledesgination_55 = '255.(lc)(reg)'

    # Deal:
    request_deal = '{ "LineOfBusiness": ["'+lob+'"], "State": ["' + state + '"],"ApprovalStatus": ["'+A_deal+'"],"class_plan":["' + class_plan + '"],"TableDesignation": ["' + Tabledesignation_49 + '"], "lc_vrsn":  ["' + lc_vrsn_deal + '"], "mjr_class" : ["deal"] }'
    print(request_deal)
    deal = api_link(request_deal)
    data_deal = apicall(deal, lc_vrsn_deal, "deal")

    # keep:
    request_keep = '{ "LineOfBusiness": ["'+lob+'"], "State": ["' + state + '"],"ApprovalStatus": ["'+A_keep+'"],"class_plan":["' + class_plan + '"],"TableDesignation": ["' + Tabledesgination_55 + '"], "lc_vrsn":  ["' + lc_vrsn_keep + '"], "mjr_class" : ["keep"] }'
    print(request_keep)
    keep = api_link(request_keep)
    data_keep = apicall(keep, lc_vrsn_keep, "keep")
    # Appending_all_data
    appended_data = data_deal.append(data_keep)

ex=appended_data.drop(columns=['importComments','metadata','rates','audit'])
data_file_name=class_plan+"_"+coverage+"_"+state_alpha+'_'+timestamp+'.xlsx'
xl=ex.to_excel('C:\\Users\\i30691\\OneDrive - Verisk Analytics\\Documents\\py\\Auto\\data\\'+data_file_name)
