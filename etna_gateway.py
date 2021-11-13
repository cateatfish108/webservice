# -*- coding: utf-8 -*-

# 
# LoginID：
# PW：
# API Key: 

import requests

def get_token(strUrl,dicHeader):
    strReqUrl = strUrl+'/token'
    vRes = requests.post(url=strReqUrl,headers=dicHeader)
    if vRes.json()['State']=='Succeeded':
        token=vRes.json()['Token']
        return token
    strErrMsg='get token failed'
    return strErrMsg

def get_order_info(strUrl,dicPath,dicData,dicHeader):
    # /v1.0/accounts/6303/orders/73552
    strReqUrl = strUrl+'/v'+dicPath['version']+'/accounts'+dicPath['accountID']+'/orders'
    vRes = requests.get(url=strReqUrl,data=dicData,headers=dicHeader)
    
    return vRes.json()

def main():
    strBaseUrl=""
    dicHeader={
        "Username":"",
        "Password":"",
        "Et-App-Key":""
    }
    token = 'Bearer '+get_token(strBaseUrl,dicHeader)

    print('token:\n'+token)

    dicPath={
        "version":"1.0",
        "accountID":"23",
    }
    dicData={
        'pageNumber':0,
        'pageSize':100,
        'desc':false,
        'useHolderInfo':false
    }
    dicOrderHeader={
        'Authorization':token,
        "Et-App-Key":""
    }
    orderInfo = get_order_info(strBaseUrl,dicPath,dicData,dicOrderHeader)
    print(orderInfo)

main()