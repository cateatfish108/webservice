# -*- coding: utf-8 -*-
# !usr/bin/env python
import json
from model.order import g_dic_order_cache
import datetime
class Parse(object):
    param_key = {}
    param_trans = {}
    def Parse(self,reqStr):
        dictRet = {}
        return dictRet

    def parse(self, dictReq):
        pass

#申报订单
class Parse_Order(Parse):
    param_key = {}
    param_trans = {}
    def __init__(self):
        self.param_key = \
            {
                "ClientId": "FID_KHH",
                "Type": "FID_DDLX",  # 订单类型
                "Side": "FID_WTLB",  # 买卖类别
                "Symbol": "FID_ZQDM",
                "Price": "FID_WTJG",
                "Quantity": "FID_WTSL",
                "TimeInforce": "FID_TIMEINFORCE",
                "StopPrice": "FID_COND_SL_TRIGGER_PRICE",
                "Exchange": "FID_FJC",
                "ExtendedHours": "FID_SDLX",
            }
        self.param_trans = \
            {
                "Type": {"Market": "200", "Limit": "0", "Stop": "201", "StopLimit": "202"},
                "Side": {"Buy": "1001", "Sell": "1002", "BuyToCover": "1001", "SellShort": "1002"},
                "TimeInforce": {"Day": "0", "GoodTillCancel": "1", "ImmediateOrCancel": "2", "FillOrKill": "3"},
                "ExtendedHours": {"PRE": "1", "POST": "2", "ALL": "3", "REGPOST": "4", "PREREG": "5", "PREPOST": "6"}
            }

    def parse(self,dictReq):
        dictRet = {}
        if dictReq.get('request') and dictReq.get('response'):
            request = dictReq['request']
            for k in request:
                if self.param_key.get(k):
                    key = self.param_key.get(k)
                    value = ""
                    if self.param_trans.get(k) and self.param_trans.get(k).get(request[k]):
                        value = self.param_trans[k][request[k]]
                    else:
                        value = request[k]
                    dictRet[key] = value
            dictRet['FuncId'] = '620001'
            dictRet['FID_ZJZH'] = dictRet['FID_KHH'] + '05'
            dictRet['FID_FJC'] = 'INNER'
            dictRet['FID_BH'] = dictReq['response']['Id']
        return dictRet

#订单回写
class Parse_Order_Confirm(Parse):
    def __init__(self):
        pass

    def parse(self,dictReq):
        dictRet = {}
        dictRet['FID_JYS'] = dictReq['FID_JYS']
        dictRet['FID_SBWTH'] = dictReq['FID_WTH']
        dictRet['FID_WTH'] = dictReq['FID_WTH']
        dictRet['FID_CXBZ'] = 'O'
        dictRet['FuncId'] = '620105'
        return dictRet


#成交回报
class Parse_Order_Return(Parse):
    def __init__(self):
        pass

    def parse(self,dictReq):
        dictRet = {}
        orderId = int(dictReq.get('Id'))
        # dictRet['FID_VERSION'] = 0
        # dictRet['FID_NODETYPE'] = 0
        # dictRet['FID_ROWNUM'] = 0
        dictRet['FID_JYS'] = g_dic_order_cache.get(orderId).get('FID_JYS')
        # dictRet['FID_QRBZ'] = 0
        dictRet['FID_CXBZ'] = "O"
        # dictRet['FID_SBBZ'] = " "
        dictRet['FID_SBWTH'] = g_dic_order_cache.get(orderId).get('FID_WTH')
        dictRet['FID_WTH'] = g_dic_order_cache.get(orderId).get('FID_WTH')
        dictRet['FID_BH'] = g_dic_order_cache.get(orderId).get('FID_WTH')
        t = int(dictReq['TransactionDate'])/1000
        timeArray = datetime.datetime.utcfromtimestamp(t)
        formatTime = timeArray.strftime("%H%M%S%f")
        formatTime = formatTime[:-3]
        dictRet['FID_CJSJ'] = formatTime
        # dictRet['FID_GDH'] = " "
        dictRet['FID_ZQDM'] = dictReq['Symbol']
        dictRet['FID_CJBH'] = orderId
        dictRet['FID_XYBZ'] = "Deal"
        dictRet['FID_HBBZ'] = 0
        # dictRet['FID_DFGDH'] = " "
        # dictRet['FID_XWDM'] = " "
        dictRet['FID_CJJG'] = dictReq['AveragePrice']
        dictRet['FID_CJSL'] = int(float(dictReq['Quantity']))
        # dictRet['FID_ZCJJE'] = 0
        # dictRet['FID_BCYE_LCBJ'] = 0
        # dictRet['FID_YWSHQD'] = 0
        # dictRet['FID_KHQR'] = 0
        # dictRet['FID_EDZ'] = 0
        # dictRet['FID_KHH'] = " "
        # dictRet['FID_ZJZH'] = " "
        dictRet['FuncId'] = '620301'
        return dictRet

#撤单
class Parse_Cancel(Parse):
    def __init__(self):
        pass

    def parse(self,dictReq):
        dictRet = {}
        if dictReq.get('request'):
            request = dictReq.get('request')
        dictRet['FuncId'] = '620021'
        return dictRet

#撤单确认
class Parse_Cancel_Confirm(Parse):
    def __init__(self):
        pass

    def parse(self,dictReq):
        dictRet = {}
        orderId = int(dictReq.get('Id'))
        dictRet['FID_JYS'] = g_dic_order_cache.get(orderId).get('FID_JYS')
        dictRet['FID_SBWTH'] = g_dic_order_cache.get(orderId).get('FID_WTH')
        dictRet['FID_WTH'] = g_dic_order_cache.get(orderId).get('FID_WTH')
        dictRet['FID_CXBZ'] = 'W'
        dictRet['FuncId'] = '620105'
        return dictRet

#撤单回报
class Parse_Cancel_Return(Parse):
    def __init__(self):
        pass

    def parse(self,dictReq):
        dictRet = {}
        orderId = int(dictReq.get('Id'))
        dictRet['FID_VERSION'] = 0
        dictRet['FID_JYS'] = g_dic_order_cache.get(orderId).get('FID_JYS')
        dictRet['FID_CXBZ'] = 'W'
        dictRet['FID_SBWTH'] = g_dic_order_cache.get(orderId).get('FID_WTH')
        dictRet['FID_WTH'] = g_dic_order_cache.get(orderId).get('FID_WTH')
        dictRet['FID_BH'] = g_dic_order_cache.get(orderId).get('FID_WTH')
        dictRet['FID_CJJG'] = 0
        #dictRet['FID_ZQDM'] = ""
        dictRet['FID_CJBH'] = orderId
        dictRet['FID_BH'] = g_dic_order_cache.get(orderId).get('FID_WTH')
        dictRet['FID_CJSL'] = int(float(dictReq['Quantity']))
        dictRet['FID_KHH'] = " "
        dictRet['FuncId'] = '620201'
        return dictRet


#订单回写
class Parse_Order_Modify(Parse):
    def __init__(self):
        pass

    def parse(self,dictReq):
        dictRet = {}
        if dictReq.get('request'):
            request = dictReq['request']
            dictRet['FID_KHH'] = request['ClientId']
            dictRet['FID_ZJZH'] = dictRet['FID_KHH'] + '05'
            dictRet['FID_WTSL'] = request['Quantity']
            dictRet['FID_WTH'] = request['Id']
            dictRet['FID_WTJG'] = request['Price']
            dictRet['FID_GGLB'] = '1'
            dictRet['FuncId'] = '620001'
        return dictRet

parse = {
    "order": Parse_Order(),
    "order_confirm":Parse_Order_Confirm(),
    "order_return":Parse_Order_Return(),
    "cancel": Parse_Cancel(),
    "cancel_confirm":Parse_Cancel_Confirm(),
    "cancel_return":Parse_Cancel_Return(),
    "modify":Parse_Order_Modify(),
}