# -*- coding: utf-8 -*-
# !usr/bin/env python
import requests,json
from comm.log import Logger
from model.order import g_dic_order_cfg,g_dic_order_cache
from model.order.parse import parse
from model.order import g_thread_pool
from comm.func import TimeRecord
import datetime


class Order(object):
    def __init__(self):
        pass

    def Send(self,dict):
        # 委托申报
        try:
            orderId = int(dict['response']['Id'])
            str_ori = json.dumps(dict)
            Logger.debug('origin order[%d]:%s'%(orderId,str_ori))

            dic_order_info = parse['order'].parse(dict)
            dic_req = {}
            g_dic_order_cache[orderId] = dic_req
            #增加标志标识正在处理，防止处理同一笔订单
            dic_req['processing'] = 1

            dic_req['order_info'] = dic_order_info.copy()
            dic_req['id'] = orderId
            dic_req['status'] = 'sending_order'
            dic_req['time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

            str_req = json.dumps(dic_req)
            Logger.debug('request order[%d]:%s'%(orderId,str_req))
            #输出发送信息到文本记录
            dic_rec = dic_req.copy()
            g_thread_pool.AddTask(type='record_order_info', data=dic_rec)

            response = requests.get(g_dic_order_cfg['Base_URL'], params=dic_req['order_info'])
            if response.status_code >= 200 and response.status_code < 300:
                reqDict = json.loads(response.text)
                if reqDict.get('FID_CODE') and int(reqDict.get('FID_CODE')) > -1:
                    dic_req['order_info']['FID_WTH'] = reqDict.get('ROWS')[0]['FID_WTH']
                    dic_req['order_info']['FID_JYS'] = reqDict.get('ROWS')[0]['FID_JYS']
                    dic_req['status'] = 'order_success'
                else:
                    dic_req['order_info']['FID_CODE'] = reqDict.get('FID_CODE')
                    dic_req['status'] = 'order_failed'
            else:
                dic_req['status'] = 'send_order_failed'
            dic_req['time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
            dic_req['processing'] = 0

            # 输出发送信息到文本记
            str_ret = json.dumps(dic_req)
            Logger.debug('response order[%d]:%s' % (orderId, str_ret))
            dic_rec = dic_req.copy()
            g_thread_pool.AddTask(type='record_order_info', data=dic_rec)

        except Exception:
            dic_req['processing'] = 0
            Logger.error('send order error:', exc_info=True)


class Cancel_Order(object):
    def __init__(self):
        pass

    #判断订单是否正在处理
    def IsOrderHandling(self, dict):
        b_pro = dict.get('processing')
        #如果订单处于发送状态，则重新加入到队列
        if b_pro and b_pro == 1:
            return True
        else:
            return False

    def Send(self,dict):
        try:
            orderId = int(dict['request']['Id'])
            str_ori = json.dumps(dict)
            Logger.debug('origin order[%d]:%s' % (orderId, str_ori))
            dic_req = g_dic_order_cache.get(orderId)
            
            #判断订单是否正在发送
            if self.IsOrderHandling(dic_req) == True:
                if dic_req.get('retry_cancel_count') is None:
                    dic_req['retry_cancel_count']=0
                if dic_req['retry_cancel_count'] < 10:
                    dic_req['retry_cancel_count']+=1
                    g_thread_pool.AddTask(type='retry_cancel_order', data=dict)
                    return
                dic_req['retry_cancel_count']=0
                return
            
            #如果订单正在发送则等待,循环刷新从全局字典的取值的本地字典,直到订单被处理完毕继续往下执行撤单处理
            # while self.IsOrderHandling(dic_req):
            #     dic_req = g_dic_order_cache.get(orderId)
            dic_req['processing'] = 1
            dic_req['order_info']['FuncId'] = '620021'
            dic_req['status'] = 'sending_cancel'
            str_req = json.dumps(dic_req)

            Logger.debug('request order[%d]:%s' % (orderId, str_req))
            # 输出发送信息到文本记录
            dic_rec = dic_req.copy()
            g_thread_pool.AddTask(type='record_order_info', data=dic_rec)

            response = requests.get(g_dic_order_cfg['Base_URL'], params=dic_req['order_info'])
            if response.status_code >= 200 and response.status_code < 300:
                reqDict = json.loads(response.text)
                if reqDict.get('FID_CODE') and int(reqDict.get('FID_CODE')) > -1:
                    dic_req['order_info']['FID_CXWTH'] = reqDict.get('ROWS')[0]['FID_WTH']
                    dic_req['order_info']['FID_JYS'] = reqDict.get('ROWS')[0]['FID_JYS']
                    dic_req['status'] = 'cancel_success'
                else:
                    dic_req['order_info']['FID_CODE'] = reqDict.get('FID_CODE')
                    dic_req['status'] = 'cancel_failed'
            else:
                dic_req['status'] = 'cancel_failed'
            dic_req['time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')


            str_ret = json.dumps(dic_req)
            Logger.debug('response order[%d]:%s' % (orderId, str_ret))

            dic_rec = dic_req.copy()
            g_thread_pool.AddTask(type='record_order_info', data=dic_rec)
            dic_req['processing'] = 0

        except Exception:
            dic_req['processing'] = 0
            Logger.error('send cancel order error:', exc_info=True)

class Modify_Order(object):
    def __init__(self):
        pass
    
    #判断订单是否正在处理
    def IsOrderHandling(self, dict):
        b_pro = dict.get('processing')
        #如果订单处于发送状态，则重新加入到队列
        if b_pro and b_pro == 1:
            return True
        else:
            return False

    def Send(self,dict):
        try:
            orderId = int(dict['request']['Id'])
            str_ori = json.dumps(dict)
            Logger.debug('origin order[%d]:%s' % (orderId, str_ori))
            dic_order_info = parse['modify'].parse(dict)

            dic_req = g_dic_order_cache.get(orderId)
            #判断订单是否正在发送
            if self.IsOrderHandling(dic_req) == True:
                if dic_req.get('retry_modify_count') is None:
                    dic_req['retry_modify_count']=0
                if dic_req['retry_modify_count'] < 10:
                    dic_req['retry_modify_count']+=1
                    g_thread_pool.AddTask(type='retry_modify_order', data=dict)
                    return
                dic_req['retry_modify_count']=0
                return

            dic_req['processing'] = 1
            dic_req['order_info']['FID_WTSL'] = dic_order_info['FID_WTSL']
            dic_req['order_info']['FID_WTJG'] = dic_order_info['FID_WTJG']
            dic_req['order_info']['FID_GGLB'] = '1'
            dic_req['order_info']['FuncId'] = '620028'
            dic_req['status'] = 'sending_modify'
            str_req = json.dumps(g_dic_order_cache.get(orderId))
            Logger.debug('request order[%d]:%s' % (orderId, str_req))

            # 输出发送信息到文本记录
            dic_rec = dic_req.copy()
            g_thread_pool.AddTask(type='record_order_info', data=dic_rec)

            #发送订单给OMS
            response = requests.get(g_dic_order_cfg['Base_URL'], params=dic_req['order_info'])
            if response.status_code >= 200 and response.status_code < 300:
                reqDict = json.loads(response.text)
                if reqDict.get('FID_CODE') and int(reqDict.get('FID_CODE')) > -1:
                    dic_req['order_info']['FID_WTH'] = reqDict.get('ROWS')[0]['FID_WTH']
                    dic_req['order_info']['FID_JYS'] = reqDict.get('ROWS')[0]['FID_JYS']
                    dic_req['status'] = 'modify_success'
                else:
                    dic_req['order_info']['FID_CODE'] = reqDict.get('FID_CODE')
                    dic_req['status'] = 'modify_failed'
            else:
                dic_req['status'] = 'send_modify_failed'
            dic_req['status_code'] = response.status_code
            dic_req['stauts_text'] = response.text
            dic_req['time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

            #输出订单记录到文本
            str_ret = json.dumps(dic_req)
            Logger.debug('response order[%d]:%s' % (orderId, str_ret))
            dic_rec = dic_req.copy()
            g_thread_pool.AddTask(type='record_order_info', data=dic_rec)
            dic_req['processing'] = 0

        except Exception:
            Logger.error('[send modify order error]:', exc_info=True)

@TimeRecord
def WorkFlow_Send_Order(data):
    order = Order()
    order.Send(data)

@TimeRecord
def WorkFlow_Send_Cancel_Order(data):
    order = Cancel_Order()
    order.Send(data)

@TimeRecord
def WorkFlow_Send_Modify_Order(data):
    order = Modify_Order()
    order.Send(data)

@TimeRecord
def WorkFlow_Record_OrderInfo(data):
    filename = 'order_' + datetime.datetime.now().strftime('%Y%m%d') + '.txt'
    try:
        with open(g_dic_order_cfg['Record_Path'] + filename, 'a') as f:
            dic_data = json.dumps(data) + '\n'
            f.write(dic_data)
    except Exception:
        Logger.error('send error:', exc_info=True)


g_dic_task = {'record_order_info':WorkFlow_Record_OrderInfo,
              'retry_cancel_order':WorkFlow_Send_Cancel_Order,
              'retry_modify_order':WorkFlow_Send_Modify_Order}
g_thread_pool.Init(2,g_dic_task)
g_thread_pool.Start()