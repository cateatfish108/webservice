# -*- coding: utf-8 -*-
# !usr/bin/env python

import os,json,datetime
from comm.log import Logger
from queue import Queue
from comm.thread import TreadPool

g_Queue_R = Queue()
g_Queue_F = Queue()

def get_queue_request():
    return g_Queue_R

def get_queue_file():
    return g_Queue_F

g_dic_order_cfg = {}

#SOD配置初始化
def InitConfig():
	str_cfg_path = "model/order/setting/config.json"
	try:
		f_config = open(str_cfg_path, 'r+')
		dic_cfg = json.loads(f_config.read())
		f_config.close()
		return dic_cfg
	except Exception:
		Logger.error("Failed to load config", exc_info=True)
		return {}


g_dic_order_cfg = InitConfig()

def InitThreadPoolForWrite():
	t_pool = TreadPool()
	return t_pool

g_thread_pool = InitThreadPoolForWrite()


#生成器实现
def foo(filename):
    seek_id = 0
    while True:
        with open(filename,'r') as f:
            f.seek(seek_id)
            data = f.readline()
            if data:
                seek_id = f.tell()
                yield data
            else:
                break

#通过遍历生成器来执行函数里面的代码
def init_unfinish_order():
    from comm.log import Logger
    dictR = {}
    filename = g_dic_order_cfg['Record_Path'] + '/order_' + datetime.datetime.now().strftime('%Y%m%d') + '.txt'
    if not os.path.exists(filename):
        return dictR

    for val in foo(filename):
        try:
            dictT = json.loads(val)
            if dictT.get('id'):
                orderId = dictT.get('id')
                status = dictT.get('status')
                #已经撤销的订单就不再读取了
                if status and (status == 'cancel_success') and dictR.get(orderId):
                    dictR.pop(orderId)
                    continue
                dictR[orderId] = dictT
        except Exception:
            Logger.error('init dictionary failed:', exc_info=True)
    return dictR

g_dic_order_cache = init_unfinish_order()








