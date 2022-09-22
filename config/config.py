# -*- coding: utf-8 -*-
# !usr/bin/env python
import json,logging

class Config(object):
    """项目配置核心类"""
    # 调试模式
    DEBUG = True
    # SQLALCHEMY_DATABASE_URI = 'oracle://quickbroker:test@172.19.81.79:7070/Orcloms'
    # SQLALCHEMY_ECHO = True
    LOG_LEVEL = logging.DEBUG
    ORDER_LIST_FILE_PATH = 'application/file/'
    RETRY_ORDER_RETURN_CNT = 5
    pass

class Dev_config(Config):
    DEBUG = True
    # SQLALCHEMY_DATABASE_URI = 'oracle://quickbroker:test@172.19.81.79:7070/Orcloms'
    # SQLALCHEMY_ECHO = True
    LOG_LEVEL = logging.DEBUG
    ORDER_LIST_FILE_PATH = 'application/file/'
    RETRY_ORDER_RETURN_CNT = 5

class Product_config(Config):
    DEBUG = False
    HOST = '10.0.1.103'
    PORT = 5000
    send_base_url = 'http://neway.10jqka.com.cn:9003/oms/'
    #SQLALCHEMY_DATABASE_URI = 'oracle://quickbroker:test@172.19.81.79:7070/Orcloms'
    LOG_LEVEL = logging.INFO
    ORDER_LIST_FILE_PATH = 'application/file/'
    RETRY_ORDER_RETURN_CNT = 10

config_flask = {
    "dev": Dev_config,
    "prod": Product_config,
}

g_config = {}

def init_config():
    with open('config/config.json', 'r') as f:
        config_file = json.load(f)
        g_config['env'] = config_file['env']
        g_config['send_base_url'] = config_file['base_url']
        g_config['host'] = config_file['host']
        g_config['port'] = config_file['port']
        g_config['order_file_path'] = config_file['order_file_path']
        if config_file['log_level'] == 'info':
            g_config['log_level'] = logging.INFO
        elif config_file['log_level'] == 'debug':
            g_config['log_level'] = logging.DEBUG
        g_config['retry_order_cnt'] = config_file['retry_order_cnt']

init_config()