# -*- coding: utf-8 -*-
# !usr/bin/env python
import logging,logging.handlers
import sys
from config.config import *


def InitLog(name):
	#logging.basicConfig(level = g_config.LOG_LEVEL,format = '%(asctime)s %(levelname)s %(name)s %(pathname)s-%(funcName)s %(threadName)s(%(thread)d) %(message)s')
	formatter = logging.Formatter('%(asctime)s %(levelname)s %(pathname)s-%(funcName)s %(threadName)s(%(thread)d) %(message)s')
	Logger = logging.getLogger(name)
	Logger.setLevel(g_config['log_level'])

	handler = logging.handlers.TimedRotatingFileHandler('logs/default.log', when='midnight', backupCount=3)
	handler.suffix = "%Y%m%d"
	handler.setLevel(g_config['log_level'])
	handler.setFormatter(formatter)

	console = logging.StreamHandler(sys.stdout)
	console.setLevel(g_config['log_level'])
	console.setFormatter(formatter)

	Logger.addHandler(handler)
	Logger.addHandler(console)

	return Logger

Logger = InitLog(__name__)
