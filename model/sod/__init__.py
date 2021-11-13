# -*- coding: utf-8 -*-
# !usr/bin/env python
import json
from comm.log import Logger

g_dic_sod_cfg = {}

#SOD配置初始化
def InitConfig():
	str_cfg_path = "model/sod/setting/config.json"
	try:
		f_config = open(str_cfg_path, 'r+')
		dic_cfg = json.loads(f_config.read())
		f_config.close()
		return dic_cfg
	except Exception:
		Logger.error("Failed to load config", exc_info=True)
		return {}


g_dic_sod_cfg = InitConfig()





