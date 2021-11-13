# -*- coding: utf-8 -*-
# !usr/bin/env python
import sys,os
path_of_current_file = os.path.abspath(__file__)
path_of_current_dir = os.path.split(path_of_current_file)[0]
sys.path.insert(0, path_of_current_dir)
bind = "" #测试环境
#bind = "" #仿真环境
#bind = "" #生产环境
#bind =
workers = 1
worker_class = "gevent"
timeout = 240
accesslog = '%s/logs/gunicorn_access.log' % (path_of_current_dir)