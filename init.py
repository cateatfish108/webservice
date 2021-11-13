from config.config import *
from model.sod.sod_work import WorkFlow_SodDownload,WorkFlow_SodTrans,WorkFlow_SodImport,WorkFlow_SodTask,WorkFlow_SodHqUpdate,WorkFlow_SodHqInit
from model.update.update_work import WorkFlow_Update,WorkFlow_Update_After_Trade,WorkFlow_Resend_Transaction,WorkFlow_Writeback
from model.sod import g_dic_sod_cfg
from model.order.order import WorkFlow_Send_Order,WorkFlow_Send_Cancel_Order,WorkFlow_Send_Modify_Order
from flask import Flask
from comm.thread import TreadPool
from comm.cron import CronJob
from model.update import g_dic_update_cfg
from apscheduler.schedulers.background import BackgroundScheduler
# 初始化配置


g_task_map = {
	'sod_download':WorkFlow_SodDownload,
	'sod_trans':WorkFlow_SodTrans,
	'sod_import':WorkFlow_SodImport,
	'sod_task':WorkFlow_SodTask,
	'sod_hq_update':WorkFlow_SodHqUpdate,
	'sod_hq_init':WorkFlow_SodHqInit,
	'update_transaction':WorkFlow_Update,
	'update_transaction_after_trade':WorkFlow_Update_After_Trade,
	'resend_transaction':WorkFlow_Resend_Transaction,
	'writeback_transaction':WorkFlow_Writeback,
	'send_order':WorkFlow_Send_Order,
	'send_cancel_order':WorkFlow_Send_Cancel_Order,
	'send_modify_order':WorkFlow_Send_Modify_Order
}

def InitApp(name):
	app = Flask(name)
	flask_conf = config_flask[g_config['env']]
	app.config.from_object(flask_conf)
	return app

def InitThreadPool():
	t_pool = TreadPool()
	t_pool.Init(4, g_task_map)
	return t_pool

def InitCronJob():
	c_job = CronJob()
	c_job.AddCronJob(WorkFlow_SodTask,g_dic_sod_cfg.get('CronJob')) #定时下载SOD
	c_job.AddCronJob(WorkFlow_SodHqUpdate,g_dic_sod_cfg.get('CronJob_HqUpdate')) #更新行情数据
	#c_job.AddCronJob(WorkFlow_Update, g_dic_update_cfg.get('CronJob')) #定时上传成交
	#c_job.AddCronJob(WorkFlow_Update_After_Trade,g_dic_update_cfg.get('CronJob_AfterTrade')) #收盘后定时上传部分成交和剩余没有传的成交
	#c_job.AddCronJob(WorkFlow_Resend_Transaction,g_dic_update_cfg.get('CronJob_Resend'))
	#c_job.AddCronJob(WorkFlow_Writeback,g_dic_update_cfg.get('CronJob_Writeback'))
	return c_job

def InitLog(name):
	#logging.basicConfig(level = g_config.LOG_LEVEL,format = '%(asctime)s %(levelname)s %(name)s %(pathname)s-%(funcName)s %(threadName)s(%(thread)d) %(message)s')
	formatter = logging.Formatter('%(asctime)s %(levelname)s %(pathname)s-%(funcName)s %(threadName)s(%(thread)d) %(message)s')
	Logger = logging.getLogger(name)
	Logger.setLevel(g_config['log_level'])

	handler = logging.handlers.TimedRotatingFileHandler('logs/default.log', when='midnight', backupCount=3)
	handler.suffix = "%Y%m%d"
	handler.setLevel(g_config['log_level'])
	handler.setFormatter(formatter)

	console = logging.StreamHandler()
	console.setLevel(g_config['log_level'])
	console.setFormatter(formatter)

	Logger.addHandler(handler)
	Logger.addHandler(console)

	return Logger