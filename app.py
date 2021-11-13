# -*- coding: utf-8 -*-
# !usr/bin/env python
#from comm.log import Logger

import json,requests
from model.order import get_queue_request

from comm.log import Logger

from flask import request
from init import InitApp,InitThreadPool,InitCronJob

treadpool = InitThreadPool()
cronjob = InitCronJob()
app = InitApp(__name__)
g_version = 'version 0.0.0.1'

# gunicorn或者pycharm启动跑这一段
if __name__ != "__main__":
	treadpool.Start()
	cronjob.Start()

# python启动跑这一段
if __name__ == "__main__":
	treadpool.Start()
	cronjob.Start()
	app.run()

@app.route('/')
def hello_world():
	return 'Welcome to use webservice(' + g_version + ')'

@app.route('/order', methods=['POST'])
def order():
	try:
		s_data = request.get_data()
		dic_data = json.loads(s_data.decode("utf-8"))
		if treadpool.AddTask(type='send_order', data=dic_data) == False:
			return '{"retcode":1,"retmsg":"send order start failed"}'
	except Exception:
		Logger.error("Failed to send order", exc_info=True)
	return '{"retcode":1,"retmsg":""}'

@app.route('/order/return', methods=['POST'])
def order_return():
	try:
		data = request.get_data()
		json_data = json.loads(data.decode("utf-8"))
		json_data['Action_Type'] = 'Order_Return'
		get_queue_request().put(json_data)
	except Exception:
		Logger.error("Failed to parse the request json", exc_info=True)
	return '{"retcode":1,"retmsg":""}'

@app.route('/cancel', methods=['POST'])
def cancel():
	try:
		s_data = request.get_data()
		dic_data = json.loads(s_data.decode("utf-8"))
		if treadpool.AddTask(type='send_cancel_order', data=dic_data) == False:
			return '{"retcode":1,"retmsg":"send cancel order start failed"}'
	except Exception:
		Logger.error("Failed to send cancel order", exc_info=True)
	return '{"retcode":1,"retmsg":""}'

@app.route('/cancel/return', methods=['POST'])
def cancel_return():
	try:
		data = request.get_data()
		json_data = json.loads(data.decode("utf-8"))
		json_data['Action_Type'] = 'Cancel_Return'
		get_queue_request().put(json_data)
	except Exception:
		Logger.error("Failed to parse the request json", exc_info=True)
	return '{"retcode":1,"retmsg":""}'

@app.route('/modify', methods=['POST'])
def modify():
	try:
		s_data = request.get_data()
		dic_data = json.loads(s_data.decode("utf-8"))
		if treadpool.AddTask(type='send_modify_order', data=dic_data) == False:
			return '{"retcode":1,"retmsg":"send modify order start failed"}'
	except Exception:
		Logger.error("Failed to send modifyorder", exc_info=True)
	return '{"retcode":1,"retmsg":""}'

@app.route('/sod/download', methods=['GET'])
def sod_download():
	try:
		s_date = request.args.get('date') or ''
		global treadpool
		if treadpool.AddTaskSet(type = 'sod_download',data = {'date':s_date}) == False:
			return '{"retcode":1,"retmsg":"Sod download has already started"}'
	except Exception:
		Logger.error("Download SOD job start failed", exc_info=True)
		return '{"retcode":-1,"retmsg":"Download SOD job start failed"}'

	return '{"retcode":1,"retmsg":"Download SOD job start"}'

@app.route('/sod/trans', methods=['GET'])
def sod_trans():
	try:
		s_date = request.args.get('date') or ''
		global treadpool
		if treadpool.AddTaskSet(type='sod_trans', data={'date': s_date}) == False:
			return '{"retcode":1,"retmsg":"Sod trans has already started"}'
	except Exception:
		Logger.error("Trans SOD job start failed", exc_info=True)
		return '{"retcode":-1,"retmsg":"Trans SOD job start failed"}'

	return '{"retcode":1,"retmsg":"Trans SOD job start"}'

@app.route('/sod/import', methods=['GET'])
def sod_import():
	try:
		s_date = request.args.get('date') or ''
		global treadpool
		if treadpool.AddTaskSet(type='sod_import', data={'date': s_date}) == False:
			return '{"retcode":1,"retmsg":"Sod import has already started"}'
	except Exception:
		Logger.error("Import SOD job start failed", exc_info=True)
		return '{"retcode":-1,"retmsg":"Import SOD job start failed"}'

	return '{"retcode":1,"retmsg":"import SOD job start"}'

@app.route('/sod/task', methods=['GET'])
def sod_task():
	try:
		s_date = request.args.get('date') or ''
		global treadpool
		if treadpool.AddTaskSet(type='sod_task', data={'date': s_date}) == False:
			return '{"retcode":1,"retmsg":"Sod task has already started"}'
	except Exception:
		Logger.error("SOD task start failed", exc_info=True)
		return '{"retcode":-1,"retmsg":"SOD task start failed"}'

	return '{"retcode":1,"retmsg":"SOD task start"}'

@app.route('/cronjob',methods=['GET'])
def ListCronJob():
	try:
		tasks = cronjob.GetJobs()
		l_jobs = []
		i_index = 1
		for job in tasks:
			d_job = {}
			d_job['Id'] = i_index
			d_job['Fund'] = job.name
			d_job['Next_time'] = job.next_run_time.strftime("%Y-%m-%d %H:%M:%S")
			l_jobs.append(d_job)
			i_index += 1
		if len(l_jobs) == 0:
			return '{"retcode":1,"retmsg":"No job"}'
		else:
			return json.dumps(l_jobs)
	except Exception:
		Logger.error("Failed to import data", exc_info=True)
		return '{"retcode":1,"retmsg":"Job get error"}'

@app.route('/update/transaction',methods=['GET'])
def UpdateTransaction():
	try:
		global treadpool
		if treadpool.AddTaskSet(type='update_transaction',data={'test':1})	== False:
			return '{"retcode":1,"retmsg":"Update task has already started"}'
	except Exception:
		Logger.error("Failed to update transaction", exc_info=True)
		return '{"retcode":-1,"retmsg":"Update task start failed"}'

	return '{"retcode":1,"retmsg":"Update task start"}'

@app.route('/update/transaction/aftertrade',methods=['GET'])
def UpdateTransactionAfterTrade():
	try:
		global treadpool
		if treadpool.AddTaskSet(type='update_transaction_after_trade',data={'test':1})	== False:
			return '{"retcode":1,"retmsg":"Update after trade task has already started"}'
	except Exception:
		Logger.error("Failed to update transaction", exc_info=True)
		return '{"retcode":-1,"retmsg":"Update after trade task start failed"}'

	return '{"retcode":1,"retmsg":"Update after trade task start"}'

@app.route('/resend/transaction',methods=['GET'])
def ResendTransaction():
	try:
		global treadpool
		s_date = request.args.get('date') or ''
		s_act_no =request.args.get('accountno') or ''
		if treadpool.AddTaskSet(type='resend_transaction',data={'date':s_date,'accountno':s_act_no}) == False:
			return '{"retcode":1,"retmsg":"Resend transaction task has already started"}'
	except Exception:
		Logger.error("Failed to resend transaction", exc_info=True)
		return '{"retcode":-1,"retmsg":"Resend transaction task start failed"}'

	return '{"retcode":1,"retmsg":"Resend transaction task start"}'

@app.route('/writeback/transaction',methods=['GET'])
def WriteTransaction():
	try:
		global treadpool
		if treadpool.AddTaskSet(type='writeback_transaction',data={'dummy':1}) == False:
			return '{"retcode":1,"retmsg":"Writeback transaction task has already started"}'
	except Exception:
		Logger.error("Failed to writeback transaction", exc_info=True)
		return '{"retcode":-1,"retmsg":"Writeback transaction task start failed"}'

	return '{"retcode":1,"retmsg":"Writeback transaction task start"}'

@app.route('/sod/hqupdate',methods=['GET'])
def SodFirstHqInit():
	try:
		s_date = request.args.get('date') or ''
		global treadpool
		if treadpool.AddTaskSet(type='sod_hq_update', data={'date': s_date}) == False:
			return '{"retcode":1,"retmsg":"hq update task has already started"}'
	except Exception:
		Logger.error("Failed to hq update", exc_info=True)
		return '{"retcode":-1,"retmsg":"hq update task start failed"}'

	return '{"retcode":1,"retmsg":"hq update task start"}'

@app.route('/sod/hqinit',methods=['GET'])
def SodHqInit():
	try:
		s_date = request.args.get('date') or ''
		global treadpool
		if treadpool.AddTaskSet(type='sod_hq_init', data={'date': s_date}) == False:
			return '{"retcode":1,"retmsg":"hq init task has already started"}'
	except Exception:
		Logger.error("Failed to hq init", exc_info=True)
		return '{"retcode":-1,"retmsg":"hq init task start failed"}'

	return '{"retcode":1,"retmsg":"hq init task start"}'

@app.route('/etnaInfo/getInfo',methods=['POST'])
def GetInfo():
	try:
		#s_data = request.get_data()
		#dic_data = json.loads(s_data.decode("utf-8"))
		return '{"success":true,"code":"0","msg":"","chnMsg":"","data":{"accountId":"45","etnaId":"1029","password":"DBC61356D5F28AFC060DD029BDE9245A"}}'
	except Exception:
		Logger.error("Failed to hq init", exc_info=True)
		return '{"retcode":-1,"retmsg":"hq init task start failed"}'

	return '{"success":false,"code":1,"msg":"hq init task start"}'


