# -*- coding: utf-8 -*-
# !usr/bin/env python
import threading,traceback
from comm.log import Logger
from queue import Queue

class TreadConsumer(threading.Thread):
	"""
	Consumer thread 消费线程，感觉来源于COOKBOOK
	"""
	def __init__(self, t_name, center):
		threading.Thread.__init__(self, name=t_name)
		self.center = center
		self.name = t_name
		self.thread_stop = False

	def Stop(self):
		self.thread_stop = True

	def run(self):
		Logger.info('thread:' + self.name + ' start to work')
		while not self.thread_stop:
			s_type,f_task, d_data = self.center.GetTask()
			if f_task and d_data:
				try:
					f_task(d_data)
				except Exception:
					Logger.error(traceback.format_exc())
				finally:
					#任务完成设置状态为空闲
					self.center.UpdateTaskStatus(s_type,0)
		Logger.info('thread:' + self.name + ' stop working')

class TreadPool():
	def __init__(self):
		self.q_task = Queue()
		self.i_threadnum = 1
		self.l_thread = []
		self.d_task_map = {}
		self.d_task_status = {}	#0-空闲 1-繁忙
		self.lock = threading.Lock()

	#添加任务集，当同类任务已经在处理时，则不再添加
	def AddTaskSet(self, *, type, data):
		# 如果任务正在处理中，则添加失败
		if self.d_task_status.get(type) == 1:
			return False
		else:
			self.lock.acquire()
			try:
				self.d_task_status[type] = 1
				d_task = {}
				d_task['type'] = type
				d_task['data'] = data
				self.q_task.put(d_task)
			finally:
				self.lock.release()

	#添加任务，不管任务类型是否重复
	def AddTask(self, *, type, data):
		self.lock.acquire()
		try:
			self.d_task_status[type] = 1
			d_task = {}
			d_task['type'] = type
			d_task['data'] = data
			self.q_task.put(d_task)
		finally:
			self.lock.release()

	def Init(self,num,task):
		self.i_threadnum = num
		self.d_task_map = task
		for i in range(self.i_threadnum):
			t = TreadConsumer('thread-'+str(i),self)
			self.l_thread.append(t)

	def Start(self):
		for t in self.l_thread:
			t.start()

	def Stop(self):
		for t in self.l_thread:
			t.stop()

	#更新任务的状态
	def UpdateTaskStatus(self,type,status):
		self.lock.acquire()
		try:
			self.d_task_status[type] = status
		finally:
			self.lock.release()

	#获取任务的工作状态
	def GetTaskStatus(self,type):
		return self.d_task_status.get(type)

	def GetTask(self):
		d_task = self.q_task.get()
		if d_task.get('type'):
			ins_task = self.d_task_map.get(d_task.get('type'))
			self.q_task.task_done()
			return d_task.get('type'),ins_task,d_task.get('data')

		return None,None,None




