# -*- coding: utf-8 -*-
# !usr/bin/env python
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.base import BaseScheduler
from apscheduler.events import EVENT_JOB_MISSED,EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from comm.log import Logger


class CronJob(object):
	def Listener(self,event):
		if event.exception:
			Logger.exception('job:%s error.', event.job_id)
		else:
			Logger.error('job:%s miss', event.job_id)

	def __init__(self):
		self.job = BackgroundScheduler()
		self.job.add_listener(self.Listener, EVENT_JOB_ERROR | EVENT_JOB_MISSED)


	def AddCronJob(self,func,time):
		d_param = {}
		self.job.add_job(func,"cron",max_instances=5,hour = time.get('Hour') or None,minute = time.get('Minute') or None,second = time.get('Second') or None,
						 args=[d_param])
	def AddIntervalJob(self,func,time):
		d_param = {}
		self.job.add_job(func,"interval",hours = time.get('Hour') or None,seconds = time.get('Second') or None,
						 args=[d_param])

	def Start(self):
		self.job.start()

	def GetJobs(self):
		return self.job.get_jobs()

