#!/usr/bin/python3
#  -*- coding: utf-8 -*-

import datetime
import pytz
import os, time
import cx_Oracle
from comm.log import Logger
from dateutil import tz


# 获取前befordays的日期 YYYMMDD格式
def GetDate(beforedays):
	today = datetime.datetime.now(pytz.timezone('America/New_York'))
	offset = datetime.timedelta(days=-beforedays)
	re_date = (today + offset).strftime("%Y%m%d")
	return re_date


# 检查文件目录是否存在，如果不存在则新建
def CheckDir(dir):
	if not os.path.exists(dir):
		os.makedirs(dir)


# 时间格式标准化改为ISO-8601标准
def TimeFormartToISO8601(dateIn, timeIn):
	tz_eastern = tz.gettz('US/Eastern')
	di = datetime.datetime(int(dateIn[0:4]),int(dateIn[4:6]),
						   int(dateIn[6:]),int(timeIn[0:2]),int(timeIn[2:4]),int(timeIn[4:6]),int(timeIn[6:])*1000,tzinfo = tz_eastern)
	ret = di.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + di.strftime('%z')
	return ret


# 解析SQL语句是否正确
def ParseSql(cursor, sql):
	try:
		# 解析sql语句
		cursor.parse(sql)
	# 捕获SQL异常
	except cx_Oracle.DatabaseError as e:
		Logger.error(e)  # ORA-00923: 未找到要求的 FROM 关键字
		return False

	return True

#计时装饰器
def TimeRecord(func):
    def deco(*args, **kwargs):
        Logger.info("function：{_funcname_} start to run：".format(_funcname_=func.__name__))
        start_time = datetime.datetime.now()
        res = func(*args, **kwargs)
        end_time = datetime.datetime.now()
        Logger.info("function：{_funcname_} cost {_second_}.{_microsecond_}：".format(_funcname_=func.__name__,
                                             _second_=(end_time - start_time).seconds,_microsecond_=(end_time - start_time).microseconds))
        return res
    return deco
