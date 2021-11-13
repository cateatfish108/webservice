#!/usr/bin/python3
# -*- coding: utf-8 -*-

import json,requests,cx_Oracle
import paramiko
import csv
import cx_Oracle
import pymysql
import datetime
import xml.dom.minidom as dom
from model.sod import g_dic_sod_cfg
from comm.func import GetDate,ParseSql,TimeRecord
from comm.func import CheckDir
from comm.xml import *
from comm.log import Logger
from model.sod.trans_func import ResetParam
import os,time
os.environ['NLS_LANG'] = ".AL32UTF8"


class Sod_Work(object):
	__config = {}

	def __init__(self):
		pass

	def Work(self, date):
		return date


class Sod_Download(Sod_Work):
	def __init__(self):
		self.__db_type=g_dic_sod_cfg['Database']['SetDatabase'].lower()
		self.__dict_cfg_db = g_dic_sod_cfg.get('Database')
		self.__dic_cfg_dwn = g_dic_sod_cfg.get('SodDownload')
		self.__dic_cfg_apex = g_dic_sod_cfg.get('ApexParam')
		self.__dic_cfg_fmt = g_dic_sod_cfg.get('SodFormat')

	# 获取最近的上一个交易
	def GetLastTradeDay(self, beforeday):
		dbparam = self.__dict_cfg_db['Oracle']['LinkParam']  # 数据库配置
		str_date = GetDate(beforeday)
		str_sql = ("select TRADE_FLAG from quickbroker.tradedate where CALENDAR = %s " % str_date)

		try:
			if self.__db_type=='oracle':
				conn_db = cx_Oracle.connect(dbparam)
			elif self.__db_type=='mysql':
				conn_db = pymysql.connect(host=self.__dict_cfg_db['MySQL']['Host'],port=self.__dict_cfg_db['MySQL']['Port'],
						user=self.__dict_cfg_db['MySQL']['User'],passwd=self.__dict_cfg_db['MySQL']['Passwd'],
						db=self.__dict_cfg_db['MySQL']['DB'],charset=self.__dict_cfg_db['MySQL']['Charset'])
			cur_db = conn_db.cursor()
			cur_db.execute(str_sql)
			rows_db = cur_db.fetchall()
		except Exception:
			Logger.error("Failed to get last trade day", exc_info=True)

		if rows_db==None:
			Logger.error("Failed to get last trade day", exc_info=True)
			return
		elif rows_db[0][0] == 1:
			cur_db.close()
			conn_db.close()
			return str_date
		else:
			if beforeday > 2:
				cur_db.close()
				conn_db.close()
				return False
			else:
				return self.GetLastTradeDay(beforeday + 1)

	# 只能处理单层目录的情况，多层目录只会处理最后一层
	def GetDirFileName(self, dir):
		dic_filenames = ()
		list_csv = []
		list_txt = []
		dic_retdata = {}
		for root, dirs, files in os.walk(dir):
			dic_filenames = files
		for k in dic_filenames:
			if k.endswith('.CSV'):
				list_csv.append(k)
			elif k.endswith('TXT'):
				list_txt.append(k)
		dic_retdata['CSV'] = list_csv
		dic_retdata['TXT'] = list_txt
		return dic_retdata

	# 改变数据表头
	def ChangeHead(self, Header, Rules):
		list_head = []
		for k in Header:
			if k in Rules.keys():
				list_head.append(Rules[k])
			else:
				list_head.append(k)
		return list_head

	# 处理CSV格式的数据，分有无表头两种类型
	# 有表头的，直接转换表头，数据暂时可以不动
	# 无表头的，需要从配置文件中读取表头，然后进行转换后写入新的CSV
	def FormatCSV(self, date):
		str_in_path = self.__dic_cfg_fmt['OriPath'] + date + '/'
		str_cfg_path = self.__dic_cfg_fmt['ConfigPath']
		dic_file_names = self.GetDirFileName(str_in_path)
		for name in dic_file_names['CSV']:
			str_cfg_name = name.split('_')[0] + '.json'
			if os.path.exists(str_cfg_path + str_cfg_name):
				file = open(str_cfg_path + str_cfg_name, 'r+')
				dic_cfg = json.loads(file.read())
				file.close()
				list_line_data = []
				with open(str_in_path + name, 'r') as csvfile:
					reader = csv.reader(csvfile)
					# 有表头的CSV文件 以原始数据的表头为键从json里取值做format数据的表头,json的键应写成原始数据的列名
					if name.split("_")[0] in self.__dic_cfg_fmt['HaveHeadFiles'].split(','):
						dict_rules = dic_cfg['ChangeRules']
						for i, item in enumerate(reader):
							if i == 0:
								list_line_data.append(self.ChangeHead(item, dict_rules))
							else:
								list_line_data.append(item)
					# 无表头CSV文件
					else:
						Head = dic_cfg['Head'].split(",")
						list_line_data.append(Head)
						for item in reader:
							list_line_data.append(item)
					with open(self.__dic_cfg_fmt['DesPath'] + date + '/' + name.split('_')[0] + '.csv', 'w',
							  newline='') as csvfile:
						writer = csv.writer(csvfile)
						writer.writerows(list_line_data)
						Logger.info(name + ':Parse Complete')
			else:
				Logger.info(name + ':find no config file')

	# 处理TXT格式的数据，从配置文件中读取表头以及转换规则，然后转换表头后写入CSV
	def FormatTXT(self, date):
		str_in_path = self.__dic_cfg_fmt['OriPath'] + date + '/'
		str_cfg_path = self.__dic_cfg_fmt['ConfigPath']
		dic_file_names = self.GetDirFileName(str_in_path)
		for name in dic_file_names['TXT']:
			Logger.info(name + ':format start')
			str_cfg_name = name.split('_')[0] + '.json'
			if os.path.exists(str_cfg_path + str_cfg_name):
				file = open(str_cfg_path + str_cfg_name, 'r+')
				dict_cfg = json.loads(file.read())
				file.close()
				Header = dict_cfg['Head'].split(',')
				dict_rules = dict_cfg['ChangeRule']
				list_new_head = self.ChangeHead(Header, dict_rules)
				str_delimiter = dict_cfg['Delimiter']
				list_line_data = []
				list_line_data.append(list_new_head)
				FileData = open(str_in_path + name, 'r+')
				for line in FileData.readlines():
					if line != '':
						list_line_data.append(line.replace("\n", "").split(str_delimiter))
				FileData.close()
				with open(self.__dic_cfg_fmt['DesPath'] + date + '/' + name.split('_')[0] + '.csv', 'w',
						  newline='') as csvfile:
					writer = csv.writer(csvfile)
					writer.writerows(list_line_data)
					Logger.info(name + ':format complete')
			else:
				Logger.info(name + ':find no config file')

	# 工作函数，用于实现多态
	def Work(self, date):
		Logger.info("Start downdload sod files")
		if date == '':
			date = self.GetLastTradeDay(1)  # 获取上一个交易日'

		# 连接APEX的SFTP
		str_pri_path = self.__dic_cfg_apex['PrivateKeyLocation']  # 私钥地址
		CheckDir(self.__dic_cfg_dwn['DesPath'] + date)
		CheckDir(self.__dic_cfg_fmt['DesPath'] + date)

		tran = paramiko.Transport((self.__dic_cfg_apex['IP'], int(self.__dic_cfg_apex['Port'])))  # 创建连接信息
		keys = paramiko.RSAKey.from_private_key_file(str_pri_path)  # 指定私钥文件
		tran.connect(username=self.__dic_cfg_apex['UserName'], pkey=keys)

		sftp = paramiko.SFTPClient.from_transport(tran)  # 连接SFTP服务器
		Logger.info('Apex link success')

		# 通过SFTP下载文件
		for k in self.__dic_cfg_dwn['FileNameAndFileType'].keys():
			str_file_name = ''
			if k in self.__dic_cfg_dwn['ExtendFile'].keys():
				str_file_name = k + '_' + self.__dic_cfg_apex['CorrespondentCode'] + '_' + \
								self.__dic_cfg_dwn['ExtendFile'][k] + '_' + date + '.' + \
								self.__dic_cfg_dwn['FileNameAndFileType'][k]
			else:
				str_file_name = k + '_' + self.__dic_cfg_apex['CorrespondentCode'] + '_' + date + '.' + \
								self.__dic_cfg_dwn['FileNameAndFileType'][k]
			str_in_path = self.__dic_cfg_dwn['OriPath'] + date + '/' + k + '/' + str_file_name
			str_out_path = self.__dic_cfg_dwn['DesPath'] + date + '/' + str_file_name

			try:
				Logger.info(str_file_name + ',start to download')
				sftp.get(str_in_path, str_out_path)
				str_file_size = str(os.path.getsize(str_out_path) / 1024) + 'kb'
				Logger.info(str_file_name + ',download finished,size=' + str_file_size)
			except Exception as e:
				Logger.info('Download:' + str_file_name + ',error. errmsg=' + str(e))

		tran.close()
		Logger.info('Apex link release')
		self.FormatTXT(date)
		self.FormatCSV(date)
		return date


s_prefix_ins = 'insert into quickbroker.'
s_prefix_del = 'delete from quickbroker.'


class Sod_Import(Sod_Work):
	def __init__(self):
		self.__db_type=g_dic_sod_cfg['Database']['SetDatabase'].lower()
		self.__dict_cfg_db = g_dic_sod_cfg['Database']
		self.__dict_cfg_imp = g_dic_sod_cfg['SodImport']

	# 加载CSV文件到map表中
	def LoadCSVFile(self, filepath, filename, dict):
		with open(os.path.join(filepath, filename), 'r') as csvfile:
			read_csv = csv.reader(csvfile)
			int_index = 0

			dict['tableHead'] = []
			dict['tableItem'] = []
			for row in read_csv:
				if int_index == 0:
					dict['tableHead'] = row
				else:
					dict['tableItem'].append(row)
				int_index = int_index + 1

	def ParseSql(self, cursor, sql):
		try:
			# 解析sql语句
			cursor.parse(sql)
		# 捕获SQL异常
		except cx_Oracle.DatabaseError as e:
			Logger.error('Sql execute failed:' + sql + ' reason:' + str(e))
			return False

		return True

	#根据字典中的数据，拼接成sql语句并插入到表中
	def ImportToDB(self, cur, tablename, dict):
		try:
			s_del_list = str.lower(self.__dict_cfg_imp['NotDeleteList'])
			# 删除表的内容
			if str.find(s_del_list, tablename, 0) < 0:
				s_sql_del = s_prefix_del + tablename
				if self.__db_type=='oracle':
					if self.ParseSql(cur, s_sql_del) == False:
						return
				cur.execute(s_sql_del)

			l_head = dict.get('tableHead')
			if l_head == None:
				return

			l_item = dict.get('tableItem')
			if l_item == None or len(l_item) == 0:
				return

			s_head = ''
			s_item = ''
			for head in l_head:
				s_head = s_head + head + ','
			s_head = s_head[:len(s_head) - 1]
			s_head = '(' + s_head + ')'

			i_index = 0
			for item in l_item:
				s_item = ''
				for item_single in item:
					s_item = s_item + '\'' + item_single + '\','

				s_item = s_item[:len(s_item) - 1]
				s_item = '(' + s_item + ')'
				s_sql = s_prefix_ins + tablename + s_head + 'values' + s_item
				if self.__db_type=='oracle':
					if self.ParseSql(cur, s_sql) == False:
						return
				cur.execute(s_sql)
				i_index += 1

				'''遇到大数据时每100条数据提交一次
				if i_index >= 100:
					i_index = 0
					conn.commit()'''

		except Exception as e:
			Logger.error("Failed to create sql"+s_sql+' reason:' + str(e), exc_info=True)

	#工作函数
	def Work(self, date):
		if date == '':
			Logger.error('Trans sod did not get date param')
			return ''

		if self.__db_type=='oracle':
			db_conn = cx_Oracle.connect(self.__dict_cfg_db['Oracle']['LinkParam'])
		elif self.__db_type=='mysql':
			db_conn = pymysql.connect(host=self.__dict_cfg_db['MySQL']['Host'],port=self.__dict_cfg_db['MySQL']['Port'],
					  user=self.__dict_cfg_db['MySQL']['User'],passwd=self.__dict_cfg_db['MySQL']['Passwd'],
					  db=self.__dict_cfg_db['MySQL']['DB'],charset=self.__dict_cfg_db['MySQL']['Charset'])
		db_cur = db_conn.cursor()
		try:
			s_path = self.__dict_cfg_imp['OriPath']
			s_path = os.path.join(s_path, date)
			l_dir = os.listdir(s_path)
			for s_filename in l_dir:
				if str.find(s_filename, '.csv') < 0:
					continue
				d_data = {}
				self.LoadCSVFile(s_path, s_filename, d_data)
				s_table = s_filename[0:str.find(s_filename, '.csv', 0)]
				Logger.info(s_table + ':start to import')
				self.ImportToDB(db_cur, s_table, d_data)
				db_conn.commit()
				Logger.info(s_table + ':import finished')
		except Exception:
			Logger.error("Failed to import data", exc_info=True)

		db_cur.close()
		db_conn.close()


# 每张表的配置类
class tableConvertConfig:
	def __init__(self, table_element):
		self.dest_file = table_element.getAttribute("dest_file").lower()
		if table_element.hasAttribute("key"):
			self.key = table_element.getAttribute("key")

		if table_element.hasAttribute("convert_func_script"):
			self.convert_func_script = table_element.getAttribute("convert_func_script")

		self.main_table_cnt = 0
		self.subtables = []
		self.default = []

		# 记录需要进行字典转换和函数转换的集合
		self.func = {}
		self.dictionary = {}
		self.dictionary_translate = {}

		for subtableElement in table_element.getElementsByTagName("subtable"):
			self.parseSubtable(subtableElement)

		for defaultElement in table_element.getElementsByTagName("default"):
			self.parseDefault(defaultElement)

	def checkValid(self):
		'''
		@brief  : 检查一个表配置是否正确，检查项：
			1：当有子表配置时候是否配有key值
			2：当有转换函数时候是否配置函数脚本路径
			3：主表是否有且只有一个
		'''
		if all([self.subtables, not hasattr(self, 'key')]):
			raise Exception("{}配有多张[subtable]但是未配[key]".format(self.dest_file))

		if all([self.func, not hasattr(self, 'convert_func_script')]):
			raise Exception("{}配有[user_defined_func]但是未配[convert_func_script]".format(self.dest_file))

		if not all([self.main_table_cnt == 1, hasattr(self, 'main_table')]):
			raise Exception("{}没有正确配置唯一的[main_table]".format(self.dest_file))

	def parseSubtable(self, subtableElement):
		'''
		@brief  : 解析subtable标签配置
		@params : subtableElement subtable标签节点
		'''
		subtable = subtableNode(subtableElement)

		if all([subtableElement.hasAttribute("main_table"), subtableElement.getAttribute("main_table") == "true"]):
			self.main_table_cnt += 1
			self.main_table = subtable
		else:
			self.subtables.append(subtable)

		self.func.update(subtable.func)
		self.dictionary.update(subtable.dictionary)
		self.dictionary_translate.update(subtable.dictionary_translate)

	def parseDefault(self, defaultElement):
		'''
		@brief  : 解析default标签配置
		@params : defaultElement default标签节点
		'''
		default = defaultNode(defaultElement)
		self.default.append(default)
		self.func.update(default.func)

	def doPreConvert(self, src_file_path, dictionarys={}):
		'''
		@brief  : 将main_table和subtables根据key合并成一张表，再拼上default字段,最后进行数据字典和函数的转换
		@params : src_file_path 原始csv数据的路径，类型：str
		@params : dictionarys 用于数据字典转换的数据字典集合，类型：dictionary的dict类型
		'''
		begin = time.time()

		# 获取主表的dataframe
		self.main_dataframe = self.main_table.getDataFrame(src_file_path)

		# subtable配置通过key左外连接
		for sub_table in self.subtables:
			sub_dataframe = sub_table.getDataFrame(src_file_path)
			self.main_dataframe = pd.merge(self.main_dataframe, sub_dataframe, on=self.key, how='inner')

		# default值加到列中
		for default in self.default:
			data_default = pd.DataFrame(columns=default.columns_mapping.keys())
			self.main_dataframe = pd.concat([self.main_dataframe, data_default], axis=1)

			default_val = []
			default_val.append(list(default.columns_mapping.values()))
			self.main_dataframe[list(default.columns_mapping.keys())] = default_val[0]

		if self.main_dataframe.empty:
			Logger.info('{} is empty'.format(self.dest_file))
			return

		# 进行列的数据字典转换
		for dest_col, dictionary_name in self.dictionary.items():
			dictionary = dictionarys.get(dictionary_name, None)
			if (dictionary):
				self.main_dataframe[dest_col] = self.main_dataframe[dest_col].map(lambda x: dictionary.get(x))

		# 进行需要位或操作的数据字典转换
		for dest_col, dictionary_translate_node in self.dictionary_translate.items():
			dictionary = dictionarys.get(dictionary_translate_node.dictionary_name)

			def doDictionaryTranslate(x):
				l_value = dictionary_translate_node.default_value
				if type(x) == str:
					for i in x:
						l_value |= int(dictionary.get(i, 0))

				return str(l_value)

			if (dictionary):
				self.main_dataframe[dest_col] = self.main_dataframe[dest_col].map(doDictionaryTranslate)

		# 若由函数转换，则加载python模块并初始化
		if (self.func):
			model_name = []
			model_name.append(self.convert_func_script)
			imp_module = __import__('model.sod.' + self.convert_func_script, globals(), locals(), model_name)
		# initBaseData = getattr(imp_module, "initBaseData")
		# if (callable(initBaseData)):
		# initBaseData(src_file_path, dictionarys)

		Logger.info(self.dest_file + ":start to trans csv file")
		# 进行列的函数转换
		ResetParam()

		for dest_col, func_config in self.func.items():
			# 获取函数配置，，获取函数
			func_name = func_config[0]
			param = func_config[1]
			func = getattr(imp_module, func_name)
			if (callable(func)):
				self.main_dataframe[dest_col] = self.main_dataframe.apply(
					lambda x: func(*[x[i.strip()] for i in param.split(',')]), axis=1)
			if (func_config[0] == "AmericanRemoveSame"):
				self.main_dataframe = self.main_dataframe.drop_duplicates(subset=func_config[1], keep='first')
			if (func_config[0] == "AmericanRemoveNan"):
				self.main_dataframe = self.main_dataframe.dropna(axis=0)
		Logger.info(self.dest_file + ":trans csv file finished")

	def writeToCsv(self, dest_path, date):
		Logger.info(self.dest_file + ":start to write csv file")
		self.main_dataframe.to_csv(os.path.join(dest_path, self.dest_file), encoding='gb18030', quoting=1, index=False)
		Logger.info(self.dest_file + ":write csv file finished")
		return len(self.main_dataframe.index)


class Sod_Trans(Sod_Work):
	def __init__(self):
		self.__dic_cfg_trans = g_dic_sod_cfg['SodTrans']
		self.table_list = [] # 这个列表里表存储tableConvertConfig对象,这些对象在parse里被写入
		self.dictionarys = {}
		self.post_work = {}

	def parse(self, xml_name):
		'''
		@brief  : 解析csv转换的配置xml文件，将配置内容存储，包括字典配置和表转换配置
		@params : xml_name 配置文件全路径，类型：str
		'''
		xmltree = dom.parse(xml_name)
		rootElement = xmltree.documentElement

		# 解析dictionary
		for dicts_element in rootElement.getElementsByTagName("dictionary_list"):
			for dict_element in dicts_element.getElementsByTagName("dictionary"):
				dict_name = dict_element.getAttribute("name")
				dict_val = dictionary(dict_element)
				self.dictionarys[dict_name] = dict_val

		# 解析table
		for table_element in rootElement.getElementsByTagName("table"):
			tbl_convert_config = tableConvertConfig(table_element)
			self.table_list.append(tbl_convert_config)

		for post_work_element in rootElement.getElementsByTagName("post_work"):
			post_work_scrip = post_work_element.getAttribute("post_work_scrip")
			func = post_work_element.getAttribute("func")
			self.post_work[post_work_scrip] = func

	def preCheck(self):
		'''
		@brief  : 配置有效性检查，无效直接抛出异常调用端捕获
		'''
		for tbl_convert_config in self.table_list:
			tbl_convert_config.checkValid()

	def doConvert(self, src_path, des_path, date):
		'''
		@brief  : 依次调用表配置类的预处理和后处理过程，进行csv数据的转换工作
		@params : path 原始csv数据的目录，类型：str
		@params : date 需要转换的业务日期，类型：int
		'''
		total_step = len(self.table_list)
		# print (total_step)
		# PushLog('共有{}张表需要转换。'.format(total_step))
		for i, tbl_convert_config in enumerate(self.table_list):
			curr_step = i + 1
			# ReplyMsg('开始转换第{}张表'.format(curr_step), curr_step, total_step)
			tbl_convert_config.doPreConvert(os.path.join(src_path, str(date)), self.dictionarys)
			row_count = tbl_convert_config.writeToCsv(os.path.join(des_path, str(date)), str(date))

	# ReplyMsg('第{}张表 {} 转换成功,共有{}条数据'.format(curr_step, tbl_convert_config.dest_file.replace('yyyymmdd', str(date)), row_count), curr_step, total_step)

	def doPostWork(self, path, date):
		'''
		@brief  : csv数据转换以后做一些后处理
		@params : path 原始csv数据的目录，类型：str
		@params : date 需要转换的业务日期，类型：int
		'''
		for post_work_scrip, func_name in self.post_work.items():
			imp_module = __import__(post_work_scrip)
			func = getattr(imp_module, func_name)
			if (callable(func)):
				func(path, date)

	def Work(self, date):
		if date == '':
			Logger.error('Trans sod did not get date param')
			return ''

		self.parse(self.__dic_cfg_trans['ConfigPath'])
		self.preCheck()
		CheckDir(os.path.join(self.__dic_cfg_trans['OriPath'], date))
		CheckDir(os.path.join(self.__dic_cfg_trans['DesPath'], date))
		self.doConvert(self.__dic_cfg_trans['OriPath'], self.__dic_cfg_trans['DesPath'], date)

		return date
# self.doPostWork(path, date)

class Sod_Hq_Init():

	def __init__(self):
		self.__db_type = g_dic_sod_cfg['Database']['SetDatabase'].lower()
		self.__dict_cfg_db = g_dic_sod_cfg['Database']
		self.__dict_cfg_hq = g_dic_sod_cfg['SodHqInit']
		self.__market_pub205 = g_dic_sod_cfg['PUB205']
		self.__token = ''
		# 1表示字符，2表示数字
		self.__dict_flt_pub205 = {'SEQ': 2, 'CTIME': 1, 'MTIME': 1, 'RTIME': 1, 'ISVALID': 2, 'SECID_PUB205': 1,
					  'SECCODE_PUB205': 1, 'SECNAME_PUB205': 1, 'F001V_PUB205': 1,
					  'F016V_PUB205': 1, 'F002V_PUB205': 1, 'F003V_PUB205': 1, 'F005V_PUB205': 1, 'F006V_PUB205': 1,
					  'F007D_PUB205': 1, 'F008D_PUB205': 1,
					  'F012V_PUB205': 1, 'F013V_PUB205': 1, 'F014V_PUB205': 1, 'F015V_PUB205': 1, 'F017V_PUB205': 1,
					  'F018V_PUB205': 1,
					  'THSCODE': 1, 'ISIN': 1, 'SEDOL': 1, 'THSCODE_HQ': 1, 'THSMARKET_CODE_HQ': 1, 'F019V_PUB205': 1,
					  'F020N_PUB205': 2, 'F021N_PUB205': 2, 'F022V_PUB205': 1, 'F023V_PUB205': 1, 'F024V_PUB205': 1,
					  'F025V_PUB205': 1, 'F026V_PUB205': 1,
					  'F027V_PUB205': 1, 'F028V_PUB205': 1, 'F029V_PUB205': 1, 'F030D_PUB205': 1, 'F031D_PUB205': 1,
					  'F032V_PUB205': 1, 'F033V_PUB205': 1,
					  'F034N_PUB205': 2, 'CUSIP_PUB205': 1, 'ISCHECK': 2
					  }
		self.__dict_flt_trade225 = {'SEQ': 2,'CTIME':1,'MTIME':1,'RTIME':1,'isvalid':2,'SECCODE_TRADE225':1,
									'SECNAME_TRADE225':1,'TRADEDATE_TRADE225':1,'F001V_TRADE225':1,'F002V_TRADE225':1,
									'F003N_TRADE225':2,'F004N_TRADE225':2,'F005N_TRADE225':2,'F006N_TRADE225':2,
									'F007N_TRADE225':2,'F008N_TRADE225':2,'ZQID_TRADE225':1,'F009N_TRADE225':2,
									'ISCHECK':2}


	def getToken(self):
		try:
			str_req_url = self.__dict_cfg_hq['BaseURL'] + self.__dict_cfg_hq['TokenURL'] + 'appkey=' + self.__dict_cfg_hq['appkey'] + '&appSecret=' + self.__dict_cfg_hq['appSecret']
			headers = {'Content-Type': 'application/json'}
			response = requests.get(str_req_url,headers=headers)
			dict_res = json.loads(response.text)
			if dict_res.get('flag') == 0:
				dict_data = dict_res['data']
				self.__token = dict_data.get('access_token')
				if self.__token != '' and self.__token != None:
					return True
				else:
					Logger.error('get b2b token failed,no token exist')
					return False
			else:
				return False
		except:
			Logger.error("get b2b token failed", exc_info=True)
			return False

	
	def importDataFromBase(self,conn,sel_t,ins_t,dict_filter,i_wait):
		str_base_url = self.__dict_cfg_hq['BaseURL']
		str_qry_url = self.__dict_cfg_hq['QueryURL']
		str_req_url = str_base_url + str_qry_url
		dic_data = {'dbtype':'pgsql','UserName':self.__dict_cfg_hq['UserName'],'PWD':self.__dict_cfg_hq['PassWord']}

		lst_data = ['1']
		i_per_req = self.__dict_cfg_hq['ReqPerCnt']
		i_index = 0
		cur = conn.cursor()
		str_token = 'Bearer' + self.__token
		ctn = 0
		try:
			while len(lst_data) > 0:
				str_sql = sel_t % (i_index, i_per_req)
				dic_data['AppSQL'] = str_sql
				Logger.info('select sql:' + str_sql)
				headers = {'open-authorization': str_token}
				response = requests.post(str_req_url, data=dic_data,headers=headers)

				dict_res = json.loads(response.text)
				if dict_res.get('status_code') == '100001':
					if dict_res.get('Data'):
						lst_data = dict_res.get('Data')
						for dict_data in lst_data:
							str_insert_key = ''
							str_insert_value = ''
							list_insert_key=[]
							list_insert_value=[]
							i_index = int(dict_data.get('SEQ'))
							for k in dict_data:
								if dict_filter.get(k) != None: #根据过滤配置过滤掉不需要的表项，以防数据库没有更新导致插入失败
									str_insert_key = str_insert_key + k + ','
									str_value = str(dict_data.get(k))
									str_value = str_value.replace("\'", " ")  # 将单引号替换为空格
									if dict_data.get(k) == None:
										str_value = '0' if dict_filter.get(k)==2 else '' # None值的数字置零
									str_insert_value = str_insert_value + "\'" + str_value + "\'" + ','
									list_insert_key.append(k)
									list_insert_value.append("\'" + str_value + "\'")
							str_sql_insert_other=""
							for i in range(len(list_insert_key)):
								tmp=list_insert_key[i]+'='+list_insert_value[i]+','
								str_sql_insert_other=str_sql_insert_other+tmp
							str_sql_insert = ins_t % (str_insert_key[0:-1], str_insert_value[0:-1], str_sql_insert_other[0:-1])
							# str_sql_insert=str_sql_insert+str_sql_insert_other[0:-1]
							if self.__db_type=='oracle':
								if ParseSql(cur, str_sql_insert) == False:
									break
							cur.execute(str_sql_insert)
						conn.commit()
					else:
						Logger.error('No Data field')
						break
					ctn = ctn + len(lst_data)
					Logger.info('insert cnt:' + str(ctn))
				else:
					str_err = 'status_code:%s,flag code:%s,error_msg:%s'%(dict_res.get('status_code'),dict_res.get('flag'),dict_res.get('status_msg') or dict_res.get('msg'))
					Logger.error(str_err)
					break
				time.sleep(i_wait)
		except:
			Logger.error("Hq init failed", exc_info=True)


	def Work(self,date):

		#先去获取token
		if self.getToken() == False:
			return
		if self.__db_type=='oracle':
			conn = cx_Oracle.connect(self.__dict_cfg_db['Oracle']['LinkParam'])
		elif self.__db_type=='mysql':
			conn = pymysql.connect(host=self.__dict_cfg_db['MySQL']['Host'],port=self.__dict_cfg_db['MySQL']['Port'],
					user=self.__dict_cfg_db['MySQL']['User'],passwd=self.__dict_cfg_db['MySQL']['Passwd'],
					db=self.__dict_cfg_db['MySQL']['DB'],charset=self.__dict_cfg_db['MySQL']['Charset'])
		cur = conn.cursor()
		
		if date:
			date_format=time.strftime("%Y-%m-%d", time.strptime(date,"%Y%m%d"))
		else:
			date=time.strftime("%Y%m%d", time.localtime())
			date_format=time.strftime("%Y-%m-%d", time.localtime())
			str_sel_date_t = "select max(calendar) from quickbroker.tradedate where trade_flag = 1 and calendar < %s"%date
			result = cur.execute(str_sel_date_t)
			if self.__db_type=='oracle':
				t = result.fetchone()
			elif self.__db_type=='mysql':
				t = cur.fetchone()
			if t:
				str_time = str(t[0])
				date_format = str_time[0:4] + '-' + str_time[4:6] + '-' + str_time[6:8]
			else:
				Logger.error('get trade date error')
				return
		
		#删除原有的数据
		cur.execute('delete from quickbroker.pub205')
		conn.commit()

		#导入行情基础数据
		str_sel_t = "select * from PUB205 where F005V_PUB205 in " + self.__market_pub205 + " and seq > %d order by SEQ limit %d"
		str_ins_t = "insert into quickbroker.pub205(%s) values(%s) on duplicate key update %s"
		self.importDataFromBase(conn,str_sel_t,str_ins_t,self.__dict_flt_pub205,10)


	def updateWork(self,date):

		#先去获取token
		if self.getToken() == False:
			return
		if self.__db_type=='oracle':
			conn = cx_Oracle.connect(self.__dict_cfg_db['Oracle']['LinkParam'])
		elif self.__db_type=='mysql':
			conn = pymysql.connect(host=self.__dict_cfg_db['MySQL']['Host'],port=self.__dict_cfg_db['MySQL']['Port'],
					user=self.__dict_cfg_db['MySQL']['User'],passwd=self.__dict_cfg_db['MySQL']['Passwd'],
					db=self.__dict_cfg_db['MySQL']['DB'],charset=self.__dict_cfg_db['MySQL']['Charset'])
		cur = conn.cursor()

		if date:
			date_format=time.strftime("%Y-%m-%d", time.strptime(date,"%Y%m%d"))
		else:
			date=time.strftime("%Y%m%d", time.localtime())
			date_format=time.strftime("%Y-%m-%d", time.localtime())
			str_sel_date_t = "select max(calendar) from quickbroker.tradedate where trade_flag = 1 and calendar < %s"%date
			result = cur.execute(str_sel_date_t)
			if self.__db_type=='oracle':
				t = result.fetchone()
			elif self.__db_type=='mysql':
				t = cur.fetchone()
			if t:
				str_time = str(t[0])
				date_format = str_time[0:4] + '-' + str_time[4:6] + '-' + str_time[6:8]
			else:
				Logger.error('get trade date error')
				return

		#删除原有的数据
		cur.execute('delete from quickbroker.trade225')
		conn.commit()

		#导入行情基础数据
		str_sel_t = "select * from PUB205 where F005V_PUB205 in " + self.__market_pub205 + " and mtime>=\'" + date_format + "\' and seq > %d order by SEQ limit %d"
		str_ins_t = "insert into quickbroker.pub205(%s) values(%s) on duplicate key update %s"
		self.importDataFromBase(conn,str_sel_t,str_ins_t,self.__dict_flt_pub205,10)

		str_sel_t = "select * from trade225 where tradedate_trade225 = \'" + date_format + "\' and isvalid = 1 and seq > %d order by SEQ limit %d"
		str_ins_t = "insert into quickbroker.trade225(%s) values(%s) on duplicate key update %s"
		self.importDataFromBase(conn,str_sel_t, str_ins_t,self.__dict_flt_trade225,10)

@TimeRecord
def WorkFlow_SodDownload(data):
	sod_download = Sod_Download()
	sod_download.Work(data.get('date'))

@TimeRecord
def WorkFlow_SodTrans(data):
	sod_trans = Sod_Trans()
	sod_trans.Work(data.get('date'))

@TimeRecord
def WorkFlow_SodImport(data):
	sod_import = Sod_Import()
	sod_import.Work(data.get('date'))

@TimeRecord
def WorkFlow_SodTask(data):
	s_date = data.get('date') or ''
	l_worker = []
	l_worker.append(Sod_Download())
	l_worker.append(Sod_Trans())
	l_worker.append(Sod_Import())
	for w in l_worker:
		s_date = w.Work(s_date)

@TimeRecord
def WorkFlow_SodHqInit(data):
	sod_hq_init = Sod_Hq_Init()
	sod_hq_init.Work(data.get('date'))

@TimeRecord
def WorkFlow_SodHqUpdate(data):
	sod_hq_init = Sod_Hq_Init()
	sod_hq_init.updateWork(data.get('date'))