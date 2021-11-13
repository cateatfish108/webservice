# -*- coding:utf-8 -*-
import pandas as pd
import os,time
from comm.log import Logger
# 需要位或操作的数据字典转换配置节点
class dictionaryTranslateNode:
	def __init__(self, dictionary_translate_element):
		self.dictionary_name = dictionary_translate_element.getAttribute("dictionary")
		self.default_value = int(dictionary_translate_element.getAttribute("default_value"))


# 子表的配置标签类
class subtableNode:
	def __init__(self, subtable_element):
		self.columns_mapping = {}
		# 筛选的列名和值对应
		self.filter = {}
		self.dictionary = {}
		self.dictionary_translate = {}
		self.func = {}
		self.copy = {}
		self.src_file = subtable_element.getAttribute("src_file")

		for mapping_element in subtable_element.getElementsByTagName("column_mapping"):
			src_col = mapping_element.getAttribute("src_col")

			# 考虑如果有只筛选不做转换的列
			if mapping_element.hasAttribute("dest_col"):
				dest_col = mapping_element.getAttribute("dest_col")

				# 对于src_col对应多个dest_col的情况，使用pandas的copy操作
				dest_col_old = self.columns_mapping.get(src_col, None)
				if dest_col_old:
					copys = self.copy.get(dest_col_old, [])
					copys.append(dest_col)
					self.copy[dest_col_old] = copys
				else:
					self.columns_mapping[src_col] = dest_col

			if mapping_element.hasAttribute("filter_value"):
				filter_value = mapping_element.getAttribute("filter_value")
				self.filter[src_col] = filter_value

			# 解析普通数据字典转换的配置
			if mapping_element.hasAttribute("dictionary"):
				self.dictionary[dest_col] = mapping_element.getAttribute("dictionary")

			# 解析需要位或操作的数据字典转换配置
			if all([mapping_element.hasAttribute("need_translate"),
					mapping_element.getAttribute("need_translate") == "true"]):
				for dictionary_translate_element in mapping_element.getElementsByTagName("translate_config"):
					dictionary_translate_node = dictionaryTranslateNode(dictionary_translate_element)
					self.dictionary_translate[dest_col] = dictionary_translate_node

				# 用户自定义
			if all([mapping_element.hasAttribute("user_defined_func"),
					mapping_element.hasAttribute("user_defined_func_param")]):
				func = []
				func_name = mapping_element.getAttribute("user_defined_func")
				param = mapping_element.getAttribute("user_defined_func_param")
				func.append(func_name)
				func.append(param)
				self.func[dest_col] = func

			# 解析函数转换配置
			if all([mapping_element.hasAttribute("convert_func"), mapping_element.hasAttribute("convert_func_param")]):
				func = []
				func_name = mapping_element.getAttribute("convert_func")
				param = mapping_element.getAttribute("convert_func_param")
				func.append(func_name)
				func.append(param)
				self.func[dest_col] = func

	def getDataFrame(self, src_file_path):
		'''
		@brief  : 将src_file_path目录下满足要求的文件合并成一个dataframe,根据filter进行字段筛选，再根据columns_mapping进行列名转换
		@params : src_file_path 原始csv文件目录，类型：str
		'''

		# 获得满足src_file配置文件全路径集合
		match_files = [os.path.join(src_file_path, x) for x in os.listdir(src_file_path) if self.matchSourceFile(x)]

		# 转换的字段和筛选的字段并集作为需要读取的字段
		all_columns = set(self.columns_mapping.keys()) | set(self.filter.keys())

		data_frams = []
		for file in match_files:
			data_fram = pd.read_csv(file, encoding='gb18030', usecols=all_columns, dtype=str)
			data_frams.append(data_fram)

		if not data_frams:
			raise FileNotFoundError('文件{}不存在'.format(self.src_file))

		# 合并所有dataframe
		dataframe = pd.concat(data_frams, axis=0)

		# 字段筛选
		for src_col, filter_value in self.filter.items():
			# filter_value可配多个值，用","分开
			dataframe = dataframe[dataframe[src_col].isin(filter_value.split(','))]

		# 产生最终需要的列
		dataframe = dataframe[list(self.columns_mapping.keys())]
		# 列名转换
		dataframe.rename(columns=self.columns_mapping, inplace=True)
		# 对一些列的拷贝
		for old_col, new_col in self.copy.items():
			for col in new_col:
				dataframe.insert(0, col, dataframe[old_col])

		return dataframe

	def matchSourceFile(self, file_name):
		'''
		@brief  : 判断文件名是否是csv原始文件
		@params : file_name 需要判断的文件名，类型：str
		'''
		pure_file_name = file_name.split('_')[0]
		pure_src_file = self.src_file.split('_')[0]
		return pure_src_file.upper() == pure_file_name.upper()


# 表默认值标签类
class defaultNode:
	def __init__(self, default_element):
		self.columns_mapping = {}
		self.func = {}

		for mapping_element in default_element.getElementsByTagName("column_mapping"):
			dest_col = mapping_element.getAttribute("dest_col")
			default_value = mapping_element.getAttribute("default_value")
			self.columns_mapping[dest_col] = default_value

			if all([mapping_element.hasAttribute("convert_func"), mapping_element.hasAttribute("convert_func_param")]):
				func = []
				func_name = mapping_element.getAttribute("convert_func")
				param = mapping_element.getAttribute("convert_func_param")
				func.append(func_name)
				func.append(param)
				self.func[dest_col] = func




# 单个数据字典配置类
class dictionary:
	def __init__(self, element):
		self.dict_map = {}
		for convert in element.getElementsByTagName("convert"):
			self.dict_map[convert.getAttribute("peer_id")] = convert.getAttribute("my_id")

	def get(self, key, default=None):
		if default != None:
			return self.dict_map.get(key, default)
		return self.dict_map.get(key, key)