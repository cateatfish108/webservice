# -*- coding: utf-8 -*-
# !usr/bin/env python

from __future__ import unicode_literals
from comm.func import TimeFormartToISO8601,ParseSql,TimeRecord
from comm.log import Logger
from model.update import g_dic_update_cfg
import requests,datetime,uuid,json,cx_Oracle
from dateutil import tz


sqlSearch = '''SELECT b.CLIENT_ID,b.ENTRUST_TYPE,e.BUSINESS_AMOUNT,e.BUSINESS_PRICE,b.CURRENCY_CODE,b.BUSINESS_TIME,b.DESTINATION,
b.ENTRUST_NO,b.STOCK_CODE,b.RTN_SERIALNO,b.BUSIN_DATE,b.OFFSET_FLAG,b.NODE_ID,e.FROZEN_COMMISION,e.REPORT_RESULT
FROM (SELECT A.*, rownum r FROM (SELECT * FROM quickbroker.business order by RTN_SERIALNO) A WHERE rownum < %d) b left join quickbroker.entrust e 
on e.entrust_no = b.entrust_no WHERE r >= %d'''


sqlSearchFailTransaction = '''SELECT CLIENT_ID,ENTRUST_TYPE,BUSINESS_AMOUNT,BUSINESS_PRICE,CURRENCY_CODE,BUSINESS_TIME,DESTINATION,
ENTRUST_NO,STOCK_CODE,RTN_SERIALNO,BUSIN_DATE,OFFSET_FLAG,NODE_ID,COMMISSION,EXTERNAL_ID FROM quickbroker.business_braggart'''

sqlSearchExist = '''SELECT * FROM QUICKBROKER.BUSINESS_BRAGGART WHERE BUSIN_DATE = %d and ENTRUST_NO = %d'''

sqlSearchAccountType = '''SELECT CLIENT_ID,ACCOUNT_TYPE FROM QUICKBROKER.FUNDACCOUNT'''

sqlIndex = '''SELECT VALUE FROM quickbroker.sysconf where SECTION_NAME='Braggart' and KEY_NAME='UpdateIndex' '''

sqlIndexAfterTrade = '''SELECT VALUE FROM quickbroker.sysconf where SECTION_NAME='Braggart' and KEY_NAME='UpdateIndexAfterTrade' '''

sqlIndexUpdate = "UPDATE quickbroker.sysconf SET VALUE = %d WHERE SECTION_NAME='Braggart' and KEY_NAME='UpdateIndex'"

sqlIndexUpdateAfterTrade = "UPDATE quickbroker.sysconf SET VALUE = %d WHERE SECTION_NAME='Braggart' and KEY_NAME='UpdateIndexAfterTrade'"

sqlHWMUpdate = '''UPDATE quickbroker.sysconf SET VALUE = %d WHERE SECTION_NAME='Braggart' and KEY_NAME='HighWaterMark' '''

sqlUpdateTransaction = "UPDATE QUICKBROKER.BUSINESS_BRAGGART SET RTN_STATUS = %d,REMARK = '%s',ID = '%s',UPDATE_DATE = %d,UPDATE_TIME = '%s'" \
					   " WHERE EXTERNAL_ID = '%s' "

sqlInsert = '''insert into quickbroker.business_braggart(EXTERNAL_ID,RTN_SERIALNO,BUSIN_DATE,ID,RTN_STATUS,CLIENT_ID,
            ENTRUST_TYPE,BUSINESS_AMOUNT,BUSINESS_PRICE,CURRENCY_CODE,BUSINESS_TIME,DESTINATION,ENTRUST_NO,
            STOCK_CODE,OFFSET_FLAG,REMARK,COMMISSION,NODE_ID,UPDATE_DATE,UPDATE_TIME) values('%s',%d,%d,'%s',%d,'%s',%d,%d,%.3f,'%s','%s','%s',%d,'%s',%d,'%s',%.3f,%d,%d,'%s')'''

sqlTradeDate = '''select calendar,trade_flag from quickbroker.tradedate'''


sqlHighwatermark = '''SELECT VALUE FROM quickbroker.sysconf where SECTION_NAME='Braggart' and KEY_NAME='HighWaterMark' '''



ExecutionTemplate = """{
            "version": 1,
            "externalId": "%s",
            "transaction": {
                "type": "EXECUTION",
                "accountNumber": "%s",
                "accountType": "%s",
                "side": %s,
                "quantity": %d,
                "price": %.3f,
                "currency": "%s",
                "transactionDateTime": "%s",
                "brokerCapacity": "AGENT",
                "route": {
                    "type":"DMA"
                },
                "exchange":"%s",
                "orderId":%d
            },
            "instrument": {
                "type": "EQUITY",
                "instrumentId": {
                    "type": "TICKER_SYMBOL",
                    "id": "%s"
                }
            },
            "charges":{
                "commissionAmount": %.3f,
                "fees": [{
                    "type": "CLIENT_CLEARING",
                    "amount": "0.00"
                }]
            }
        }"""

ExecutionTemplateSettlement = """{
            "version": 1,
            "externalId": "%s",
            "transaction": {
                "type": "EXECUTION",
                "accountNumber": "%s",
                "accountType": "%s",
                "side": %s,
                "quantity": %d,
                "price": %.3f,
                "currency": "%s",
                "transactionDateTime": "%s",
                "brokerCapacity": "AGENT",
                "route": {
                    "type":"DMA"
                },
                "exchange":"%s",
                "orderId":%d
            },
            "instrument": {
                "type": "EQUITY",
                "instrumentId": {
                    "type": "TICKER_SYMBOL",
                    "id": "%s"
                }
            },
            "charges":{
                "commissionAmount": %.3f,
                "fees": [{
                    "type": "CLIENT_CLEARING",
                    "amount": "0.00"
                }]
            },
            "settlementInstructions":{
				"settlementDate":"%s"
			}
        }"""

class Update_Work(object):
	dic_act_type = {}
	dic_trade_date = {} #交易日历缓存
	i_up_cnt = 0 #上传次数
	i_up_cnt_date = 0 #上传次数
	def __init__(self):
		self.__s_base_url = g_dic_update_cfg.get('Base_URL')
		self.__s_db = g_dic_update_cfg['Dbparam']
		self.__i_send_num = g_dic_update_cfg['CountOneTime']
		self.__i_index = 1

	#将请求数据封装打包
	def PackTransactions(self,oneTrade,listUpdate,dictParam,settlementDate = False):
		accountNo = oneTrade[0]
		accountTypeT = Update_Work.dic_act_type.get(accountNo)
		accountType = ''
		if accountTypeT == 1:
			accountType = 'CASH'
		elif accountTypeT == 2:
			accountType = 'MARGIN'
		directionDic = {}
		if oneTrade[1] == 1001:
			directionDic['type'] = 'BUY'
		elif oneTrade[1] == 1002:
			directionDic['type'] = 'SELL'
		if oneTrade[1] == 1002 and oneTrade[11] == 1:  # 卖空开仓
			directionDic['shortType'] = 'SHORT'
		elif oneTrade[1] == 1001 and oneTrade[11] == 2:  # 买入填仓
			directionDic['shortType'] = 'COVER'

		direction = json.dumps(directionDic)
		quantity = abs(oneTrade[2])

		price = oneTrade[3]
		currency = oneTrade[4]
		tradeTime = TimeFormartToISO8601(str(oneTrade[10]), str(oneTrade[5]).rstrip())
		exchange = 'AMEX'
		orderId = oneTrade[7]
		stock = oneTrade[8].rstrip()
		externalId = uuid.uuid1()
		commission = oneTrade[13]

		if settlementDate == True: #重传的数据中第14位是扩展id，而不是订单状态
			s_date = str(oneTrade[10])
			d1 = datetime.datetime(int(s_date[0:4]),int(s_date[4:6]),int(s_date[6:8]))
			s1 = d1 + datetime.timedelta(days=2)

			while(1):
				i_s1 = int(s1.strftime('%Y%m%d'))
				if Update_Work.dic_trade_date.get(i_s1) == 1 or Update_Work.dic_trade_date.get(i_s1) == None:
					break
				else:
					s1 = s1 + datetime.timedelta(days=1)

			s_settle_date = s1.strftime('%Y-%m-%d')
			externalId = oneTrade[14]
			template = ExecutionTemplateSettlement % (
			externalId, accountNo, accountType, direction, quantity, price, currency, tradeTime, exchange, orderId, stock,commission,s_settle_date)
		else:
			template = ExecutionTemplate % (
				externalId, accountNo, accountType, direction, quantity, price, currency, tradeTime, exchange, orderId,
				stock, commission)

		# 将上传的数据做一份记录，用于判断成交是否上传成功，如果失败则需要重传
		dictUpdate = {}
		dictUpdate['ID'] = ''
		dictUpdate['EXTERNAL_ID'] = str(externalId)
		dictUpdate['RTN_SERIALNO'] = oneTrade[9]
		dictUpdate['BUSIN_DATE'] = oneTrade[10]
		dictUpdate['RTN_STATUS'] = 0
		dictUpdate['CLIENT_ID'] = oneTrade[0]
		dictUpdate['ENTRUST_TYPE'] = oneTrade[1]
		dictUpdate['BUSINESS_AMOUNT'] = oneTrade[2]
		dictUpdate['BUSINESS_PRICE'] = oneTrade[3]
		dictUpdate['CURRENCY_CODE'] = oneTrade[4]
		dictUpdate['BUSINESS_TIME'] = oneTrade[5]
		dictUpdate['DESTINATION'] = oneTrade[6]
		dictUpdate['ENTRUST_NO'] = oneTrade[7]
		dictUpdate['STOCK_CODE'] = oneTrade[8].rstrip()
		dictUpdate['OFFSET_FLAG'] = oneTrade[11]
		dictUpdate['NODE_ID'] = oneTrade[12]
		dictUpdate['COMMISSION'] = oneTrade[13]
		dictUpdate['REMARK'] = ''
		listUpdate.append(dictUpdate)
		dictParam['TransactionList'] += template + ','
		dictParam['updateCount'] += 1

	#处理上传的返回结果，根据返回结果做记录
	def HandleTransactionReturn(self,response,listUpdate):
		try:
			responseDict = json.loads(response)
			# 上传成功，则将每一笔成交的id做记录，并更新上传状态
			if responseDict.get('retCode') == 0:
				for updateDict in listUpdate:
					for retList in json.loads(responseDict['retMsg']):
						if retList['externalId'] == updateDict['EXTERNAL_ID'] and updateDict['RTN_STATUS'] != 1:
							if retList['status'] == 'ERROR':
								updateDict['RTN_STATUS'] = 2  # 设置状态为提交失败
								errorDetailsDict = json.loads(retList['errorDetails'])
								str1 = json.dumps(errorDetailsDict['errorDescription'])
								str1 = str1.replace("\'", " ")
								updateDict['REMARK'] = str1
							else:
								updateDict['RTN_STATUS'] = 1  # 设置状态为已成功提交
								updateDict['ID'] = retList['id']
								updateDict['REMARK'] = 'update success'
							break
			else:
				for errorList in json.loads(responseDict.get('retMsg')):
					# 标记发生错误的成交
					for updateDict in listUpdate:
						if 'externalId' in errorList and errorList[0]['externalId'] == updateDict['EXTERNAL_ID'] and updateDict[
							'RTN_STATUS'] != 2:
							errorDetailsDict = json.loads(errorList['errorDetails'])
							str1 = json.dumps(errorDetailsDict['errorList'])
							str1 = str1.replace("\'", " ")
							updateDict['REMARK'] = str1
							updateDict['ID'] = errorList['id']
							updateDict['RTN_STATUS'] = 2
							break
						else:
							updateDict['REMARK'] = 'no match error info'
							updateDict['RTN_STATUS'] = 2
					# 标记和发生错误的成交一起提交的正常成交
					for updateDict in listUpdate:
						if updateDict['RTN_STATUS'] != 2:
							updateDict['RTN_STATUS'] = 3
		except Exception:
			Logger.error("Pack transaction return failed", exc_info=True)
			return False

		return True

	# 处理ALE的返回结果，根据返回结果做回写处理
	def HandleAleReturn(self, cur,response):
		i_HWM = 0
		try:
			responseDict = json.loads(response)
			# 上传成功，则将每一笔成交的id做记录，并更新上传状态
			if responseDict.get('retCode') == 0:
				for retList in json.loads(responseDict.get('retMsg')):
					i_HWM = retList.get('id')
					dic_payload = json.loads(retList.get('payload'))
					d_status = 1
					s_id = dic_payload.get('id')
					s_exId = dic_payload.get('externalId')
					s_remark = ''
					b_update = False
					if dic_payload.get('status') == 'ERROR' or dic_payload.get('status') == 'INVALID':
						b_update = True
						d_status = 4
						s_remark = dic_payload.get('message')

					if b_update == True:
						d1 = datetime.datetime.now(tz=tz.gettz('US/Eastern'))
						sqlUpdateTransactionT = sqlUpdateTransaction + ' AND RTN_STATUS = 1'
						sqlUpdateTransactionT = sqlUpdateTransactionT % (
							d_status, s_remark, s_id, int(d1.strftime('%Y%m%d')), d1.strftime('%H%M%S%f')[:-3], s_exId)
						if ParseSql(cur, sqlUpdateTransactionT) == False:
							return -1
						cur.execute(sqlUpdateTransactionT)
			else:
				Logger.error("Send ale request get error,error message:%s"%(responseDict['retMsg']))
				return -1
		except Exception:
			Logger.error("Pack transaction return failed", exc_info=True)
			return -1

		return i_HWM

	#重传失败的成交数据
	def ReTransaction(self,AccountNo,Date):
		global sqlSearchFailTransaction
		conn = cx_Oracle.connect(self.__s_db)
		sqlSearchFailTransactionT = ''
		if AccountNo == None:
			sqlSearchFailTransactionT = sqlSearchFailTransaction + " WHERE RTN_STATUS <> 1"
		else:
			sqlSearchFailTransactionT = sqlSearchFailTransaction + " WHERE CLIENT_ID='%s' and (RTN_STATUS <>1 OR RTN_STATUS <> 4) "%AccountNo
		while(1):
			cur = conn.cursor()
			# 从sysconf表中获取上次读取的成交序列号，从上次序列号开始重新读取
			if ParseSql(cur, sqlSearchFailTransactionT) == False:
				break
			cur.execute(sqlSearchFailTransactionT)
			listUpdate = []
			dictParam = {
				'TransactionList': '',
				'updateCount': 0,
			}

			TransactionCache = []
			# 将成交数据存入缓存
			for row in cur:
				TransactionCache.append(row)

			#缓存交易日历
			if Update_Work.i_up_cnt_date > 100:
				Update_Work.i_up_cnt_date = 0

			if Update_Work.i_up_cnt_date == 0:
				if ParseSql(cur, sqlTradeDate) == False:
					break
				cur.execute(sqlTradeDate)
				Update_Work.dic_trade_date.clear()
				for row in cur:
					Update_Work.dic_trade_date[row[0]] = row[1]

			# 第一次上传或者更新次数满100次则查询一下账户列表，更新缓存
			if Update_Work.i_up_cnt > 100:
				Update_Work.i_up_cnt = 0

			if Update_Work.i_up_cnt == 0:
				if ParseSql(cur, sqlSearchAccountType) == False:
					break
				result = cur.execute(sqlSearchAccountType)
				for row in cur:
					Update_Work.dic_act_type[row[0]] = row[1]
			Update_Work.i_up_cnt += 1

			for oneTrade in TransactionCache:
				if oneTrade[0]:
					self.PackTransactions(oneTrade, listUpdate, dictParam,True)

			if dictParam['TransactionList'] != '':
				Transaction = dictParam['TransactionList'][:-1]
				Transaction = '[' + Transaction + ']'
				Logger.info(Transaction)
				headers = {'Content-Type': 'application/json'}
				response = requests.post(self.__s_base_url + '/midoffice/transaction/reportSubmit', headers=headers,
										 data=Transaction)
				if self.HandleTransactionReturn(response.text,listUpdate) == False:
					break

				# 更新成交记录的状态
				for updateDict in listUpdate:
					d1 = datetime.datetime.now(tz=tz.gettz('US/Eastern'))
					sqlUpdateTransactionT = sqlUpdateTransaction % (
					updateDict['RTN_STATUS'], updateDict['REMARK'], updateDict['ID'],
					int(d1.strftime('%Y%m%d')), d1.strftime('%H%M%S%f')[:-3],updateDict['EXTERNAL_ID'])
					if ParseSql(cur, sqlUpdateTransactionT) == False:
						return
					cur.execute(sqlUpdateTransactionT)
				conn.commit()
			return

	# 上传成交数据
	def Work(self,afterTrade = False):
		global sqlSearch, sqlValue, sqlUpdate, sqlInsert, ExecutionTemplate
		conn = cx_Oracle.connect(self.__s_db)
		while (1):
			cur = conn.cursor()
			# 从sysconf表中获取上次读取的成交序列号，从上次序列号开始重新读取
			sqlIndexT = ''
			if afterTrade == True:
				sqlIndexT = sqlIndexAfterTrade
			else:
				sqlIndexT = sqlIndex

			if ParseSql(cur, sqlIndexT) == False:
				break
			result = cur.execute(sqlIndexT)
			t = result.fetchone()
			if t:
				self.__i_index = int(t[0])

			sqlSearchT = sqlSearch % (self.__i_index + self.__i_send_num, self.__i_index)
			cur.execute(sqlSearchT)

			listUpdate = []
			dictParam = {
				'TransactionList': '',
				'updateCount': 0,
			}
			TransactionCache = []
			# 将成交数据存入缓存
			for row in cur:
				TransactionCache.append(row)

			# 第一次上传或者更新次数满100次则查询一下账户列表，更新缓存
			if Update_Work.i_up_cnt > 100:
				Update_Work.i_up_cnt = 0

			if Update_Work.i_up_cnt == 0:
				if ParseSql(cur, sqlSearchAccountType) == False:
					break
				result = cur.execute(sqlSearchAccountType)
				for row in cur:
					Update_Work.dic_act_type[row[0]] = row[1]
			Update_Work.i_up_cnt += 1

			dicUpdated = {}
			# 格式化成交数据
			for oneTrade in TransactionCache:
				oneTrade = list(oneTrade)
				if oneTrade[0]:
					sqlSearchExistT = sqlSearchExist % (oneTrade[10], oneTrade[7])
					if ParseSql(cur, sqlSearchExistT) == False:
						dictParam['updateCount'] += 1
						continue
					else:
						# 如果能找到该笔成交已经被上传过了，则不再上传
						result = cur.execute(sqlSearchExistT)
						if result.fetchone() or None != dicUpdated.get(oneTrade[7]):
							dictParam['updateCount'] += 1
							continue
					if oneTrade[6] == 'ETNA': #发给etna的订单就不上传给apex了，由etna自己上传
						dictParam['updateCount'] += 1
						continue
					# 如果委托已经处于终态：部成部撤或者全部成交,或者是收盘后的部分成交，则进行提交
					if (oneTrade[14] == 6 or oneTrade[14] == 7) or (afterTrade == True and oneTrade[14] == 5):
						#记录一下本轮上传中已经上传的委托编号，如果本轮中有相同的委托已经准备处理，则这一笔不再处理
						dicUpdated[oneTrade[7]] = 1
						self.PackTransactions(oneTrade, listUpdate, dictParam)
					else:
						dictParam['updateCount'] += 1
						continue

			# 调用braggart的submit接口上传成交
			if dictParam['TransactionList'] != '':
				Transaction = dictParam['TransactionList'][:-1]
				Transaction = '[' + Transaction + ']'
				Logger.info(Transaction)
				headers = {'Content-Type': 'application/json'}

				try:
					response = requests.post(self.__s_base_url + '/midoffice/transaction/reportSubmit', headers=headers, data=Transaction)
					if self.HandleTransactionReturn(response.text, listUpdate) == False:
						break
				except Exception:
					Logger.error("Send update request failed",exc_info=True)

				# 将上传过的数据插入到上传流水表记录
				for updateDict in listUpdate:
					d1 = datetime.datetime.now(tz = tz.gettz('US/Eastern'))
					sqlInsertT = sqlInsert % (
						updateDict['EXTERNAL_ID'], updateDict['RTN_SERIALNO'], updateDict['BUSIN_DATE'],
						updateDict['ID'], updateDict['RTN_STATUS'], updateDict['CLIENT_ID'], updateDict['ENTRUST_TYPE'],
						updateDict['BUSINESS_AMOUNT'], updateDict['BUSINESS_PRICE'], updateDict['CURRENCY_CODE'],
						updateDict['BUSINESS_TIME'], updateDict['DESTINATION'], updateDict['ENTRUST_NO'],
						updateDict['STOCK_CODE'], updateDict['OFFSET_FLAG'], updateDict['REMARK'],updateDict['COMMISSION'],updateDict['NODE_ID'],
						int(d1.strftime('%Y%m%d')),d1.strftime('%H%M%S%f')[:-3])
					if ParseSql(cur, sqlInsertT) == False:
						return
					cur.execute(sqlInsertT)

			# 更新成交序列号到sysconf表
			if dictParam['updateCount'] > 0:
				if dictParam['updateCount'] < self.__i_send_num:
					self.__i_index += dictParam['updateCount']
				else:
					self.__i_index += self.__i_send_num

				Logger.info("updateCount:" + str(dictParam['updateCount']))
				Logger.info("countOnetime:" + str(self.__i_send_num))
				Logger.info("indexUpdate:" + str(self.__i_index))
				sqlUpdateT = ''
				if afterTrade == True:
					sqlUpdateT = sqlIndexUpdateAfterTrade % (self.__i_index)
				else:
					sqlUpdateT = sqlIndexUpdate % (self.__i_index)

				if ParseSql(cur, sqlUpdateT) == False:
					break
				cur.execute(sqlUpdateT)
				conn.commit()

			return

	def WriteBack(self):
		global sqlSearch, sqlValue, sqlUpdate, sqlInsert, ExecutionTemplate
		conn = cx_Oracle.connect(self.__s_db)
		while (1):
			cur = conn.cursor()
			# 从sysconf表中获取上次读取的成交序列号，从上次序列号开始重新读取
			sqlHWMT = sqlHighwatermark
			if ParseSql(cur,sqlHighwatermark) == False:
				break

			result = cur.execute(sqlHighwatermark)
			t = result.fetchone()
			i_hwm = 0
			if t:
				i_hwm = int(t[0])
			else:
				break

			headers = {'Content-Type': 'application/json'}
			try:
				response = requests.post(self.__s_base_url + '/midoffice/transaction/tradePostingStatus?highWaterMark=%d&limit=50'%(i_hwm), headers=headers)
				if response.status_code == 200:
					i_hwmR = self.HandleAleReturn(cur,response.text)
					if i_hwmR > 0 and i_hwmR != i_hwm:
						sqlHWMUpdateT = sqlHWMUpdate % i_hwmR
						if ParseSql(cur, sqlHWMUpdateT) == False:
							break
						cur.execute(sqlHWMUpdateT)
						conn.commit()
				else:
					Logger.error("Send ale search request failed,error code:%d"%(response.status_code), exc_info=True)
			except Exception:
				Logger.error("Send ale search failed", exc_info=True)

			return


#盘中上传全部成交和部成部撤
@TimeRecord
def WorkFlow_Update(data):
	update_work = Update_Work()
	update_work.Work()

#收盘后上传部分成交
@TimeRecord
def WorkFlow_Update_After_Trade(data):
	update_work = Update_Work()
	update_work.Work(afterTrade = True)

#成交上传状态回写
@TimeRecord
def WorkFlow_Writeback(data):
	update_work = Update_Work()
	update_work.WriteBack()

#重传上传失败或者漏传的部分
@TimeRecord
def WorkFlow_Resend_Transaction(data):
	update_work = Update_Work()
	update_work.ReTransaction(AccountNo=data.get('accountNo'),Date=data.get('date'))