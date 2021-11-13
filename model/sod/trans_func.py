# -*- coding: utf-8 -*-
#import pandas as pd
import os
import time
import random
import math
'''def getDataFrame(src_file_path, file_name, cols):
    filelists = os.listdir(src_file_path)
    data_frams = []
    for file in filter(lambda x:x.upper().endswith(file_name.upper()), filelists):
        filename = os.path.join(src_file_path, file)
        data_fram = pd.read_csv(filename, encoding='gb18030', usecols=cols, dtype=str)
        data_frams.append(data_fram)

    if data_frams:
        # 合并多份文件，增加默认列
        dataframe = pd.concat(data_frams, axis=0)
        return dataframe
    else:
        raise FileNotFoundError('文件{}不存在'.format(file_name))'''

g_i_serial_no = 0

def GetSerialNo(no):
    global g_i_serial_no
    g_i_serial_no += 1
    return g_i_serial_no

def ResetParam():
    global g_i_serial_no
    g_i_serial_no = 0
	
def AmericanAsubtractB(a,b):
    '''
    :param a、b
    :return: a-b
    '''
    return int(float(a)) - int(float(b))

def TrimSpecialChar(input):
    input = str(input)
    input = input.replace('\'',' ')
    return input


def AmericanRemoveSame(str1):
    '''
    去除主键相同的行
    :param string
    :return: part_string
    '''
    return str1

def AmericanTransMarket(str1):
    return
	
def AmericanRemoveNan(str1):
    '''
    去除含有Nan值的行
    :param string
    :return: part_string
    '''
    return str1
	
def AmericanCutPointString(sor_string):
    '''
    :param string
    :return: part_string
    '''
    #print ('aaa')
    if sor_string.find('.')>0:
        return sor_string[:(sor_string.find('.'))]
        #print ("1123")
    else:
        return sor_string
        #print("456")

def AmericanRemoveSepcialCharacter(str):
    str1 = str.replace('\'',' ')
    return str1

def AmericanGetEntrustType(str):
    amount = int(str)
    if amount > 0:
        return '1001'
    elif amount < 0:
        return '1002'
    else:
        return '1000'

def AmericanGetAccountType(str):
    if str == 'Y':
        return '1002'
    else:
        return '1001'



def StrSub(x, y):
    '''
    做入参为str的减法
    :return: 差值
    '''
    return str(float(x) - float(y))


def FormatDate(Date):
    '''
    :param Date:mm/dd/yyyy or m/d/yyyy
    :return: yyyymmdd
    '''
    if type(Date) == float or Date == '':
        return '19700101'
    else :
        DateList = str(Date).split('/')
        if len(DateList) == 3:
            year = DateList[2]
            mouth = DateList[0] if len(DateList[0]) == 2 else '0' + DateList[0]
            day = DateList[1] if len(DateList[1]) == 2 else '0' + DateList[1]

            return year + mouth + day
        else :
            return Date

def GetTodayDate(Date):
    return time.strftime("%Y%m%d", time.localtime(time.time()))

def DeleteDollar(amount):
    if str(amount).startswith('$'):
        return str(amount)[1:].strip()
    else:
        return str(amount).strip()


def DefaultNumber(number):
    if math.isnan(float(number)):
        return 0
    else :
        return number

g_PrimaryKey={'EXT250':0,'EXT871':0,'EXT901':0,'EXT982':0}
def PrimaryKeyForExt250(str):
    global  g_PrimaryKey
    g_PrimaryKey['EXT250'] += 1
    return g_PrimaryKey['EXT250']


def PrimaryKeyForExt871(str):
    global  g_PrimaryKey
    g_PrimaryKey['EXT871'] += 1
    return g_PrimaryKey['EXT871']

def PrimaryKeyForExt901(str):
    global  g_PrimaryKey
    g_PrimaryKey['EXT901'] += 1
    return g_PrimaryKey['EXT901']

def PrimaryKeyForExt982(str):
    global  g_PrimaryKey
    g_PrimaryKey['EXT982'] += 1
    return g_PrimaryKey['EXT982']



def DefaultBalance(str):
    if math.isnan(float(str)):
        return 0
    else:
        return str


def SplitTimeForDate(str):
    if str == '' :
        return '19700101'
    DateList = []
    DateList.clear()
    if str.find('/') != -1:
        DateList=str.split(' ')[0].split('/')
    elif str.find('-') != -1:
        DateList = str.split(' ')[0].split('-')
    date=''
    for i in DateList:
        if len(i)<2:
            i = '0'+i
        date += i
    return date


def SplitTimeForTime(str):
    if str == '' :
        return '000000'
    TimeList=str.split(' ')[1].split(':')
    time=''
    for i in TimeList:
        time += i
    return time



