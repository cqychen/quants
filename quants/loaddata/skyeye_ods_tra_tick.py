#coding=utf8

import tushare as ts;
import pymysql;
import time as dt
from datashape.coretypes import string
from pandas.io.sql import SQLDatabase
import sqlalchemy
import datetime
from sqlalchemy import create_engine
from pandas.io import sql
import threading
import pandas as pd;
import sys
sys.path.append('../') #添加配置文件
from common_function import  *
def get_date():
    '''
    :return:读取文件获取最新日期，没有的从2010-01-01开始
    '''
    with open(date_file, 'r') as f:  #打开文件
        lines = f.readlines() #读取所有行
        try:
            return lines[0].strip('\n') #取第一行
        except :
            return '2010-01-01'
def insert_date():
    '''
    :return:将当前日期插入到文件中保存
    '''
    now_date= dt.strftime('%Y-%m-%d',dt.localtime(dt.time()))
    f=file(date_file,"w+")
    f.write(now_date)
    f.close()
def load_stock_tick(stock_code):
    '''
    :param stock_code:
    :return: 保存每个股票的文件
    '''
    start_date=get_date()
    end_date=dt.strftime('%Y-%m-%d',dt.localtime(dt.time()))
    path_or_buf=path+'/ods_tra_tick_'+stock_code+'.gz'
    while True:
        if cmp(start_date,end_date)<=0:
            df = ts.get_tick_data(stock_code,date=start_date,src='tt')
            if df is not None and len(df)>=5:
                df['date']=start_date
                df.to_csv(path_or_buf=path_or_buf,sep=',',mode='a',index=False,header=False,compression='gzip')
            start_date=get_date_add_days(start_date,1)
        else:
            break

def load_data():
    stock_code = get_stock_info().index
    total_num = len(stock_code);
    tempnum = 1;
    for tmp_stock_code in stock_code:
        tempnum = tempnum + 1
	print(tempnum,tmp_stock_code)
	load_stock_tick(tmp_stock_code)
if __name__ == '__main__':
    #--------------------设置基本信息---------------------------------
    print("--------------加载股票日k线-----------------------------")
    startTime=dt.time()
    path='/data/ods_tra_tick'
    date_file=path+'/date_parameter.txt'
    #--------------------脚本运行开始--------------------------------
    #load_stock_tick('000001')
    load_data()
    insert_date()
    endTime=dt.time()
    print("---------------脚本运行完毕,共计耗费时间%sS------------------"%(endTime-startTime))
