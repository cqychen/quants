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
import commands
import dateutil
import smtplib
from email.mime.text import MIMEText
from email.header import Header
import sys
sys.path.append('../') #添加配置文件
from common_function import  *

def read_data(stock_code):
    '''
    :param stock_code:
    :return:
    '''
    cmd='''
    select * from ods_tra_day_k where `code`='%s' order by `date` DESC
    '''%stock_code
    return pd.read_sql(cmd,conn)

def calculate_price_stock(stock_code,days_to_now=0):
    '''
    :param stock_code:
    :return:
    '''
    rs=read_data(stock_code)
    print(rs.head(10))
    rs_close=rs['close']
    print(rs_close.head(10))
    print("frist",rs_close[1])
    price_rate=[[0]*20]
    for i in range(1,10):
        price_rate[i]= float(rs_close[days_to_now+i]-rs_close[days_to_now])/rs_close[days_to_now+i]
    print(price_rate)


if __name__ == '__main__':
    print("--------------任务开始-----------------------------")
    startTime=dt.time()
    iphost,user,passwd=get_mysql_conn()
    db='ods_data'
    charset='utf8'
    conn = pymysql.connect(user=user, passwd=passwd,host=iphost, db=db,charset=charset)
    #--------------------脚本运行开始-------------------------------
    calculate_price_stock('000001')
    endTime=dt.time()
    print("---------------脚本运行完毕,共计耗费时间%sS------------------"%(endTime-startTime))
