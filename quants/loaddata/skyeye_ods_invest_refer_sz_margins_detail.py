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
def create_table(table_name):
    cmd='''
    create table if not exists %s
    (     
     stockCode         varchar (63)    comment '股票代码'
     ,securityAbbr     varchar (63)    comment '标的证券简称'    
     ,rzmre            BIGINT          comment '本日融资买入额(元)'  
     ,rzye             BIGINT          comment '本日融资余额(元)'
     ,rqmcl            BIGINT          comment '本日融券卖出量'  
     ,rqyl             BIGINT          comment '本日融券余量'  
     ,rqye             BIGINT          comment '本日融券余量(元)'  
     ,rzrqye           BIGINT          comment ' 融资融券余额(元)'    
     ,opDate           VARCHAR (63)    comment '信用交易日期'
     ,PRIMARY KEY(stockCode,`opDate`)
     ,index(stockCode)
    )DEFAULT CHARSET=utf8
    '''%table_name
    print (cmd)
    run_mysql_cmd(cmd,conn)
def load_data_stock(data_date):
    '''
    :param stock_code:传递股票代码，将其装载进入mysql
    :return: 
    '''
    rs = ts.sz_margin_details(date=data_date)#获取数据
    pd.DataFrame.to_sql(rs, table_name, con=conn, flavor='mysql', if_exists='append', index=False)

def load_data():
    start_date=get_date_add_days(get_max_date_sz_margins_detail(), 1) #获取股票最大日期
    while True:
        if cmp(start_date,end_date)<=0:
            print (start_date,end_date)
            load_data_stock(data_date=start_date)
            start_date=get_date_add_days(start_date, 1)
        else:
            break

if __name__ == '__main__':
    #--------------------设置基本信息---------------------------------
    print("--------------加载股票日k线-----------------------------")
    startTime=dt.time()
    iphost,user,passwd=get_mysql_conn()
    db='ods_data'
    charset='utf8'
    table_name='ods_invest_refer_sz_margins_detail'
    conn = pymysql.connect(user=user, passwd=passwd,host=iphost, db=db,charset=charset)
    end_date= dt.strftime('%Y-%m-%d',dt.localtime(dt.time()))
    #--------------------脚本运行开始--------------------------------
    create_table(table_name=table_name)
    load_data()
    endTime=dt.time()
    print("---------------脚本运行完毕,共计耗费时间%sS------------------"%(endTime-startTime))
