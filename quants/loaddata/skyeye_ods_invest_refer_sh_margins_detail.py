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
     opDate           VARCHAR (63)    comment '信用交易日期'
     ,stockCode       varchar (63)    comment '股票代码'
     ,securityAbbr    varchar (63)    comment '标的证券简称'   
     ,rzye            BIGINT          comment '本日融资余额(元)'
     ,rzmre           BIGINT          comment '本日融资买入额(元)'  
     ,rzche           BIGINT          comment '本日融资偿还额(元)'  
     ,rqyl            BIGINT          comment '本日融券余量'  
     ,rqmcl           BIGINT          comment '本日融券卖出量'  
     ,rqchl           BIGINT          comment '本日融券偿还量'
     ,PRIMARY KEY(stockCode,`opDate`)
     ,index(stockCode)
    )DEFAULT CHARSET=utf8
    '''%table_name
    print (cmd)
    run_mysql_cmd(cmd,conn)
def load_data_stock(stock_code):
    '''
    :param stock_code:传递股票代码，将其装载进入mysql
    :return: 
    '''
    start_date = get_date_add_days(get_max_date_sh_margins_detail(stock_code), 1) #获取股票最大日期
    rs = ts.sh_margin_details(start=start_date, end=end_date, symbol=stock_code)#获取数据
    pd.DataFrame.to_sql(rs, table_name, con=conn, flavor='mysql', if_exists='append', index=False)

def load_data():

    stock_code = get_stock_info().index
    total_num = len(stock_code);
    tempnum = 1;
    for tmp_stock_code in stock_code:
        tempnum = tempnum + 1
        print(tempnum,tmp_stock_code)
        load_data_stock(tmp_stock_code)

if __name__ == '__main__':
    #--------------------设置基本信息---------------------------------
    print("--------------加载股票日k线-----------------------------")
    startTime=dt.time()
    iphost,user,passwd=get_mysql_conn()
    db='ods_data'
    charset='utf8'
    table_name='ods_invest_refer_sh_margins_detail'
    conn = pymysql.connect(user=user, passwd=passwd,host=iphost, db=db,charset=charset)
    end_date= dt.strftime('%Y-%m-%d',dt.localtime(dt.time()))
    #--------------------脚本运行开始--------------------------------
    create_table(table_name=table_name)
    load_data()
    endTime=dt.time()
    print("---------------脚本运行完毕,共计耗费时间%sS------------------"%(endTime-startTime))
