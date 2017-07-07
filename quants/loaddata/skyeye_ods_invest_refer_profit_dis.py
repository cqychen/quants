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
    code             VARCHAR (10)  comment '股票代码'
    ,`name`          VARCHAR (63)  comment '股票名称'
    ,`year`          VARCHAR (10)  comment '分配年份'
    ,report_date     VARCHAR (63)  comment '公布日期'
    ,divi            DOUBLE        comment '分红金额（每10股）'
    ,shares          DOUBLE        comment '转增和送股数（每10股）'
    ,PRIMARY KEY(code,report_date)
    ,index(code)
    )DEFAULT CHARSET=utf8
    '''%table_name
    print (cmd)
    run_mysql_cmd(cmd,conn)

def load_data():
    #下载公司基本信息，包括股票代码、pe、市盈率等数据
    max_year=int(get_max_date_profit_dis())
    (year,quarter,mon,day,hour,min,sec,wday,yday,isdst)=get_date_struct()
    while True:
        if(max_year<=year):
            print("geting year is %s"%max_year)
            rs=ts.profit_data(year=max_year,top=3000)
            print("\n\n")

            pd.DataFrame.to_sql(rs, table_name, con=conn , flavor='mysql', if_exists='append',index=False)
            max_year=max_year+1
if __name__ == '__main__':
    #--------------------设置基本信息---------------------------------
    print("--------------加载公司基本信息开始-----------------------------")
    startTime=dt.time()
    iphost,user,passwd=get_mysql_conn()
    db='ods_data'
    charset='utf8'
    table_name='ods_invest_refer_profit_dis'
    conn = pymysql.connect(user=user, passwd=passwd,host=iphost, db=db,charset=charset)
    #--------------------脚本运行开始--------------------------------
    create_table(table_name=table_name) #建立表格
    load_data()
    endTime=dt.time()
    print("---------------脚本运行完毕,共计耗费时间%sS------------------"%(endTime-startTime))
