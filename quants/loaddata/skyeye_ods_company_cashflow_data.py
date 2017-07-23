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
import numpy as np
import math
import sys
sys.path.append('../') #添加配置文件
from common_function import  *
def create_table(table_name):
    cmd='''
    create table if not exists %s
    (
    `code`           VARCHAR(10)  comment '股票代码'
    ,`name`          VARCHAR(63)  comment '股票名称'  
    ,cf_sales         double       comment '经营现金净流量对销售收入比率'
    ,rateofreturn     double       comment '资产的经营现金流量回报率'     
    ,cf_nm       double       comment '经营现金净流量与净利润的比率'
    ,cf_liabilities     double       comment '经营现金净流量对负债比率'
    ,cashflowratio      double       comment '现金流量比率' 
    ,`year`          VARCHAR(63)       comment '业绩年份'
    ,`quarter`       VARCHAR(63)       comment '业绩季度'
    ,index(code)
    )DEFAULT CHARSET=utf8
    '''%table_name
    print (cmd)
    run_mysql_cmd(cmd,conn)
def get_data_date(year,quarter):
    cmd='''delete  from %s where `year`='%s' and quarter='%s' '''%(table_name,year,quarter)
    print cmd
    run_mysql_cmd(cmd=cmd,conn=conn) #先删除指定年和指定季度的数据
    try:
        rs=ts.get_cashflow_data(year=int(year),quarter=int(quarter))
        rs['year']=year
        rs['quarter']=quarter
        rs=rs.drop_duplicates() #去除重复的数据，没想到还有重复的，心塞塞，这个api不咋地啊，挖地兔
        pd.DataFrame.to_sql(rs, table_name, con=conn , flavor='mysql', if_exists='append',index=False)
        return rs
    except:
        print("get data year=%s and quarter =%s wrong %s" % (year, quarter,table_name))
def load_data():
    #下载公司基本信息，包括股票代码、pe、市盈率等数据
    max_year=int(get_max_year_table(table_name)) #还要获取前一年的情况
    (year,quarter,mon,day,hour,min,sec,wday,yday,isdst)=get_date_struct()
    while True:
        print(max_year,year)
        if(max_year<=year):
            print("geting year is %s"%max_year)
            get_data_date(year=max_year,quarter=1)
            get_data_date(year=max_year,quarter=2)
            get_data_date(year=max_year,quarter=3)
            get_data_date(year=max_year,quarter=4)
            print("\n\n")
            max_year=max_year+1
        else:
            break
if __name__ == '__main__':
    #--------------------设置基本信息---------------------------------
    print("--------------加载公司基本信息开始-----------------------------")
    startTime=dt.time()
    iphost,user,passwd=get_mysql_conn()
    db='ods_data'
    charset='utf8'
    table_name='ods_company_cashflow_data'
    conn = pymysql.connect(user=user, passwd=passwd,host=iphost, db=db,charset=charset)
    #--------------------脚本运行开始--------------------------------
    create_table(table_name=table_name) #建立表格
    load_data()
    endTime=dt.time()
    print("---------------脚本运行完毕,共计耗费时间%sS------------------"%(endTime-startTime))
