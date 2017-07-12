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
    `code`           VARCHAR (10)  comment '股票代码'
    ,`name`          VARCHAR (63)  comment '股票名称'
    ,`date`          VARCHAR (63)  comment '报告日期'
    ,`nums`          INT            comment '基金家数'
    ,`nlast`         DOUBLE         comment '与上期相比增加减少'
    ,`count`         DOUBLE         comment '基金持股数（万股）'
    ,`clast`         DOUBLE         comment '与上期相比'
    ,`amount`        DOUBLE         comment '基金持股市值'
    ,`ratio`         DOUBLE         comment '占流通盘比率'
    ,`year`          DOUBLE         comment '报告年份'
    ,`quarter`       DOUBLE         comment '报告季度'
    ,index(code)
    )DEFAULT CHARSET=utf8
    '''%table_name
    print (cmd)
    run_mysql_cmd(cmd,conn)
def get_data_date(year,quarter):
    cmd='''delete  from %s where `year`=%s and quarter=%s '''%(table_name,year,quarter)
    print cmd
    run_mysql_cmd(cmd=cmd,conn=conn) #先删除指定年和指定季度的数据
    try:
        rs=ts.fund_holdings(year=int(year),quarter=int(quarter))
        rs['year']=year
        rs['quarter']=quarter
        rs=rs.drop_duplicates() #去除重复的数据
        pd.DataFrame.to_sql(rs, table_name, con=conn , flavor='mysql', if_exists='append',index=False)
        return rs
    except:
        print("getting data wrong %s ---%s"%(year,quarter))
def load_data():
    max_year=int(get_max_year_table(table_name))
    (year,quarter,mon,day,hour,min,sec,wday,yday,isdst)=get_date_struct()
    max_year=2012
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
    table_name='ods_invest_refer_fund_holdings'
    conn = pymysql.connect(user=user, passwd=passwd,host=iphost, db=db,charset=charset)
    #--------------------脚本运行开始--------------------------------
    create_table(table_name=table_name) #建立表格
    load_data()
    endTime=dt.time()
    print("---------------脚本运行完毕,共计耗费时间%sS------------------"%(endTime-startTime))
